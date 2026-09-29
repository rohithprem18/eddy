"""Eddy streaming feature job.

Kafka (Avro, schema registry) -> Spark Structured Streaming -> Redis.

Three concurrent streaming queries share the decoded event stream:
  * window_5m  - 5-minute sliding window (1-minute slide) per-user aggregates
  * window_1h  - 1-hour sliding window (10-minute slide) per-user aggregates
  * profile    - last-seen profile attributes per user
Throughput and latency for every query are published to Redis so the
feature API can expose them.
"""

from __future__ import annotations

import json
import logging
import os
import time
from functools import partial

import redis
from pyspark.sql import DataFrame, SparkSession

from eddy.config import get_settings, metrics_key
from eddy.features import decode_confluent_avro, latest_profile, select_current_windows, windowed_features
from eddy.online_store import OnlineStoreWriter
from eddy.schema_registry import SchemaRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("eddy.feature_job")

# Each event updates window/slide overlapping windows: 5 for window_5m, 6 for window_1h.
WINDOWS = [("5m", "5 minutes", "1 minute"), ("1h", "1 hour", "10 minutes")]
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/eddy/checkpoints")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
WATERMARK = os.getenv("WATERMARK_DELAY", "2 minutes")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "500000")


def load_schemas(settings, attempts: int = 60) -> tuple[dict[int, str], str]:
    registry = SchemaRegistry(settings.schema_registry_url)
    for i in range(attempts):
        try:
            writers = registry.writer_schemas(settings.subject)
            reader = registry.get_version(settings.subject, "latest")["schema"]
            log.info("Loaded writer schema ids %s for %s", sorted(writers), settings.subject)
            return writers, reader
        except Exception as exc:  # noqa: BLE001
            if i == attempts - 1:
                raise
            log.info("Waiting for schemas in registry (%s)", exc)
            time.sleep(2)
    raise RuntimeError("unreachable")


def _writer_kwargs(settings) -> dict:
    return {
        "host": settings.redis_host,
        "port": settings.redis_port,
        "password": settings.redis_password,
        "prefix": settings.feature_prefix,
        "ttl_seconds": settings.feature_ttl_seconds,
    }


def _write_partition(rows, suffix, guard_field, writer_kwargs):
    writer = OnlineStoreWriter(**writer_kwargs)
    writer.write((r.asDict() for r in rows), suffix, guard_field)


def window_sink(suffix: str, writer_kwargs: dict):
    def sink(batch: DataFrame, batch_id: int) -> None:
        current = select_current_windows(batch).drop("window_start", "window_end")
        current.foreachPartition(partial(_write_partition, suffix=suffix, guard_field="window_end_ms",
                                         writer_kwargs=writer_kwargs))
    return sink


def profile_sink(writer_kwargs: dict):
    def sink(batch: DataFrame, batch_id: int) -> None:
        latest_profile(batch).foreachPartition(
            partial(_write_partition, suffix=None, guard_field="last_seen_ms", writer_kwargs=writer_kwargs)
        )
    return sink


def publish_metrics(r: redis.Redis, prefix: str, queries) -> None:
    for q in queries:
        progress = q.lastProgress
        if not progress:
            continue
        durations = progress.get("durationMs", {})
        r.hset(
            metrics_key(prefix, q.name),
            mapping={
                "batch_id": progress.get("batchId", -1),
                "timestamp": progress.get("timestamp", ""),
                "num_input_rows": progress.get("numInputRows", 0),
                "input_rows_per_second": round(progress.get("inputRowsPerSecond") or 0.0, 2),
                "processed_rows_per_second": round(progress.get("processedRowsPerSecond") or 0.0, 2),
                "trigger_execution_ms": durations.get("triggerExecution", 0),
                "state_rows": sum(op.get("numRowsTotal", 0) for op in progress.get("stateOperators", [])),
                "raw": json.dumps(progress)[:20000],
            },
        )


def main() -> None:
    settings = get_settings()
    writers, reader = load_schemas(settings)

    spark = (
        SparkSession.builder.appName("eddy-feature-job")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )
    events = decode_confluent_avro(raw, writers, reader)
    writer_kwargs = _writer_kwargs(settings)

    queries = []
    for suffix, window, slide in WINDOWS:
        queries.append(
            windowed_features(events, window, slide, WATERMARK)
            .writeStream.queryName(f"window_{suffix}")
            .outputMode("update")
            .foreachBatch(window_sink(suffix, writer_kwargs))
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/window_{suffix}")
            .trigger(processingTime=TRIGGER)
            .start()
        )
    queries.append(
        events.writeStream.queryName("profile")
        .foreachBatch(profile_sink(writer_kwargs))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/profile")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    log.info("Started queries: %s", [q.name for q in queries])

    metrics_client = redis.Redis(host=settings.redis_host, port=settings.redis_port,
                                 password=settings.redis_password or None)
    while not spark.streams.awaitAnyTermination(timeout=10):
        try:
            publish_metrics(metrics_client, settings.feature_prefix, queries)
        except redis.RedisError as exc:
            log.warning("Could not publish metrics: %s", exc)
    for q in queries:
        if q.exception():
            raise q.exception()


if __name__ == "__main__":
    main()
