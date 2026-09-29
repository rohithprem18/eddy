"""Measure pipeline throughput and online-serving latency on the running stack.

    python -m eddy.bench --seconds 60

* Ingest rate: growth of the topic's end offsets over the interval (events/min).
* Processing rate: Spark's processedRowsPerSecond reported per query.
* Serving latency: p50/p95/p99 of feature API lookups for random users.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time

import requests
from confluent_kafka import Consumer, TopicPartition

from eddy.config import get_settings


def end_offsets(consumer: Consumer, topic: str) -> int:
    meta = consumer.list_topics(topic, timeout=10)
    total = 0
    for pid in meta.topics[topic].partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
        total += high
    return total


def pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round((len(ordered) - 1) * p / 100)))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--lookups", type=int, default=2000)
    parser.add_argument("--users", type=int, default=10000)
    args = parser.parse_args()

    settings = get_settings()
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": "eddy-bench"})
    start = end_offsets(consumer, settings.topic)
    t0 = time.monotonic()
    print(f"Measuring ingest for {args.seconds}s...")
    time.sleep(args.seconds)
    produced = end_offsets(consumer, settings.topic) - start
    per_min = produced / (time.monotonic() - t0) * 60
    consumer.close()

    session = requests.Session()
    latencies, hits = [], 0
    for _ in range(args.lookups):
        uid = f"u{random.randrange(args.users):06d}"
        t = time.perf_counter()
        resp = session.get(f"{args.api}/features/{uid}", timeout=5)
        latencies.append((time.perf_counter() - t) * 1000)
        hits += resp.status_code == 200

    stats = session.get(f"{args.api}/stats", timeout=5).json()
    report = {
        "ingest_events_per_minute": round(per_min),
        "spark_queries": {
            name: {k: q.get(k) for k in ("input_rows_per_second", "processed_rows_per_second",
                                         "trigger_execution_ms")} if q else None
            for name, q in stats["queries"].items()
        },
        "online_store_keys": stats["online_store_keys"],
        "serving": {
            "lookups": args.lookups,
            "hit_rate": round(hits / args.lookups, 3),
            "p50_ms": round(statistics.median(latencies), 2),
            "p95_ms": round(pct(latencies, 95), 2),
            "p99_ms": round(pct(latencies, 99), 2),
        },
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
