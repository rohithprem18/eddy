"""High-throughput Avro producer with schema-registry-enforced contracts.

Every record is serialised against the newest schema file, which must
already be registered under the topic's subject. Schemas are never auto-registered: a producer cannot push
an unreviewed contract change. Records that fail serialisation are routed to
a dead-letter topic with the error attached instead of crashing the stream.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from eddy.config import get_settings
from eddy.events import EventGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("eddy.producer")


class Stats:
    def __init__(self):
        self.sent = self.delivered = self.failed = self.dlq = 0
        self.window_start = time.monotonic()
        self.window_delivered = 0

    def report(self) -> None:
        elapsed = time.monotonic() - self.window_start
        rate = (self.delivered - self.window_delivered) / elapsed if elapsed else 0.0
        log.info(
            "sent=%d delivered=%d failed=%d dlq=%d rate=%.0f ev/s (%.0f ev/min)",
            self.sent, self.delivered, self.failed, self.dlq, rate, rate * 60,
        )
        self.window_start = time.monotonic()
        self.window_delivered = self.delivered


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "eddy-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 262144,
            "queue.buffering.max.messages": 500000,
        }
    )


def run(rate: int, duration: int, invalid_rate: float, users: int, seed: int | None) -> Stats:
    settings = get_settings()
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    serializer = AvroSerializer(
        registry,
        settings.latest_schema(),
        # The id is looked up once and cached; an unregistered schema fails fast.
        conf={"auto.register.schemas": False},
    )
    key_serializer = StringSerializer("utf_8")
    producer = build_producer(settings.kafka_bootstrap)
    gen = EventGenerator(n_users=users, seed=seed)
    stats = Stats()
    ctx = SerializationContext(settings.topic, MessageField.VALUE)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    def on_delivery(err, _msg):
        if err is not None:
            stats.failed += 1
            log.warning("Delivery failed: %s", err)
        else:
            stats.delivered += 1

    def send_to_dlq(event: dict, error: Exception) -> None:
        payload = json.dumps({"error": str(error), "event": event}, default=str).encode()
        producer.produce(settings.dlq_topic, value=payload, key=str(event.get("user_id", "")).encode())
        stats.dlq += 1

    tick = 0.05  # pace in 50 ms slices for a smooth rate
    per_tick = max(1, round(rate * tick))
    started = time.monotonic()
    last_report = started
    log.info("Producing ~%d events/s to %s (invalid rate %.2f%%)", rate, settings.topic, invalid_rate * 100)

    while running and (duration <= 0 or time.monotonic() - started < duration):
        slice_start = time.monotonic()
        for event in gen.batch(per_tick):
            if invalid_rate and gen.rng.random() < invalid_rate:
                event = gen.invalid_event()
            try:
                value = serializer(event, ctx)
            except Exception as exc:  # noqa: BLE001 - any contract violation goes to the DLQ
                send_to_dlq(event, exc)
                continue
            while True:
                try:
                    producer.produce(
                        settings.topic,
                        key=key_serializer(event["user_id"]),
                        value=value,
                        on_delivery=on_delivery,
                    )
                    stats.sent += 1
                    break
                except BufferError:
                    producer.poll(0.1)  # local queue full: let librdkafka drain
        producer.poll(0)

        if time.monotonic() - last_report >= 10:
            stats.report()
            last_report = time.monotonic()
        sleep_for = tick - (time.monotonic() - slice_start)
        if sleep_for > 0:
            time.sleep(sleep_for)

    log.info("Flushing...")
    producer.flush(30)
    stats.report()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=int(os.getenv("EVENTS_PER_SECOND", "1000")))
    parser.add_argument("--duration", type=int, default=int(os.getenv("DURATION_SECONDS", "0")),
                        help="seconds to run; 0 = forever")
    parser.add_argument("--invalid-rate", type=float, default=float(os.getenv("INVALID_EVENT_RATE", "0.001")))
    parser.add_argument("--users", type=int, default=int(os.getenv("NUM_USERS", "10000")))
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    try:
        run(args.rate, args.duration, args.invalid_rate, args.users, args.seed)
    except KafkaException as exc:
        raise SystemExit(f"Kafka error: {exc}") from exc


if __name__ == "__main__":
    main()
