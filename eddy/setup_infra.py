"""One-shot infrastructure bootstrap: create topics, pin compatibility, register schemas.

Runs as the `setup` container before producers and consumers start, and is
safe to re-run: existing topics and identical schemas are left as they are.
"""

from __future__ import annotations

import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic

from eddy.config import get_settings
from eddy.schema_registry import SchemaRegistry, SchemaRegistryError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("eddy.setup")


def wait_for(check, what: str, attempts: int = 60, delay: float = 2.0):
    for i in range(attempts):
        try:
            return check()
        except Exception as exc:  # noqa: BLE001
            if i == attempts - 1:
                raise
            log.info("Waiting for %s (%s)", what, exc)
            time.sleep(delay)


def create_topics(admin: AdminClient, settings) -> None:
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [
        NewTopic(settings.topic, num_partitions=settings.topic_partitions, replication_factor=1,
                 config={"retention.ms": str(7 * 24 * 3600 * 1000)}),
        NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1,
                 config={"retention.ms": str(30 * 24 * 3600 * 1000)}),
    ]
    missing = [t for t in wanted if t.topic not in existing]
    if not missing:
        log.info("Topics already exist")
        return
    for topic, fut in admin.create_topics(missing).items():
        fut.result()
        log.info("Created topic %s", topic)


def register_schemas(registry: SchemaRegistry, settings) -> None:
    subject = settings.subject
    registry.set_compatibility(subject, settings.compatibility)
    log.info("Compatibility for %s set to %s", subject, settings.compatibility)
    for path in settings.schema_files():
        schema = path.read_text()
        ok, messages = registry.is_compatible(subject, schema)
        if not ok:
            raise SchemaRegistryError(f"{path.name} is not {settings.compatibility} compatible: {messages}")
        schema_id = registry.register(subject, schema)
        log.info("Registered %s as schema id %d", path.name, schema_id)


def main() -> None:
    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    wait_for(lambda: admin.list_topics(timeout=5), "Kafka")
    create_topics(admin, settings)

    registry = SchemaRegistry(settings.schema_registry_url)
    wait_for(registry.subjects, "Schema Registry")
    register_schemas(registry, settings)
    log.info("Setup complete")


if __name__ == "__main__":
    main()
