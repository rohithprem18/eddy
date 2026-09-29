"""Environment-driven settings shared by the producer, setup job, Spark job and API."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    schema_registry_url: str = field(default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8085"))
    topic: str = field(default_factory=lambda: _env("EVENTS_TOPIC", "user-events"))
    dlq_topic: str = field(default_factory=lambda: _env("DLQ_TOPIC", "user-events-dlq"))
    topic_partitions: int = field(default_factory=lambda: int(_env("TOPIC_PARTITIONS", "6")))
    compatibility: str = field(default_factory=lambda: _env("SCHEMA_COMPATIBILITY", "BACKWARD"))
    schema_dir: Path = field(default_factory=lambda: Path(_env("SCHEMA_DIR", str(ROOT / "schemas" / "user_event"))))

    redis_host: str = field(default_factory=lambda: _env("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(_env("REDIS_PORT", "6379")))
    redis_password: str = field(default_factory=lambda: _env("REDIS_PASSWORD", ""))
    feature_prefix: str = field(default_factory=lambda: _env("FEATURE_PREFIX", "eddy"))
    feature_ttl_seconds: int = field(default_factory=lambda: int(_env("FEATURE_TTL_SECONDS", "86400")))

    @property
    def subject(self) -> str:
        # TopicNameStrategy: the value schema of topic T lives under subject "T-value".
        return f"{self.topic}-value"

    def schema_files(self) -> list[Path]:
        """Schema versions in order (v1.avsc, v2.avsc, ...)."""
        return sorted(self.schema_dir.glob("v*.avsc"), key=lambda p: int(p.stem[1:]))

    def latest_schema(self) -> str:
        return self.schema_files()[-1].read_text()


def get_settings() -> Settings:
    return Settings()


def user_key(prefix: str, user_id: str) -> str:
    return f"{prefix}:user:{user_id}"


def metrics_key(prefix: str, query_name: str) -> str:
    return f"{prefix}:metrics:{query_name}"
