"""Online feature-serving API over the Redis store, plus the console UI."""

from __future__ import annotations

import json
import os
import statistics
import threading
import time
import uuid
from collections import deque
from pathlib import Path

import redis
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from eddy.config import get_settings, metrics_key, user_key
from eddy.schema_registry import SchemaRegistry

settings = get_settings()
app = FastAPI(
    title="Eddy Feature API",
    version="2.0.0",
    description="Low-latency online features computed by Spark Structured Streaming.",
)
_pool = redis.ConnectionPool(
    host=settings.redis_host, port=settings.redis_port, password=settings.redis_password or None,
    decode_responses=True,
)
QUERIES = ("window_5m", "window_1h", "profile")

# The React console is built into this directory by the Docker image (see docker/app.Dockerfile).
STATIC_DIR = Path(os.getenv("EDDY_STATIC_DIR", Path(__file__).parent / "static"))
app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets", check_dir=False), name="assets")

# Rolling window of real lookup latencies (per worker process) for the console.
_lookup_ms: deque[float] = deque(maxlen=500)


class _TTLCache:
    """Tiny time-based cache so the console's polling never hammers Kafka or the registry."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[float, object]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, ttl: float, compute):
        with self._lock:
            hit = self._data.get(key)
            if hit and time.monotonic() - hit[0] < ttl:
                return hit[1]
        value = compute()
        with self._lock:
            self._data[key] = (time.monotonic(), value)
        return value


_cache = _TTLCache()


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)


def parse_value(value: str):
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            continue
    return value


def parse_hash(raw: dict[str, str]) -> dict:
    return {k: parse_value(v) for k, v in sorted(raw.items())}


class FeatureVector(BaseModel):
    user_id: str
    features: dict
    found: bool
    freshness_ms: int | None = Field(None, description="Milliseconds since the user's latest event")
    latency_ms: float


class BatchRequest(BaseModel):
    user_ids: list[str] = Field(..., min_length=1, max_length=1000)


def _vector(user_id: str, raw: dict[str, str], started: float) -> FeatureVector:
    features = parse_hash(raw)
    last_seen = features.get("last_seen_ms")
    return FeatureVector(
        user_id=user_id,
        features=features,
        found=bool(raw),
        freshness_ms=int(time.time() * 1000) - last_seen if isinstance(last_seen, int) else None,
        latency_ms=round((time.perf_counter() - started) * 1000, 3),
    )


# ---------------------------------------------------------------- console
@app.get("/", include_in_schema=False)
def console():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html")
    return HTMLResponse(
        "<h1>Eddy</h1><p>The console has not been built. Run <code>npm run build</code> in <code>web/</code>, "
        "or use the Docker image. The API is available at <a href='/docs'>/docs</a>.</p>"
    )


# ---------------------------------------------------------------- features
@app.get("/users/sample")
def sample_users(n: int = Query(8, ge=1, le=50), r: redis.Redis = Depends(get_redis)):
    """A handful of user ids that currently have features (for demos and the console)."""
    prefix = user_key(settings.feature_prefix, "")
    users: list[str] = []
    for key in r.scan_iter(match=f"{prefix}*", count=500):
        users.append(key[len(prefix):])
        if len(users) >= n:
            break
    return {"user_ids": sorted(users)}


@app.get("/health")
def health(r: redis.Redis = Depends(get_redis)):
    try:
        r.ping()
    except redis.RedisError as exc:
        raise HTTPException(503, f"redis unavailable: {exc}") from exc
    return {"status": "ok"}


@app.get("/features/{user_id}", response_model=FeatureVector)
def get_features(user_id: str, r: redis.Redis = Depends(get_redis)):
    started = time.perf_counter()
    raw = r.hgetall(user_key(settings.feature_prefix, user_id))
    if not raw:
        raise HTTPException(404, f"no features for user '{user_id}'")
    vector = _vector(user_id, raw, started)
    _lookup_ms.append(vector.latency_ms)
    return vector


@app.post("/features/batch", response_model=list[FeatureVector])
def get_features_batch(req: BatchRequest, r: redis.Redis = Depends(get_redis)):
    started = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    for uid in req.user_ids:
        pipe.hgetall(user_key(settings.feature_prefix, uid))
    return [_vector(uid, raw, started) for uid, raw in zip(req.user_ids, pipe.execute(), strict=True)]


def _serving_stats() -> dict:
    samples = list(_lookup_ms)
    if not samples:
        return {"samples": 0, "p50_ms": None, "p95_ms": None}
    ordered = sorted(samples)
    return {
        "samples": len(samples),
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))], 3),
    }


@app.get("/stats")
def stats(r: redis.Redis = Depends(get_redis)):
    """Streaming throughput per Spark query, online-store size and serving latency."""
    pipe = r.pipeline(transaction=False)
    for q in QUERIES:
        pipe.hgetall(metrics_key(settings.feature_prefix, q))
    queries = {}
    for q, raw in zip(QUERIES, pipe.execute(), strict=True):
        raw.pop("raw", None)
        queries[q] = parse_hash(raw) if raw else None
    input_rate = (queries.get("profile") or {}).get("input_rows_per_second", 0) or 0
    return {
        "online_store_keys": r.dbsize(),
        "events_per_minute": round(float(input_rate) * 60),
        "queries": queries,
        "serving": _serving_stats(),
        "server_time_ms": int(time.time() * 1000),
    }


# ---------------------------------------------------------------- data contracts
def _field_types(schema: dict) -> dict[str, str]:
    def render(t) -> str:
        if isinstance(t, list):
            return " | ".join(render(x) for x in t)
        if isinstance(t, dict):
            return t.get("logicalType") or t.get("type", "record")
        return str(t)

    return {f["name"]: render(f["type"]) for f in schema.get("fields", [])}


def _load_contracts() -> dict:
    registry = SchemaRegistry(settings.schema_registry_url, timeout=5)
    subject = settings.subject
    versions = []
    previous: dict[str, str] = {}
    for number in registry.versions(subject):
        entry = registry.get_version(subject, number)
        schema = json.loads(entry["schema"])
        fields = _field_types(schema)
        versions.append(
            {
                "version": number,
                "id": entry["id"],
                "doc": schema.get("doc", ""),
                "fields": [
                    {"name": name, "type": ftype, "added": bool(previous) and name not in previous}
                    for name, ftype in fields.items()
                ],
                "removed": [name for name in previous if name not in fields],
            }
        )
        previous = fields
    return {
        "available": True,
        "subject": subject,
        "compatibility": registry.get_compatibility(subject),
        "versions": versions,
    }


@app.get("/contracts")
def contracts():
    """Schema versions registered for the events topic, with field-level changes."""
    try:
        return _cache.get("contracts", 30, _load_contracts)
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": str(exc)}


def _load_dlq(limit: int) -> dict:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"eddy-console-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(settings.dlq_topic, timeout=5).topics[settings.dlq_topic].partitions
        total, records = 0, []
        for pid in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(settings.dlq_topic, pid), timeout=5)
            total += high - low
            if high > low:
                consumer.assign([TopicPartition(settings.dlq_topic, pid, max(low, high - limit))])
                deadline = time.monotonic() + 3
                while len(records) < limit and time.monotonic() < deadline:
                    msg = consumer.poll(0.5)
                    if msg is None or msg.error():
                        continue
                    body = json.loads(msg.value())
                    records.append(
                        {"error": body.get("error"), "event": body.get("event"), "timestamp_ms": msg.timestamp()[1]}
                    )
                    if msg.offset() >= high - 1:
                        break
        records.sort(key=lambda r: r["timestamp_ms"] or 0, reverse=True)
        return {"available": True, "topic": settings.dlq_topic, "total": total, "recent": records[:limit]}
    finally:
        consumer.close()


@app.get("/dlq")
def dead_letters(limit: int = Query(8, ge=1, le=50)):
    """Events rejected by the data contract: the error and the offending payload."""
    try:
        return _cache.get(f"dlq:{limit}", 5, lambda: _load_dlq(limit))
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": str(exc)}
