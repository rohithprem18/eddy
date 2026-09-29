import json

import fakeredis
import pytest
from fastapi.testclient import TestClient

from eddy import api
from eddy.config import metrics_key, user_key
from eddy.online_store import OnlineStoreWriter, to_mapping


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def writer(redis_client):
    return OnlineStoreWriter(prefix="eddy", ttl_seconds=60, client=redis_client)


def test_to_mapping_suffixes_and_skips_nulls():
    row = {"user_id": "u1", "spend": 12.5, "event_count": 3, "cart_conversion": None}
    assert to_mapping(row, "5m") == {"spend_5m": "12.5", "event_count_5m": "3"}
    assert to_mapping({"user_id": "u1", "last_device": "ios"}, None) == {"last_device": "ios"}


def test_guarded_write_rejects_stale_windows(writer, redis_client):
    key = user_key("eddy", "u1")
    assert writer.write([{"user_id": "u1", "event_count": 5, "window_end_ms": 2000}], "5m", "window_end_ms") == (1, 0)
    # An older window arriving late must not overwrite the newer one.
    assert writer.write([{"user_id": "u1", "event_count": 1, "window_end_ms": 1000}], "5m", "window_end_ms") == (0, 1)
    assert redis_client.hget(key, "event_count_5m") == "5"
    # Same window with more data does update.
    writer.write([{"user_id": "u1", "event_count": 7, "window_end_ms": 2000}], "5m", "window_end_ms")
    assert redis_client.hget(key, "event_count_5m") == "7"
    assert 0 < redis_client.ttl(key) <= 60


def test_windows_do_not_clobber_each_other(writer, redis_client):
    writer.write([{"user_id": "u2", "spend": 10.0, "window_end_ms": 5}], "5m", "window_end_ms")
    writer.write([{"user_id": "u2", "spend": 99.0, "window_end_ms": 1}], "1h", "window_end_ms")
    h = redis_client.hgetall(user_key("eddy", "u2"))
    assert h["spend_5m"] == "10.0" and h["spend_1h"] == "99.0"


def test_batching_flushes(redis_client):
    w = OnlineStoreWriter(prefix="eddy", ttl_seconds=60, client=redis_client, flush_every=7)
    rows = [{"user_id": f"u{i}", "last_seen_ms": i} for i in range(50)]
    assert w.write(rows, None, "last_seen_ms") == (50, 0)
    assert redis_client.dbsize() == 50


@pytest.fixture
def client(redis_client, monkeypatch):
    api.app.dependency_overrides[api.get_redis] = lambda: redis_client
    yield TestClient(api.app)
    api.app.dependency_overrides.clear()


def test_api_get_features(client, writer):
    writer.write([{"user_id": "u9", "spend": 42.5, "event_count": 3, "window_end_ms": 1}], "5m", "window_end_ms")
    writer.write([{"user_id": "u9", "last_device": "web", "last_seen_ms": 1_700_000_000_000}], None, "last_seen_ms")
    body = client.get("/features/u9").json()
    assert body["found"] is True
    assert body["features"]["spend_5m"] == 42.5
    assert body["features"]["event_count_5m"] == 3
    assert body["features"]["last_device"] == "web"
    assert body["freshness_ms"] > 0


def test_api_missing_user_and_batch(client, writer):
    assert client.get("/features/nobody").status_code == 404
    writer.write([{"user_id": "a", "last_seen_ms": 1}], None, "last_seen_ms")
    resp = client.post("/features/batch", json={"user_ids": ["a", "b"]}).json()
    assert [r["found"] for r in resp] == [True, False]


def test_api_stats_and_health(client, redis_client):
    redis_client.hset(metrics_key("eddy", "profile"), mapping={"input_rows_per_second": "1000.5", "raw": "{}"})
    body = client.get("/stats").json()
    assert body["events_per_minute"] == 60030
    assert body["queries"]["window_5m"] is None
    assert "raw" not in body["queries"]["profile"]
    assert client.get("/health").json() == {"status": "ok"}


def test_console_served_and_fallback(client, monkeypatch, tmp_path):
    monkeypatch.setattr(api, "STATIC_DIR", tmp_path)
    assert "has not been built" in client.get("/").text
    (tmp_path / "index.html").write_text("<div id='root'></div>")
    assert client.get("/").text == "<div id='root'></div>"


def test_sample_users(client, writer):
    writer.write([{"user_id": f"u{i}", "last_seen_ms": 1} for i in range(12)], None, "last_seen_ms")
    ids = client.get("/users/sample?n=5").json()["user_ids"]
    assert len(ids) == 5 and all(i.startswith("u") for i in ids)


def test_stats_reports_serving_latency(client, writer):
    api._lookup_ms.clear()
    assert client.get("/stats").json()["serving"] == {"samples": 0, "p50_ms": None, "p95_ms": None}
    writer.write([{"user_id": "u1", "last_seen_ms": 1}], None, "last_seen_ms")
    for _ in range(3):
        client.get("/features/u1")
    serving = client.get("/stats").json()["serving"]
    assert serving["samples"] == 3 and serving["p50_ms"] >= 0


def test_contracts_diff_fields(client, monkeypatch):
    v1 = {"doc": "v1", "fields": [{"name": "user_id", "type": "string"}, {"name": "amount", "type": "double"}]}
    v2 = {"doc": "v2", "fields": v1["fields"] + [{"name": "device", "type": ["null", "string"], "default": None}]}

    class FakeRegistry:
        def __init__(self, *a, **k):
            pass

        def versions(self, subject):
            return [1, 2]

        def get_version(self, subject, n):
            return {"id": 10 + n, "schema": json.dumps(v1 if n == 1 else v2)}

        def get_compatibility(self, subject):
            return "BACKWARD"

    monkeypatch.setattr(api, "SchemaRegistry", FakeRegistry)
    monkeypatch.setattr(api, "_cache", api._TTLCache())
    body = client.get("/contracts").json()
    assert body["available"] and body["compatibility"] == "BACKWARD"
    latest = body["versions"][-1]
    assert latest["id"] == 12
    added = [(f["name"], f["added"]) for f in latest["fields"]]
    assert added == [("user_id", False), ("amount", False), ("device", True)]
    assert latest["fields"][2]["type"] == "null | string"


def test_contracts_and_dlq_degrade_gracefully(client, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("registry down")

    monkeypatch.setattr(api, "_cache", api._TTLCache())
    monkeypatch.setattr(api, "_load_contracts", boom)
    monkeypatch.setattr(api, "_load_dlq", boom)
    assert client.get("/contracts").json() == {"available": False, "error": "registry down"}
    assert client.get("/dlq").json() == {"available": False, "error": "registry down"}
