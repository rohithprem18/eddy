import io
import json
from collections import Counter

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.validation import validate

from eddy.config import get_settings
from eddy.events import EVENT_WEIGHTS, EventGenerator
from eddy.schema_registry import backward_compatibility_issues

SETTINGS = get_settings()
V1, V2 = (json.loads(p.read_text()) for p in SETTINGS.schema_files())


def test_schema_files_ordered_and_valid():
    assert [p.name for p in SETTINGS.schema_files()] == ["v1.avsc", "v2.avsc"]
    for schema in (V1, V2):
        parse_schema(schema)


def test_generated_events_match_latest_schema():
    gen = EventGenerator(n_users=100, seed=7)
    schema = parse_schema(V2)
    for event in gen.batch(500):
        assert validate(event, schema, raise_errors=True)


def test_invalid_events_violate_contract():
    gen = EventGenerator(n_users=100, seed=7)
    schema = parse_schema(V2)
    for _ in range(50):
        assert not validate(gen.invalid_event(), schema, raise_errors=False)


def test_event_mix_follows_funnel_weights():
    counts = Counter(e["event_type"] for e in EventGenerator(n_users=500, seed=1).batch(20000))
    for event_type, weight in EVENT_WEIGHTS.items():
        assert abs(counts[event_type] / 20000 - weight) < 0.02


def test_user_traffic_is_skewed():
    counts = Counter(e["user_id"] for e in EventGenerator(n_users=1000, seed=3).batch(20000))
    top_10pct = sum(c for _, c in counts.most_common(100))
    assert top_10pct / 20000 > 0.3  # heavy hitters exist


def test_v1_data_readable_with_v2_reader():
    """BACKWARD compatibility, proven with real bytes."""
    event = EventGenerator(n_users=10, seed=2).event()
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(V1), event)  # v1 writer drops v2-only fields
    buf.seek(0)
    decoded = schemaless_reader(buf, parse_schema(V1), parse_schema(V2))
    assert decoded["user_id"] == event["user_id"]
    assert decoded["device"] is None and decoded["session_id"] is None


def test_offline_lint_accepts_v1_to_v2():
    assert backward_compatibility_issues(json.dumps(V1), json.dumps(V2)) == []


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda s: s["fields"].append({"name": "coupon", "type": "string"}), "no default"),
        (lambda s: next(f for f in s["fields"] if f["name"] == "amount").update(type="string"), "changed type"),
    ],
)
def test_offline_lint_rejects_breaking_changes(mutate, expected):
    broken = json.loads(json.dumps(V2))
    mutate(broken)
    issues = backward_compatibility_issues(json.dumps(V2), json.dumps(broken))
    assert issues and expected in issues[0]
