"""Spark transformation tests on static DataFrames (requires Java 17 + pyspark)."""

import io
import json
import os
import struct
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
from fastavro import parse_schema, schemaless_writer  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from eddy.config import get_settings  # noqa: E402
from eddy.features import (  # noqa: E402
    decode_confluent_avro,
    latest_profile,
    select_current_windows,
    windowed_features,
)

V1_TEXT, V2_TEXT = (p.read_text() for p in get_settings().schema_files())
T0 = datetime(2026, 9, 29, 12, 0, 0)


@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder.master("local[2]").appName("eddy-tests")
    local_jar = os.getenv("SPARK_AVRO_JAR")
    if local_jar:  # pre-downloaded jar; avoids Maven resolution (and winutils on Windows)
        builder = builder.config("spark.driver.extraClassPath", local_jar)
    else:
        builder = builder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.3")
    session = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def confluent_frame(schema_text: str, schema_id: int, record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(json.loads(schema_text)), record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def event(user="u1", etype="view", amount=10.0, ts=T0, product="p1", **extra):
    return {
        "event_id": "e", "user_id": user, "event_type": etype, "product_id": product,
        "category": "books", "amount": amount, "currency": "USD",
        "event_ts": int(ts.timestamp() * 1000), **extra,
    }


def events_df(spark, rows):
    return spark.createDataFrame(
        [(r["user_id"], r["event_type"], r["product_id"], r["category"], r["amount"],
          # PySpark reads naive datetimes as local time, so build them the same way.
          datetime.fromtimestamp(r["event_ts"] / 1000), r.get("device")) for r in rows],
        "user_id string, event_type string, product_id string, category string, amount double, "
        "event_ts timestamp, device string",
    )


def test_decode_mixed_schema_versions(spark):
    values = [
        (confluent_frame(V1_TEXT, 1, event(user="old")),),
        (confluent_frame(V2_TEXT, 2, event(user="new", device="ios", session_id="s1")),),
        (b"\x01garbage-without-magic-byte",),
        (confluent_frame(V2_TEXT, 99, event(user="unknown-schema")),),
    ]
    raw = spark.createDataFrame(values, "value binary").withColumn("timestamp", F.current_timestamp())
    out = {r.user_id: r for r in decode_confluent_avro(raw, {1: V1_TEXT, 2: V2_TEXT}, V2_TEXT).collect()}
    assert set(out) == {"old", "new"}
    assert out["old"].device is None  # v1 record projected onto v2 reader schema
    assert out["new"].device == "ios" and out["new"].session_id == "s1"
    assert isinstance(out["new"].event_ts, datetime)


def test_windowed_features(spark):
    rows = [
        event(etype="view", ts=T0, product="p1"),
        event(etype="add_to_cart", ts=T0 + timedelta(seconds=10), product="p1"),
        event(etype="purchase", amount=30.0, ts=T0 + timedelta(seconds=20), product="p1"),
        event(etype="add_to_cart", ts=T0 + timedelta(seconds=30), product="p2"),
        event(etype="purchase", amount=50.0, ts=T0 + timedelta(seconds=40), product="p2"),
        event(etype="refund", amount=30.0, ts=T0 + timedelta(seconds=50), product="p1"),
        event(user="u2", etype="view", ts=T0),
    ]
    feats = windowed_features(events_df(spark, rows), "1 minute", "1 minute")
    u1 = feats.where("user_id = 'u1'").collect()
    assert len(u1) == 1
    r = u1[0]
    assert (r.event_count, r.view_count, r.cart_count, r.purchase_count) == (6, 1, 2, 2)
    assert r.spend == 80.0 and r.refund_amount == 30.0
    assert r.avg_order_value == 40.0 and r.cart_conversion == 1.0
    assert r.distinct_products == 2
    assert r.last_event_ms == int((T0 + timedelta(seconds=50)).timestamp() * 1000)
    u2 = feats.where("user_id = 'u2'").collect()[0]
    assert u2.avg_order_value is None and u2.cart_conversion is None


def test_select_current_windows_prefers_longest_open_window(spark):
    rows = [(("u1"), T0 + timedelta(minutes=m)) for m in (1, 2, 3, 4, 5)]
    rows += [("u2", T0 - timedelta(minutes=10)), ("u2", T0 - timedelta(minutes=5))]
    df = spark.createDataFrame(rows, "user_id string, window_end timestamp")
    now = F.lit(T0 + timedelta(seconds=30)).cast("timestamp")
    picked = {r.user_id: r.window_end for r in select_current_windows(df, now).collect()}
    assert picked["u1"] == T0 + timedelta(minutes=1)  # open and closes soonest
    assert picked["u2"] == T0 - timedelta(minutes=5)  # none open: most recent


def test_latest_profile(spark):
    rows = [
        event(etype="view", ts=T0, device="web"),
        event(etype="purchase", ts=T0 + timedelta(seconds=5), device="ios", product="p9"),
    ]
    prof = latest_profile(events_df(spark, rows)).collect()
    assert len(prof) == 1
    assert (prof[0].last_event_type, prof[0].last_device, prof[0].last_product_id) == ("purchase", "ios", "p9")
