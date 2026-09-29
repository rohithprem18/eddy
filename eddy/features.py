"""Spark transformations for the streaming feature pipeline.

Kept free of I/O so every function can be unit-tested on static DataFrames.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

# Feature columns written to the online store for each window size.
WINDOW_FEATURES = [
    "event_count",
    "view_count",
    "cart_count",
    "purchase_count",
    "spend",
    "refund_amount",
    "avg_order_value",
    "cart_conversion",
    "distinct_products",
    "distinct_categories",
    "last_event_ms",
    "window_end_ms",
]

PROFILE_FEATURES = ["last_event_type", "last_category", "last_product_id", "last_device", "last_seen_ms"]


def decode_confluent_avro(raw: DataFrame, writer_schemas: dict[int, str], reader_schema: str) -> DataFrame:
    """Decode Confluent wire-format Avro (magic byte + 4-byte schema id + payload).

    Each record is decoded with the exact writer schema it was produced with
    (looked up by id), then projected onto the latest reader schema. This is
    real schema evolution: v1 and v2 events coexist on the topic and both
    decode into the same struct, with v2-only fields defaulting to null.
    """
    if not writer_schemas:
        raise ValueError("at least one writer schema is required")

    framed = raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.expr("substring(value, 1, 1) = X'00'").alias("valid_magic"),
        F.expr("cast(conv(hex(substring(value, 2, 4)), 16, 10) as int)").alias("schema_id"),
        F.expr("substring(value, 6, length(value) - 5)").alias("payload"),
    ).where("valid_magic")

    options = {"mode": "PERMISSIVE", "avroSchema": reader_schema}
    decoded: Column | None = None
    for schema_id, writer in sorted(writer_schemas.items()):
        branch = from_avro(F.col("payload"), writer, options)
        cond = F.col("schema_id") == schema_id
        decoded = F.when(cond, branch) if decoded is None else decoded.when(cond, branch)

    return (
        framed.select(decoded.alias("e"), "kafka_ts")
        .where(F.col("e").isNotNull() & F.col("e.user_id").isNotNull())
        .select("e.*", "kafka_ts")
    )


def _count_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def _sum_if(cond: Column, value: Column) -> Column:
    return F.sum(F.when(cond, value).otherwise(F.lit(0.0)))


def windowed_features(events: DataFrame, window: str, slide: str, watermark: str = "2 minutes") -> DataFrame:
    """Per-user sliding-window aggregates (stateful in streaming mode)."""
    et = F.col("event_type")
    agg = (
        events.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", window, slide).alias("w"), "user_id")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            _count_if(et == "view").alias("view_count"),
            _count_if(et == "add_to_cart").alias("cart_count"),
            _count_if(et == "purchase").alias("purchase_count"),
            _sum_if(et == "purchase", F.col("amount")).alias("spend"),
            _sum_if(et == "refund", F.col("amount")).alias("refund_amount"),
            F.approx_count_distinct("product_id").alias("distinct_products"),
            F.approx_count_distinct("category").alias("distinct_categories"),
            F.max("event_ts").alias("last_event_ts"),
        )
    )
    return agg.select(
        "user_id",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "event_count",
        "view_count",
        "cart_count",
        "purchase_count",
        F.round("spend", 2).alias("spend"),
        F.round("refund_amount", 2).alias("refund_amount"),
        F.round(F.when(F.col("purchase_count") > 0, F.col("spend") / F.col("purchase_count")), 2).alias(
            "avg_order_value"
        ),
        F.round(F.when(F.col("cart_count") > 0, F.col("purchase_count") / F.col("cart_count")), 4).alias(
            "cart_conversion"
        ),
        "distinct_products",
        "distinct_categories",
        F.unix_millis("last_event_ts").alias("last_event_ms"),
        F.unix_millis("window_end").alias("window_end_ms"),
    )


def select_current_windows(batch: DataFrame, now: Column | None = None) -> DataFrame:
    """Pick one window per user to serve as the "trailing" feature value.

    Sliding windows overlap, so an event updates several windows. The window
    that is still open (end > now) and closes soonest covers the longest
    trailing span up to now. If none are open (replayed or late data), fall
    back to the most recent window.
    """
    now = now if now is not None else F.current_timestamp()
    is_open = F.col("window_end") > now
    order = Window.partitionBy("user_id").orderBy(
        is_open.desc(),
        F.when(is_open, F.col("window_end")).asc_nulls_last(),
        F.col("window_end").desc(),
    )
    return batch.withColumn("_rn", F.row_number().over(order)).where("_rn = 1").drop("_rn")


def latest_profile(batch: DataFrame) -> DataFrame:
    """Most recent event per user in a micro-batch -> 'last seen' profile features."""
    order = Window.partitionBy("user_id").orderBy(F.col("event_ts").desc())
    return (
        batch.withColumn("_rn", F.row_number().over(order))
        .where("_rn = 1")
        .select(
            "user_id",
            F.col("event_type").alias("last_event_type"),
            F.col("category").alias("last_category"),
            F.col("product_id").alias("last_product_id"),
            F.col("device").alias("last_device"),
            F.unix_millis("event_ts").alias("last_seen_ms"),
        )
    )
