"""
schemas.py — PySpark StructType definitions for all event types.

Using explicit schemas (never inferSchema in streaming) ensures:
  - Bad records are caught deterministically and routed to DLQ
  - Schema enforcement is testable independently of Kafka
  - Schema evolution is explicit and documented
"""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Core event envelope ───────────────────────────────────────────────────────
# Matches the JSON structure emitted by simulator/schemas.py → OrderEvent

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("schema_version", IntegerType(), nullable=False),
    StructField("event_ts", StringType(), nullable=False),   # parsed to TimestampType downstream
    StructField("produced_ts", StringType(), nullable=True),
    StructField("order_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),        # nullable — bad records have null city
    StructField("payload", MapType(StringType(), StringType()), nullable=True),
    # Chaos tracking field — present only on chaos-injected events
    StructField("_chaos", StringType(), nullable=True),
])

# ── Silver: order_state table (one row per order, latest state) ───────────────

ORDER_STATE_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("restaurant_id", StringType(), nullable=True),
    StructField("rider_id", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("current_status", StringType(), nullable=True),
    StructField("order_value", DoubleType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("item_count", IntegerType(), nullable=True),
    StructField("placed_ts", TimestampType(), nullable=True),
    StructField("confirmed_ts", TimestampType(), nullable=True),
    StructField("preparing_ts", TimestampType(), nullable=True),
    StructField("picked_up_ts", TimestampType(), nullable=True),
    StructField("delivered_ts", TimestampType(), nullable=True),
    StructField("cancelled_ts", TimestampType(), nullable=True),
    StructField("rejected_ts", TimestampType(), nullable=True),
    StructField("last_event_ts", TimestampType(), nullable=False),
    StructField("is_delivered", BooleanType(), nullable=True),
    StructField("is_cancelled", BooleanType(), nullable=True),
    StructField("is_rejected", BooleanType(), nullable=True),
    StructField("distance_km", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=False),
])

# ── Gold: city_minute_metrics ─────────────────────────────────────────────────

CITY_MINUTE_METRICS_SCHEMA = StructType([
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("orders_placed", IntegerType(), nullable=True),
    StructField("orders_delivered", IntegerType(), nullable=True),
    StructField("orders_cancelled", IntegerType(), nullable=True),
    StructField("orders_rejected", IntegerType(), nullable=True),
    StructField("total_order_value", DoubleType(), nullable=True),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("computed_at", TimestampType(), nullable=False),
])

# ── Gold: delivery_time_stats ─────────────────────────────────────────────────

DELIVERY_TIME_STATS_SCHEMA = StructType([
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("completed_orders", IntegerType(), nullable=True),
    StructField("avg_delivery_minutes", DoubleType(), nullable=True),
    StructField("p50_delivery_minutes", DoubleType(), nullable=True),
    StructField("p95_delivery_minutes", DoubleType(), nullable=True),
    StructField("computed_at", TimestampType(), nullable=False),
])

# ── DLQ envelope ─────────────────────────────────────────────────────────────

DLQ_SCHEMA = StructType([
    StructField("raw_event", StringType(), nullable=True),
    StructField("error_reason", StringType(), nullable=True),
    StructField("error_stage", StringType(), nullable=True),  # "parse" | "validate" | "schema"
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", StringType(), nullable=True),
    StructField("quarantined_at", TimestampType(), nullable=False),
])

# ── Restaurant master schema ──────────────────────────────────────────────────

RESTAURANT_MASTER_SCHEMA = StructType([
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("cuisine", StringType(), nullable=True),
    StructField("commission_tier", StringType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("updated_ts", StringType(), nullable=True),
    StructField("schema_version", IntegerType(), nullable=True),
])
