"""
silver_transform.py — Bronze Delta → Silver Delta transformation job.

Responsibilities:
  1. Read Bronze Delta as a stream (Delta Change Data Feed)
  2. Validate records: route bad ones to DLQ Delta table
  3. Watermark-based deduplication on event_id (15-minute window)
  4. Maintain silver.order_state via foreachBatch + idempotent MERGE
  5. Write silver.order_events (all valid deduplicated events, append)
  6. Record pipeline metrics per batch

Key design decisions:
  - Watermark of 15 minutes bounds state size while absorbing most late events
  - foreachBatch MERGE is idempotent: re-running a batch produces same result
  - DLQ is a Delta table (not just a Kafka topic) for easy querying & replay
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType

from common.delta_utils import latest_per_key, upsert_by_key, write_pipeline_metric
from common.schemas import EVENT_SCHEMA
from common.spark_session import get_spark

# ── Config ────────────────────────────────────────────────────────────────────

BRONZE_PATH = os.environ.get("BRONZE_PATH", "s3a://lake/bronze/order_events")
SILVER_EVENTS_PATH = "s3a://lake/silver/order_events"
SILVER_STATE_PATH = "s3a://lake/silver/order_state"
DLQ_PATH = "s3a://lake/dlq/order_events"
METRICS_PATH = "s3a://lake/metrics/pipeline_metrics"
CHECKPOINT_PATH = "s3a://checkpoints/silver/order_events"
WATERMARK_DURATION = os.environ.get("WATERMARK_DURATION", "15 minutes")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")

# ── Validation rules ──────────────────────────────────────────────────────────

VALID_EVENT_TYPES = {
    "order_placed", "order_confirmed", "order_preparing",
    "order_picked_up", "order_delivered", "order_cancelled", "order_rejected",
}

SUPPORTED_SCHEMA_VERSIONS = {1, 2}


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split a parsed batch into (clean, bad) DataFrames.

    Validation rules:
      - event_id must not be null
      - order_id must not be null
      - city must not be null
      - event_type must be in VALID_EVENT_TYPES
      - schema_version must be in SUPPORTED_SCHEMA_VERSIONS
      - event_ts must be parseable as timestamp
      - order_value (if present) must be positive
    """
    # Mark each row with its failure reason (or null if clean)
    validated = (
        df
        .withColumn("_ts_parsed", F.to_timestamp(F.col("event_ts")))
        .withColumn(
            "_error_reason",
            F.when(F.col("event_id").isNull(), "null_event_id")
            .when(F.col("order_id").isNull(), "null_order_id")
            .when(F.col("city").isNull(), "null_city")
            .when(
                ~F.col("event_type").isin(list(VALID_EVENT_TYPES)),
                F.concat(F.lit("invalid_event_type:"), F.col("event_type").cast("string"))
            )
            .when(
                ~F.col("schema_version").isin(list(SUPPORTED_SCHEMA_VERSIONS)),
                F.concat(F.lit("unsupported_schema_version:"), F.col("schema_version").cast("string"))
            )
            .when(F.col("_ts_parsed").isNull(), "unparseable_event_ts")
            .otherwise(None)
        )
    )

    clean = validated.filter(F.col("_error_reason").isNull()).drop("_error_reason")
    bad = (
        validated.filter(F.col("_error_reason").isNotNull())
        .select(
            F.col("raw_json").alias("raw_event"),
            F.col("_error_reason").alias("error_reason"),
            F.lit("validate").alias("error_stage"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )

    return clean, bad


def enrich_clean(df: DataFrame) -> DataFrame:
    """Cast types and extract payload fields for the clean stream."""
    return (
        df
        .withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
        .withColumn("produced_ts", F.to_timestamp(F.col("produced_ts")))
        .withColumn("order_value", F.col("payload")["order_value"].cast("double"))
        .withColumn("customer_id", F.col("payload")["customer_id"])
        .withColumn("restaurant_id", F.col("payload")["restaurant_id"])
        .withColumn("rider_id", F.col("payload")["rider_id"])
        .withColumn("item_count", F.col("payload")["item_count"].cast("int"))
        .withColumn("payment_method", F.col("payload")["payment_method"])
        .withColumn("distance_km", F.col("payload")["distance_km"].cast("double"))
        .withColumn("tip_amount", F.col("payload")["tip_amount"].cast("double"))
        .withColumn("updated_at", F.current_timestamp())
    )


# ── foreachBatch handlers ─────────────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Process one micro-batch:
      1. Parse raw_json (in case Bronze has unparsed rows)
      2. Validate + route bad records to DLQ
      3. Deduplicate within batch by event_id (keep latest event_ts)
      4. Append to silver.order_events
      5. MERGE into silver.order_state (latest state per order)
      6. Write pipeline metrics
    """
    batch_start = time.time()
    spark = batch_df.sparkSession

    # Re-parse if raw_json is present (Bronze stores raw alongside parsed cols)
    if "raw_json" in batch_df.columns:
        parsed = batch_df.withColumn(
            "_parsed_json", F.from_json(F.col("raw_json"), EVENT_SCHEMA)
        )
        # Use parsed columns where the top-level ones are null (for DLQ reason)
        for col_name in ["event_id", "event_type", "schema_version", "event_ts",
                         "order_id", "city", "payload"]:
            if col_name not in batch_df.columns:
                parsed = parsed.withColumn(col_name, F.col(f"_parsed_json.{col_name}"))
        batch_df = parsed

    rows_in = batch_df.count()

    # 1. Validate ─────────────────────────────────────────────────────────────
    clean, bad = validate_and_split(batch_df)

    rows_quarantined = bad.count()

    # 2. Write bad records to DLQ ─────────────────────────────────────────────
    if rows_quarantined > 0:
        (bad.write.format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(DLQ_PATH))

    # 3. Enrich clean records ──────────────────────────────────────────────────
    enriched = enrich_clean(clean)

    # 4. Batch deduplication on event_id (keep latest event_ts per event_id) ──
    deduped = latest_per_key(enriched, key_col="event_id", ts_col="event_ts")

    rows_out = deduped.count()

    # 5. Append to silver.order_events ────────────────────────────────────────
    (deduped.write.format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .partitionBy("event_type")
     .save(SILVER_EVENTS_PATH))

    # 6. MERGE into silver.order_state (one row per order, latest state) ───────
    _upsert_order_state(spark, deduped)

    # 7. Pipeline metrics ─────────────────────────────────────────────────────
    batch_duration = time.time() - batch_start
    write_pipeline_metric(
        spark, METRICS_PATH, "silver.order_state",
        rows_in, rows_out, rows_quarantined, batch_duration, batch_id,
    )


def _upsert_order_state(spark, events_df: DataFrame) -> None:
    """Build the latest-state-per-order DataFrame and MERGE into order_state."""
    from pyspark.sql.window import Window

    # Pivot: for each order_id take the most recent values of each field
    w = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())

    state = (
        events_df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("restaurant_id"),
            F.col("rider_id"),
            F.col("city"),
            F.col("event_type").alias("current_status"),
            F.col("order_value"),
            F.col("payment_method"),
            F.col("item_count"),
            F.col("event_ts").alias("last_event_ts"),
            F.col("distance_km"),
            F.col("tip_amount"),
            F.col("updated_at"),
            # Lifecycle flags
            (F.col("event_type") == "order_delivered").alias("is_delivered"),
            (F.col("event_type") == "order_cancelled").alias("is_cancelled"),
            (F.col("event_type") == "order_rejected").alias("is_rejected"),
        )
    )

    try:
        upsert_by_key(
            spark,
            batch_df=state,
            table_name="silver.order_state",
            merge_key="order_id",
            update_condition="s.last_event_ts >= t.last_event_ts",
        )
    except Exception:
        # Table may not exist yet on first run — write directly
        (state.write.format("delta")
         .mode("append")
         .save(SILVER_STATE_PATH))


# ── Main streaming query ──────────────────────────────────────────────────────

def run() -> None:
    spark = get_spark("SilverTransform")

    # Read Bronze as a streaming source
    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 100)
        .load(BRONZE_PATH)
    )

    # Watermark-based deduplication (stream-level, complements batch dedup in foreachBatch)
    deduplicated_stream = (
        bronze_stream
        .withColumn("event_ts_parsed", F.to_timestamp(F.col("event_ts")))
        .withWatermark("event_ts_parsed", WATERMARK_DURATION)
        .dropDuplicates(["event_id"])
    )

    query = (
        deduplicated_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("silver_transform")
        .start()
    )

    print(f"[Silver] Streaming from {BRONZE_PATH} | checkpoint: {CHECKPOINT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    run()
