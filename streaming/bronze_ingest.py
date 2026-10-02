"""
bronze_ingest.py — Kafka → Delta Bronze layer ingestion job.

Responsibilities:
  1. Read from Kafka topic `orders.events` (explicit schema, never infer)
  2. Write the raw JSON payload + Kafka metadata to Bronze Delta (append-only)
  3. Partition by event_date for efficient downstream reads
  4. Use checkpoint on MinIO for exactly-once restart semantics

Run via:
    spark-submit --packages <delta,hadoop-aws,kafka jars> bronze_ingest.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import functions as F

from common.schemas import EVENT_SCHEMA
from common.spark_session import get_spark

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_ORDERS", "orders.events")
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000"))
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")

BRONZE_PATH = "s3a://lake/bronze/order_events"
CHECKPOINT_PATH = "s3a://checkpoints/bronze/order_events"
METRICS_PATH = "s3a://lake/metrics/pipeline_metrics"


def run() -> None:
    spark = get_spark("BronzeIngest")

    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # on first run; uses checkpoint afterward
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .option("failOnDataLoss", "false")        # tolerate topic retention gaps
        .load()
    )

    # ── 2. Parse JSON with explicit schema ────────────────────────────────────
    #
    # We parse the value column using our explicit EVENT_SCHEMA.
    # Records that don't match are NOT dropped here — we keep them as-is
    # in Bronze (raw is raw). Silver does the validation and DLQ routing.
    #
    parsed = (
        raw_stream
        .select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").cast("string").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("parsed", F.from_json(F.col("raw_json"), EVENT_SCHEMA))
        # Kafka metadata + parsed fields side by side
        .select(
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "parsed.*",
        )
        # Add ingestion timestamp and partition date
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "event_date",
            F.coalesce(
                F.to_date(F.col("event_ts")),
                F.to_date(F.col("kafka_timestamp")),
            ),
        )
    )

    # ── 3. Write to Bronze Delta (append-only) ─────────────────────────────────
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("path", BRONZE_PATH)
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("bronze_ingest")
        .start()
    )

    print(f"[Bronze] Streaming to {BRONZE_PATH} | checkpoint: {CHECKPOINT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    run()
