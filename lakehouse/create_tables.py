"""
create_tables.py — Delta Lake table DDL.

Creates all Delta tables with:
  - Explicit schemas (no schema inference)
  - Schema enforcement enabled
  - Change Data Feed enabled (for downstream CDC reads)
  - Correct partition columns (event_date only — not high-cardinality)
  - Table properties for auto-compaction

Run once before starting streaming jobs:
    python lakehouse/create_tables.py

Or call from Airflow on first pipeline setup.
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from streaming.common.spark_session import get_spark

# ── Table definitions ─────────────────────────────────────────────────────────

TABLES = [
    # ── Bronze ─────────────────────────────────────────────────────────────
    {
        "name": "bronze.order_events",
        "path": "s3a://lake/bronze/order_events",
        "partition_by": ["event_date"],
        "schema": """
            raw_json         STRING,
            kafka_topic      STRING,
            kafka_partition  INT,
            kafka_offset     STRING,
            kafka_timestamp  TIMESTAMP,
            event_id         STRING,
            event_type       STRING,
            schema_version   INT,
            event_ts         STRING,
            produced_ts      STRING,
            order_id         STRING,
            city             STRING,
            payload          MAP<STRING, STRING>,
            ingested_at      TIMESTAMP,
            event_date       DATE
        """,
    },

    # ── Silver ─────────────────────────────────────────────────────────────
    {
        "name": "silver.order_events",
        "path": "s3a://lake/silver/order_events",
        "partition_by": ["event_type"],
        "schema": """
            event_id         STRING NOT NULL,
            order_id         STRING NOT NULL,
            city             STRING NOT NULL,
            event_type       STRING NOT NULL,
            schema_version   INT,
            event_ts         TIMESTAMP NOT NULL,
            produced_ts      TIMESTAMP,
            updated_at       TIMESTAMP NOT NULL,
            kafka_partition  INT,
            kafka_offset     STRING,
            kafka_timestamp  TIMESTAMP,
            order_value      DOUBLE,
            customer_id      STRING,
            restaurant_id    STRING,
            rider_id         STRING,
            item_count       INT,
            payment_method   STRING,
            distance_km      DOUBLE,
            tip_amount       DOUBLE,
            rejection_reason STRING,
            cancelled_by     STRING,
            event_date       DATE
        """,
    },
    {
        "name": "silver.order_state",
        "path": "s3a://lake/silver/order_state",
        "partition_by": [],
        "schema": """
            order_id              STRING NOT NULL,
            customer_id           STRING,
            restaurant_id         STRING,
            rider_id              STRING,
            city                  STRING,
            current_status        STRING,
            order_value           DOUBLE,
            payment_method        STRING,
            item_count            INT,
            last_event_ts         TIMESTAMP NOT NULL,
            is_delivered          BOOLEAN,
            is_cancelled          BOOLEAN,
            is_rejected           BOOLEAN,
            distance_km           DOUBLE,
            tip_amount            DOUBLE,
            updated_at            TIMESTAMP NOT NULL
        """,
    },
    {
        "name": "silver.restaurant_master",
        "path": "s3a://lake/silver/restaurant_master",
        "partition_by": [],
        "schema": """
            restaurant_id    STRING NOT NULL,
            name             STRING,
            city             STRING,
            cuisine          STRING,
            commission_tier  STRING,
            is_active        BOOLEAN,
            updated_ts       TIMESTAMP,
            schema_version   INT
        """,
    },

    # ── Gold ───────────────────────────────────────────────────────────────
    {
        "name": "gold.city_minute_metrics",
        "path": "s3a://lake/gold/city_minute_metrics",
        "partition_by": ["city"],
        "schema": """
            window_start      TIMESTAMP NOT NULL,
            window_end        TIMESTAMP NOT NULL,
            city              STRING NOT NULL,
            orders_placed     INT,
            orders_delivered  INT,
            orders_cancelled  INT,
            orders_rejected   INT,
            total_order_value DOUBLE,
            avg_order_value   DOUBLE,
            computed_at       TIMESTAMP NOT NULL
        """,
    },
    {
        "name": "gold.delivery_time_stats",
        "path": "s3a://lake/gold/delivery_time_stats",
        "partition_by": [],
        "schema": """
            window_start           TIMESTAMP NOT NULL,
            window_end             TIMESTAMP NOT NULL,
            city                   STRING NOT NULL,
            completed_orders       INT,
            avg_delivery_minutes   DOUBLE,
            p50_delivery_minutes   DOUBLE,
            p95_delivery_minutes   DOUBLE,
            computed_at            TIMESTAMP NOT NULL
        """,
    },
    {
        "name": "gold.cancellation_rate",
        "path": "s3a://lake/gold/cancellation_rate",
        "partition_by": ["city"],
        "schema": """
            window_start           TIMESTAMP NOT NULL,
            window_end             TIMESTAMP NOT NULL,
            city                   STRING NOT NULL,
            total_placed           INT,
            total_cancelled        INT,
            total_rejected         INT,
            cancellation_rate_pct  DOUBLE,
            computed_at            TIMESTAMP NOT NULL
        """,
    },
    {
        "name": "gold.rider_metrics",
        "path": "s3a://lake/gold/rider_metrics",
        "partition_by": [],
        "schema": """
            window_start          TIMESTAMP NOT NULL,
            window_end            TIMESTAMP NOT NULL,
            rider_id              STRING NOT NULL,
            city                  STRING,
            deliveries_completed  INT,
            total_distance_km     DOUBLE,
            avg_tip               DOUBLE,
            computed_at           TIMESTAMP NOT NULL
        """,
    },

    # ── DLQ ────────────────────────────────────────────────────────────────
    {
        "name": "dlq.order_events",
        "path": "s3a://lake/dlq/order_events",
        "partition_by": ["error_stage"],
        "schema": """
            raw_event        STRING,
            error_reason     STRING,
            error_stage      STRING,
            kafka_partition  INT,
            kafka_offset     STRING,
            quarantined_at   TIMESTAMP NOT NULL
        """,
    },

    # ── Metrics ────────────────────────────────────────────────────────────
    {
        "name": "metrics.pipeline_metrics",
        "path": "s3a://lake/metrics/pipeline_metrics",
        "partition_by": [],
        "schema": """
            batch_id           INT,
            table_name         STRING,
            rows_in            BIGINT,
            rows_out           BIGINT,
            rows_quarantined   BIGINT,
            batch_duration_s   DOUBLE,
            run_ts             STRING
        """,
    },
]


def create_all_tables() -> None:
    spark = get_spark("CreateTables")

    # Create databases/schemas
    for db in ["bronze", "silver", "gold", "dlq", "metrics"]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    for table in TABLES:
        name = table["name"]
        path = table["path"]
        schema = table["schema"].strip()
        partitions = table.get("partition_by", [])

        partition_clause = ""
        if partitions:
            cols = ", ".join(partitions)
            partition_clause = f"PARTITIONED BY ({cols})"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {name} ({schema})
        USING DELTA
        LOCATION '{path}'
        {partition_clause}
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.minWriterVersion' = '7',
            'delta.minReaderVersion' = '3'
        )
        """
        spark.sql(ddl)
        print(f"✓ Table ready: {name} → {path}")

    print(f"\n✓ All {len(TABLES)} Delta tables created/verified.")
    spark.stop()


if __name__ == "__main__":
    create_all_tables()
