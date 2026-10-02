"""
delta_utils.py — Shared Delta Lake helpers used by all streaming jobs.

Provides:
  - DeltaTable MERGE helpers (idempotent upserts)
  - OPTIMIZE and VACUUM wrappers
  - Table creation helpers
  - Time-travel utilities
"""
from __future__ import annotations

import structlog
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = structlog.get_logger(__name__)


# ── Table management ──────────────────────────────────────────────────────────

def create_table_if_not_exists(
    spark: SparkSession,
    table_name: str,
    location: str,
    schema_ddl: str,
    partition_by: list[str] | None = None,
) -> None:
    """Create a Delta table at the given S3 path if it doesn't exist."""
    partition_clause = ""
    if partition_by:
        cols = ", ".join(partition_by)
        partition_clause = f"PARTITIONED BY ({cols})"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({schema_ddl})
        USING DELTA
        LOCATION '{location}'
        {partition_clause}
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    log.info("delta.table_ready", table=table_name, location=location)


# ── Merge helpers ─────────────────────────────────────────────────────────────

def upsert_by_key(
    spark: SparkSession,
    batch_df: DataFrame,
    table_name: str,
    merge_key: str,
    update_condition: str | None = None,
) -> None:
    """
    Generic MERGE: update if row exists and condition is met, insert otherwise.

    Args:
        spark: Active SparkSession.
        batch_df: Incoming batch DataFrame.
        table_name: Fully-qualified Delta table name.
        merge_key: Column to join on (e.g. "order_id").
        update_condition: Optional SQL expression for update guard
                          (e.g. "s.event_ts >= t.last_event_ts").
    """
    if batch_df.isEmpty():
        return

    target = DeltaTable.forName(spark, table_name)
    merge_builder = (
        target.alias("t")
        .merge(batch_df.alias("s"), f"t.{merge_key} = s.{merge_key}")
    )

    if update_condition:
        merge_builder = merge_builder.whenMatchedUpdateAll(condition=update_condition)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()

    log.debug("delta.merge_done", table=table_name, key=merge_key)


def latest_per_key(df: DataFrame, key_col: str, ts_col: str) -> DataFrame:
    """
    Deduplicate a batch to keep only the latest row per key (by ts_col).
    Used in foreachBatch to ensure idempotent MERGE with last-write-wins.
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy(key_col).orderBy(F.col(ts_col).desc())
    return (
        df.withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# ── Maintenance ───────────────────────────────────────────────────────────────

def optimize_table(spark: SparkSession, table_name: str, z_order_cols: list[str] | None = None) -> None:
    """Run OPTIMIZE (compaction) optionally with Z-ORDER."""
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        log.info("delta.optimize_zorder", table=table_name, cols=z_order_cols)
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        log.info("delta.optimize", table=table_name)


def vacuum_table(spark: SparkSession, table_name: str, retention_hours: int = 168) -> None:
    """Run VACUUM to clean up old file versions. Default retention = 7 days."""
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    log.info("delta.vacuum", table=table_name, retention_hours=retention_hours)


def get_table_history(spark: SparkSession, table_name: str, limit: int = 10) -> DataFrame:
    """Return the Delta table history (for time travel demo)."""
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def restore_to_version(spark: SparkSession, table_name: str, version: int) -> None:
    """Restore a Delta table to a previous version (used in failure demo)."""
    spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
    log.info("delta.restore", table=table_name, version=version)


# ── Pipeline metrics writer ───────────────────────────────────────────────────

def write_pipeline_metric(
    spark: SparkSession,
    metrics_table: str,
    table_name: str,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int,
    batch_duration_s: float,
    batch_id: int,
) -> None:
    """Append one row to the pipeline_metrics Delta table."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    data = [{
        "batch_id": batch_id,
        "table_name": table_name,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_quarantined": rows_quarantined,
        "batch_duration_s": round(batch_duration_s, 3),
        "run_ts": now.isoformat(),
    }]

    try:
        metrics_df = spark.createDataFrame(data)
        (metrics_df.write.format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(metrics_table))
    except Exception as e:
        log.error("metrics.write_failed", error=str(e))
