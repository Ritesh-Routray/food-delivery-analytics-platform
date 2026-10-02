"""
maintenance.py — Delta Lake OPTIMIZE, VACUUM, and Z-ORDER scripts.

Can be run directly (CLI) or called from the Airflow maintenance DAG.

Usage:
    python lakehouse/maintenance.py --action optimize
    python lakehouse/maintenance.py --action vacuum
    python lakehouse/maintenance.py --action all
    python lakehouse/maintenance.py --action time-travel --table silver.order_state --version 5
"""
from __future__ import annotations

import argparse
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from streaming.common.delta_utils import (
    get_table_history,
    optimize_table,
    restore_to_version,
    vacuum_table,
)
from streaming.common.spark_session import get_spark

# ── Table maintenance config ──────────────────────────────────────────────────

MAINTENANCE_CONFIG = [
    {
        "name": "bronze.order_events",
        "z_order": None,
        "vacuum_hours": 168,   # 7 days
        "notes": "Bronze is append-only. No Z-ORDER needed — queries filter by event_date partition.",
    },
    {
        "name": "silver.order_events",
        "z_order": ["city"],
        "vacuum_hours": 168,
        "notes": "Z-ORDER by city because all dashboard queries filter by city.",
    },
    {
        "name": "silver.order_state",
        "z_order": ["city", "current_status"],
        "vacuum_hours": 168,
        "notes": "Z-ORDER by city + status for operations dashboard queries.",
    },
    {
        "name": "silver.restaurant_master",
        "z_order": ["city"],
        "vacuum_hours": 336,   # 14 days — infrequent updates
        "notes": "Small table. Z-ORDER by city for join performance.",
    },
    {
        "name": "gold.city_minute_metrics",
        "z_order": ["city"],
        "vacuum_hours": 336,
        "notes": "Gold tables kept longer — used for historical dashboard views.",
    },
    {
        "name": "gold.delivery_time_stats",
        "z_order": ["city"],
        "vacuum_hours": 336,
        "notes": "Z-ORDER by city for p95 delivery time queries.",
    },
    {
        "name": "gold.cancellation_rate",
        "z_order": ["city"],
        "vacuum_hours": 336,
        "notes": "Z-ORDER by city for cancellation rate trend queries.",
    },
    {
        "name": "dlq.order_events",
        "z_order": ["error_stage"],
        "vacuum_hours": 720,   # 30 days — DLQ kept longer for replay
        "notes": "Long retention for DLQ — bad records may need manual replay.",
    },
    {
        "name": "metrics.pipeline_metrics",
        "z_order": None,
        "vacuum_hours": 720,
        "notes": "Pipeline health history kept 30 days for trend analysis.",
    },
]


def run_optimize(spark, tables: list[dict]) -> None:
    print(f"\n{'='*60}")
    print(f"  OPTIMIZE  —  {datetime.utcnow().isoformat()}")
    print(f"{'='*60}")
    for cfg in tables:
        print(f"\n▶ {cfg['name']}")
        print(f"  Note: {cfg['notes']}")
        optimize_table(spark, cfg["name"], cfg.get("z_order"))
        print(f"  ✓ Done")


def run_vacuum(spark, tables: list[dict]) -> None:
    print(f"\n{'='*60}")
    print(f"  VACUUM  —  {datetime.utcnow().isoformat()}")
    print(f"{'='*60}")
    # Must disable the safety check for testing (only in dev)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    for cfg in tables:
        print(f"\n▶ {cfg['name']}  (retention: {cfg['vacuum_hours']}h)")
        vacuum_table(spark, cfg["name"], cfg["vacuum_hours"])
        print(f"  ✓ Done")


def show_history(spark, table_name: str, limit: int = 10) -> None:
    print(f"\nTable history for {table_name}:")
    get_table_history(spark, table_name, limit).show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake maintenance tool")
    parser.add_argument(
        "--action",
        choices=["optimize", "vacuum", "all", "history", "restore"],
        required=True,
    )
    parser.add_argument("--table", help="Table name (for history/restore actions)")
    parser.add_argument("--version", type=int, help="Version to restore to")
    parser.add_argument("--limit", type=int, default=10, help="History rows to show")
    args = parser.parse_args()

    spark = get_spark("DeltaMaintenance")

    if args.action == "optimize":
        run_optimize(spark, MAINTENANCE_CONFIG)
    elif args.action == "vacuum":
        run_vacuum(spark, MAINTENANCE_CONFIG)
    elif args.action == "all":
        run_optimize(spark, MAINTENANCE_CONFIG)
        run_vacuum(spark, MAINTENANCE_CONFIG)
    elif args.action == "history":
        if not args.table:
            print("ERROR: --table required for history action")
            sys.exit(1)
        show_history(spark, args.table, args.limit)
    elif args.action == "restore":
        if not args.table or args.version is None:
            print("ERROR: --table and --version required for restore action")
            sys.exit(1)
        print(f"⚠ Restoring {args.table} to version {args.version}...")
        restore_to_version(spark, args.table, args.version)
        print("✓ Restore complete.")

    spark.stop()


if __name__ == "__main__":
    main()
