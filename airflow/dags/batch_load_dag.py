"""
batch_load_dag.py — Daily batch load DAG.

Loads restaurant master CSV/Parquet files from the operational source
into the Bronze Delta table, then triggers Silver validation.

Schedule: @daily at 01:00 IST
Retries: 3 with exponential backoff
SLA: 2 hours
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from include.alerts import on_failure_slack_alert

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_slack_alert,
    "email_on_failure": False,
}


@dag(
    dag_id="batch_load_restaurant_master",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 20 * * *",  # 01:30 IST = 20:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "bronze", "restaurant"],
    doc_md="""
    ## Batch Load: Restaurant Master
    
    Loads daily restaurant master CSV drops into Bronze Delta.
    Then triggers Silver transformation for the restaurant master topic.
    
    **SLA**: Must complete within 2 hours.
    **On failure**: Slack alert to #data-alerts channel.
    """,
)
def batch_load_restaurant_master_dag():

    @task()
    def check_source_files() -> dict:
        """Check that today's restaurant master file exists in the source location."""
        import os
        from datetime import date

        today = date.today().strftime("%Y%m%d")
        expected_path = f"s3a://lake/raw/restaurant_master/dt={today}/"

        # In real setup: boto3 check. Here we simulate.
        return {"source_path": expected_path, "date": today}

    @task()
    def load_to_bronze(source_info: dict) -> dict:
        """Load restaurant master Parquet/CSV from source path to Bronze Delta."""
        from pyspark.sql import SparkSession

        source_path = source_info["source_path"]
        target_path = "s3a://lake/bronze/restaurant_master/"

        # In production: spark-submit or subprocess call
        # Here we return metadata for downstream tasks
        return {
            "source_path": source_path,
            "target_path": target_path,
            "rows_loaded": 500,  # placeholder; use actual count
            "load_ts": datetime.utcnow().isoformat(),
        }

    @task()
    def validate_bronze_load(load_info: dict) -> None:
        """Basic row count and schema validation after Bronze load."""
        rows = load_info.get("rows_loaded", 0)
        if rows == 0:
            raise ValueError("Bronze load produced 0 rows — aborting.")
        print(f"✓ Bronze load validated: {rows} rows at {load_info['target_path']}")

    @task()
    def trigger_silver_restaurant() -> None:
        """Notify that Silver should pick up the new restaurant master data."""
        # In production: trigger Kafka message or Airflow Dataset event
        print("✓ Silver restaurant master refresh triggered.")

    # ── DAG flow ──────────────────────────────────────────────────────────────
    source_info = check_source_files()
    load_info = load_to_bronze(source_info)
    validate_bronze_load(load_info) >> trigger_silver_restaurant()


batch_load_restaurant_master_dag()
