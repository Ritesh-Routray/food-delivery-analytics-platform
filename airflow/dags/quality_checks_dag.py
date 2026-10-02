"""
quality_checks_dag.py — Hourly data quality checks DAG.

Runs freshness, volume, null-rate, schema, referential, and business-rule checks.
On failure: Slack alert. Upstream dbt run is blocked until checks pass.

Schedule: Hourly
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from include.alerts import on_failure_slack_alert
from include.quality_checks import (
    check_business_rules,
    check_freshness,
    check_null_rates,
    check_referential_integrity,
    check_schema_stability,
    check_volume_anomaly,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_slack_alert,
}


@dag(
    dag_id="data_quality_checks",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "monitoring", "alerts"],
    doc_md="""
    ## Hourly Data Quality Checks
    
    Runs 6 categories of checks on Silver tables:
    
    | Check | Threshold |
    |---|---|
    | Freshness | Warn >1h, Error >4h since last event |
    | Volume | ±3σ from 7-day rolling baseline |
    | Null rates | event_id/order_id/city < 0.001% null |
    | Schema | Column names + types match expected |
    | Referential | fact_orders FKs exist in dims |
    | Business rules | order_value>0, delivered>placed, state machine |
    
    **On failure**: immediate Slack alert to #data-alerts.
    """,
)
def data_quality_checks_dag():

    @task()
    def run_freshness_check() -> dict:
        return check_freshness()

    @task()
    def run_volume_check() -> dict:
        return check_volume_anomaly()

    @task()
    def run_null_rate_check() -> dict:
        return check_null_rates()

    @task()
    def run_schema_check() -> dict:
        return check_schema_stability()

    @task()
    def run_referential_check() -> dict:
        return check_referential_integrity()

    @task()
    def run_business_rules_check() -> dict:
        return check_business_rules()

    @task()
    def aggregate_results(
        freshness: dict, volume: dict, nulls: dict,
        schema: dict, referential: dict, business: dict
    ) -> None:
        results = [freshness, volume, nulls, schema, referential, business]
        failures = [r for r in results if not r.get("passed", True)]
        if failures:
            for f in failures:
                print(f"❌ FAILED: {f}")
            raise ValueError(f"{len(failures)} quality check(s) failed. See logs above.")
        print(f"✓ All {len(results)} quality checks passed.")

    # ── DAG flow (parallel checks → aggregate) ────────────────────────────────
    f = run_freshness_check()
    v = run_volume_check()
    n = run_null_rate_check()
    s = run_schema_check()
    r = run_referential_check()
    b = run_business_rules_check()
    aggregate_results(f, v, n, s, r, b)


data_quality_checks_dag()
