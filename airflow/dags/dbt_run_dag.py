"""
dbt_run_dag.py — Daily dbt run + test DAG.

Runs after batch_load_dag succeeds.
Executes: dbt snapshot → dbt run → dbt test → dbt source freshness

Schedule: @daily at 03:00 IST (depends on batch_load completing)
Retries: 3 with exponential backoff
"""
from __future__ import annotations

import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from include.alerts import on_failure_slack_alert

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=3),
    "on_failure_callback": on_failure_slack_alert,
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/include/dbt_project"


def _run_dbt(command: str, select: str | None = None) -> str:
    """Run a dbt CLI command and return stdout."""
    cmd = ["dbt", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    if select:
        cmd.extend(["--select", select])

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} failed:\n{result.stderr}")
    return result.stdout


@dag(
    dag_id="dbt_run_and_test",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 22 * * *",  # 03:30 IST = 22:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "transform", "quality"],
    doc_md="""
    ## dbt Run and Test

    Daily dbt pipeline:
    1. `dbt source freshness` — verify Silver tables are fresh
    2. `dbt snapshot` — update SCD2 restaurant dimension
    3. `dbt run` — rebuild all models (incremental where configured)
    4. `dbt test` — run all generic + custom singular tests
    
    On test failure: Slack alert, upstream dashboards NOT refreshed.
    """,
)
def dbt_run_and_test_dag():

    @task()
    def dbt_source_freshness() -> None:
        """Check source table freshness before running models."""
        try:
            _run_dbt("source", "freshness")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Source freshness check failed: {e}") from e

    @task()
    def dbt_snapshot() -> None:
        """Update SCD2 snapshots (restaurant master)."""
        _run_dbt("snapshot")
        print("✓ Snapshots updated.")

    @task()
    def dbt_run_staging() -> None:
        """Run staging models first."""
        _run_dbt("run", select="staging.*")

    @task()
    def dbt_run_intermediate() -> None:
        """Run intermediate models."""
        _run_dbt("run", select="intermediate.*")

    @task()
    def dbt_run_marts() -> None:
        """Run mart models (fact + dim tables)."""
        _run_dbt("run", select="marts.*")

    @task()
    def dbt_test() -> None:
        """Run all dbt tests (generic + custom singular)."""
        _run_dbt("test")
        print("✓ All dbt tests passed.")

    @task()
    def dbt_generate_docs() -> None:
        """Generate dbt docs (lineage graph) for the README."""
        _run_dbt("docs", "generate")
        print("✓ dbt docs generated.")

    # ── DAG flow ──────────────────────────────────────────────────────────────
    freshness = dbt_source_freshness()
    snapshot = dbt_snapshot()
    staging = dbt_run_staging()
    intermediate = dbt_run_intermediate()
    marts = dbt_run_marts()
    tests = dbt_test()
    docs = dbt_generate_docs()

    freshness >> snapshot >> staging >> intermediate >> marts >> tests >> docs


dbt_run_and_test_dag()
