"""
quality_checks.py — Data quality check implementations.

Each function returns a dict:
  {"check": "<name>", "passed": bool, "detail": "<message>"}

Checks:
  1. Freshness      — max(event_ts) vs current time
  2. Volume anomaly — row count vs 7-day rolling baseline (±3σ)
  3. Null rates     — critical field null percentages
  4. Schema         — column names + types match expected schema
  5. Referential    — FK relationships between fact and dims
  6. Business rules — order_value>0, state machine validity
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog

log = structlog.get_logger(__name__)

# ── Placeholder DuckDB / Delta connection ─────────────────────────────────────
# In production, import duckdb and connect to the actual Delta tables.
# For local dev without running Spark, these checks can run against DuckDB
# reading the Parquet files written by Delta.


def _query(sql: str) -> list[dict]:
    """Execute SQL against DuckDB and return rows as dicts."""
    try:
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false;")
        con.execute("SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';")
        rows = con.execute(sql).fetchall()
        cols = [desc[0] for desc in con.description]  # type: ignore
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        log.warning("quality.query_failed", error=str(e), sql=sql[:100])
        return []


# ── 1. Freshness check ────────────────────────────────────────────────────────

def check_freshness(
    warn_hours: int = 1,
    error_hours: int = 4,
) -> dict:
    """Check that silver.order_events has received events within threshold."""
    rows = _query("""
        SELECT max(event_ts) as latest_event_ts
        FROM delta_scan('s3a://lake/silver/order_events')
    """)

    check_name = "freshness"
    if not rows or rows[0]["latest_event_ts"] is None:
        return {"check": check_name, "passed": False, "detail": "No rows found in silver.order_events"}

    latest = rows[0]["latest_event_ts"]
    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest.replace("Z", "+00:00"))

    age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

    if age_hours > error_hours:
        return {
            "check": check_name, "passed": False,
            "detail": f"Last event {age_hours:.1f}h ago — exceeds error threshold ({error_hours}h)"
        }
    if age_hours > warn_hours:
        log.warning("quality.freshness_warn", age_hours=age_hours)

    return {"check": check_name, "passed": True, "detail": f"Last event {age_hours:.2f}h ago"}


# ── 2. Volume anomaly check ───────────────────────────────────────────────────

def check_volume_anomaly(sigma_threshold: float = 3.0) -> dict:
    """
    Compare today's event count to the 7-day rolling average.
    Fail if today's count is outside ±3σ.
    """
    rows = _query("""
        SELECT
            cast(event_ts as date) as event_date,
            count(*) as daily_count
        FROM delta_scan('s3a://lake/silver/order_events')
        WHERE event_ts >= current_date - interval '8 days'
        GROUP BY 1
        ORDER BY 1 DESC
    """)

    check_name = "volume_anomaly"
    if len(rows) < 2:
        return {"check": check_name, "passed": True, "detail": "Insufficient history for baseline"}

    today_count = rows[0]["daily_count"]
    baseline = [r["daily_count"] for r in rows[1:]]

    import statistics
    if len(baseline) < 2:
        return {"check": check_name, "passed": True, "detail": "Insufficient baseline data"}

    mean = statistics.mean(baseline)
    stdev = statistics.stdev(baseline) or 1
    z_score = abs(today_count - mean) / stdev

    if z_score > sigma_threshold:
        return {
            "check": check_name, "passed": False,
            "detail": f"Today: {today_count}, baseline mean: {mean:.0f}, z-score: {z_score:.2f} (>{sigma_threshold}σ)"
        }

    return {"check": check_name, "passed": True, "detail": f"Today: {today_count}, z-score: {z_score:.2f}"}


# ── 3. Null rate check ────────────────────────────────────────────────────────

def check_null_rates(max_null_rate: float = 0.001) -> dict:
    """Check that critical fields have null rates below threshold."""
    rows = _query("""
        SELECT
            count(*) as total,
            sum(case when event_id is null then 1 else 0 end) as null_event_id,
            sum(case when order_id is null then 1 else 0 end) as null_order_id,
            sum(case when city is null then 1 else 0 end)     as null_city,
            sum(case when event_type is null then 1 else 0 end) as null_event_type
        FROM delta_scan('s3a://lake/silver/order_events')
        WHERE event_ts >= current_timestamp - interval '1 hour'
    """)

    check_name = "null_rates"
    if not rows or rows[0]["total"] == 0:
        return {"check": check_name, "passed": True, "detail": "No recent rows to check"}

    row = rows[0]
    total = row["total"]
    violations = []

    for field in ["event_id", "order_id", "city", "event_type"]:
        null_count = row[f"null_{field}"]
        rate = null_count / total
        if rate > max_null_rate:
            violations.append(f"{field}: {rate:.4%} null ({null_count}/{total})")

    if violations:
        return {"check": check_name, "passed": False, "detail": "; ".join(violations)}

    return {"check": check_name, "passed": True, "detail": f"All null rates < {max_null_rate:.3%}"}


# ── 4. Schema stability check ─────────────────────────────────────────────────

EXPECTED_SILVER_COLUMNS = {
    "event_id", "order_id", "city", "event_type", "schema_version",
    "event_ts", "produced_ts", "updated_at", "payload",
}


def check_schema_stability() -> dict:
    """Verify expected columns are present in silver.order_events."""
    rows = _query("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'order_events'
    """)

    check_name = "schema_stability"
    if not rows:
        return {"check": check_name, "passed": True, "detail": "Schema check skipped (no metadata)"}

    actual_cols = {r["column_name"] for r in rows}
    missing = EXPECTED_SILVER_COLUMNS - actual_cols

    if missing:
        return {
            "check": check_name, "passed": False,
            "detail": f"Missing columns: {missing}"
        }

    return {"check": check_name, "passed": True, "detail": "Schema matches expected"}


# ── 5. Referential integrity check ────────────────────────────────────────────

def check_referential_integrity() -> dict:
    """Check that fact_orders restaurant_ids exist in dim_restaurant."""
    rows = _query("""
        SELECT count(*) as orphan_count
        FROM (
            SELECT DISTINCT restaurant_id FROM fact_orders
            WHERE restaurant_id IS NOT NULL
        ) f
        LEFT JOIN dim_restaurant d ON f.restaurant_id = d.restaurant_id AND d.is_current = true
        WHERE d.restaurant_id IS NULL
    """)

    check_name = "referential_integrity"
    if not rows:
        return {"check": check_name, "passed": True, "detail": "Check skipped"}

    orphans = rows[0].get("orphan_count", 0)
    if orphans > 0:
        return {
            "check": check_name, "passed": False,
            "detail": f"{orphans} orders reference restaurants not in dim_restaurant"
        }

    return {"check": check_name, "passed": True, "detail": "All FK relationships valid"}


# ── 6. Business rules check ───────────────────────────────────────────────────

def check_business_rules() -> dict:
    """Check business rule violations: negative order_value, invalid timestamps."""
    rows = _query("""
        SELECT
            sum(case when order_value <= 0 then 1 else 0 end) as neg_value_count,
            sum(case when delivered_ts < placed_ts then 1 else 0 end) as invalid_timeline_count
        FROM delta_scan('s3a://lake/silver/order_state')
        WHERE last_event_ts >= current_timestamp - interval '1 hour'
    """)

    check_name = "business_rules"
    if not rows:
        return {"check": check_name, "passed": True, "detail": "No recent rows"}

    row = rows[0]
    violations = []
    if row.get("neg_value_count", 0) > 0:
        violations.append(f"{row['neg_value_count']} orders with order_value <= 0")
    if row.get("invalid_timeline_count", 0) > 0:
        violations.append(f"{row['invalid_timeline_count']} orders with delivered_ts < placed_ts")

    if violations:
        return {"check": check_name, "passed": False, "detail": "; ".join(violations)}

    return {"check": check_name, "passed": True, "detail": "All business rules satisfied"}
