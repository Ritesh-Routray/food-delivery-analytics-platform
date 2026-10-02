# Failure and Recovery Scenarios

For each scenario: **injection method**, **expected behavior**, **evidence to collect**.

---

## Scenario 1: Spark Job Crash Mid-Batch

**What it tests:** Checkpoint-based exactly-once recovery.

**Injection:**
```bash
# Find the Spark driver PID and kill it
docker exec spark-master kill -9 $(pgrep -f silver_transform)
```

**Expected behavior:**
- Spark job restarts from the last committed checkpoint offset.
- Silver table row count after restart = row count before crash + rows from missed batches (no duplicates).
- The DLQ table is unaffected.

**Evidence to collect:**
- Row count in `silver.order_state` before kill and after restart (must be equal for committed batches).
- Checkpoint directory contents (shows last committed offset).
- Spark UI showing the job restarted from the correct offset.

---

## Scenario 2: Bad Record Flood

**What it tests:** DLQ routing, Silver table isolation from bad records.

**Injection:**
```bash
# Set bad record rate to 50% in simulator
CHAOS_MODE=true BAD_RECORD_RATE=0.5 docker compose restart simulator
```

**Expected behavior:**
- Silver `order_events` table continues to grow with clean records only.
- DLQ table `dlq.order_events` shows a spike in `quarantined_at` timestamps.
- Pipeline metrics table shows high `rows_quarantined` for affected batches.
- Quality check DAG fires a Slack alert when DLQ rate exceeds threshold.

**Evidence to collect:**
- Side-by-side row counts: silver events vs DLQ for the same time window.
- Pipeline metrics: `rows_quarantined / rows_in` ratio before/after injection.
- Screenshot of Metabase Pipeline Health panel showing DLQ spike.

---

## Scenario 3: Schema Drift (Unsupported Version)

**What it tests:** Schema version guard in Silver validation, DLQ routing for v3 events.

**Injection:**
```bash
SCHEMA_DRIFT_RATE=0.2 docker compose restart simulator
```
The simulator will inject events with `schema_version=3` (unsupported) at 20% rate.

**Expected behavior:**
- Events with `schema_version=3` are caught by the `unsupported_schema_version` validation rule.
- They are routed to the DLQ with `error_reason="unsupported_schema_version:3"`.
- Silver tables see no schema-version-3 records.

**Evidence to collect:**
- DLQ query: `SELECT error_reason, count(*) FROM dlq.order_events GROUP BY 1`.
- Silver query confirming `schema_version IN (1,2)` only.
- Spark job logs showing no parse errors (schema drift is handled gracefully, not as a crash).

---

## Scenario 4: Late Event Storm

**What it tests:** Watermark behaviour — what falls within window vs what is dropped.

**Injection:**
```bash
LATE_EVENT_RATE=0.3 docker compose restart simulator
```
30% of events will have `event_ts` backdated by 1–15 minutes.

**Expected behavior:**
- Events backdated within the 15-minute watermark: absorbed and deduplicated correctly.
- Events backdated beyond 15 minutes: dropped by `dropDuplicates` watermark filter.
- Silver `order_state` reflects correct current state (not affected by ordering of late events due to `event_ts >= last_event_ts` MERGE condition).

**Evidence to collect:**
- Count of events with `event_ts < (kafka_timestamp - INTERVAL 15 MINUTES)` in Bronze (should be non-zero).
- Count of same events in Silver (should be zero — dropped by watermark).
- Silver `order_state` row count is consistent (no phantom orders from late events).

---

## Scenario 5: Kafka Topic Retention Expiry

**What it tests:** `failOnDataLoss=false` setting and recovery from offset gaps.

**Injection:**
```bash
# Reduce retention to 1 minute, wait, then restart Spark
docker exec kafka kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type topics --entity-name orders.events \
  --add-config retention.ms=60000
sleep 90
# Then restart spark consumer
docker compose restart spark-master spark-worker
```

**Expected behavior:**
- Spark restarts and finds that committed offsets no longer exist in Kafka.
- `failOnDataLoss=false` causes Spark to skip to the earliest available offset (with a warning).
- No job crash; gap is documented in logs.
- Data in Bronze from before the retention gap is preserved (Bronze is the replay source).

**Evidence to collect:**
- Spark logs showing "data may have been lost" warning.
- Bronze table continuity (gap in Kafka offsets ≠ gap in Bronze, since Bronze was already written).
- Row count reconciliation: Bronze events written before gap match Silver.

---

## Scenario 6: dbt Test Failure Blocking Airflow

**What it tests:** Quality gate — bad data in Silver blocks downstream models.

**Injection:**
```sql
-- Directly insert a bad row into silver.order_state (via DuckDB or Spark)
INSERT INTO silver.order_state VALUES (
  'ORD-BAD-001', 'CUS-001', 'RST-001', 'RID-001', 'Bengaluru',
  'order_delivered', -100.0, 'upi', 1,
  current_timestamp(), true, false, false, 5.0, null, current_timestamp()
);
```

**Expected behavior:**
- `dbt test` runs `assert_positive_order_value.sql` and finds 1 failing row.
- dbt exits with non-zero return code.
- `dbt_run_dag` fails at the `dbt_test` task.
- Airflow `on_failure_callback` fires → Slack alert.
- Downstream Metabase dashboards do NOT get refreshed with the bad data.

**Evidence to collect:**
- dbt test output showing the failing test name and row count.
- Airflow DAG graph screenshot with the failed task highlighted red.
- Slack message screenshot (or Airflow log of the alert).
- Metabase dashboard timestamp confirming it was not refreshed.

---

## Recovery Playbook Template

For each scenario, document in `failure-scenarios.md`:

```
### Scenario N: [Name]

**Date injected:** YYYY-MM-DD HH:MM IST
**Hardware:** [your machine spec]

**Injection command:**
[exact command]

**Observed behaviour:**
- [what happened]

**Recovery time:** [X minutes]

**Row count before:** [N]
**Row count after:**  [M]
**Duplicates introduced:** [0 — if correctly recovered]

**Evidence:**
- [link to screenshot / log snippet]
```
