# Design Decisions

Each entry covers: the options considered, the choice made, and the trade-off accepted.

---

## 1. Kafka over Kinesis / Pulsar / RabbitMQ

**Options considered:** AWS Kinesis, Apache Pulsar, RabbitMQ, Apache Kafka.

**Choice:** Apache Kafka (KRaft mode — no ZooKeeper).

**Reasoning:** Kafka is the dominant event streaming system in data engineering interviews and production systems. KRaft removes the ZooKeeper dependency, cutting the Docker Compose service count and eliminating split-brain failure modes. Partitioning by `order_id` guarantees event ordering within a partition, which is critical for the MERGE logic to correctly handle the `order_placed → order_confirmed → order_delivered` lifecycle sequence.

**Trade-off accepted:** Kafka has more operational surface area than managed alternatives (MSK, Confluent). For production at scale, Confluent Cloud or MSK would be preferable. Cost reasoning: always-on managed Kafka at moderate throughput costs ~$200–500/month; local Kafka on existing hardware costs $0.

---

## 2. Delta Lake over Apache Iceberg or Apache Hudi

**Options considered:** Apache Iceberg, Apache Hudi, Delta Lake.

**Choice:** Delta Lake 3.2 with `delta-spark` library.

**Reasoning:** Delta has first-party PySpark integration (`DeltaTable` class, `foreachBatch` MERGE), the best documentation for Spark Structured Streaming, and the widest adoption in the Databricks ecosystem. The `MERGE` statement with condition guards (`s.event_ts >= t.last_event_ts`) makes exactly-once upserts straightforward.

**Trade-off accepted:** Iceberg is better for multi-engine environments (Flink + Trino + Spark reading the same table). Hudi has better CDC (Change Data Capture) primitives for row-level deletes. For this Spark-primary stack, Delta is lowest friction.

---

## 3. dbt-DuckDB for Batch Modeling

**Options considered:** dbt-Spark (submit to running cluster), dbt-DuckDB (in-process), dbt-Trino.

**Choice:** dbt-DuckDB for local development; adapter is swappable for production.

**Reasoning:** dbt-Spark requires a running Spark cluster for every `dbt run`, adding 2–5 minutes of startup time and requiring Spark to be running even for a quick model change. DuckDB reads the same Parquet/Delta files from MinIO directly and runs SQL queries in seconds on a laptop. In production, the same `dbt_project/` directory can target Databricks SQL or BigQuery by changing only `profiles.yml`.

**Trade-off accepted:** DuckDB has different SQL dialect quirks from Spark SQL (e.g., `percentile_cont` vs `percentile_approx`). Some functions need macros to abstract the difference. DuckDB's Delta Lake support is also newer and may lag Delta protocol versions.

---

## 4. LocalExecutor for Airflow (vs CeleryExecutor / KubernetesExecutor)

**Options considered:** LocalExecutor, CeleryExecutor + Redis, KubernetesExecutor.

**Choice:** LocalExecutor with a single Airflow scheduler + webserver.

**Reasoning:** LocalExecutor is sufficient for a single-machine development environment. CeleryExecutor requires Redis and multiple worker containers, adding complexity without benefit at this scale. KubernetesExecutor is the production choice for dynamic scaling but requires a K8s cluster.

**Trade-off accepted:** LocalExecutor cannot scale to parallel DAG runs across multiple machines. `max_active_runs=1` mitigates overlap risk on LocalExecutor, but a high-DAG-count production environment would need CeleryExecutor or KubernetesExecutor.

---

## 5. Watermark Duration of 15 Minutes

**Options considered:** 5 minutes, 15 minutes, 1 hour, no watermark.

**Choice:** 15-minute watermark on `event_ts`.

**Reasoning:** The simulator injects late events with delays of 1–15 minutes (mobile network lag). A 15-minute watermark absorbs all simulated late events while bounding Spark's state size. Without a watermark, Spark would accumulate state forever. With a 5-minute watermark, 3% of late events would be dropped.

**Trade-off accepted:** Events arriving more than 15 minutes late are dropped from the streaming path. These are picked up by the next daily `dbt run` via the 3-day lookback window on `updated_at` in `fact_orders`. Very-late events (>3 days) are visible in Bronze (raw, append-only) and can be replayed via a backfill DAG.

---

## 6. Partitioning by `event_date` (not `order_id` or `city`)

**Options considered:** Partition by `order_id`, by `city`, by `event_type`, by `event_date`.

**Choice:** `event_date` for Bronze and Silver event tables; `city` for Gold tables.

**Reasoning:** Partitioning by `order_id` (high cardinality: millions of unique values) produces millions of single-row directories — the classic small-file explosion anti-pattern. Partitioning by `event_date` produces one directory per day, which Spark's partition pruning uses when queries filter by date range (the most common dashboard query pattern). Gold tables partition by `city` because live dashboard queries always filter by city.

**Trade-off accepted:** `event_date` partitioning means queries filtering by `city` must scan all partitions. Z-ORDER on `city` in the Silver tables mitigates this — co-locating city data within each daily partition for faster scanning even without partition pruning.

---

## 7. `foreachBatch` + Delta MERGE (not streaming MERGE)

**Options considered:** Spark's experimental streaming MERGE, Hudi's COW/MOR, `foreachBatch` with Delta MERGE.

**Choice:** `foreachBatch` with an idempotent Delta `MERGE`.

**Reasoning:** `foreachBatch` gives explicit control over exactly what happens per micro-batch: deduplicate within the batch first, then MERGE against the target table. The MERGE condition `s.event_ts >= t.last_event_ts` ensures a retry of the same batch produces identical results (idempotency). Spark's streaming MERGE is experimental and less predictable.

**Trade-off accepted:** `foreachBatch` makes the streaming job stateless between batches (state is in the checkpoint + the Delta table). This is actually a feature: the job can be killed, restarted, or re-run from any checkpoint without corrupting state.

---

## 8. `foreachBatch` Deduplication + Watermark Deduplication (Both)

**Options considered:** Watermark-only deduplication, batch-only deduplication, both.

**Choice:** Both layers — watermark `dropDuplicates` at stream level + `latest_per_key` within `foreachBatch`.

**Reasoning:** Watermark deduplication handles duplicates arriving within the 15-minute window across different micro-batches. Batch-level deduplication handles duplicates appearing in the same micro-batch (possible with high duplicate rates). The two layers are complementary, not redundant.

**Trade-off accepted:** Slight computational overhead from deduplicating twice. Negligible at our scale.
