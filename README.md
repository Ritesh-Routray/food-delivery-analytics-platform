# 🍕 Real-Time Food Delivery Analytics Platform

> A production-style data engineering portfolio project: end-to-end streaming + batch pipeline for a fictional food delivery company.

![CI](https://github.com/YOUR_GITHUB/food-delivery-analytics/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/apache_spark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/delta_lake-3.2-blue)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B)
![Airflow](https://img.shields.io/badge/airflow-2.9-017CEE)
![Kafka](https://img.shields.io/badge/kafka-3.7_KRaft-231F20)

---

## Overview

Every step of a food delivery order (placed → confirmed → preparing → picked up → delivered) emits a structured event. This platform:

- **Ingests** those events in near real-time at 500+ events/sec via Kafka
- **Processes** them with Spark Structured Streaming through a Bronze → Silver → Gold lakehouse
- **Models** them into a star schema using dbt for analytical queries
- **Orchestrates** daily batch jobs and quality checks with Airflow
- **Visualises** live metrics and business KPIs on Metabase

The simulator deliberately injects chaos (duplicates, late events, schema drift, bad values) to prove the pipeline handles real-world messiness correctly.

---

## Architecture

```
[Python Simulator] ──► [Apache Kafka] ──► [Spark Structured Streaming]
  500 events/sec         6 partitions           │              │
  5 chaos types          KRaft (no ZK)          ▼              ▼
                                         [Delta: Bronze]   [DLQ table]
                                         raw + metadata    bad records
                                                │
                                                ▼
                                        [Delta: Silver]
                                        validated + deduped
                                        one row per order
                                                │
                            ┌───────────────────┴──────────────────┐
                            ▼                                       ▼
                  [Delta: Gold tables]                   [dbt: star schema]
                  4 streaming aggregates                 fact_orders + 4 dims
                            │                                       │
                            └──────────────► [Metabase] ◄──────────┘
                                            4 dashboard panels

[Airflow] ─ batch_load · dbt_run · quality_checks · delta_maintenance
```

---

## Quickstart

```bash
# 1. Clone and configure
git clone https://github.com/YOUR_GITHUB/food-delivery-analytics.git
cd food-delivery-analytics
cp .env.example .env

# 2. Start all services
make up

# 3. Run the full pipeline demo
make demo
```

**Services:**

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8082 | admin / admin |
| Spark UI | http://localhost:8081 | — |
| Metabase | http://localhost:3000 | set on first visit |

---

## Business Questions Answered

| # | Question | Source |
|---|---|---|
| 1 | Orders/min per city — right now? | Gold: `city_minute_metrics` |
| 2 | Avg + p95 delivery time per city/hour? | Gold: `delivery_time_stats` |
| 3 | Highest cancellation/rejection rate restaurants? | dbt: `fact_orders + dim_restaurant` |
| 4 | Wait time between "accepted" and "rider assigned"? | dbt: `int_order_timeline` |
| 5 | Most/least productive riders? | Gold: `rider_metrics`, dbt: `dim_rider` |
| 6 | Daily GMV + avg order value by city? | dbt: `fact_orders + dim_date` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 3.7 (KRaft) |
| Stream processing | Apache Spark 3.5 Structured Streaming |
| Lakehouse storage | Delta Lake 3.2 on MinIO (S3-compatible) |
| Batch modeling | dbt-core 1.8 with DuckDB adapter |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Data quality | 6-category checks + Great Expectations |
| Dashboard | Metabase 0.49 |
| CI | GitHub Actions |

---

## Repository Structure

```
food_delivery_analytics/
├── simulator/          Python event producer with 5 chaos injection types
├── streaming/          Spark jobs: bronze_ingest, silver_transform, gold_aggregates
│   └── common/         spark_session, schemas, delta_utils
├── lakehouse/          Delta DDL, PostgreSQL init, maintenance scripts
├── dbt_project/        Star schema: staging → intermediate → marts + SCD2 + tests
├── airflow/
│   ├── dags/           4 DAGs: batch_load, dbt_run, quality_checks, maintenance
│   └── include/        quality_checks.py, alerts.py
├── tests/              pytest unit + integration tests
├── docs/               design-decisions, failure-scenarios, benchmarks, data-dictionary
└── .github/workflows/  CI: lint + test + dbt + docker
```

---

## Benchmark Results

> Measured on [YOUR HARDWARE]. See [docs/benchmarks.md](docs/benchmarks.md) for methodology.

| Metric | Result |
|---|---|
| Simulator throughput | [X] events/sec |
| p95 end-to-end latency | [Y] seconds |
| Bronze files before OPTIMIZE | [A] |
| Bronze files after OPTIMIZE | [B] |
| Query speedup from Z-ORDER | [Z]% |
| dbt full refresh | [T1]s |
| dbt incremental refresh | [T2]s |

---

## Data Quality Gates

Six independent check categories run hourly:

| Check | Threshold | Catches |
|---|---|---|
| Freshness | Warn >1h, Error >4h | Pipeline down |
| Volume anomaly | ±3σ baseline | Silent data loss |
| Null rates | <0.001% on key fields | Schema contract broken |
| Schema stability | Exact column match | Accidental schema change |
| Referential integrity | 0 orphan FK rows | Missing dim load |
| Business rules | value>0, delivered>placed | Validation gap |

---

## Failure & Recovery Demos

See [docs/failure-scenarios.md](docs/failure-scenarios.md) for 6 scenarios:

1. **Spark job crash** → restart from checkpoint, zero duplicates
2. **Bad record flood** → all routed to DLQ, Silver unaffected
3. **Schema drift (v3 events)** → caught by version guard, routed to DLQ
4. **Late event storm** → watermark absorbs, correct state maintained
5. **Kafka retention expiry** → `failOnDataLoss=false`, gap logged not crashed
6. **dbt test failure** → blocks downstream refresh, Slack alert fires

---

## Key Commands

```bash
make up              # Start all services
make demo            # Start + run end-to-end demo
make test            # Run unit tests with coverage
make submit-bronze   # Submit Spark bronze ingestion job
make submit-silver   # Submit Spark silver transform job
make submit-gold     # Submit Spark gold aggregation job
make dbt-run         # Run all dbt models
make dbt-test        # Run all dbt tests
make benchmark       # Run throughput benchmark
make optimize        # Run Delta OPTIMIZE on all tables
make vacuum          # Run Delta VACUUM on all tables
make down            # Stop all services
```

---

## Design Decisions

See [docs/design-decisions.md](docs/design-decisions.md) for 8 detailed decisions:
- Why Kafka over Kinesis/Pulsar
- Why Delta over Iceberg/Hudi
- Why dbt-DuckDB for modeling
- Why `foreachBatch` MERGE over streaming MERGE
- Why 15-minute watermark
- Why `event_date` partitioning (not `order_id`)
- Why LocalExecutor for Airflow
- Why both batch + streaming deduplication

---

## Known Limitations & Next Steps

**Current limitations:**
- `dim_customer` and `dim_rider` are derived from `fact_orders`, not a true operational source (no separate customer/rider event stream in the simulator).
- Metabase dashboards require manual setup after first `make up` (no auto-provisioning).
- Terraform is scaffolded but not fully implemented (cloud deployment is optional).

**What I would do next:**
- Add CDC source connector (Debezium) for the operational Postgres tables
- Deploy to AWS (MSK + S3 + EMR Serverless + MWAA) using the Terraform skeleton
- Add Prometheus metrics from Spark + Kafka, visualised in Grafana
- Implement schema registry (Confluent Schema Registry or AWS Glue Schema Registry)
- Add a streaming ML model for delivery time prediction

---

## Links

- [Implementation Plan](docs/implementation-plan.md)
- [Design Decisions](docs/design-decisions.md)
- [Failure Scenarios](docs/failure-scenarios.md)
- [Benchmarks](docs/benchmarks.md)
- [Data Dictionary](docs/data-dictionary.md)

---

*Built from [`Data_Engineer_Project_Guide.docx`](Data_Engineer_Project_Guide.docx)*
