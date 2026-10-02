# ─────────────────────────────────────────────────────────
#  Makefile — Food Delivery Analytics Platform
# ─────────────────────────────────────────────────────────

.PHONY: up down demo test benchmark lint format clean logs shell-simulator shell-spark

## ── Environment ──────────────────────────────────────────
cp-env:
	@if not exist .env copy .env.example .env && echo ".env created from .env.example"

## ── Docker ───────────────────────────────────────────────
up: cp-env
	docker compose up -d --build
	@echo ""
	@echo "  Services starting..."
	@echo "  kafka-ui   → http://localhost:8080"
	@echo "  minio      → http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  airflow    → http://localhost:8082  (admin / admin)"
	@echo "  spark-ui   → http://localhost:8081"
	@echo "  metabase   → http://localhost:3000"
	@echo ""

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

logs-simulator:
	docker compose logs -f simulator

logs-spark:
	docker compose logs -f spark-master spark-worker

## ── Demo ─────────────────────────────────────────────────
demo: up
	@echo "Waiting for Kafka to be ready..."
	docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
	@echo "Starting Spark Bronze job..."
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/bitnami/spark/work-dir/streaming/bronze_ingest.py &
	@echo "Demo running. Open http://localhost:8080 to watch events flowing."

## ── Spark Jobs ───────────────────────────────────────────
submit-bronze:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/bitnami/spark/work-dir/streaming/bronze_ingest.py

submit-silver:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/bitnami/spark/work-dir/streaming/silver_transform.py

submit-gold:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/bitnami/spark/work-dir/streaming/gold_aggregates.py

## ── dbt ──────────────────────────────────────────────────
dbt-run:
	cd dbt_project && dbt run

dbt-test:
	cd dbt_project && dbt test

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve --port 8090

dbt-lineage:
	cd dbt_project && dbt docs generate

## ── Quality ──────────────────────────────────────────────
quality-check:
	python quality/run_checks.py

## ── Tests ────────────────────────────────────────────────
test:
	pytest tests/ -v --cov=simulator --cov=streaming --cov-report=term-missing

test-unit:
	pytest tests/ -v -m "unit"

test-integration:
	pytest tests/ -v -m "integration"

## ── Lint & Format ────────────────────────────────────────
lint:
	ruff check .
	black --check .

format:
	ruff check --fix .
	black .

## ── Benchmark ────────────────────────────────────────────
benchmark:
	@echo "Running throughput benchmark..."
	python -c "from simulator.generator import benchmark_throughput; benchmark_throughput(duration_seconds=30)"

## ── Maintenance ──────────────────────────────────────────
optimize:
	python lakehouse/maintenance.py --action optimize

vacuum:
	python lakehouse/maintenance.py --action vacuum

## ── Shells ───────────────────────────────────────────────
shell-simulator:
	docker compose exec simulator /bin/bash

shell-spark:
	docker compose exec spark-master /bin/bash

shell-kafka:
	docker compose exec kafka /bin/bash

## ── Clean ────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage

## ── Help ─────────────────────────────────────────────────
help:
	@echo "Available targets:"
	@echo "  make up           — Start all services"
	@echo "  make down         — Stop all services and remove volumes"
	@echo "  make demo         — Start services + run full demo pipeline"
	@echo "  make test         — Run all unit tests"
	@echo "  make lint         — Run ruff + black check"
	@echo "  make format       — Auto-fix lint + format"
	@echo "  make benchmark    — Run throughput benchmark"
	@echo "  make submit-bronze — Submit bronze Spark streaming job"
	@echo "  make submit-silver — Submit silver Spark streaming job"
	@echo "  make submit-gold   — Submit gold Spark streaming job"
	@echo "  make dbt-run      — Run all dbt models"
	@echo "  make dbt-test     — Run all dbt tests"
	@echo "  make optimize     — Run Delta OPTIMIZE on all tables"
	@echo "  make vacuum       — Run Delta VACUUM on all tables"
