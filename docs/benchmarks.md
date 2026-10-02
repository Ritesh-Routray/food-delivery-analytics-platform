# Performance Benchmarks

> **Rule**: Never invent numbers. Measure on your own hardware. Record CPU/RAM/config.

Fill this document after running the benchmarks described in Section 8 of the project guide.

---

## Hardware Specification

```
Machine:    [your machine — e.g., MacBook Pro M3, 16GB RAM]
OS:         [e.g., macOS 14.5 / Windows 11 / Ubuntu 22.04]
Docker:     [e.g., Docker Desktop 4.30, 8 CPUs allocated, 12GB RAM]
Disk:       [e.g., NVMe SSD 512GB]
```

---

## Benchmark 1: Simulator Throughput

**What:** Maximum events generated per second (in-process, no Kafka overhead).

**Method:** `make benchmark` → runs `generator.py::benchmark_throughput(duration_seconds=30)`.

```
Events generated:    [N]
Duration:            30 seconds
Events/second:       [X]   ← fill in after running
```

**Notes:**
- This measures Python generation speed only (no network/Kafka overhead).
- With Kafka enabled: add ~20–30% latency for producer batching + lz4 compression.

---

## Benchmark 2: End-to-End Latency (event_ts → Gold table)

**What:** Time from `event_ts` in the simulator to the record appearing in a Gold table.

**Method:**
1. Note `event_ts` of a specific `order_id` from kafka-ui.
2. Poll `gold.city_minute_metrics` every 5 seconds until the window containing that event appears.
3. Measure delta.

```
p50 latency:    [Y] seconds
p95 latency:    [Z] seconds
```

**Spark trigger interval:** 10 seconds (the theoretical minimum latency floor).

---

## Benchmark 3: Small File Problem — Before vs After OPTIMIZE

**What:** File count and query time in `silver.order_events` before and after running `OPTIMIZE`.

**Method:**
```bash
# Count files before
aws s3 ls s3://lake/silver/order_events/ --recursive | wc -l

# Run OPTIMIZE
python lakehouse/maintenance.py --action optimize

# Count files after
aws s3 ls s3://lake/silver/order_events/ --recursive | wc -l

# Query time before and after (DuckDB)
time duckdb -c "SELECT city, count(*) FROM delta_scan('s3a://lake/silver/order_events') GROUP BY 1"
```

```
Files before OPTIMIZE:      [A]   files
Files after OPTIMIZE:       [B]   files
Reduction ratio:            [A/B]x

Query time before:          [T1] seconds
Query time after:           [T2] seconds
Speedup:                    [T1/T2]x
```

---

## Benchmark 4: Z-ORDER Impact on City-Filtered Queries

**What:** Query time for city-filtered queries with and without Z-ORDER on `city`.

**Method:** After running `OPTIMIZE ... ZORDER BY (city)` on `silver.order_events`:
```sql
-- Measure this query before and after Z-ORDER
SELECT event_type, count(*)
FROM delta_scan('s3a://lake/silver/order_events')
WHERE city = 'Bengaluru'
GROUP BY 1
```

```
Query time without Z-ORDER:    [T1] seconds
Query time with Z-ORDER:       [T2] seconds
Speedup:                       [T1/T2]x
```

---

## Benchmark 5: dbt Run Time — Full vs Incremental

**What:** dbt model execution time for full refresh vs incremental refresh.

**Method:**
```bash
# Full refresh
time dbt run --full-refresh --profiles-dir . --target dev

# Incremental (next day, small delta)
time dbt run --profiles-dir . --target dev
```

```
Full refresh run time:       [T1] seconds
Incremental run time:        [T2] seconds
Speedup:                     [T1/T2]x
Rows processed (full):       [N1]
Rows processed (incremental):[N2]
```

---

## Benchmark 6: Kafka Producer Throughput

**What:** Maximum events/second the producer can sustain to Kafka.

**Method:**
```bash
# Set burst mode, run for 60 seconds, measure from kafka-ui Consumer Lag
BURST_MODE=true EVENTS_PER_SECOND=5000 docker compose restart simulator
# Watch kafka-ui for consumer lag growth rate (= producer rate)
```

```
Sustained producer rate:    [X] events/second
With compression (lz4):     [Y] MB/second
Kafka consumer lag at 1x:   [Z] messages
```

---

## Tuning Levers Tested

| Lever | Values Tested | Best Value | Impact |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | 8, 16, 32 | [best] | [impact] |
| `maxOffsetsPerTrigger` | 50k, 100k, 200k | [best] | [impact] |
| Kafka partitions | 3, 6, 12 | [best] | [impact] |
| Delta file target size | 64MB, 128MB | 128MB | [impact] |
| Watermark duration | 5min, 15min, 30min | 15min | [impact] |

---

*Measured on: [date] · Fill all [placeholders] with actual measurements before publishing.*
