-- init.sql
-- PostgreSQL initialisation script.
-- Creates:
--   1. Airflow database (created by env var already, this adds extensions)
--   2. A mock operational source database with customer and rider tables.

-- Airflow DB extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ── Operational source schema ──────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ops;

-- Customers table (operational source for dim_customer seed data)
CREATE TABLE IF NOT EXISTS ops.customers (
    customer_id     VARCHAR(20) PRIMARY KEY,
    full_name       VARCHAR(100),
    email           VARCHAR(150),
    phone           VARCHAR(15),
    city            VARCHAR(50),
    registered_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT true
);

-- Riders table (operational source for dim_rider)
CREATE TABLE IF NOT EXISTS ops.riders (
    rider_id        VARCHAR(20) PRIMARY KEY,
    full_name       VARCHAR(100),
    phone           VARCHAR(15),
    city            VARCHAR(50),
    vehicle_type    VARCHAR(20) DEFAULT 'bike',
    joined_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT true
);

-- Restaurant master snapshots (for SCD2 history reference)
CREATE TABLE IF NOT EXISTS ops.restaurant_master_log (
    id              SERIAL PRIMARY KEY,
    restaurant_id   VARCHAR(20) NOT NULL,
    name            VARCHAR(200),
    city            VARCHAR(50),
    cuisine         VARCHAR(50),
    commission_tier VARCHAR(20),
    is_active       BOOLEAN,
    changed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline metrics mirror (optional — for Metabase dashboards via Postgres)
CREATE TABLE IF NOT EXISTS ops.pipeline_metrics_mirror (
    batch_id            INTEGER,
    table_name          VARCHAR(100),
    rows_in             BIGINT,
    rows_out            BIGINT,
    rows_quarantined    BIGINT,
    batch_duration_s    NUMERIC(10,3),
    run_ts              TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for dashboard queries
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_run_ts
    ON ops.pipeline_metrics_mirror (run_ts DESC);

CREATE INDEX IF NOT EXISTS idx_restaurant_log_id
    ON ops.restaurant_master_log (restaurant_id, changed_at DESC);
