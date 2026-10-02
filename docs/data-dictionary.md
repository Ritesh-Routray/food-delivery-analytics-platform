# Data Dictionary

All tables, columns, data types, and business definitions for the Food Delivery Analytics Platform.

---

## Bronze Layer

### `bronze.order_events`
Raw events as received from Kafka. Append-only. Never modified after write.

| Column | Type | Description |
|---|---|---|
| `raw_json` | STRING | Original JSON string from Kafka (preserved for replay) |
| `kafka_topic` | STRING | Kafka topic name (`orders.events`) |
| `kafka_partition` | INT | Kafka partition (0–5) |
| `kafka_offset` | STRING | Kafka offset within the partition |
| `kafka_timestamp` | TIMESTAMP | When Kafka received the message |
| `event_id` | STRING | UUID — unique event identifier (may be null for truly malformed records) |
| `event_type` | STRING | Lifecycle stage (see Event Types below) |
| `schema_version` | INT | Event schema version (1 = baseline, 2 = extended) |
| `event_ts` | STRING | When the event happened (event time, kept as string in Bronze) |
| `produced_ts` | STRING | When the simulator produced the event |
| `order_id` | STRING | Order identifier (format: `ORD-XXXXXXXX`) |
| `city` | STRING | City where the order was placed |
| `payload` | MAP<STRING,STRING> | Event-specific fields (varies by event_type) |
| `ingested_at` | TIMESTAMP | When this row was written to Bronze |
| `event_date` | DATE | Partition column — derived from `event_ts` |

---

## Silver Layer

### `silver.order_events`
Validated, deduplicated events with typed columns extracted from payload.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `event_id` | STRING | NO | Unique event UUID |
| `order_id` | STRING | NO | Order identifier |
| `city` | STRING | NO | City (validated: never null in Silver) |
| `event_type` | STRING | NO | One of 7 valid lifecycle stages |
| `schema_version` | INT | YES | 1 or 2 (3+ → DLQ) |
| `event_ts` | TIMESTAMP | NO | Event time (typed from Bronze string) |
| `produced_ts` | TIMESTAMP | YES | Producer time |
| `updated_at` | TIMESTAMP | NO | When this row was written to Silver |
| `order_value` | DOUBLE | YES | Order total in INR (positive; negative → DLQ) |
| `customer_id` | STRING | YES | Customer identifier (`CUS-XXXXXX`) |
| `restaurant_id` | STRING | YES | Restaurant identifier (`RST-XXXXXX`) |
| `rider_id` | STRING | YES | Rider identifier (`RID-XXXXXX`) — present on pickup/delivery |
| `item_count` | INT | YES | Number of items in the order |
| `payment_method` | STRING | YES | `upi`, `card`, `wallet`, `cod`, `netbanking` |
| `distance_km` | DOUBLE | YES | Delivery distance in km (delivery event only) |
| `tip_amount` | DOUBLE | YES | Tip paid to rider in INR (nullable) |
| `rejection_reason` | STRING | YES | Why the restaurant rejected (rejection event only) |
| `cancelled_by` | STRING | YES | `customer`, `restaurant`, or `system` |
| `event_date` | DATE | YES | Partition column |

### `silver.order_state`
One row per order. Updated via MERGE as new events arrive. Last-write-wins on `event_ts`.

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING (PK) | Order identifier |
| `current_status` | STRING | Most recent lifecycle stage |
| `customer_id` | STRING | From `order_placed` event |
| `restaurant_id` | STRING | From `order_placed` / `order_confirmed` |
| `rider_id` | STRING | From `order_picked_up` event |
| `city` | STRING | From `order_placed` event |
| `order_value` | DOUBLE | Gross order value in INR |
| `payment_method` | STRING | Payment channel |
| `is_delivered` | BOOLEAN | Terminal state: order delivered |
| `is_cancelled` | BOOLEAN | Terminal state: order cancelled |
| `is_rejected` | BOOLEAN | Terminal state: restaurant rejected |
| `last_event_ts` | TIMESTAMP | Timestamp of the most recent event processed |
| `distance_km` | DOUBLE | From `order_delivered` |
| `tip_amount` | DOUBLE | From `order_delivered` |
| `updated_at` | TIMESTAMP | When Silver last wrote this row |

---

## Gold Layer

### `gold.city_minute_metrics`
Streaming aggregation: 1-minute tumbling windows by city.

| Column | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Window start time |
| `window_end` | TIMESTAMP | Window end time |
| `city` | STRING | City name |
| `orders_placed` | INT | `order_placed` count in window |
| `orders_delivered` | INT | `order_delivered` count |
| `orders_cancelled` | INT | `order_cancelled` count |
| `orders_rejected` | INT | `order_rejected` count |
| `total_order_value` | DOUBLE | Sum of `order_value` for placed orders |
| `avg_order_value` | DOUBLE | Average order value |
| `computed_at` | TIMESTAMP | When this aggregate was written |

### `gold.delivery_time_stats`
5-minute sliding window (1-min slide) delivery time statistics.

| Column | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Window start |
| `window_end` | TIMESTAMP | Window end |
| `city` | STRING | City |
| `completed_orders` | INT | Count of `order_delivered` in window |
| `avg_delivery_minutes` | DOUBLE | Mean delivery time (placed → delivered) |
| `p50_delivery_minutes` | DOUBLE | Median delivery time |
| `p95_delivery_minutes` | DOUBLE | 95th percentile delivery time |
| `computed_at` | TIMESTAMP | Write time |

### `gold.cancellation_rate`
1-minute tumbling window cancellation and rejection percentages.

| Column | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Window start |
| `window_end` | TIMESTAMP | Window end |
| `city` | STRING | City |
| `total_placed` | INT | Orders placed in window |
| `total_cancelled` | INT | Orders cancelled |
| `total_rejected` | INT | Orders rejected by restaurant |
| `cancellation_rate_pct` | DOUBLE | `(cancelled+rejected)/placed * 100` |
| `computed_at` | TIMESTAMP | Write time |

### `gold.rider_metrics`
15-minute sliding window (5-min slide) per-rider productivity.

| Column | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Window start |
| `window_end` | TIMESTAMP | Window end |
| `rider_id` | STRING | Rider identifier |
| `city` | STRING | City |
| `deliveries_completed` | INT | Deliveries in window |
| `total_distance_km` | DOUBLE | Total distance covered |
| `avg_tip` | DOUBLE | Average tip received |
| `computed_at` | TIMESTAMP | Write time |

---

## dbt Mart Layer (Star Schema)

### `fact_orders`
One row per order. Incremental merge. Central fact table.

| Column | Type | Description |
|---|---|---|
| `order_sk` | STRING | Surrogate key (hash of order_id) |
| `order_id` | STRING (NK) | Natural key |
| `customer_id` | STRING (FK) | → `dim_customer.customer_id` |
| `restaurant_id` | STRING (FK) | → `dim_restaurant.restaurant_id` |
| `rider_id` | STRING (FK) | → `dim_rider.rider_id` |
| `city` | STRING | City of order |
| `order_date` | DATE (FK) | → `dim_date.date_key` |
| `placed_ts` through `rejected_ts` | TIMESTAMP | Lifecycle stage timestamps |
| `order_value` | DOUBLE | Gross order value (INR) |
| `item_count` | INT | Items ordered |
| `payment_method` | STRING | Payment channel |
| `distance_km` | DOUBLE | Delivery distance |
| `tip_amount` | DOUBLE | Tip paid |
| `total_order_value` | DOUBLE | `order_value + tip_amount` |
| `confirmation_wait_minutes` | INT | placed → confirmed |
| `rider_assignment_wait_minutes` | INT | confirmed → picked_up |
| `transit_minutes` | INT | picked_up → delivered |
| `total_delivery_minutes` | INT | placed → delivered |
| `is_delivered` | BOOLEAN | Final delivery flag |
| `is_cancelled` | BOOLEAN | Cancellation flag |
| `is_rejected` | BOOLEAN | Restaurant rejection flag |
| `gmv_contribution` | DOUBLE | `order_value` if delivered, else 0 |
| `updated_at` | TIMESTAMP | Last Silver update |

### `dim_restaurant`
SCD Type 2 restaurant dimension. Multiple rows per restaurant (one per version).

| Column | Type | Description |
|---|---|---|
| `restaurant_sk` | STRING | Surrogate key |
| `restaurant_id` | STRING | Natural key |
| `restaurant_name` | STRING | Display name |
| `city` | STRING | City |
| `cuisine` | STRING | Cuisine category |
| `commission_tier` | STRING | `standard`, `premium`, `enterprise` |
| `is_active` | BOOLEAN | Whether restaurant is currently accepting orders |
| `dbt_valid_from` | TIMESTAMP | When this version became effective |
| `dbt_valid_to` | TIMESTAMP | When this version was superseded (null = current) |
| `is_current` | BOOLEAN | True for the current active version |

### `dim_customer`

| Column | Type | Description |
|---|---|---|
| `customer_sk` | STRING | Surrogate key |
| `customer_id` | STRING | Natural key |
| `home_city` | STRING | Most common city for orders |
| `is_repeat_customer` | BOOLEAN | Has placed >1 order |

### `dim_rider`

| Column | Type | Description |
|---|---|---|
| `rider_sk` | STRING | Surrogate key |
| `rider_id` | STRING | Natural key |
| `city` | STRING | Primary city |
| `total_deliveries` | INT | All-time deliveries |
| `avg_delivery_minutes` | DOUBLE | Average delivery time |
| `performance_tier` | STRING | `bronze` (<100), `silver` (100–499), `gold` (500+) |

---

## Event Types

| Event Type | Description | Key Payload Fields |
|---|---|---|
| `order_placed` | Customer submits order | `customer_id`, `restaurant_id`, `order_value`, `item_count`, `payment_method` |
| `order_confirmed` | Restaurant accepts | `restaurant_id`, `estimated_prep_minutes` |
| `order_preparing` | Kitchen starts cooking | `restaurant_id`, `actual_prep_start_ts` |
| `order_picked_up` | Rider collects order | `rider_id`, `restaurant_id`, `pickup_ts` |
| `order_delivered` | Customer receives order | `rider_id`, `customer_id`, `delivery_ts`, `distance_km`, `tip_amount` |
| `order_cancelled` | Order cancelled | `reason`, `cancelled_by`, `refund_amount` |
| `order_rejected` | Restaurant refuses | `restaurant_id`, `rejection_reason` |

---

## Dead Letter Queue

### `dlq.order_events`
All records that failed Silver validation. Partitioned by `error_stage`.

| Column | Type | Description |
|---|---|---|
| `raw_event` | STRING | Original JSON from Bronze |
| `error_reason` | STRING | Why it was quarantined (e.g., `null_city`, `invalid_event_type:order_teleported`) |
| `error_stage` | STRING | `parse` or `validate` |
| `kafka_partition` | INT | Source Kafka partition |
| `kafka_offset` | STRING | Source Kafka offset (for replay) |
| `quarantined_at` | TIMESTAMP | When it was quarantined |
