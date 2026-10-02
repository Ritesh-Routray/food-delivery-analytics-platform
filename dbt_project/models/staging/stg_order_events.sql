-- stg_order_events.sql
-- Staging model for silver.order_events
-- Only renaming, casting, and light cleaning — no business logic.

with source as (
    select * from {{ source('silver', 'order_events') }}
),

renamed as (
    select
        -- Keys
        event_id,
        order_id,
        city,

        -- Event metadata
        event_type,
        schema_version,
        cast(event_ts as timestamp)     as event_ts,
        cast(produced_ts as timestamp)  as produced_ts,
        cast(updated_at as timestamp)   as updated_at,

        -- Kafka provenance
        kafka_partition,
        kafka_offset,
        cast(kafka_timestamp as timestamp) as kafka_timestamp,

        -- Payload fields (extracted from map)
        payload['customer_id']                      as customer_id,
        payload['restaurant_id']                    as restaurant_id,
        payload['rider_id']                         as rider_id,
        cast(payload['order_value'] as double)      as order_value,
        cast(payload['item_count'] as integer)      as item_count,
        payload['payment_method']                   as payment_method,
        cast(payload['distance_km'] as double)      as distance_km,
        cast(payload['tip_amount'] as double)       as tip_amount,
        payload['rejection_reason']                 as rejection_reason,
        payload['cancelled_by']                     as cancelled_by,

        -- Partition column
        cast(event_ts as date) as event_date

    from source
)

select * from renamed
