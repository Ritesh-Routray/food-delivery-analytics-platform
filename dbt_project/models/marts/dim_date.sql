-- dim_date.sql
-- Date dimension: pre-generated spine from 2024-01-01 to 2027-12-31.
-- Used for time-intelligence in Metabase dashboards.

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date)                  as date_key,
        date_day                                as full_date,

        -- Calendar attributes
        year(date_day)                          as year,
        month(date_day)                         as month,
        day(date_day)                           as day_of_month,
        dayofweek(date_day)                     as day_of_week,        -- 1=Sun
        dayofyear(date_day)                     as day_of_year,
        weekofyear(date_day)                    as week_of_year,
        quarter(date_day)                       as quarter,

        -- Human-readable labels
        strftime(date_day, '%B')                as month_name,
        strftime(date_day, '%A')                as day_name,
        strftime(date_day, '%Y-%m')             as year_month,

        -- Business calendar flags
        dayofweek(date_day) in (1, 7)           as is_weekend,
        dayofweek(date_day) not in (1, 7)       as is_weekday,

        -- Indian food delivery peak seasons (approximate)
        month(date_day) in (10, 11, 12)         as is_festival_season,  -- Diwali, Christmas

        -- Fiscal year (April–March for India)
        case
            when month(date_day) >= 4 then year(date_day)
            else year(date_day) - 1
        end                                     as fiscal_year

    from date_spine
)

select * from final
