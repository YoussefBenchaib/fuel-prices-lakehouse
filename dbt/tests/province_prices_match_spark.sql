-- The province table is computed twice, in Spark (gold) and here in SQL.
-- Any row that exists on one side only, or has different numbers, fails the test.
with spark as (
    select
        cast(extraction_date as date) as price_date, province, fuel, is_self,
        stations, cast(avg_price as decimal(6, 3)) as avg_price,
        cast(min_price as decimal(6, 3)) as min_price, cast(max_price as decimal(6, 3)) as max_price
    from {{ source('lake', 'gold_province_prices') }}
),

dbt as (
    select price_date, province, fuel, is_self, stations, avg_price, min_price, max_price
    from {{ ref('fct_province_daily_prices') }}
)

(select 'only in spark' as side, * from (select * from spark except select * from dbt))
union all
(select 'only in dbt' as side, * from (select * from dbt except select * from spark))
