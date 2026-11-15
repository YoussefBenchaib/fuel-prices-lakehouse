-- Daily price statistics per province, fuel and self/served mode.
-- Stale prices (not updated for more than 7 days) are left out: an old price is real data,
-- but it shouldn't move today's average.
select
    price_date,
    province,
    fuel,
    is_self,
    count(distinct station_id)                   as stations,
    {{ exact_avg('price') }}                    as avg_price,
    min(price)                                   as min_price,
    max(price)                                   as max_price
from {{ ref('stg_prices') }}
where not is_stale
group by all
