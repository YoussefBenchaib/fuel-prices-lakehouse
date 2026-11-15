-- How each brand prices against the local market: the average gap between its stations
-- and their province average, per day, fuel and self/served mode.
select
    price_date,
    coalesce(brand, 'Unknown')                                 as brand,
    fuel,
    is_self,
    count(distinct station_id)                                 as stations,
    {{ exact_avg('price') }}                                  as avg_price,
    cast(round(avg(gap_to_province_avg), 3) as decimal(6, 3))  as avg_gap_to_province
from {{ ref('fct_station_price_position') }}
group by all
