-- Within a province, day, fuel and mode, gaps to the average must cancel out
-- (allowing for rounding to three decimals).
select price_date, province, fuel, is_self, avg(gap_to_province_avg) as mean_gap
from {{ ref('fct_station_price_position') }}
group by all
having abs(avg(gap_to_province_avg)) > 0.001
