-- Where each station stands in its province on a given day: gap to the province average
-- and rank from cheapest. This is the question a station owner asks every morning.
select
    prices.price_date,
    prices.station_id,
    prices.province,
    prices.brand,
    prices.fuel,
    prices.is_self,
    prices.price,
    province.avg_price                 as province_avg_price,
    prices.price - province.avg_price  as gap_to_province_avg,
    rank() over (
        partition by prices.price_date, prices.province, prices.fuel, prices.is_self
        order by prices.price
    )                                  as rank_from_cheapest,
    province.stations                  as province_stations
from {{ ref('stg_prices') }} as prices
inner join {{ ref('fct_province_daily_prices') }} as province
    using (price_date, province, fuel, is_self)
where not prices.is_stale
