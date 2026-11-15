-- Clean prices from the lake, with the names used in the marts.
select
    cast(extraction_date as date)   as price_date,
    station_id,
    fuel,
    is_self,
    cast(price as decimal(6, 3))    as price,
    reported_at,
    is_stale,
    nullif(trim(brand), '')         as brand,
    station_type,
    name                            as station_name,
    municipality,
    province,
    latitude,
    longitude
from {{ source('lake', 'silver_prices') }}
