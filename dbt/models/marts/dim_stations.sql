-- One row per station, with its details as of the last day it appeared.
with ranked as (
    select
        *,
        row_number() over (partition by station_id order by price_date desc) as recency
    from {{ ref('stg_prices') }}
)

select
    station_id,
    station_name,
    brand,
    station_type,
    municipality,
    province,
    latitude,
    longitude,
    price_date as last_seen_date
from ranked
where recency = 1
