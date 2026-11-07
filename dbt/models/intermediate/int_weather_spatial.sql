-- Enrich live weather readings with the Kerala district each station falls in.
-- Ephemeral: consumed only by the marts layer.

with weather as (
    select * from {{ ref('stg_weather') }}
),

boundaries as (
    select name, admin_level, domain_owner, geometry
    from {{ source('geoplatform', 'admin_boundaries') }}
)

select
    w.station_id,
    w.city_name,
    w.event_time,
    w.temperature_c,
    w.humidity_pct,
    w.wind_speed_ms,
    w.cloud_cover_pct,
    w.rainfall_1h_mm,
    w.weather_condition,
    w.quality_flag,
    w.is_anomaly,
    w.window_avg_temp,
    b.name as admin_region_name,
    b.admin_level,
    b.domain_owner as data_domain
from weather w
left join boundaries b
    on ST_Contains(
        b.geometry,
        ST_SetSRID(ST_MakePoint(w.longitude, w.latitude), 4326)
    )
