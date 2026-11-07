-- Newest reading per weather station, from the live stream and the batch history.
-- Ephemeral: consumed only by fct_spatial_climate_mart.

with readings as (
    select
        station_id,
        event_time as observed_at,
        temperature_c,
        humidity_pct,
        rainfall_1h_mm as rainfall_mm,
        wind_speed_ms,
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) as station_geom
    from {{ ref('stg_weather') }}
    where quality_flag = 'GOOD'

    union all

    select
        station_id,
        observed_at,
        temperature_c,
        humidity_pct,
        rainfall_mm,
        wind_speed_ms,
        station_geom
    from {{ ref('stg_weather_history') }}
)

select distinct on (station_id) *
from readings
order by station_id, observed_at desc
