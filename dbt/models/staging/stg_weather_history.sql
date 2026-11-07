{{ config(
    materialized='view'
) }}

/*
  Staging model for batch weather history (Airflow backfill).
*/

with source_weather as (
    select * from {{ source('geoplatform', 'weather_observations') }}
),

renamed as (
    select
        station_id,
        observed_at,
        temperature_c,
        humidity_pct,
        coalesce(rainfall_mm, 0.0) as rainfall_mm,
        wind_speed_ms,
        cloud_cover_pct,
        location as station_geom,
        ST_X(location) as longitude,
        ST_Y(location) as latitude,
        ingested_at
    from source_weather
    where location is not null
      and ST_IsValid(location)
)

select * from renamed
