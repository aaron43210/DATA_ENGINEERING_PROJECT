{{ config(
    materialized='table',
    indexes=[
      {'columns': ['event_time']},
      {'columns': ['station_id']}
    ]
) }}

with enriched_weather as (
    select * from {{ ref('int_weather_spatial') }}
)

select
    station_id,
    city_name,
    admin_region_name,
    event_time,
    temperature_c,
    humidity_pct,
    wind_speed_ms,
    cloud_cover_pct,
    rainfall_1h_mm,
    weather_condition,
    is_anomaly,
    window_avg_temp,

    -- Business logic: heat index category
    case
        when temperature_c >= 40 then 'Extreme Heat'
        when temperature_c >= 35 then 'High Heat'
        when temperature_c >= 28 then 'Warm'
        when temperature_c >= 20 then 'Comfortable'
        else 'Cool'
    end as heat_index_category,

    -- Business logic: rainfall intensity
    case
        when rainfall_1h_mm = 0           then 'No Rain'
        when rainfall_1h_mm < 2.5         then 'Light Rain'
        when rainfall_1h_mm < 7.6         then 'Moderate Rain'
        when rainfall_1h_mm < 50          then 'Heavy Rain'
        else 'Extreme Rain'
    end as rainfall_intensity

from enriched_weather

-- Only expose clean, non-anomalous readings to the gold layer
where quality_flag = 'GOOD'
