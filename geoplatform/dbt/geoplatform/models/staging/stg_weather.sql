with source as (
    select * from {{ source('geoplatform', 'weather_stream_observations') }}
),

renamed as (
    select
        station_id,
        city_name,
        event_time,
        temperature_c,
        humidity_pct,
        wind_speed_ms,
        cloud_cover_pct,
        rainfall_1h_mm,
        weather_condition,
        quality_flag,
        is_anomaly,
        window_avg_temp,
        window_max_temp,
        window_min_temp,
        processed_at as row_ingested_at
    from source
    -- Exclude API errors from downstream models
    where quality_flag != 'API_ERROR'
)

select * from renamed
