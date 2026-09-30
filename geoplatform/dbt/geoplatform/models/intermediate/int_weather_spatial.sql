-- Spatial join: enrich weather readings with the admin boundary they fall in.
-- Uses PostGIS ST_Contains to determine which admin region each station is in.
-- Ephemeral (no materialization) — consumed only by the marts layer.

with weather as (
    select * from {{ ref('stg_weather') }}
),

boundaries as (
    select region_id, name, admin_level, domain_owner, data_product_id,
           geometry
    from {{ source('geoplatform', 'admin_boundaries') }}
),

joined as (
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
        -- Spatial attribution from admin boundary
        b.name as admin_region_name,
        b.admin_level,
        b.domain_owner as data_domain
    from weather w
    left join boundaries b
        on ST_Contains(
            b.geometry,
            ST_SetSRID(
                ST_MakePoint(
                    -- Use approximate station coordinates
                    case w.station_id
                        when 'WX_TVM' then 76.9366
                        when 'WX_COK' then 76.2673
                        when 'WX_CCJ' then 75.7804
                        when 'WX_TCR' then 76.2144
                        when 'WX_QLN' then 76.6141
                        else 76.5
                    end,
                    case w.station_id
                        when 'WX_TVM' then 8.5241
                        when 'WX_COK' then 9.9312
                        when 'WX_CCJ' then 11.2588
                        when 'WX_TCR' then 10.5276
                        when 'WX_QLN' then 8.8932
                        else 9.5
                    end
                ),
                4326
            )
        )
)

select * from joined
