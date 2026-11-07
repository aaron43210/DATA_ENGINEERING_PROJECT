{{ config(
    materialized='table',
    indexes=[
        {'columns': ['feature_key'], 'unique': true},
        {'columns': ['district_name']},
        {'columns': ['acquisition_time']},
        {'columns': ['feature_geom'], 'type': 'gist'}
    ]
) }}

/*
  Gold mart: fct_spatial_climate_mart (Owner: Angel)
  Grain: one row per OSM feature, showing its current exposure.

    district  ST_Contains: the Kerala district holding the feature
    scene     ST_Intersects: the newest Sentinel-2 scene covering the feature
    weather   ST_DWithin: newest reading from the nearest station within 25 km
*/

with features as (
    select * from {{ ref('stg_osm') }}
),

admin as (
    select
        region_id as district_code,
        name as district_name,
        geometry as district_geom
    from {{ source('geoplatform', 'admin_boundaries') }}
),

stations as (
    select * from {{ ref('int_station_weather_latest') }}
),

located as (
    select f.*, a.district_code, a.district_name
    from features f
    left join admin a
        on ST_Contains(a.district_geom, f.anchor_point)
),

with_scene as (
    select l.*, s.scene_id, s.satellite, s.acquisition_time,
           s.cloud_percentage, s.cloud_usability_flag
    from located l
    left join lateral (
        select *
        from {{ ref('stg_sentinel2') }} scene
        where ST_Intersects(scene.footprint_geom, l.anchor_point)
        order by scene.acquisition_time desc
        limit 1
    ) s on true
),

with_weather as (
    select ws.*, w.station_id as nearest_weather_station,
           w.observed_at as weather_observed_at, w.temperature_c, w.humidity_pct,
           w.rainfall_mm, w.wind_speed_ms, w.distance_to_station_meters
    from with_scene ws
    left join lateral (
        select
            st.*,
            ST_Distance(ws.anchor_point::geography, st.station_geom::geography)
                as distance_to_station_meters
        from stations st
        where ST_DWithin(ws.anchor_point::geography, st.station_geom::geography, 25000)
        order by distance_to_station_meters
        limit 1
    ) w on true
)

select
    feature_key,
    osm_type,
    osm_id,
    feature_name,
    feature_category,
    amenity_type,
    coalesce(district_name, 'Unknown District') as district_name,
    district_code,

    scene_id,
    satellite,
    acquisition_time,
    cloud_percentage,
    cloud_usability_flag,

    nearest_weather_station,
    weather_observed_at,
    temperature_c,
    humidity_pct,
    rainfall_mm,
    wind_speed_ms,
    distance_to_station_meters,

    case
        when temperature_c is null then 'UNKNOWN'
        when temperature_c >= 38.0 then 'CRITICAL_HEAT_WAVE'
        when temperature_c >= 32.0 then 'ELEVATED_HEAT'
        else 'NORMAL'
    end as heat_stress_index,

    case
        when rainfall_mm is null then 'UNKNOWN'
        when rainfall_mm >= 50.0 then 'FLASH_FLOOD_WARNING'
        when rainfall_mm >= 15.0 then 'HEAVY_MONSOON_RAIN'
        when rainfall_mm > 0.0 then 'LIGHT_MODERATE_RAIN'
        else 'DRY'
    end as flood_vulnerability_index,

    geom as feature_geom,
    now() as mart_generated_at

from with_weather
