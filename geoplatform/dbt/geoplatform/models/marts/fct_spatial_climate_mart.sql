{{ config(
    materialized='table',
    indexes=[
        {'columns': ['osm_id']},
        {'columns': ['district_name']},
        {'columns': ['acquisition_time']},
        {'columns': ['feature_geom'], 'type': 'gist'},
        {'columns': ['station_buffer_25km'], 'type': 'gist'}
    ]
) }}

/*
  Gold Layer Mart: fct_spatial_climate_mart
  ==========================================
  Owner: Angel (Transformation Layer Lead)

  Performs core multi-modal spatial joins:
    1. ST_Intersects: Correlates Sentinel-2 satellite scenes with OSM critical infrastructure.
    2. ST_Buffer: Creates a 25km environmental buffer around weather stations to compute
       local microclimate exposure for hospitals and road corridors.
    3. ST_Contains: Enriches points with Kerala administrative district boundaries.
*/

with osm_features as (
    select * from {{ ref('stg_osm') }}
),

sentinel2 as (
    select * from {{ ref('stg_sentinel2') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

admin as (
    select
        region_id,
        name as district_name,
        geometry as district_geom
    from {{ source('geoplatform', 'admin_boundaries') }}
),

-- 1. Spatial join: Associate each OSM feature with its encompassing administrative district
osm_district_joined as (
    select
        osm.osm_id,
        osm.feature_category,
        osm.feature_name,
        osm.amenity_type,
        osm.bed_capacity,
        osm.geom as feature_geom,
        osm.longitude,
        osm.latitude,
        coalesce(adm.district_name, 'Unknown District') as district_name,
        adm.region_id as district_code
    from osm_features osm
    left join admin adm
        on ST_Contains(adm.district_geom, osm.geom)
),

-- 2. Spatial join: Match OSM features with intersecting Sentinel-2 satellite observation footprints
osm_satellite_joined as (
    select
        od.*,
        sat.scene_id,
        sat.satellite,
        sat.acquisition_time,
        sat.cloud_percentage,
        sat.cloud_usability_flag
    from osm_district_joined od
    left join sentinel2 sat
        on ST_Intersects(sat.footprint_geom, od.feature_geom)
),

-- 3. Spatial buffer & join: Nearest weather station within a 25km buffer zone
osm_weather_buffered as (
    select distinct on (osj.osm_id, osj.scene_id)
        osj.*,
        w.station_id as nearest_weather_station,
        w.observed_at as weather_observed_at,
        w.temperature_c,
        w.humidity_pct,
        w.rainfall_mm,
        w.wind_speed_ms,
        -- Generate 25km buffer polygon around weather station in WGS84
        ST_Buffer(w.station_geom::geography, 25000)::geometry(Polygon, 4326) as station_buffer_25km,
        -- Calculate geodesic distance in meters from infrastructure to weather station
        ST_Distance(osj.feature_geom::geography, w.station_geom::geography) as distance_to_station_meters
    from osm_satellite_joined osj
    left join weather w
        on ST_DWithin(osj.feature_geom::geography, w.station_geom::geography, 25000)
    order by osj.osm_id, osj.scene_id, distance_to_station_meters asc
)

select
    -- Primary Keys & Identifiers
    osm_id,
    feature_name,
    feature_category,
    amenity_type,
    district_name,
    district_code,

    -- Remote Sensing Context (Sentinel-2)
    scene_id,
    satellite,
    acquisition_time,
    cloud_percentage,
    cloud_usability_flag,

    -- Weather & Climate Metrics
    nearest_weather_station,
    weather_observed_at,
    temperature_c,
    humidity_pct,
    rainfall_mm,
    wind_speed_ms,
    distance_to_station_meters,

    -- Environmental Risk Categorization
    case
        when temperature_c >= 38.0 then 'CRITICAL_HEAT_WAVE'
        when temperature_c >= 32.0 then 'ELEVATED_HEAT'
        when temperature_c is null then 'UNKNOWN'
        else 'NORMAL'
    end as heat_stress_index,

    case
        when rainfall_mm >= 50.0 then 'FLASH_FLOOD_WARNING'
        when rainfall_mm >= 15.0 then 'HEAVY_MONSOON_RAIN'
        when rainfall_mm > 0.0   then 'LIGHT_MODERATE_RAIN'
        else 'DRY'
    end as flood_vulnerability_index,

    -- Spatial Geometries for GIS / Serving Layer
    feature_geom,
    station_buffer_25km,
    now() as mart_generated_at

from osm_weather_buffered
