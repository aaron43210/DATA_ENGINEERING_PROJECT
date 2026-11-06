-- =============================================================================
-- Cloud-Native Geospatial Data Platform: PostgreSQL + PostGIS Initialization
-- Script: scripts/init_postgis.sql
-- Managed by: Angel (Storage & Infrastructure Lead)
-- =============================================================================

-- 1. Create Airflow Database & User if not already present
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow_meta_pass_2026';
    END IF;
END
$$;

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Switch to geoplatform application database
\connect geoplatform;

-- 2. Enable PostGIS Core & Topology Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Verify PostGIS Version
DO $$
BEGIN
    RAISE NOTICE 'PostGIS Extension Enabled: %', postgis_full_version();
END $$;

-- 3. Create Schemas for Multi-Tier Geospatial Data Architecture
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS quality;

-- Grant permissions to geoplatform role
GRANT ALL ON SCHEMA public, raw, staging, marts, quality TO geoplatform;

-- =============================================================================
-- 4. TABLE DEFINITIONS
-- =============================================================================

-- 4.1 OSM Vector Features (ELT ingestion target for Overpass API)
CREATE TABLE IF NOT EXISTS public.vector_features (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT UNIQUE NOT NULL,
    feature_type VARCHAR(50) NOT NULL,            -- e.g., 'hospital', 'highway_trunk'
    name TEXT,
    tags JSONB NOT NULL DEFAULT '{}'::jsonb,      -- Full raw OSM key-value attributes
    geometry GEOMETRY(Geometry, 4326) NOT NULL,   -- Points, LineStrings, or Polygons in WGS84
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.2 Satellite Observations (Sentinel-2 scene metadata & spatial footprints)
CREATE TABLE IF NOT EXISTS public.satellite_observations (
    id SERIAL PRIMARY KEY,
    scene_id VARCHAR(100) UNIQUE NOT NULL,
    satellite VARCHAR(50) NOT NULL DEFAULT 'Sentinel-2',
    acquisition_time TIMESTAMPTZ NOT NULL,
    cloud_percentage FLOAT CHECK (cloud_percentage BETWEEN 0 AND 100),
    crs VARCHAR(50) DEFAULT 'EPSG:4326',
    s3_bronze_path TEXT,                          -- Raw JSON / STAC metadata in MinIO Bronze
    s3_silver_path TEXT,                          -- Curated Iceberg Parquet path in MinIO Silver
    bbox GEOMETRY(Polygon, 4326) NOT NULL,        -- Satellite footprint polygon
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.3 Weather Observations (Historical & batch weather stations)
CREATE TABLE IF NOT EXISTS public.weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(100) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c FLOAT,
    humidity_pct FLOAT,
    rainfall_mm FLOAT,
    wind_speed_ms FLOAT,
    cloud_cover_pct FLOAT,
    location GEOMETRY(Point, 4326) NOT NULL,
    raw_s3_path TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_station_observed UNIQUE (station_id, observed_at)
);

-- 4.4 Admin Boundaries (Spatial polygon reference for Kerala districts & regions)
CREATE TABLE IF NOT EXISTS public.admin_boundaries (
    id SERIAL PRIMARY KEY,
    region_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    admin_level INT NOT NULL DEFAULT 4,           -- 2=Country, 4=State, 6=District
    country_code VARCHAR(10) DEFAULT 'IND',
    domain_owner VARCHAR(100) DEFAULT 'GIS Domain',
    geometry GEOMETRY(MultiPolygon, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.5 Real-Time IoT Sensor Observations (Streaming Flink target)
CREATE TABLE IF NOT EXISTS public.sensor_observations (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    measurement_type VARCHAR(50) NOT NULL,        -- e.g. 'air_quality', 'water_level', 'soil_moisture'
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(20),
    quality_flag VARCHAR(50) DEFAULT 'UNCHECKED',
    window_avg DOUBLE PRECISION,
    window_max DOUBLE PRECISION,
    window_min DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    nearest_weather_station VARCHAR(50),
    ambient_temperature_c DOUBLE PRECISION,
    ambient_humidity_pct DOUBLE PRECISION,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.6 Real-Time Live Weather Stream (Streaming Flink target)
CREATE TABLE IF NOT EXISTS public.weather_stream_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(100) NOT NULL,
    city_name VARCHAR(100),
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    temperature_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    cloud_cover_pct DOUBLE PRECISION,
    rainfall_1h_mm DOUBLE PRECISION,
    weather_condition VARCHAR(50),
    quality_flag VARCHAR(20) DEFAULT 'GOOD',
    window_avg_temp DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_weather_stream UNIQUE (station_id, event_time)
);

-- 4.7 Quality Runs (Airflow & GX Quality Audit Log)
CREATE TABLE IF NOT EXISTS public.quality_runs (
    id SERIAL PRIMARY KEY,
    dataset VARCHAR(100) NOT NULL,
    suite VARCHAR(100) NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    run_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pass_rate FLOAT NOT NULL,
    total_expectations INT NOT NULL,
    passed_expectations INT NOT NULL,
    failed_expectations INT NOT NULL,
    quarantine_count INT DEFAULT 0,
    details JSONB,
    CONSTRAINT uq_quality_check UNIQUE (dataset, suite, logical_date)
);

-- 4.8 Data Mesh Products Registry
CREATE TABLE IF NOT EXISTS public.data_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    domain VARCHAR(100) NOT NULL,
    owner_team VARCHAR(100),
    description TEXT,
    sla_freshness_hours INT,
    sla_quality_pass_rate FLOAT,
    output_port_rest TEXT,
    output_port_graphql TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4.9 Lineage Events Store (OpenLineage Audit Trail)
CREATE TABLE IF NOT EXISTS public.lineage_events (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(200) NOT NULL,
    job_name VARCHAR(200) NOT NULL,
    job_namespace VARCHAR(200) NOT NULL,
    event_type VARCHAR(50) NOT NULL,             -- START, COMPLETE, FAIL
    input_datasets JSONB,
    output_datasets JSONB,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- 5. SPATIAL & PERFORMANCE INDEXING
-- =============================================================================

-- Spatial GIST Indexes
CREATE INDEX IF NOT EXISTS idx_vector_features_geom ON public.vector_features USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_satellite_obs_bbox ON public.satellite_observations USING GIST (bbox);
CREATE INDEX IF NOT EXISTS idx_weather_obs_location ON public.weather_observations USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_admin_boundaries_geom ON public.admin_boundaries USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_geom ON public.sensor_observations USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_weather_stream_geom ON public.weather_stream_observations USING GIST (geom);

-- B-Tree Temporal & Lookup Indexes
CREATE INDEX IF NOT EXISTS idx_vector_features_type ON public.vector_features (feature_type);
CREATE INDEX IF NOT EXISTS idx_satellite_obs_time ON public.satellite_observations (acquisition_time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_obs_station_time ON public.weather_observations (station_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_time ON public.sensor_observations (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_anomaly ON public.sensor_observations (is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_weather_stream_time ON public.weather_stream_observations (event_time DESC);

-- =============================================================================
-- 6. SEED DATA (Kerala District Boundaries & Spatial Ground Truth)
-- =============================================================================

INSERT INTO public.admin_boundaries (region_id, name, admin_level, country_code, domain_owner, geometry)
VALUES
    (
        'KL_TVM',
        'Thiruvananthapuram',
        6,
        'IND',
        'District Administration',
        ST_Multi(ST_GeomFromText('POLYGON((76.75 8.25, 77.20 8.25, 77.20 8.75, 76.75 8.75, 76.75 8.25))', 4326))
    ),
    (
        'KL_EKM',
        'Ernakulam',
        6,
        'IND',
        'District Administration',
        ST_Multi(ST_GeomFromText('POLYGON((76.10 9.75, 76.70 9.75, 76.70 10.25, 76.10 10.25, 76.10 9.75))', 4326))
    ),
    (
        'KL_KKD',
        'Kozhikode',
        6,
        'IND',
        'District Administration',
        ST_Multi(ST_GeomFromText('POLYGON((75.60 11.10, 76.10 11.10, 76.10 11.65, 75.60 11.65, 75.60 11.10))', 4326))
    ),
    (
        'KL_TCR',
        'Thrissur',
        6,
        'IND',
        'District Administration',
        ST_Multi(ST_GeomFromText('POLYGON((75.95 10.20, 76.55 10.20, 76.55 10.75, 75.95 10.75, 75.95 10.20))', 4326))
    )
ON CONFLICT (region_id) DO UPDATE SET
    geometry = EXCLUDED.geometry,
    name = EXCLUDED.name;

-- Seed Data Mesh Product Registry
INSERT INTO public.data_products
    (product_id, name, domain, owner_team, description, sla_freshness_hours, sla_quality_pass_rate, output_port_rest, output_port_graphql)
VALUES
    ('osm_vector_product', 'Kerala Critical Vector Infrastructure', 'Geospatial Assets', 'Angel Team',
     'Standardized OpenStreetMap vector layers (hospitals, roads, emergency shelters)', 168, 98.0,
     '/api/v1/vectors/kerala', 'query { vectorFeatures { osmId name geometry } }'),
    ('climate_spatial_mart', 'Spatial Climate & Extreme Weather Mart', 'Environmental Intelligence', 'Angel Team',
     'Curated Gold mart intersecting satellite scenes, weather metrics, and buffered critical infrastructure', 24, 95.0,
     '/api/v1/marts/spatial-climate', 'query { spatialClimateMart { regionName heatIndex hospitalCount } }')
ON CONFLICT (product_id) DO NOTHING;
