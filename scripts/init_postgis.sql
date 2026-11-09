-- =============================================================================
-- Cloud-Native Geospatial Data Platform: PostgreSQL + PostGIS Initialization
-- Script: scripts/init_postgis.sql
-- Managed by: Angel (Storage & Infrastructure Lead)
-- =============================================================================

-- The Airflow role and database are created by scripts/init_airflow_db.sh.

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

GRANT ALL ON SCHEMA public, raw, staging, marts, quality TO CURRENT_USER;

-- =============================================================================
-- 4. TABLE DEFINITIONS
-- =============================================================================

-- 4.1 OSM Vector Features (ELT ingestion target for Overpass API)
CREATE TABLE IF NOT EXISTS public.vector_features (
    id SERIAL PRIMARY KEY,
    osm_type VARCHAR(10) NOT NULL DEFAULT 'node', -- OSM ids are unique per type only
    osm_id BIGINT NOT NULL,
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
    admin_level INT NOT NULL DEFAULT 5,           -- OSM India: 4=State, 5=District
    country_code VARCHAR(10) DEFAULT 'IND',
    domain_owner VARCHAR(100) DEFAULT 'GIS Domain',
    data_product_id VARCHAR(100),
    osm_relation_id BIGINT,
    wikidata_id VARCHAR(20),
    geometry GEOMETRY(MultiPolygon, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.5 Real-Time IoT Sensor Observations (Streaming Flink target)
-- Written by flink/jobs/geoplatform_stream_job.py. source_* identify the Redpanda
-- record so checkpoint replays are idempotent (ON CONFLICT DO NOTHING).
CREATE TABLE IF NOT EXISTS public.sensor_observations (
    id BIGSERIAL PRIMARY KEY,
    source_topic TEXT,
    source_partition INT,
    source_offset BIGINT,
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
-- One row per distinct station observation; repeat polls dedupe on (station_id, event_time).
CREATE TABLE IF NOT EXISTS public.weather_stream_observations (
    id BIGSERIAL PRIMARY KEY,
    source_topic TEXT,
    source_partition INT,
    source_offset BIGINT,
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
    api_poll_ms INT,
    quality_flag VARCHAR(20) DEFAULT 'GOOD',
    window_avg_temp DOUBLE PRECISION,
    window_max_temp DOUBLE PRECISION,
    window_min_temp DOUBLE PRECISION,
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
    schema_version VARCHAR(20),
    status VARCHAR(20) DEFAULT 'active',
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4.8b GDPR: fields holding PII and how they are masked
CREATE TABLE IF NOT EXISTS public.gdpr_field_registry (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    field_name VARCHAR(100),
    pii_category VARCHAR(50),
    masking_strategy VARCHAR(50),
    retention_days INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_gdpr_field UNIQUE (table_name, field_name)
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

-- 4.10 Streaming schema upgrade for databases created before the Flink job was wired in.
-- Safe to re-run: psql -f scripts/init_postgis.sql against an existing volume.
ALTER TABLE public.sensor_observations ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE public.sensor_observations ADD COLUMN IF NOT EXISTS source_partition INT;
ALTER TABLE public.sensor_observations ADD COLUMN IF NOT EXISTS source_offset BIGINT;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS source_partition INT;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS source_offset BIGINT;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS api_poll_ms INT;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS window_max_temp DOUBLE PRECISION;
ALTER TABLE public.weather_stream_observations ADD COLUMN IF NOT EXISTS window_min_temp DOUBLE PRECISION;

-- 4.11 Batch schema upgrade for databases created before this version. Safe to re-run.
ALTER TABLE public.vector_features ADD COLUMN IF NOT EXISTS osm_type VARCHAR(10) NOT NULL DEFAULT 'node';
ALTER TABLE public.vector_features DROP CONSTRAINT IF EXISTS vector_features_osm_id_key;
ALTER TABLE public.admin_boundaries ADD COLUMN IF NOT EXISTS data_product_id VARCHAR(100);
ALTER TABLE public.admin_boundaries ADD COLUMN IF NOT EXISTS osm_relation_id BIGINT;
ALTER TABLE public.admin_boundaries ADD COLUMN IF NOT EXISTS wikidata_id VARCHAR(20);
ALTER TABLE public.data_products ADD COLUMN IF NOT EXISTS schema_version VARCHAR(20);
ALTER TABLE public.data_products ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'active';
ALTER TABLE public.data_products ADD COLUMN IF NOT EXISTS tags TEXT[];
ALTER TABLE public.data_products ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

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
CREATE UNIQUE INDEX IF NOT EXISTS uq_vector_features_osm ON public.vector_features (osm_type, osm_id);
CREATE INDEX IF NOT EXISTS idx_vector_features_type ON public.vector_features (feature_type);
CREATE INDEX IF NOT EXISTS idx_satellite_obs_time ON public.satellite_observations (acquisition_time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_obs_station_time ON public.weather_observations (station_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_time ON public.sensor_observations (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_anomaly ON public.sensor_observations (is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_weather_stream_time ON public.weather_stream_observations (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_sensor_time ON public.sensor_observations (sensor_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_stream_station_time ON public.weather_stream_observations (station_id, event_time DESC);

-- Idempotency keys for the Flink JDBC sinks. Legacy rows with NULL source_* are allowed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_observations_source
    ON public.sensor_observations (source_topic, source_partition, source_offset);

-- =============================================================================
-- 6. SEED DATA (Data Mesh registry and GDPR registry)
-- =============================================================================

-- admin_boundaries is filled with real OSM polygons by the admin_boundaries_ingest DAG.

-- Seed Data Mesh Product Registry
INSERT INTO public.data_products
    (product_id, name, domain, owner_team, description, sla_freshness_hours, sla_quality_pass_rate, output_port_rest, output_port_graphql)
VALUES
    ('satellite_imagery', 'Satellite Imagery Observations', 'Remote Sensing', 'RS Team',
     'Sentinel-2 scene metadata with spatial bounding boxes and cloud cover', 24, 95.0,
     '/imagery', 'query { imagery { sceneId cloudPercentage } }'),
    ('weather_data', 'Environmental Conditions', 'Climate', 'Climate Team',
     'Clean live weather enriched with Kerala districts', 2, 98.0,
     '/weather', 'query { weather { stationId temperatureC } }'),
    ('osm_vector_product', 'Kerala Critical Vector Infrastructure', 'Geospatial Assets', 'Angel Team',
     'Standardized OpenStreetMap vector layers (hospitals, roads, emergency shelters)', 168, 98.0,
     '/vector/features', NULL),
    ('climate_spatial_mart', 'Spatial Climate & Extreme Weather Mart', 'Environmental Intelligence', 'Angel Team',
     'Curated Gold mart intersecting satellite scenes, weather metrics, and buffered critical infrastructure', 24, 95.0,
     NULL, NULL),
    ('iot_sensor_stream', 'Real-Time IoT Sensor Observations', 'IoT', 'Lubaba Team',
     'Validated MQTT sensor readings (sensor.live) with Flink z-score anomaly flags and nearest-station ambient weather', 1, 90.0,
     '/sensors', 'query { sensors { sensorId value isAnomaly } }'),
    ('live_weather_stream', 'Live Weather Stream Observations', 'Climate', 'Lubaba Team',
     'OpenWeatherMap observations for Kerala stations (weather.live), deduplicated per station with temperature anomaly flags', 1, 95.0,
     '/weather', 'query { weather { stationId temperatureC } }')
ON CONFLICT (product_id) DO NOTHING;

-- Seed GDPR registry (location data is PII)
INSERT INTO public.gdpr_field_registry (table_name, field_name, pii_category, masking_strategy, retention_days)
VALUES
    ('sensor_observations', 'latitude', 'location', 'truncate_2dp', 365),
    ('sensor_observations', 'longitude', 'location', 'truncate_2dp', 365),
    ('weather_observations', 'location', 'location', 'null', 730),
    ('weather_stream_observations', 'latitude', 'location', 'truncate_2dp', 365),
    ('weather_stream_observations', 'longitude', 'location', 'truncate_2dp', 365)
ON CONFLICT (table_name, field_name) DO NOTHING;

-- ── 5. Auth: User accounts and API key management ─────────────────────────────

CREATE TABLE IF NOT EXISTS public.api_users (
    id          SERIAL PRIMARY KEY,
    google_sub  TEXT UNIQUE NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    name        TEXT,
    role        TEXT NOT NULL DEFAULT 'reader',
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.api_keys (
    id              SERIAL PRIMARY KEY,
    user_id         INT REFERENCES public.api_users(id) ON DELETE CASCADE,
    key_prefix      TEXT NOT NULL,
    key_hash        TEXT UNIQUE NOT NULL,
    name            TEXT,
    quota_day       INT NOT NULL DEFAULT 500,
    calls_today     INT NOT NULL DEFAULT 0,
    quota_reset_at  DATE NOT NULL DEFAULT CURRENT_DATE,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_used_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- scope: 'all' or one data product id; expires_at NULL = never expires
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'all';
ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON public.api_keys (key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON public.api_keys (user_id);
