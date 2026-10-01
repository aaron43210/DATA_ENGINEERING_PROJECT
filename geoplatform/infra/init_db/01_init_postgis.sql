\c geoplatform

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Satellite observations (ETL load target)
CREATE TABLE IF NOT EXISTS satellite_observations (
    id SERIAL PRIMARY KEY,
    scene_id VARCHAR(100) UNIQUE NOT NULL,
    satellite VARCHAR(50),
    acquisition_time TIMESTAMPTZ NOT NULL,
    cloud_percentage FLOAT CHECK (cloud_percentage BETWEEN 0 AND 100),
    crs VARCHAR(50),
    s3_bronze_path TEXT,       -- raw JSON in MinIO bronze
    s3_silver_path TEXT,       -- Parquet in MinIO silver (Iceberg)
    lakefs_commit_id TEXT,     -- LakeFS commit for this version
    bbox GEOMETRY(POLYGON, 4326),
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Weather observations (ELT: raw loaded, transformed by dbt) - Historical batch table
CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(100) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c FLOAT,
    humidity_pct FLOAT,
    rainfall_mm FLOAT,
    wind_speed_ms FLOAT,
    cloud_cover_pct FLOAT,
    location GEOMETRY(POINT, 4326),
    raw_s3_path TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(station_id, observed_at)
);

-- Vector features (OSM roads, boundaries)
CREATE TABLE IF NOT EXISTS vector_features (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    feature_type VARCHAR(50),
    name TEXT,
    tags JSONB,
    geometry GEOMETRY(GEOMETRY, 4326),
    serialization_format VARCHAR(20) DEFAULT 'parquet',  -- tracks ORC/Parquet/Avro
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Admin boundaries
CREATE TABLE IF NOT EXISTS admin_boundaries (
    id SERIAL PRIMARY KEY,
    region_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200),
    admin_level INT,
    country_code VARCHAR(10),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    -- Data Mesh: domain ownership
    domain_owner VARCHAR(100) DEFAULT 'GIS Domain',
    data_product_id VARCHAR(100)
);

-- Sensor stream observations (Real-time IoT)
CREATE TABLE IF NOT EXISTS sensor_observations (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100),
    event_time TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    measurement_type VARCHAR(50),
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    quality_flag VARCHAR(50),
    window_avg DOUBLE PRECISION,
    window_max DOUBLE PRECISION,
    window_min DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    nearest_weather_station VARCHAR(20),
    ambient_temperature_c DOUBLE PRECISION,
    ambient_humidity_pct DOUBLE PRECISION,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Weather stream observations (Real-time Weather)
CREATE TABLE IF NOT EXISTS weather_stream_observations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(100) NOT NULL,
    city_name VARCHAR(100),
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    cloud_cover_pct DOUBLE PRECISION,
    rainfall_1h_mm DOUBLE PRECISION,
    weather_condition VARCHAR(50),
    api_poll_ms INT,
    quality_flag VARCHAR(20),
    window_avg_temp DOUBLE PRECISION,
    window_max_temp DOUBLE PRECISION,
    window_min_temp DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Data quality results
CREATE TABLE IF NOT EXISTS quality_runs (
    id SERIAL PRIMARY KEY,
    dataset VARCHAR(100),
    suite VARCHAR(100),
    run_time TIMESTAMPTZ DEFAULT NOW(),
    pass_rate FLOAT,
    total_expectations INT,
    passed_expectations INT,
    failed_expectations INT,
    quarantine_count INT DEFAULT 0,
    details JSONB
);

-- Data Mesh: Data Product registry (Data-as-a-Product)
CREATE TABLE IF NOT EXISTS data_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    domain VARCHAR(100) NOT NULL,
    owner_team VARCHAR(100),
    description TEXT,
    sla_freshness_hours INT,         -- SLA: max staleness allowed
    sla_quality_pass_rate FLOAT,     -- SLA: minimum quality pass rate
    output_port_rest TEXT,           -- REST endpoint
    output_port_graphql TEXT,        -- GraphQL field
    schema_version VARCHAR(20),
    status VARCHAR(20) DEFAULT 'active',
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- GDPR: tracks fields that contain PII and their masking rules
CREATE TABLE IF NOT EXISTS gdpr_field_registry (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    field_name VARCHAR(100),
    pii_category VARCHAR(50),        -- e.g. location, personal_id
    masking_strategy VARCHAR(50),    -- e.g. hash, null, truncate
    retention_days INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Lineage store (lightweight, DataHub is primary but this is queryable)
CREATE TABLE IF NOT EXISTS lineage_events (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(200),
    job_name VARCHAR(200),
    job_namespace VARCHAR(200),
    event_type VARCHAR(50),
    input_datasets JSONB,
    output_datasets JSONB,
    event_time TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_satellite_bbox ON satellite_observations USING GIST(bbox);
CREATE INDEX IF NOT EXISTS idx_satellite_time ON satellite_observations(acquisition_time);
CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_observations USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_observations(observed_at);
CREATE INDEX IF NOT EXISTS idx_vector_geom ON vector_features USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_sensor_time ON sensor_observations(event_time);
CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_observations(sensor_id);
CREATE INDEX IF NOT EXISTS idx_weather_stream_time ON weather_stream_observations(event_time);
CREATE INDEX IF NOT EXISTS idx_weather_stream_station ON weather_stream_observations(station_id);

-- Seed: data product registry (Data-as-a-Product)
INSERT INTO data_products
    (product_id, name, domain, owner_team, description, sla_freshness_hours, sla_quality_pass_rate, output_port_rest, output_port_graphql)
VALUES
    ('satellite_imagery', 'Satellite Imagery Observations', 'Remote Sensing', 'RS Team',
     'Sentinel-2 scene metadata with spatial bounding boxes and cloud cover', 24, 95.0,
     '/imagery', 'query { imagery { sceneId cloudPercentage } }'),
    ('weather_data', 'Hourly Weather Observations', 'Climate', 'Climate Team',
     'OpenWeatherMap hourly data for Kerala stations', 2, 98.0,
     '/weather', 'query { weather { stationId temperatureC } }'),
    ('iot_sensors', 'IoT Sensor Readings', 'IoT', 'Sensor Team',
     'Real-time sensor readings with anomaly detection', 0, 90.0,
     '/sensors', 'query { sensors { sensorId value isAnomaly } }')
ON CONFLICT DO NOTHING;

-- Seed: GDPR registry (location data is PII)
INSERT INTO gdpr_field_registry (table_name, field_name, pii_category, masking_strategy, retention_days)
VALUES
    ('sensor_observations', 'latitude', 'location', 'truncate_2dp', 365),
    ('sensor_observations', 'longitude', 'location', 'truncate_2dp', 365),
    ('weather_observations', 'location', 'location', 'null', 730),
    ('weather_stream_observations', 'latitude', 'location', 'truncate_2dp', 365),
    ('weather_stream_observations', 'longitude', 'location', 'truncate_2dp', 365)
ON CONFLICT DO NOTHING;
