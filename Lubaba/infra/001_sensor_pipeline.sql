-- Idempotent schema for fresh databases plus source-coordinate migration for old tables.
CREATE TABLE IF NOT EXISTS sensor_observations (
  id BIGSERIAL PRIMARY KEY, source_topic TEXT, source_partition INT, source_offset BIGINT,
  sensor_id VARCHAR(100) NOT NULL, event_time TIMESTAMPTZ NOT NULL, latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL, measurement_type VARCHAR(50) NOT NULL, value DOUBLE PRECISION NOT NULL,
  unit VARCHAR(20), quality_flag VARCHAR(50), window_avg DOUBLE PRECISION, window_max DOUBLE PRECISION,
  window_min DOUBLE PRECISION, is_anomaly BOOLEAN NOT NULL DEFAULT FALSE, nearest_weather_station VARCHAR(20),
  ambient_temperature_c DOUBLE PRECISION, ambient_humidity_pct DOUBLE PRECISION, processed_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE sensor_observations ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE sensor_observations ADD COLUMN IF NOT EXISTS source_partition INT;
ALTER TABLE sensor_observations ADD COLUMN IF NOT EXISTS source_offset BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_observations_source
  ON sensor_observations(source_topic, source_partition, source_offset);
CREATE INDEX IF NOT EXISTS idx_sensor_obs_time ON sensor_observations(event_time);

CREATE TABLE IF NOT EXISTS weather_stream_observations (
  id BIGSERIAL PRIMARY KEY, source_topic TEXT, source_partition INT, source_offset BIGINT,
  station_id VARCHAR(100) NOT NULL, city_name VARCHAR(100), event_time TIMESTAMPTZ NOT NULL,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, temperature_c DOUBLE PRECISION, humidity_pct DOUBLE PRECISION,
  wind_speed_ms DOUBLE PRECISION, cloud_cover_pct DOUBLE PRECISION, rainfall_1h_mm DOUBLE PRECISION,
  weather_condition VARCHAR(50), api_poll_ms INT, quality_flag VARCHAR(20), window_avg_temp DOUBLE PRECISION,
  window_max_temp DOUBLE PRECISION, window_min_temp DOUBLE PRECISION, is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
  processed_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE weather_stream_observations ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE weather_stream_observations ADD COLUMN IF NOT EXISTS source_partition INT;
ALTER TABLE weather_stream_observations ADD COLUMN IF NOT EXISTS source_offset BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_stream_observations_source
  ON weather_stream_observations(source_topic, source_partition, source_offset);
CREATE INDEX IF NOT EXISTS idx_weather_stream_time ON weather_stream_observations(event_time);
