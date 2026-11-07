# Cloud-Native Geospatial Data Platform: Architecture

The platform turns four Kerala data sources into Gold data products that users get over a REST API. Data moves through a Bronze → Silver → Gold medallion: raw objects in object storage, versioned Iceberg tables, then spatial marts in PostGIS.

## 1. Data sources

| Source | Type | Path | Frequency |
|---|---|---|---|
| Sentinel-2 (Element84 STAC) | Scene metadata + footprints | Batch, Airflow `sentinel2_ingest` | Daily 02:00 |
| OpenWeatherMap | Weather readings | Stream (`weather-producer`) and batch (`weather_historical_backfill`) | 60 s / daily |
| OSM Overpass | Hospitals, clinics, main roads, district boundaries | Batch, Airflow `osm_elt_dag`, `admin_boundaries_ingest` | Weekly / monthly |
| IoT sensors (MQTT) | Temperature, humidity, soil, AQI | Stream (`sensor-producer`) | Sub-second |

## 2. Data flow

```text
Sentinel-2 / OSM / weather API          IoT MQTT / weather API
            │                                     │
         Airflow                     producers → Redpanda (Avro, Schema Registry)
            │                                     │
            ▼                                     ▼
  BRONZE  MinIO s3://bronze/                    Flink `geoplatform-stream`
          source=<x>/ingest_date=<d>/hour=<h>/   validate · z-score anomalies ·
          raw JSONL, immutable, versioned        nearest-station join · dead letters
            │            │                        │
            │            ▼                        ▼
            │     PostGIS landing tables   ◄──  PostGIS streaming tables
            │     (vector_features,              (sensor_observations,
            │      satellite_observations,        weather_stream_observations)
            │      weather_observations,          │
            │      admin_boundaries)              │
            ▼                                     ▼
  SILVER  Iceberg on MinIO s3://warehouse/   ◄── stream_to_silver (every 5 min)
          bronze_to_silver (on new Bronze)
          versioned Parquet, time travel, one snapshot per load
            
  GOLD    dbt marts in PostGIS (schema `marts`), built from the PostGIS tables
          satellite_observation · environmental_conditions · fct_spatial_climate_mart
            │
            ▼
  SERVE   FastAPI REST (API key or JWT, GDPR coordinate masking)
          /datasets  /imagery  /weather  /sensors  /vector/features
```

## 3. What each layer is for

- **Bronze (MinIO).** The raw record of everything fetched, kept unchanged so any later layer can be rebuilt from it. The bucket is private and versioned, and objects expire after `BRONZE_RETENTION_DAYS` (GDPR retention).
- **Silver (Iceberg).** Typed, versioned Parquet tables: `silver.osm_features`, `silver.sentinel2_scenes`, `silver.weather_observations` (from Bronze) and `silver.sensor_records`, `silver.weather_stream_records` (from the streaming tables). Every load is a snapshot, which gives time travel and safe replay. This is the analytical copy: on AWS, Athena queries it in place.
- **Gold (PostGIS + dbt).** The serving store. PostGIS holds the spatially indexed tables the API reads, and dbt builds the marts with spatial joins (`ST_Contains`, `ST_Intersects`, `ST_DWithin`). dbt tests on each mart are the data quality gate: a failed test fails the `dbt_transform` run.
- **Serving (FastAPI).** `/datasets` is the data product catalog. The other endpoints return the products themselves.

Silver trails the live PostGIS tables by up to 5 minutes plus a 1 minute commit guard. Users who need live data read the API; analysts who need history and versions read Silver.

## 4. Orchestration

| DAG | Trigger | Writes |
|---|---|---|
| `admin_boundaries_ingest` | Monthly (and first boot) | Bronze, `admin_boundaries` |
| `osm_elt_dag` | Sundays 04:00 UTC | Bronze, `vector_features` (after a spatial quality gate) |
| `sentinel2_ingest` | Daily 02:00 | Bronze, `satellite_observations` |
| `weather_historical_backfill` | Daily 00:00 | Bronze, `weather_observations` |
| `bronze_to_silver` | New Bronze data | Iceberg Silver batch tables |
| `stream_to_silver` | Every 5 min | Iceberg Silver streaming tables |
| `dbt_transform` | New Bronze or PostGIS data | Gold marts, then their tests |

## 5. Streaming guarantees

- Kafka offsets and keyed state are checkpointed every 10 s in exactly-once mode; `flink-job-submit` restores the newest checkpoint after a restart.
- PostgreSQL writes are idempotent: `sensor_observations` is unique on `(source_topic, source_partition, source_offset)`, `weather_stream_observations` on `(station_id, event_time)`.
- `stream_to_silver` resumes from the highest PostGIS id already in Silver, so a crashed run neither skips nor duplicates rows.
- Dead-letter and alert topics are at-least-once; deduplicate on source coordinates or `alert_id`.

## 6. Moving to AWS

All clients read standard settings from the environment, so the move is a configuration change.

| Component | Local | AWS |
|---|---|---|
| Object storage | MinIO | S3: unset `AWS_ENDPOINT_URL_S3` and the keys; boto3 and PyIceberg use the IAM role |
| Iceberg catalog | `iceberg-rest` container | Glue Data Catalog or the same REST catalog on ECS; Athena queries Silver |
| PostgreSQL + PostGIS | `postgres` container | RDS for PostgreSQL with PostGIS; set `POSTGRES_SSLMODE=require` |
| Airflow | Compose, LocalExecutor | MWAA, or ECS with the same image |
| Redpanda + Schema Registry | Compose | Redpanda on ECS, or Redpanda/Confluent Cloud. Not MSK: it has no Confluent-compatible Schema Registry |
| Flink | Compose | Managed Service for Apache Flink, or ECS; checkpoints to `s3://` |
| FastAPI | Compose | ECS Fargate behind an ALB |
| Monitoring | none yet | CloudWatch |
| Secrets | `.env` | Secrets Manager |
