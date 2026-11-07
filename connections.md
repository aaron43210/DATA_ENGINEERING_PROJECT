# Service Connections & Interfaces

Applies to the root stack (`docker-compose.yml`). All services share the `geoplatform-net`
bridge network and reach each other by **service name**. Credentials come from `.env`
(copy `.env.example`); none are hard-coded in this document.

## Host ports

| Service | Inside the network | Host | Purpose |
|---|---|---|---|
| PostgreSQL + PostGIS | `postgres:5432` | `5434` | Warehouse (`geoplatform` DB) + Airflow metadata (`airflow` DB) |
| MinIO | `minio:9000` / `9001` | `9000` / `9001` | S3 API / web console |
| Iceberg REST | `iceberg-rest:8181` | `8181` | Lakehouse catalog (warehouse `s3://warehouse/`) |
| Redpanda Kafka | `redpanda:9092` | `19092` | Broker (external listener advertises `localhost:19092`) |
| Schema Registry | `redpanda:8081` | `8081` | Avro schemas for `*.live` topics |
| Pandaproxy | `redpanda:8082` | `8082` | HTTP REST for Kafka |
| Redpanda Console | `redpanda-console:8080` | `8088` | Topics, consumer lag, schemas |
| Airflow | `airflow-webserver:8080` | `8080` | DAG UI |
| Flink JobManager | `flink-jobmanager:8081` | `8083` | Flink UI / REST |

## 1. PostgreSQL (PostGIS)
- Host `postgres`, port `5432`, DB `${POSTGRES_DB}` (default `geoplatform`), user `${POSTGRES_USER}`.
- Schema: `scripts/init_postgis.sql` (runs on a fresh volume; safe to re-run for upgrades).
- Streaming tables written by Flink: `public.sensor_observations`, `public.weather_stream_observations`.

## 2. MinIO
- Endpoint `http://minio:9000`, keys `${MINIO_ROOT_USER}` / `${MINIO_ROOT_PASSWORD}`.
- Buckets: `bronze`, `silver`, `gold`, `warehouse`.

- Built from source by `infra/minio/Dockerfile`; `minio-setup` applies `infra/minio/setup.sh` (private buckets, Bronze versioning and lifecycle).
- Iceberg REST catalog keeps its metadata in the `iceberg_catalog` volume; tables live in `s3://warehouse/`.

## 3. Redpanda
- Broker `redpanda:9092`, Schema Registry `http://redpanda:8081`.
- Topics (created by `redpanda-topics`, 3 partitions, 7-day retention by default):

| Topic | Format | Producer | Consumer |
|---|---|---|---|
| `sensor.live` | Confluent Avro (`redpanda/producer/sensor_event.avsc`), key `{sensor_id}_{measurement_type}` | `sensor-producer` (MQTT bridge) | Flink |
| `weather.live` | Confluent Avro (`redpanda/weather_producer/weather_event.avsc`), key `station_id` | `weather-producer` (OpenWeatherMap) | Flink |
| `sensor.dead-letter` | JSON, two shapes: from Flink `{source_topic, source_partition, source_offset, source_key_base64, payload_base64, reason}`; from the producer `{source: "mqtt", mqtt_topic, mqtt_mid, payload_utf8, reason}` | sensor-producer (bad MQTT input), Flink (invalid/late) | operators |
| `weather.dead-letter` | JSON, same shape | Flink (malformed weather) | operators |
| `platform.quality.alerts` | JSON: `alert_id`, `dataset`, `source_id`, `alert_type`, `value`, `event_time`, `severity` | Flink | alerting / monitoring |

Avro subjects use the default topic-name strategy: `sensor.live-value`, `weather.live-value`.

## 4. Apache Flink
- JobManager `flink-jobmanager:8081`; one TaskManager; checkpoints in volume `geoplatform_flink_checkpoints`.
- Job `geoplatform-stream` (`flink/jobs/geoplatform_stream_job.py`) is submitted and supervised by
  `flink-job-submit`, which restores from the newest retained checkpoint.
- Consumer groups: `${KAFKA_GROUP_PREFIX}-sensor`, `${KAFKA_GROUP_PREFIX}-weather` (default prefix `geoplatform-flink`).
- PostgreSQL via JDBC (`jdbc:postgresql://postgres:5432/${POSTGRES_DB}`), Kafka via the bundled
  `flink-connector-kafka` 3.2.0-1.18.

## 5. Producers (opt-in profiles)
- `weather-producer` (`--profile weather`): needs `OPENWEATHER_API_KEY`; polls 5 Kerala stations every `POLL_INTERVAL_SECONDS`.
- `sensor-producer` (`--profile sensor`): needs `MQTT_BROKER` and broker credentials/TLS files under `MQTT_CERTS_DIR`.
