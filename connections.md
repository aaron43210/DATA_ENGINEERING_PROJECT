# Service Connections & Interfaces

## Internal Docker Networking
All services run on the `geoplatform_net` Docker network. Services communicate using their container names as hostnames.

## 1. PostgreSQL (with PostGIS)
- **Hostname**: `postgres`
- **Port**: `5432` (Mapped to `5432` on host)
- **Databases**: 
  - `geoplatform` (Main Data Warehouse)
  - `airflow` (Airflow Metadata)
- **Credentials**: `geoplatform` / `geoplatform`

## 2. MinIO (S3 Compatible)
- **Endpoint**: `http://minio:9000`
- **Port**: `9000` (API), `9001` (Console)
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
- **Bucket**: `geoplatform-bronze`

## 3. Redpanda (Kafka Compatible)
- **Broker Hostname**: `redpanda`
- **Port**: `9092`
- **Schema Registry**: `http://redpanda:8081`
- **Topics**:
  - `sensor.live`: Handled by `sensor_producer.py`
  - `weather.live`: Handled by `weather_producer.py`

## 4. Apache Flink
- **JobManager**: `flink-jobmanager:8081`
- **Dependencies**: Requires `apache-flink`, `psycopg2-binary`, and `confluent-kafka[avro]` (Avro deserialization via the Redpanda Schema Registry).

## 5. FastAPI
- **Internal Host**: `api:8000`
- **External Port**: `8000`
- **Database Connection**: Uses `asyncpg` to connect to `postgresql://geoplatform:geoplatform@postgres:5432/geoplatform`.

## 6. Observability
- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (admin/admin)
- **Redpanda Console**: `http://localhost:8080`
