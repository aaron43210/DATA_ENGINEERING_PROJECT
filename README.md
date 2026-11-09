# Cloud-Native Geospatial Data Platform

## Project Overview
The platform ingests geospatial and environmental data for Kerala (Sentinel-2 scenes, OpenStreetMap features, weather and IoT sensor readings), refines it through a Bronze → Silver → Gold medallion, and serves the Gold data products to users over a REST API. Streaming data (IoT sensors, live weather) and batch data (satellite metadata, OSM vectors, weather history) share the same storage layers.

See `architecture.md` for the data flow and the AWS mapping, and `connections.md` for hosts, ports and topic contracts.

## Architecture

1. **Sources**: Sentinel-2 STAC API, OpenWeatherMap API, OSM Overpass API, IoT sensors over MQTT.
2. **Ingestion**: Airflow DAGs land batch data; Python producers publish live events to Redpanda as Avro.
3. **Bronze**: MinIO holds every raw object as JSONL, immutable and partitioned by source, date and hour.
4. **Stream processing**: Flink validates events, flags z-score anomalies, joins each sensor to its nearest weather station and writes PostGIS.
5. **Silver**: Apache Iceberg tables on MinIO, loaded from Bronze and from the streaming tables. Versioned, with time travel.
6. **Gold**: dbt builds spatial marts in PostGIS; dbt tests are the data quality gate.
7. **Serving**: FastAPI REST with API key or JWT auth and GDPR coordinate masking.

## Technology Stack
- **Message broker**: Redpanda, Schema Registry (Avro)
- **Stream processing**: Apache Flink (PyFlink)
- **Orchestration**: Apache Airflow
- **Object storage and lakehouse**: MinIO (S3 API), Apache Iceberg (REST catalog, PyIceberg)
- **Warehouse**: PostgreSQL 16 + PostGIS 3.4
- **Transformation and data tests**: dbt
- **Serving**: FastAPI, Uvicorn
- **Infrastructure**: Docker Compose locally; AWS later (see `architecture.md`)

## Repository Structure

- `airflow/` - Airflow image (dbt and PyIceberg in their own virtualenvs)
- `dags/` - Airflow DAGs for ingestion, Silver loads and dbt
- `iceberg/` - Bronze → Silver and streaming → Silver loaders (PyIceberg)
- `dbt/` - staging, intermediate and Gold mart models with their tests
- `flink/` - PyFlink streaming job and its Kafka connector
- `redpanda/` - sensor and weather producers, Avro schemas, console config
- `api/` - FastAPI serving layer
- `infra/minio/` - MinIO image and bucket/lifecycle setup
- `scripts/` - PostgreSQL schema and Airflow database init
- `tests/` - offline tests for producers, the Flink logic and the Silver sync

## Quick Start

```bash
cp .env.example .env        # set every secret; on Linux set AIRFLOW_UID=$(id -u)
docker compose up -d --build # first build compiles MinIO from source (several minutes)
docker compose --profile weather up -d weather-producer   # needs OPENWEATHER_API_KEY
docker compose --profile sensor  up -d sensor-producer    # needs MQTT_BROKER + credentials
```

| UI / endpoint | URL |
|---|---|
| API docs (Swagger) | http://localhost:8000/docs |
| Airflow | http://localhost:8080 |
| Flink | http://localhost:8083 |
| Redpanda Console | http://localhost:8088 |
| MinIO console | http://localhost:9001 |

DAGs other than `admin_boundaries_ingest` start paused: unpause them in the Airflow UI.

| DAG | Trigger | What it does |
|---|---|---|
| `admin_boundaries_ingest` | First boot, then monthly | Real polygons for Kerala's 14 districts from OSM |
| `osm_elt_dag` | Sundays 04:00 UTC | Hospitals, clinics and main roads: Bronze, PostGIS, spatial quality gate |
| `sentinel2_ingest` | Daily 02:00 | Sentinel-2 scenes over Kerala: Bronze, PostGIS |
| `weather_historical_backfill` | Daily 00:00 | Weather snapshot per station: Bronze, PostGIS (needs `OPENWEATHER_API_KEY`) |
| `bronze_to_silver` | New Bronze data | Loads Bronze into Iceberg Silver tables |
| `stream_to_silver` | Every 5 minutes | Copies new Flink rows from PostGIS into Iceberg Silver |
| `dbt_transform` | New Bronze or PostGIS data | `dbt build`: staging, intermediate, Gold marts and their tests |

Query the data products:

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8000/datasets/   # the catalog
curl -H "X-API-Key: $API_KEY" http://localhost:8000/imagery/
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/sensors/?anomalies_only=true"
```

Check the streaming job itself, not just its containers:

```bash
curl -fsS http://localhost:8083/jobs/overview        # geoplatform-stream must be RUNNING
docker compose logs flink-job-submit
```

MinIO is built from its official source (`infra/minio/`) because the free images were removed from Docker Hub and quay.io. The upstream repository is archived and receives no security fixes, so keep MinIO local; on AWS it is replaced by S3.

Existing database volume? Re-run the idempotent schema:
`docker compose exec -T postgres psql -U geoplatform -d geoplatform < scripts/init_postgis.sql`

## Pipeline on AWS (one EC2 host)

The API runs on ECS. Airflow + dbt, Redpanda, Flink, the Iceberg REST catalog, Prometheus and Grafana run on one EC2 host (`infra/terraform/pipeline.tf`) with `docker-compose.aws.yml`, which swaps the postgres container for RDS and MinIO for S3. The host's IAM role grants S3 access, so no keys are stored.

1. Add `pipeline_ssh_cidr = "<your IP>/32"` and `pipeline_public_key = "<contents of ~/.ssh/id_ed25519.pub>"` to `prod.tfvars`, then `terraform apply -var-file=prod.tfvars` in `infra/terraform`.
2. `ssh ubuntu@$(terraform output -raw pipeline_public_ip)`. First boot installs Docker and clones the repo to `~/DE_PROJECT` (wait for `cloud-init status --wait`).
3. `cd ~/DE_PROJECT && cp .env.aws.example .env && chmod 600 .env`, then fill in every `<...>`.
4. Start it:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.aws.yml --profile weather --profile monitoring up -d --build
   ```
5. Open the UIs through a tunnel from your machine:
   ```bash
   ssh -L 8080:localhost:8080 -L 8083:localhost:8083 -L 8088:localhost:8088 -L 3001:localhost:3001 ubuntu@<pipeline_public_ip>
   ```
   Airflow http://localhost:8080, Flink http://localhost:8083, Redpanda Console http://localhost:8088, Grafana http://localhost:3001.
6. In Airflow, unpause the DAGs. Run `admin_boundaries_ingest` first; dbt's spatial joins need it.

The host is off 01:00–07:00 India time (`pipeline_stop_cron` / `pipeline_start_cron`); its containers come back on their own when it starts, and DAG runs missed overnight run once then.

Update: `git pull`, then the same `up -d --build` command.

## Tests

```bash
pip install -r requirements-test.txt && python -m pytest -q tests/   # producers, Flink logic, Silver sync
cd api && pip install -r requirements.txt pytest httpx && PYTHONPATH=. pytest -q tests/
```

## Team Members and Contributions

This project was built collaboratively. Each member owned specific architectural components and microservices to ensure clear interface contracts and separation of concerns.

- **Aaron** 
  - Role: FastAPI Core, dbt Models, Redpanda Producers, Airflow ETL
  - GitHub: [Insert GitHub Profile Link Here]

- **Lubaba** 
  - Role: FastAPI (GraphQL), Flink Stream Algorithms, CI/CD, Testing
  - GitHub: [Insert GitHub Profile Link Here]

- **Angel** 
  - Role: Infrastructure, dbt Spatial Models, Flink Plumbing
  - GitHub: [Insert GitHub Profile Link Here]

- **Fidal** 
  - Role: Redpanda Administration, Infrastructure, Monitoring Rules
  - GitHub: [Insert GitHub Profile Link Here]

- **Adhityan** 
  - Role: Airflow (OSM), Grafana Dashboards, Pytest Suites
  - GitHub: [Insert GitHub Profile Link Here]
