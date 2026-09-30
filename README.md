# Cloud-Native Geospatial Data Platform

## Project Overview
This project is a modern, distributed microservices data platform designed for the ingestion, processing, storage, and serving of geospatial and environmental data. It leverages a cloud-native architecture to handle both high-velocity streaming data (IoT sensors, live weather) and batch data processing (satellite imagery metadata, OpenStreetMap vectors).

The platform enforces data governance, data mesh principles, and GDPR compliance while exposing the finalized "Gold" data products through both REST and GraphQL APIs.

## Architecture

The platform follows a Distributed Data Architecture (Modern Data Stack) pattern, completely decoupled across 16 containerized microservices. 

1. **Data Sources**: Sentinel-2 STAC API, OpenWeatherMap API, OSM Overpass API, Simulated IoT Sensors.
2. **Ingestion Layer (Batch & Stream)**: Airflow orchestrates ELT/ETL DAGs. Python producers publish live events to Redpanda topics using Avro schemas.
3. **Stream Processing**: Apache Flink consumes from Redpanda for real-time spatial joins, rolling window aggregations, and Z-score anomaly detection.
4. **Data Lake & Storage**: MinIO serves as the underlying object storage layer, version-controlled by LakeFS. PostgreSQL (with PostGIS) acts as the relational and spatial data warehouse.
5. **Data Transformation**: dbt (Data Build Tool) executes complex spatial joins and data mart generation over the loaded data.
6. **Serving Layer**: A FastAPI application serves the data through REST endpoints and a Strawberry GraphQL schema, incorporating automated PII/GDPR masking.
7. **Observability & Governance**: Prometheus and Grafana handle metrics and alerting. OpenLineage tracks data provenance and pushes events to DataHub.

## Technology Stack
- **Message Broker**: Redpanda, Schema Registry (Avro)
- **Stream Processing**: Apache Flink, PyFlink
- **Batch Orchestration**: Apache Airflow
- **Data Transformation**: dbt (Data Build Tool)
- **Database**: PostgreSQL, PostGIS, MongoDB (Landing area)
- **Object Storage**: MinIO, LakeFS
- **Serving**: FastAPI, GraphQL (Strawberry), Uvicorn
- **Observability**: Prometheus, Grafana, OpenLineage
- **Infrastructure**: Docker, Docker Compose

## Repository Structure

The project utilizes a monorepo structure where each root directory acts as a dedicated microservice or component:

- `/airflow` - DAGs for ETL/ELT pipelines and Data Quality checks.
- `/api` - The FastAPI and GraphQL application, including database connection pooling and routers.
- `/dbt` - SQL models for staging views, spatial joins, and Gold data mart materialization.
- `/flink` - Real-time stream processing jobs and algorithms.
- `/infra` - Database initialization scripts and environment configurations.
- `/lakefs` - Repository initialization and branching scripts for the data lake.
- `/monitoring` - Prometheus scrape configs, alert rules, and Grafana dashboards.
- `/redpanda` - Python producers, Avro schemas, and console configurations.

## Setup and Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.10+
- Git

### Bootstrapping the Platform

1. **Clone the repository**
   ```bash
   git clone https://github.com/aaron43210/DATA_ENGINEERING_PROJECT.git
   cd DATA_ENGINEERING_PROJECT/geoplatform
   ```

2. **Configure Environment Variables**
   Ensure the `.env` file is present in the `geoplatform/` directory and `OPENWEATHER_API_KEY` is populated.

3. **Start Core Infrastructure**
   ```bash
   bash setup.sh
   docker compose up -d postgres minio mongodb redpanda
   sleep 20
   docker compose up minio-setup
   docker compose up -d lakefs iceberg-rest redpanda-console
   bash lakefs/setup_repos.sh
   docker compose up airflow-init
   docker compose up -d airflow-webserver airflow-scheduler datahub-quickstart
   ```

4. **Start Monitoring**
   ```bash
   docker compose up -d prometheus grafana
   ```

5. **Start Ingestion and Processing**
   ```bash
   docker compose up -d sensor-producer weather-producer
   docker compose up -d flink-jobmanager flink-taskmanager
   ```

6. **Execute Batch Pipelines and Transformations**
   Trigger the DAGs from the Airflow UI, then run dbt:
   ```bash
   cd dbt/geoplatform
   dbt run --profiles-dir .
   ```

7. **Start the API Server**
   ```bash
   docker compose up -d api
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
