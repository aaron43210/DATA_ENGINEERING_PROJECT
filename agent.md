# AI Agent Onboarding Guide

Welcome, Agent. This repository contains a complex, cloud-native geospatial data platform based on a microservices architecture. 

## Repository Structure & Rules
This is a **monorepo**. Each top-level directory acts as an independent microservice.
- **Do not modify files outside the specific directory you are instructed to work on.**
- `flink/`: PyFlink streaming jobs.
- `dags/`, `airflow/`: Airflow DAGs and the Airflow image (batch ingestion, Silver loads, dbt).
- `iceberg/`: Bronze → Silver and streaming → Silver loaders (PyIceberg).
- `api/`: FastAPI REST serving the Gold data products.
- `infra/minio/`: MinIO image built from source, plus bucket and lifecycle setup.
- `dbt/`: dbt models for PostGIS transformations; its tests are the data quality gate.
- `redpanda/`: Python producers for streaming data.

## Working with Docker
- The v2 stack is defined in the root `docker-compose.yml`.
- Custom builds: `api`, `airflow`, `flink`, `minio` and the 2 `redpanda` producers. Everything else is pulled.
- Object storage clients read the standard AWS SDK variables (`AWS_ENDPOINT_URL_S3` points them at MinIO locally). Never hard-code a MinIO endpoint.
- **CRITICAL**: If you add new pip dependencies to a custom service, you MUST update its `requirements.txt` and its `Dockerfile`.

## No Mock Data Policy
This platform is strictly production-ready. 
- Do NOT generate fake data using `random`.
- The sensor producer uses a physical MQTT bridge (`paho-mqtt`).
- The weather producer strictly enforces `OPENWEATHER_API_KEY`.
- Data quality checks are dbt tests on the Gold marts and SQL spatial gates in `dags/data_quality_checks.py`.
- If you write new code, integrate it with real endpoints and databases.

## Commands for Agents
- If you need to view the database schema, check `scripts/init_postgis.sql`.
- If you need to see environment variables, look at `.env.example`.
- To see how the services are linked, consult `docker-compose.yml`.

When modifying code, always prioritize robustness, logging, and error handling over quick fixes. Assume this code will run continuously in a cloud environment.
