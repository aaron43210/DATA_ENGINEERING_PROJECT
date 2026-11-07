# AI Agent Onboarding Guide

Welcome, Agent. This repository contains a complex, cloud-native geospatial data platform based on a microservices architecture. 

## Repository Structure & Rules
This is a **monorepo**. Each top-level directory acts as an independent microservice.
- **Do not modify files outside the specific directory you are instructed to work on.**
- `geoplatform/`: legacy v1 stack; still holds the API, Aaron's DAGs and monitoring.
- `flink/`: PyFlink streaming jobs.
- `dags/`, `airflow/`: Airflow DAGs and the Airflow image (batch ingestion, data quality).
- `iceberg/`: Bronze to Silver loader (PyIceberg).
- `infra/minio/`: MinIO image built from source, plus bucket and lifecycle setup.
- `dbt/`: Data Build Tool models for PostGIS transformations.
- `redpanda/`: Python producers for streaming data.

## Working with Docker
- The v2 stack is defined in the root `docker-compose.yml`.
- There are 16 containers. 11 are infrastructure (pulled from Docker Hub), and 5 are custom builds (`api`, `flink`, `airflow`, and 2 `redpanda` producers).
- **CRITICAL**: If you add new pip dependencies to a custom service, you MUST update its `requirements.txt` and its `Dockerfile`.

## No Mock Data Policy
This platform is strictly production-ready. 
- Do NOT generate fake data using `random`.
- The sensor producer uses a physical MQTT bridge (`paho-mqtt`).
- The weather producer strictly enforces `OPENWEATHER_API_KEY`.
- The Data Quality DAG runs true Great Expectations database checkpoints. 
- If you write new code, integrate it with real endpoints and databases.

## Commands for Agents
- If you need to view the database schema, check `scripts/init_postgis.sql`.
- If you need to see environment variables, look at `.env.example`.
- To see how the services are linked, consult `docker-compose.yml`.

When modifying code, always prioritize robustness, logging, and error handling over quick fixes. Assume this code will run continuously in a cloud environment.
