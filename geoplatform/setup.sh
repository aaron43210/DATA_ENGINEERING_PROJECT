#!/bin/bash
set -e

echo "🚀 Setting up Cloud-Native Geospatial Data Platform directories..."

mkdir -p infra/init_db
mkdir -p lakefs
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p openlineage/config
mkdir -p redpanda/producer
mkdir -p redpanda/weather_producer
mkdir -p flink/jobs
mkdir -p dbt/geoplatform
mkdir -p data_quality/expectations
mkdir -p api/app/routers
mkdir -p api/app/db
mkdir -p api/tests
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/grafana/provisioning/dashboards
mkdir -p .github/workflows

chmod +x infra/init_db/*.sh
chmod +x lakefs/*.sh

echo "✅ Directories created successfully. You can now run docker compose."
