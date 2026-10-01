#!/bin/bash
set -e

# Creates the databases for airflow and the main geoplatform application
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set in the environment}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    
    -- Ensure geoplatform user has full access to geoplatform db (already created by env vars but making sure)
    GRANT ALL PRIVILEGES ON DATABASE geoplatform TO geoplatform;
EOSQL

echo "Databases initialized successfully."
