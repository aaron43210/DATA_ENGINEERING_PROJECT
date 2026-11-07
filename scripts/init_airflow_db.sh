#!/bin/bash
# Creates Airflow's metadata role and database on first Postgres boot. The role
# owns its database, which PostgreSQL 15+ requires for creating tables in `public`.
set -euo pipefail
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v role="${AIRFLOW_DB_USER:-airflow}" \
     -v db="${AIRFLOW_DB_NAME:-airflow}" \
     -v password="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
CREATE ROLE :"role" WITH LOGIN PASSWORD :'password';
CREATE DATABASE :"db" OWNER :"role";
EOSQL
