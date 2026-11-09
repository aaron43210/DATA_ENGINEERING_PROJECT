#!/bin/bash
# Creates Airflow's metadata role and database. The role owns its database, which
# PostgreSQL 15+ requires for creating tables in `public`. Safe to re-run: it is the
# local postgres init script and, on EC2, the airflow-db-init one-shot against RDS.
set -euo pipefail
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v role="${AIRFLOW_DB_USER:-airflow}" \
     -v db="${AIRFLOW_DB_NAME:-airflow}" \
     -v password="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
-- RDS's master user is not a superuser: it must be a member to create a database owned by the role.
GRANT :"role" TO CURRENT_USER;
SELECT format('CREATE DATABASE %I OWNER %I', :'db', :'role')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db') \gexec
EOSQL
