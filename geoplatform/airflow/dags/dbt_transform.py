"""
Airflow DAG: dbt_transform
==========================
Drives the dbt transformation layer: Bronze (raw PostGIS tables) -> Silver
(staging) -> Gold (marts).

It runs automatically whenever ANY ingestion DAG lands fresh data — the
schedule is data-aware on the three Bronze datasets, so this is the "cook" that
answers the "doorbell" rung by sentinel2_ingest / osm_vector_ingest /
weather_historical_backfill. After a successful build it publishes GOLD_READY
so downstream DAGs (e.g. data-quality) can react to fresh Gold tables.

`dbt build` runs models AND tests. It executes from an isolated dbt virtualenv
baked into the Airflow image (/opt/airflow/dbt-venv) against the dbt project
mounted at /opt/airflow/dbt. The project's profiles.yml reads POSTGRES_* from
the environment, which docker-compose already injects.

NOTE: the `A | B | C` (run on ANY) dataset schedule requires Airflow >= 2.9
(this project is on 2.9.1).
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.config import (
    BASE_DEFAULT_ARGS,
    BRONZE_OSM,
    BRONZE_SENTINEL2,
    BRONZE_WEATHER,
    GOLD_READY,
)

DBT_BIN = os.environ.get("DBT_BIN", "/opt/airflow/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

with DAG(
    dag_id="dbt_transform",
    default_args=BASE_DEFAULT_ARGS,
    description="Run dbt build (Bronze -> Silver -> Gold) when new data lands",
    schedule=(BRONZE_SENTINEL2 | BRONZE_OSM | BRONZE_WEATHER),  # any source lands
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform", "silver", "gold"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--target {DBT_TARGET}"
        ),
        outlets=[GOLD_READY],  # tell downstream the Gold marts are refreshed
    )
