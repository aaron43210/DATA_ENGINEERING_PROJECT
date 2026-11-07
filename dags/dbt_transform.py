"""
Airflow DAG: dbt_transform
==========================
Runs `dbt build` (models and tests) whenever fresh data is ready. A successful
build publishes GOLD_READY, which triggers data_quality_dag.

dbt lives in its own virtualenv in the Airflow image. Target and log paths are
redirected in docker-compose so nothing is written into the mounted project.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.config import (
    BASE_DEFAULT_ARGS,
    BRONZE_SENTINEL2,
    BRONZE_WEATHER,
    GOLD_READY,
    POSTGIS_BOUNDARIES,
    POSTGIS_OSM,
)

DBT_BIN = os.environ.get("DBT_BIN", "/opt/airflow/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")

with DAG(
    dag_id="dbt_transform",
    default_args=BASE_DEFAULT_ARGS,
    description="dbt build (staging -> intermediate -> Gold) when new data is ready",
    schedule=(BRONZE_SENTINEL2 | BRONZE_WEATHER | POSTGIS_OSM | POSTGIS_BOUNDARIES),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "transform", "gold"],
) as dag:
    BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} build "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
        outlets=[GOLD_READY],
    )
