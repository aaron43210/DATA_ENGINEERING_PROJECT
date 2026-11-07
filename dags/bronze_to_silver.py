"""
Airflow DAG: bronze_to_silver (Owner: Angel)
============================================
Copies new Bronze objects into the Iceberg Silver tables whenever a source lands.
Each load is a new Iceberg snapshot, which provides versioning and time travel.

The loader runs in its own virtualenv because PyIceberg needs a newer pyarrow
than Airflow's constraints allow. It is also runnable by hand:
    /opt/airflow/iceberg-venv/bin/python /opt/airflow/iceberg/bronze_to_silver.py
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.config import (
    BASE_DEFAULT_ARGS,
    BRONZE_OSM,
    BRONZE_SENTINEL2,
    BRONZE_WEATHER,
)

with DAG(
    dag_id="bronze_to_silver",
    default_args={**BASE_DEFAULT_ARGS, "owner": "angel"},
    description="Load new Bronze objects into Iceberg Silver tables",
    schedule=(BRONZE_OSM | BRONZE_SENTINEL2 | BRONZE_WEATHER),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "silver", "angel"],
) as dag:
    BashOperator(
        task_id="load_new_bronze_objects",
        bash_command=(
            "/opt/airflow/iceberg-venv/bin/python "
            "/opt/airflow/iceberg/bronze_to_silver.py"
        ),
    )
