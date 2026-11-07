"""
Airflow DAG: stream_to_silver
=============================
Copies new rows from the Flink-written PostGIS tables (sensor_observations,
weather_stream_observations) into Iceberg Silver every 5 minutes.

PostGIS is the live serving store; Iceberg Silver is the versioned copy and lags it
by up to 5 minutes plus a 1 minute commit guard. Runnable by hand:
    /opt/airflow/iceberg-venv/bin/python /opt/airflow/iceberg/stream_to_silver.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.config import BASE_DEFAULT_ARGS

with DAG(
    dag_id="stream_to_silver",
    default_args={
        **BASE_DEFAULT_ARGS,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Sync Flink streaming tables from PostGIS into Iceberg Silver",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "silver", "streaming"],
) as dag:
    BashOperator(
        task_id="sync_stream_tables",
        bash_command=(
            "/opt/airflow/iceberg-venv/bin/python "
            "/opt/airflow/iceberg/stream_to_silver.py"
        ),
    )
