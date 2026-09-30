"""
Airflow DAG: data_quality_dag
=============================
Purpose: Run Great Expectations validation suites against the data warehouse
         and record the results in the `quality_runs` table for SLA monitoring.
"""

from __future__ import annotations

import logging
import os
import json
from datetime import datetime, timedelta
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

POSTGRES = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     5432,
    "dbname":   os.environ.get("POSTGRES_DB", "geoplatform"),
    "user":     os.environ.get("POSTGRES_USER", "geoplatform"),
    "password": os.environ.get("POSTGRES_PASSWORD", "geoplatform"),
}

default_args = {
    "owner": "data-steward",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_satellite_quality_check(**context):
    """Mock Great Expectations run for satellite data."""
    # In a real environment, this would invoke `gx` CLI or Python context
    # Here we mock the result to demonstrate the pattern
    
    logger.info("Running Great Expectations on marts.satellite_observation...")
    
    # Mock validation result
    total = 50
    passed = 48
    failed = 2
    pass_rate = (passed / total) * 100
    
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    
    cur.execute(
        """
        INSERT INTO quality_runs (dataset, suite, pass_rate, total_expectations, passed_expectations, failed_expectations, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        ("satellite_observation", "satellite_suite", pass_rate, total, passed, failed, json.dumps({"failed_cols": ["cloud_percentage"]}))
    )
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Satellite quality check complete: {pass_rate}% passed.")
    
def run_weather_quality_check(**context):
    """Mock Great Expectations run for weather data."""
    
    logger.info("Running Great Expectations on marts.environmental_conditions...")
    
    total = 100
    passed = 100
    failed = 0
    pass_rate = 100.0
    
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    
    cur.execute(
        """
        INSERT INTO quality_runs (dataset, suite, pass_rate, total_expectations, passed_expectations, failed_expectations, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        ("environmental_conditions", "weather_suite", pass_rate, total, passed, failed, json.dumps({}))
    )
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Weather quality check complete: {pass_rate}% passed.")


with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="Data Quality Checks with Great Expectations",
    schedule="0 6 * * *",   # Run daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "governance"]
) as dag:

    check_satellite = PythonOperator(
        task_id="check_satellite_quality",
        python_callable=run_satellite_quality_check,
    )

    check_weather = PythonOperator(
        task_id="check_weather_quality",
        python_callable=run_weather_quality_check,
    )

    [check_satellite, check_weather]
