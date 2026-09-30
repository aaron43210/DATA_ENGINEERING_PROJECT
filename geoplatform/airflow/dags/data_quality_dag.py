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

def execute_gx_checkpoint(checkpoint_name: str, dataset_name: str, suite_name: str):
    """Executes a Great Expectations checkpoint and logs results to PostGIS."""
    try:
        import great_expectations as gx
    except ImportError:
        logger.error("PRODUCTION ERROR: great_expectations library not installed.")
        raise
        
    context_root_dir = "/opt/airflow/dags/data_quality/gx"
    
    if not os.path.exists(context_root_dir) or not os.path.exists(os.path.join(context_root_dir, "great_expectations.yml")):
        raise FileNotFoundError(f"PRODUCTION ERROR: Great Expectations context not initialized at {context_root_dir}. Please run 'gx init'.")
        
    # Load the Data Context
    gx_context = gx.get_context(context_root_dir=context_root_dir)
    
    # Run the Checkpoint
    logger.info(f"Executing Checkpoint: {checkpoint_name}")
    result = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)
    
    # Extract metrics
    try:
        stats = result.list_validation_results()[0]["statistics"]
        total = stats["evaluated_expectations"]
        passed = stats["successful_expectations"]
        pass_rate = stats["success_percent"]
    except Exception as e:
        logger.error(f"Failed to parse GX results: {e}")
        raise
    
    # Insert into quality_runs table
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO quality_runs (dataset, suite, pass_rate, total_expectations, passed_expectations, failed_expectations) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (dataset_name, suite_name, pass_rate, total, passed, total-passed)
    )
    conn.commit()
    cur.close()
    conn.close()
    
    if pass_rate < 100.0:
        logger.warning(f"🚨 Data Quality Check Failed! Pass rate: {pass_rate}%")
    else:
        logger.info(f"✅ Data Quality Check Passed! Pass rate: {pass_rate}%")


def run_satellite_quality_check(**context):
    execute_gx_checkpoint("satellite_checkpoint", "satellite_observation", "satellite_suite")

def run_weather_quality_check(**context):
    execute_gx_checkpoint("weather_checkpoint", "environmental_conditions", "weather_suite")


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
