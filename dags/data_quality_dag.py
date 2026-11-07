"""
Airflow DAG: data_quality_dag
=============================
Run Great Expectations validation suites against the warehouse, record results
in `quality_runs` for SLA monitoring, AND fail the task (red) when the pass
rate falls below the configured threshold so the gate actually gates and
alerting fires.

Threshold: env QUALITY_MIN_PASS_RATE (percent, default 95).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator

from common.config import GOLD_READY, alert_on_failure, pg_connection

logger = logging.getLogger(__name__)

GX_CONTEXT_ROOT = os.environ.get("GX_CONTEXT_ROOT", "/opt/airflow/data_quality/gx")

default_args = {
    "owner": "data-steward",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


def execute_gx_checkpoint(
    checkpoint_name: str, dataset_name: str, suite_name: str, logical_date
):
    """Run a GX checkpoint, record metrics to quality_runs, then enforce the
    pass-rate threshold by raising on failure."""
    try:
        import great_expectations as gx
    except ImportError:
        logger.error("PRODUCTION ERROR: great_expectations library not installed.")
        raise

    if not os.path.exists(os.path.join(GX_CONTEXT_ROOT, "great_expectations.yml")):
        raise FileNotFoundError(
            f"PRODUCTION ERROR: Great Expectations context not initialized at "
            f"{GX_CONTEXT_ROOT}. Please run 'gx init'."
        )

    gx_context = gx.get_context(context_root_dir=GX_CONTEXT_ROOT)
    logger.info("Executing Checkpoint: %s", checkpoint_name)
    result = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)

    try:
        stats = result.list_validation_results()[0]["statistics"]
        total = stats["evaluated_expectations"]
        passed = stats["successful_expectations"]
        pass_rate = stats["success_percent"]
    except Exception as e:
        logger.error("Failed to parse GX results: %s", e)
        raise

    # Record the run first so history is captured even when the gate fails.
    # Keyed by logical_date so a re-run/backfill updates the row instead of
    # inserting a duplicate (idempotent).
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quality_runs
                (dataset, suite, logical_date, pass_rate, total_expectations,
                 passed_expectations, failed_expectations)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataset, suite, logical_date) DO UPDATE SET
                pass_rate = EXCLUDED.pass_rate,
                total_expectations = EXCLUDED.total_expectations,
                passed_expectations = EXCLUDED.passed_expectations,
                failed_expectations = EXCLUDED.failed_expectations,
                run_time = NOW()
            """,
            (
                dataset_name,
                suite_name,
                logical_date,
                pass_rate,
                total,
                passed,
                total - passed,
            ),
        )

    threshold = float(os.environ.get("QUALITY_MIN_PASS_RATE", "95"))
    if pass_rate < threshold:
        # Fail the task so it turns red and on_failure_callback alerts.
        raise AirflowException(
            f"🚨 Data quality gate FAILED for {dataset_name}/{suite_name}: "
            f"pass rate {pass_rate}% < threshold {threshold}%"
        )
    logger.info("✅ Quality gate passed for %s: %s%%", dataset_name, pass_rate)


def run_satellite_quality_check(**context):
    # marts.satellite_observation is Aaron's model; skip until it is in this stack.
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('marts.satellite_observation')")
        if cur.fetchone()[0] is None:
            raise AirflowSkipException("marts.satellite_observation does not exist yet")
    execute_gx_checkpoint(
        "satellite_checkpoint",
        "satellite_observation",
        "satellite_suite",
        context["data_interval_start"],
    )


def run_weather_quality_check(**context):
    execute_gx_checkpoint(
        "weather_checkpoint",
        "environmental_conditions",
        "weather_suite",
        context["data_interval_start"],
    )


with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="Data Quality Checks with Great Expectations",
    schedule=[GOLD_READY],  # run right after dbt_transform rebuilds the Gold marts
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "governance"],
) as dag:

    PythonOperator(
        task_id="check_satellite_quality",
        python_callable=run_satellite_quality_check,
    )
    PythonOperator(
        task_id="check_weather_quality",
        python_callable=run_weather_quality_check,
    )
