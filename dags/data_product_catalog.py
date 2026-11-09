"""
Airflow DAG: data_product_catalog
=================================
Marks which data products the catalog (/datasets) lists: the 4 with a live
source are active, the rest inactive. Idempotent, so a fresh database ends up
right after one run.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import BASE_DEFAULT_ARGS, pg_connection

logger = logging.getLogger(__name__)

ACTIVE_PRODUCTS = [
    "satellite_imagery",
    "osm_vector_product",
    "live_weather_stream",
    "live_air_quality",
]


def sync_catalog():
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE data_products
               SET status = CASE WHEN product_id = ANY(%s) THEN 'active' ELSE 'inactive' END,
                   updated_at = NOW()
            """,
            (ACTIVE_PRODUCTS,),
        )
        logger.info("Catalog synced: %d products", cur.rowcount)


with DAG(
    dag_id="data_product_catalog",
    default_args=BASE_DEFAULT_ARGS,
    description="Set which data products the catalog lists",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["catalog"],
) as dag:
    PythonOperator(task_id="sync_catalog", python_callable=sync_catalog)
