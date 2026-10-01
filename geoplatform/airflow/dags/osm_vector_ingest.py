"""
Airflow DAG: osm_vector_ingest
==============================
Pattern: ELT (Extract -> Load -> Transform)
Purpose: Ingest OpenStreetMap vector data (e.g. roads, hospitals) for spatial context.

- EXTRACT: Query OSM Overpass API for features in Kerala.
- LOAD (Raw): Save raw GeoJSON to MinIO Bronze.
- LOAD (DB): Parse features and load directly into PostGIS `vector_features` table.

Integrations: OpenLineage.
"""

from __future__ import annotations

import json
import logging
import os
import requests
from datetime import datetime, timedelta

import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
OVERPASS_URL = "http://overpass-api.de/api/interpreter"
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
BUCKET = "geoplatform-bronze"

POSTGRES = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
    "dbname": os.environ.get("POSTGRES_DB", "geoplatform"),
    "user": os.environ.get("POSTGRES_USER", "geoplatform"),
    "password": os.environ.get("POSTGRES_PASSWORD", "geoplatform"),
}

default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}

# ── Tasks ──────────────────────────────────────────────────────────────────────


def fetch_osm_data(**context):
    """EXTRACT: Fetch hospitals and major roads in Kerala using Overpass QL."""
    # Overpass QL query:
    # 1. Bounding box around Kerala (approx)
    # 2. Get hospitals and trunk roads
    query = """
    [out:json][timeout:25];
    (
      node["amenity"="hospital"](8.1,74.8,12.8,77.5);
      way["highway"="trunk"](8.1,74.8,12.8,77.5);
    );
    out body;
    >;
    out skel qt;
    """

    logger.info("Querying Overpass API...")
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    elements = data.get("elements", [])
    logger.info(f"Extracted {len(elements)} OSM elements")

    context["ti"].xcom_push(key="osm_elements", value=elements)


def load_raw_to_bronze(**context):
    """LOAD (Raw): Write the raw OSM JSON to MinIO."""
    elements = context["ti"].xcom_pull(key="osm_elements", task_ids="fetch_osm_data")
    execution_date = context["execution_date"]
    date_prefix = execution_date.strftime("%Y/%m/%d")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    key = f"osm/{date_prefix}/kerala_extract.json"

    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({"elements": elements}),
        ContentType="application/json",
    )

    logger.info(f"Uploaded raw OSM data to s3://{BUCKET}/{key}")
    context["ti"].xcom_push(key="s3_path", value=f"s3://{BUCKET}/{key}")


def load_to_postgis(**context):
    """LOAD (DB): Load elements into the vector_features table."""
    elements = context["ti"].xcom_pull(key="osm_elements", task_ids="fetch_osm_data")

    if not elements:
        logger.info("No elements to load.")
        return

    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()

    inserted = 0

    for el in elements:
        try:
            feature_type = (
                el.get("tags", {}).get("amenity")
                or el.get("tags", {}).get("highway")
                or "unknown"
            )
            name = el.get("tags", {}).get("name", "Unnamed")

            # Simple point conversion for hospitals
            if el["type"] == "node" and el.get("tags", {}).get("amenity") == "hospital":
                cur.execute(
                    """
                    INSERT INTO vector_features (osm_id, feature_type, name, tags, geometry)
                    VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    """,
                    (
                        el["id"],
                        feature_type,
                        name,
                        json.dumps(el.get("tags", {})),
                        el["lon"],
                        el["lat"],
                    ),
                )
                inserted += 1

        except Exception as e:
            logger.warning(f"Failed to insert OSM element {el.get('id')}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {inserted} vector features into PostGIS")


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="osm_vector_ingest",
    default_args=default_args,
    description="ELT DAG for OpenStreetMap vector data",
    schedule="0 4 * * 0",  # Run weekly on Sunday at 4 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "vector", "osm"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_osm_data",
        python_callable=fetch_osm_data,
    )

    task_load_s3 = PythonOperator(
        task_id="load_raw_to_bronze",
        python_callable=load_raw_to_bronze,
    )

    task_load_db = PythonOperator(
        task_id="load_to_postgis",
        python_callable=load_to_postgis,
    )

    task_fetch >> task_load_s3 >> task_load_db
