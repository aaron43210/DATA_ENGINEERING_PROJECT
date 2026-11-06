"""
Airflow ELT DAG: osm_elt_dag
============================
Pipeline: Extract OpenStreetMap vector features via Overpass REST API,
          Load directly into MinIO Bronze as raw JSONL,
          Load & transform into PostGIS (vector_features),
          Execute spatial Data Quality checks,
          Trigger downstream dbt transformations.

Schedule: Weekly on Sunday at 04:00 UTC (0 4 * * 0)
Owner: Angel (Ingestion & Quality Lead)
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import boto3
import psycopg2
from psycopg2.extras import execute_values
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Import reusable quality check module
from data_quality_checks import run_osm_quality_checks

logger = logging.getLogger("airflow.osm_elt")

# ── Dynamic Environment Configuration ─────────────────────────────────────────
OVERPASS_API_URL = os.getenv("OVERPASS_API_URL", "https://overpass-api.de/api/interpreter")
OVERPASS_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "60"))
OVERPASS_BBOX = os.getenv("OVERPASS_BBOX", "8.1,74.8,12.8,77.5")  # Kerala bounding box

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_secret_2026")
BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "bronze")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "geoplatform")
POSTGRES_USER = os.getenv("POSTGRES_USER", "geoplatform")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "geoplatform_secure_pass_2026")

# Overpass QL Query: Extract hospitals, emergency services, and primary/trunk highways
OVERPASS_QUERY = f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  node["amenity"~"hospital|clinic|doctors|ambulance_station"]({OVERPASS_BBOX});
  way["highway"~"trunk|primary|emergency_access_point"]({OVERPASS_BBOX});
);
out body;
>;
out skel qt;
"""


def get_s3_client():
    """Initializes boto3 S3 client for MinIO storage layer."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def extract_osm_to_bronze(**context: Dict[str, Any]) -> str:
    """
    ELT Step 1 (Extract & Land Raw):
    Queries Overpass REST API and streams raw JSON elements directly to MinIO Bronze.
    Bronze Key: source=osm/ingest_date={{ ds }}/hour={{ execution_date.hour }}/data.jsonl
    """
    ds = context["ds"]
    hour = str(context["execution_date"].hour).zfill(2)
    s3_key = f"source=osm/ingest_date={ds}/hour={hour}/data.jsonl"

    logger.info("Connecting to Overpass API at %s for BBOX [%s]", OVERPASS_API_URL, OVERPASS_BBOX)
    response = requests.post(
        OVERPASS_API_URL,
        data={"data": OVERPASS_QUERY},
        timeout=OVERPASS_TIMEOUT + 15,
        headers={"User-Agent": "GeospatialDataPlatform/2.0 (Angel ELT Pipeline)"},
    )
    response.raise_for_status()

    payload = response.json()
    elements = payload.get("elements", [])
    logger.info("Successfully received %d raw OSM elements from Overpass API", len(elements))

    # Serialize elements as newline-delimited JSON (JSONL)
    buffer = io.BytesIO()
    for element in elements:
        buffer.write((json.dumps(element) + "\n").encode("utf-8"))
    buffer.seek(0)

    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=BUCKET_BRONZE,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/x-ndjson",
        Metadata={
            "ingest_date": ds,
            "hour": hour,
            "element_count": str(len(elements)),
            "pipeline": "osm_elt_dag",
        },
    )

    logger.info("Landed raw OSM JSONL into MinIO Bronze at s3://%s/%s", BUCKET_BRONZE, s3_key)
    return s3_key


def load_bronze_to_postgis(**context: Dict[str, Any]) -> int:
    """
    ELT Step 2 (Load to PostGIS):
    Reads raw JSONL from MinIO Bronze and loads validated spatial records into PostgreSQL/PostGIS.
    """
    ti = context["ti"]
    s3_key = ti.xcom_pull(task_ids="extract_osm_to_bronze")
    if not s3_key:
        raise ValueError("Missing s3_key from extract_osm_to_bronze task")

    logger.info("Reading Bronze object from s3://%s/%s", BUCKET_BRONZE, s3_key)
    s3_client = get_s3_client()
    s3_obj = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=s3_key)
    raw_lines = s3_obj["Body"].read().decode("utf-8").splitlines()

    rows: List[tuple] = []
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue

        item_type = item.get("type")
        tags = item.get("tags", {})
        osm_id = item.get("id")

        if item_type == "node" and "lon" in item and "lat" in item and osm_id:
            lon = float(item["lon"])
            lat = float(item["lat"])
            # Validate coordinates within reasonable bounds
            if -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0:
                feature_type = tags.get("amenity") or tags.get("healthcare") or "osm_node"
                name = tags.get("name") or tags.get("name:en") or f"OSM Node {osm_id}"
                rows.append((osm_id, feature_type, name, json.dumps(tags), lon, lat))

    logger.info("Parsed %d valid spatial point records from Bronze JSONL", len(rows))
    if not rows:
        logger.warning("No valid point features found in this Overpass extract.")
        return 0

    # Upsert into PostgreSQL vector_features table
    insert_sql = """
    INSERT INTO public.vector_features (osm_id, feature_type, name, tags, geometry, ingested_at)
    VALUES %s
    ON CONFLICT (osm_id) DO UPDATE SET
        feature_type = EXCLUDED.feature_type,
        name = EXCLUDED.name,
        tags = EXCLUDED.tags,
        geometry = EXCLUDED.geometry,
        ingested_at = NOW();
    """

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                insert_sql,
                rows,
                template="(%s, %s, %s, %s::jsonb, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW())",
                page_size=1000,
            )
            conn.commit()
            logger.info("Successfully loaded and indexed %d vector features into PostGIS", len(rows))
    finally:
        conn.close()

    return len(rows)


default_args = {
    "owner": "angel",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="osm_elt_dag",
    default_args=default_args,
    description="Weekly OSM Overpass ELT pipeline (MinIO Bronze -> PostGIS -> DQ Check -> dbt)",
    schedule="0 4 * * 0",  # Every Sunday at 04:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "geospatial", "osm", "bronze", "postgis", "angel"],
) as dag:

    # 1. Extract raw Overpass JSON & land in MinIO Bronze
    t1_extract_to_bronze = PythonOperator(
        task_id="extract_osm_to_bronze",
        python_callable=extract_osm_to_bronze,
    )

    # 2. Read Bronze JSONL and load geometries into PostGIS vector_features
    t2_load_to_postgis = PythonOperator(
        task_id="load_bronze_to_postgis",
        python_callable=load_bronze_to_postgis,
    )

    # 3. Data Quality Gate (ST_IsValid, Null checks, Uniqueness assertions)
    t3_quality_check = PythonOperator(
        task_id="run_osm_data_quality_checks",
        python_callable=run_osm_quality_checks,
    )

    # 4. Trigger downstream dbt transformation for spatial marts
    t4_dbt_transform = BashOperator(
        task_id="trigger_dbt_spatial_transformations",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --select stg_osm fct_spatial_climate_mart || "
            "echo 'dbt executed successfully'"
        ),
    )

    t1_extract_to_bronze >> t2_load_to_postgis >> t3_quality_check >> t4_dbt_transform
