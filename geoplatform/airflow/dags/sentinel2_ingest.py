"""
Airflow DAG: sentinel2_ingest
=============================
Pattern: ETL (Extract -> Transform -> Load)
Purpose: Ingest satellite imagery metadata from Sentinel-2 STAC API.

- EXTRACT: Fetch metadata for a specific area (Kerala, India) over the last day.
- TRANSFORM: Python validates bounds, formats timestamps, calculates polygons.
- LOAD:
    1. Raw JSON to MinIO (Bronze Layer).
    2. Transformed metadata to PostgreSQL (PostGIS) `satellite_observations`.
    3. Triggers LakeFS commit on the `bronze` branch.

Integrations: OpenLineage (automatic via Airflow backend), LakeFS.
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
STAC_API_URL = "https://earth-search.aws.element84.com/v1/search"
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
BUCKET = "geoplatform-bronze"

POSTGRES = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
    "dbname": os.environ.get("POSTGRES_DB", "geoplatform"),
    "user": os.environ.get("POSTGRES_USER", "geoplatform"),
    "password": os.environ.get("POSTGRES_PASSWORD", "geoplatform"),
}

# Kerala bounding box [min_lon, min_lat, max_lon, max_lat]
BBOX = [74.8, 8.2, 77.5, 12.8]

default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}

# ── Tasks ──────────────────────────────────────────────────────────────────────


def extract_stac_data(**context):
    """EXTRACT: Fetch Sentinel-2 STAC metadata for the last 24 hours."""
    execution_date = context["execution_date"]

    # We query the previous day based on the DAG run date
    start_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    end_date = execution_date.strftime("%Y-%m-%dT00:00:00Z")
    time_range = f"{start_date}/{end_date}"

    payload = {
        "collections": ["sentinel-2-c1-l2a"],
        "bbox": BBOX,
        "datetime": time_range,
        "limit": 100,
    }

    logger.info(f"Querying STAC API for {time_range} with bbox {BBOX}")
    resp = requests.post(STAC_API_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    logger.info(f"Extracted {len(features)} scenes")

    # Pass the raw features to the next task
    context["ti"].xcom_push(key="raw_scenes", value=features)


def transform_and_load_bronze(**context):
    """
    TRANSFORM & LOAD (Raw):
    1. Filter out scenes with > 80% cloud cover.
    2. Upload raw JSON to MinIO Bronze bucket.
    """
    raw_scenes = context["ti"].xcom_pull(key="raw_scenes", task_ids="extract_stac")
    execution_date = context["execution_date"]
    date_prefix = execution_date.strftime("%Y/%m/%d")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    transformed = []

    for scene in raw_scenes:
        props = scene.get("properties", {})
        cloud_pct = props.get("eo:cloud_cover", 100)

        # TRANSFORM: Apply quality filter (drop >80% cloudy scenes)
        if cloud_pct > 80:
            continue

        scene_id = scene.get("id")

        # LOAD: Write raw JSON to MinIO
        key = f"sentinel2/{date_prefix}/{scene_id}.json"

        # Check if bucket exists, create if not (fail-safe for local dev)
        try:
            s3.head_bucket(Bucket=BUCKET)
        except Exception:
            s3.create_bucket(Bucket=BUCKET)

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(scene),
            ContentType="application/json",
        )

        # Build clean dict for PostGIS
        transformed.append(
            {
                "scene_id": scene_id,
                "satellite": "Sentinel-2",
                "acquisition_time": props.get("datetime"),
                "cloud_percentage": cloud_pct,
                "crs": "EPSG:4326",
                "s3_bronze_path": f"s3://{BUCKET}/{key}",
                "geometry": scene.get("geometry"),
            }
        )

    logger.info(f"Transformed and loaded {len(transformed)} scenes to Bronze layer")
    context["ti"].xcom_push(key="clean_scenes", value=transformed)


def load_to_postgis_and_lakefs(**context):
    """
    LOAD (Clean): Write transformed data to PostGIS.
    Also trigger a LakeFS commit to snapshot this ingestion run.
    """
    clean_scenes = context["ti"].xcom_pull(
        key="clean_scenes", task_ids="transform_and_load_bronze"
    )
    execution_date = context["execution_date"]

    if not clean_scenes:
        logger.info("No valid scenes to load. Skipping DB insert and LakeFS commit.")
        return

    # 1. Trigger LakeFS Commit
    lakefs_endpoint = os.environ.get("LAKEFS_ENDPOINT", "http://lakefs:8000")
    lakefs_auth = (
        os.environ.get("LAKEFS_ACCESS_KEY", "accesskey"),
        os.environ.get("LAKEFS_SECRET_KEY", "secretkey"),
    )

    commit_id = "unknown"
    try:
        commit_resp = requests.post(
            f"{lakefs_endpoint}/api/v1/repositories/geoplatform/branches/bronze/commits",
            auth=lakefs_auth,
            json={
                "message": f"sentinel2 ingest {execution_date.date()} — {len(clean_scenes)} scenes",
                "metadata": {
                    "dag_run": str(execution_date),
                    "record_count": str(len(clean_scenes)),
                    "source": "sentinel-2-stac-api",
                },
            },
            timeout=10,
        )
        if commit_resp.status_code == 201:
            commit_id = commit_resp.json().get("id", "unknown")
            logger.info(f"LakeFS commit created: {commit_id}")
    except Exception as e:
        logger.warning(f"LakeFS commit failed (non-critical): {e}")

    # 2. Insert into PostgreSQL
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()

    for scene in clean_scenes:
        # Convert GeoJSON geometry to PostGIS EWKT
        geom_json = json.dumps(scene["geometry"])

        cur.execute(
            """
            INSERT INTO satellite_observations
                (scene_id, satellite, acquisition_time, cloud_percentage,
                 crs, s3_bronze_path, lakefs_commit_id, bbox)
            VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s))
            ON CONFLICT (scene_id) DO UPDATE SET
                s3_bronze_path = EXCLUDED.s3_bronze_path,
                lakefs_commit_id = EXCLUDED.lakefs_commit_id
            """,
            (
                scene["scene_id"],
                scene["satellite"],
                scene["acquisition_time"],
                scene["cloud_percentage"],
                scene["crs"],
                scene["s3_bronze_path"],
                commit_id,
                geom_json,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {len(clean_scenes)} scenes into PostGIS")


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="sentinel2_ingest",
    default_args=default_args,
    description="ETL DAG for Sentinel-2 STAC metadata",
    schedule="0 2 * * *",  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "satellite", "stac"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_stac",
        python_callable=extract_stac_data,
    )

    task_transform = PythonOperator(
        task_id="transform_and_load_bronze",
        python_callable=transform_and_load_bronze,
    )

    task_load = PythonOperator(
        task_id="load_postgis_and_commit",
        python_callable=load_to_postgis_and_lakefs,
    )

    task_extract >> task_transform >> task_load
