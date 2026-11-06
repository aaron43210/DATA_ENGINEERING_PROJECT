"""
Airflow DAG: sentinel2_ingest
=============================
Pattern: ETL — Extract STAC metadata, archive it RAW to the MinIO Bronze layer,
TRANSFORM it (drop cloudy scenes, validate geometry, shape rows), then LOAD the
curated result into PostGIS. The transform happens in Airflow *before* the load,
which is what makes this ETL; the raw Bronze copy is kept intact for replay.

Flow:
  extract_to_bronze   : query STAC API -> archive ALL scenes as JSONL to Bronze
  transform_and_load  : read Bronze -> filter/validate (TRANSFORM) -> load PostGIS

Storage: MinIO only (object storage + Bronze landing zone). No LakeFS, no Mongo.
Integrations: OpenLineage (automatic via the Airflow backend).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import (
    BRONZE_BUCKET,
    BRONZE_SENTINEL2,
    BASE_DEFAULT_ARGS,
    bronze_key,
    get_s3_client,
    pg_connection,
)

logger = logging.getLogger(__name__)

STAC_API_URL = "https://earth-search.aws.element84.com/v1/search"
# Kerala bounding box [min_lon, min_lat, max_lon, max_lat]
BBOX = [74.8, 8.2, 77.5, 12.8]
SOURCE = "sentinel-2"
CLOUD_MAX = 80  # TRANSFORM threshold: drop scenes with > this % cloud cover


def extract_to_bronze(**context):
    """EXTRACT + LAND RAW: fetch STAC metadata for the run's data interval and
    write every scene unfiltered to Bronze as JSONL. Pushes only the object key
    (a reference) over XCom — never the payload."""
    start = context["data_interval_start"]
    end = context["data_interval_end"]
    time_range = f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    payload = {
        "collections": ["sentinel-2-c1-l2a"],
        "bbox": BBOX,
        "datetime": time_range,
        "limit": 100,
    }
    logger.info("Querying STAC API for %s bbox=%s", time_range, BBOX)
    resp = requests.post(STAC_API_URL, json=payload, timeout=30)
    resp.raise_for_status()
    features = resp.json().get("features", [])
    logger.info("Extracted %d scenes", len(features))

    key = bronze_key(SOURCE, start, "scenes.jsonl")
    body = "\n".join(json.dumps(f) for f in features)
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET, Key=key, Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote raw scenes to s3://%s/%s", BRONZE_BUCKET, key)
    context["ti"].xcom_push(key="bronze_key", value=key)


def transform_and_load(**context):
    """TRANSFORM + LOAD (ETL): read the raw Bronze JSONL, drop scenes above the
    cloud-cover threshold, validate geometry, then upsert the curated scenes
    into PostGIS. The raw Bronze copy is left intact for replay/backfill."""
    key = context["ti"].xcom_pull(key="bronze_key", task_ids="extract_to_bronze")
    if not key:
        logger.info("No Bronze object produced; nothing to load.")
        return

    obj = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()
    s3_path = f"s3://{BRONZE_BUCKET}/{key}"

    rows, dropped = [], 0
    for line in lines:
        if not line.strip():
            continue
        scene = json.loads(line)
        props = scene.get("properties", {})
        cloud = props.get("eo:cloud_cover", 100)
        if cloud > CLOUD_MAX:  # TRANSFORM: quality filter (this is the "T" in ETL)
            dropped += 1
            continue
        acq = props.get("datetime")
        if not acq:  # acquisition_time is NOT NULL in the schema
            logger.warning("Skipping scene %s with no datetime", scene.get("id"))
            continue
        geom = scene.get("geometry")
        geom_json = json.dumps(geom) if geom and geom.get("type") == "Polygon" else None
        rows.append((
            scene.get("id"), "Sentinel-2", acq,
            cloud, "EPSG:4326", s3_path, geom_json,
        ))

    logger.info("Dropped %d cloudy scenes (> %d%%)", dropped, CLOUD_MAX)
    if not rows:
        logger.info("No scenes passed the quality filter.")
        return

    from psycopg2.extras import execute_values

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO satellite_observations
                (scene_id, satellite, acquisition_time, cloud_percentage,
                 crs, s3_bronze_path, bbox)
            VALUES %s
            ON CONFLICT (scene_id) DO UPDATE SET
                s3_bronze_path = EXCLUDED.s3_bronze_path,
                cloud_percentage = EXCLUDED.cloud_percentage
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s))",
        )
    logger.info("Upserted %d curated scenes into PostGIS", len(rows))


with DAG(
    dag_id="sentinel2_ingest",
    default_args=BASE_DEFAULT_ARGS,
    description="ETL for Sentinel-2 STAC metadata (raw Bronze + transform + PostGIS)",
    schedule="0 2 * * *",  # daily at 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "satellite", "stac"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=extract_to_bronze,
        outlets=[BRONZE_SENTINEL2],  # data-aware trigger for the dbt layer
    )
    task_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    task_extract >> task_load
