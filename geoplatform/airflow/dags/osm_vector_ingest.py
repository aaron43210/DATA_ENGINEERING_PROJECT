"""
Airflow DAG: osm_vector_ingest
==============================
Pattern: ELT — fetch OpenStreetMap vector data, land it RAW in MinIO Bronze,
then load features into PostGIS. Heavier spatial transforms live in dbt.

Flow:
  fetch_to_bronze : query Overpass API -> write raw elements as JSONL to Bronze
  load_postgis    : read Bronze JSONL -> insert hospital nodes into PostGIS

Storage: MinIO only. Integrations: OpenLineage.
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
    BRONZE_OSM,
    BASE_DEFAULT_ARGS,
    bronze_key,
    get_s3_client,
    pg_connection,
)

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
SOURCE = "osm"

OVERPASS_QUERY = """
[out:json][timeout:25];
(
  node["amenity"="hospital"](8.1,74.8,12.8,77.5);
  way["highway"="trunk"](8.1,74.8,12.8,77.5);
);
out body;
>;
out skel qt;
"""


def fetch_to_bronze(**context):
    """EXTRACT + LAND RAW: fetch Kerala OSM elements and write them unfiltered
    to Bronze as JSONL. Pushes only the Bronze object key over XCom."""
    logger.info("Querying Overpass API...")
    resp = requests.post(OVERPASS_URL, data={"data": OVERPASS_QUERY}, timeout=30)
    resp.raise_for_status()
    elements = resp.json().get("elements", [])
    logger.info("Extracted %d OSM elements", len(elements))

    key = bronze_key(SOURCE, context["data_interval_start"], "kerala_extract.jsonl")
    body = "\n".join(json.dumps(el) for el in elements)
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote raw OSM data to s3://%s/%s", BRONZE_BUCKET, key)
    context["ti"].xcom_push(key="bronze_key", value=key)


def load_postgis(**context):
    """LOAD: read raw Bronze JSONL and batch-insert hospital point features.
    Rows are validated in Python first, so one bad element cannot abort the
    whole transaction."""
    key = context["ti"].xcom_pull(key="bronze_key", task_ids="fetch_to_bronze")
    if not key:
        logger.info("No Bronze object produced; nothing to load.")
        return

    obj = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()

    rows = []
    for line in lines:
        if not line.strip():
            continue
        el = json.loads(line)
        tags = el.get("tags", {})
        # Only hospital nodes carry a usable point geometry here.
        if (
            el.get("type") == "node"
            and tags.get("amenity") == "hospital"
            and "lon" in el
            and "lat" in el
        ):
            feature_type = tags.get("amenity") or tags.get("highway") or "unknown"
            rows.append(
                (
                    el.get("id"),
                    feature_type,
                    tags.get("name", "Unnamed"),
                    json.dumps(tags),
                    el["lon"],
                    el["lat"],
                )
            )

    if not rows:
        logger.info("No loadable features found.")
        return

    from psycopg2.extras import execute_values

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO vector_features (osm_id, feature_type, name, tags, geometry)
            VALUES %s
            ON CONFLICT (osm_id) DO UPDATE SET
                feature_type = EXCLUDED.feature_type,
                name = EXCLUDED.name,
                tags = EXCLUDED.tags,
                geometry = EXCLUDED.geometry
            """,
            rows,
            template="(%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
        )
    logger.info("Loaded %d vector features into PostGIS", len(rows))


with DAG(
    dag_id="osm_vector_ingest",
    default_args=BASE_DEFAULT_ARGS,
    description="ELT DAG for OpenStreetMap vector data (MinIO Bronze + PostGIS)",
    schedule="0 4 * * 0",  # weekly, Sunday 04:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "vector", "osm"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_to_bronze",
        python_callable=fetch_to_bronze,
        outlets=[BRONZE_OSM],
    )
    task_load = PythonOperator(
        task_id="load_postgis",
        python_callable=load_postgis,
    )

    task_fetch >> task_load
