"""
Airflow ELT DAG: osm_elt_dag (Owner: Angel)
===========================================
Weekly OpenStreetMap extract for Kerala:

  extract_osm_to_bronze  raw Overpass elements -> MinIO Bronze (JSONL)
  load_bronze_to_postgis Bronze -> public.vector_features (points, roads, areas)
  run_osm_quality_gate   spatial checks; a failure stops dbt from running

The quality gate publishes POSTGIS_OSM, which triggers dbt_transform. Landing in
Bronze publishes BRONZE_OSM, which triggers the Iceberg Silver load.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import (
    BASE_DEFAULT_ARGS,
    BRONZE_BUCKET,
    BRONZE_OSM,
    POSTGIS_OSM,
    bronze_key,
    get_s3_client,
    pg_connection,
)
from common.overpass import AREA_FROM_WAYS_SQL, run_overpass, to_wkt
from data_quality_checks import run_osm_quality_checks

logger = logging.getLogger(__name__)

SOURCE = "osm"
OVERPASS_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "180"))
OVERPASS_BBOX = os.getenv("OVERPASS_BBOX", "8.1,74.8,12.8,77.5")  # Kerala

# `out geom` returns coordinates on each way, so no extra skeleton nodes come
# back to be mistaken for features.
OVERPASS_QUERY = f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  nwr["amenity"~"^(hospital|clinic|doctors)$"]({OVERPASS_BBOX});
  node["emergency"="ambulance_station"]({OVERPASS_BBOX});
  way["highway"~"^(trunk|primary)$"]({OVERPASS_BBOX});
);
out geom;
"""


def classify(tags: dict) -> str:
    """Stable feature_type, e.g. hospital, clinic, highway_trunk."""
    if tags.get("amenity"):
        return tags["amenity"]
    if tags.get("emergency"):
        return tags["emergency"]
    if tags.get("highway"):
        return f"highway_{tags['highway']}"
    return "other"


def extract_osm_to_bronze(**context) -> str:
    """Land the raw response, unfiltered, in Bronze. Returns the object key."""
    elements = run_overpass(OVERPASS_QUERY, OVERPASS_TIMEOUT)
    if not elements:
        raise ValueError("Overpass returned no elements for the Kerala extract.")
    logger.info("Received %d OSM elements", len(elements))

    key = bronze_key(SOURCE, context["data_interval_end"], "data.jsonl")
    body = "\n".join(json.dumps(element) for element in elements)
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote s3://%s/%s", BRONZE_BUCKET, key)
    return key


def parse_elements(lines):
    """Turn Bronze JSONL into (simple_rows, relation_rows, skipped)."""
    simple, relations, skipped = [], [], 0
    for line in lines:
        if not line.strip():
            continue
        element = json.loads(line)
        converted = to_wkt(element)
        if converted is None or not element.get("tags"):
            skipped += 1
            continue
        wkt, is_relation = converted
        tags = element["tags"]
        row = (
            element["type"],
            element["id"],
            classify(tags),
            tags.get("name") or tags.get("name:en"),
            json.dumps(tags),
            wkt,
        )
        (relations if is_relation else simple).append(row)
    return simple, relations, skipped


def load_bronze_to_postgis(**context) -> int:
    """Upsert Bronze elements into vector_features, repairing invalid geometry."""
    from psycopg2.extras import execute_values

    key = context["ti"].xcom_pull(task_ids="extract_osm_to_bronze")
    if not key:
        raise ValueError("No Bronze key received from extract_osm_to_bronze.")

    obj = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()
    simple, relations, skipped = parse_elements(lines)
    logger.info(
        "Parsed %d simple features, %d relations, skipped %d",
        len(simple),
        len(relations),
        skipped,
    )

    upsert = """
        INSERT INTO public.vector_features
            (osm_type, osm_id, feature_type, name, tags, geometry)
        VALUES %s
        ON CONFLICT (osm_type, osm_id) DO UPDATE SET
            feature_type = EXCLUDED.feature_type,
            name = EXCLUDED.name,
            tags = EXCLUDED.tags,
            geometry = EXCLUDED.geometry,
            ingested_at = NOW()
    """
    prefix = "(%s, %s, %s, %s, %s::jsonb, "
    with pg_connection() as conn, conn.cursor() as cur:
        if simple:
            execute_values(
                cur,
                upsert,
                simple,
                template=prefix + "ST_MakeValid(ST_GeomFromText(%s, 4326)))",
                page_size=1000,
            )
        if relations:
            execute_values(
                cur,
                upsert,
                relations,
                template=prefix + AREA_FROM_WAYS_SQL + ")",
                page_size=200,
            )
    return len(simple) + len(relations)


with DAG(
    dag_id="osm_elt_dag",
    default_args={**BASE_DEFAULT_ARGS, "owner": "angel"},
    description="Weekly OSM ELT: Bronze -> PostGIS -> quality gate",
    schedule="0 4 * * 0",  # Sundays 04:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "geospatial", "osm", "bronze", "postgis", "angel"],
) as dag:
    extract = PythonOperator(
        task_id="extract_osm_to_bronze",
        python_callable=extract_osm_to_bronze,
        outlets=[BRONZE_OSM],
    )
    load = PythonOperator(
        task_id="load_bronze_to_postgis",
        python_callable=load_bronze_to_postgis,
    )
    quality_gate = PythonOperator(
        task_id="run_osm_quality_gate",
        python_callable=run_osm_quality_checks,
        outlets=[POSTGIS_OSM],
    )

    extract >> load >> quality_gate
