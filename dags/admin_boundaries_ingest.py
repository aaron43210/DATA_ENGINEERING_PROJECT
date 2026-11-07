"""
Airflow ELT DAG: admin_boundaries_ingest (Owner: Angel)
=======================================================
Loads the real polygons of Kerala's 14 districts from OpenStreetMap into
public.admin_boundaries. dbt's spatial joins depend on this table.

Unpaused on creation, so it fills the table on first boot, then refreshes monthly.
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
    POSTGIS_BOUNDARIES,
    bronze_key,
    get_s3_client,
    pg_connection,
)
from common.overpass import AREA_FROM_WAYS_SQL, run_overpass, to_wkt
from data_quality_checks import EXPECTED_KERALA_DISTRICTS, run_admin_boundary_checks

logger = logging.getLogger(__name__)

SOURCE = "osm-admin"
OVERPASS_TIMEOUT = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "180"))

# OSM maps Indian districts at admin_level 5.
OVERPASS_QUERY = f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
area["name"="Kerala"]["boundary"="administrative"]["admin_level"="4"]->.kerala;
rel(area.kerala)["boundary"="administrative"]["admin_level"="5"];
out geom;
"""


def extract_boundaries_to_bronze(**context) -> str:
    elements = run_overpass(OVERPASS_QUERY, OVERPASS_TIMEOUT)
    if len(elements) != EXPECTED_KERALA_DISTRICTS:
        raise ValueError(
            f"Expected {EXPECTED_KERALA_DISTRICTS} district relations, "
            f"got {len(elements)}. Refusing to replace the boundaries."
        )
    key = bronze_key(SOURCE, context["data_interval_end"], "kerala_districts.jsonl")
    body = "\n".join(json.dumps(element) for element in elements)
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return key


def load_boundaries(**context) -> int:
    from psycopg2.extras import execute_values

    key = context["ti"].xcom_pull(task_ids="extract_boundaries_to_bronze")
    if not key:
        raise ValueError("No Bronze key received from the extract task.")

    obj = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
    rows = []
    for line in obj["Body"].read().decode("utf-8").splitlines():
        element = json.loads(line)
        converted = to_wkt(element)
        if converted is None:
            raise ValueError(f"Relation {element.get('id')} has no usable geometry.")
        tags = element.get("tags", {})
        rows.append(
            (
                f"OSM_{element['id']}",
                tags.get("name:en") or tags["name"],
                int(tags["admin_level"]),
                element["id"],
                tags.get("wikidata"),
                converted[0],
            )
        )

    # geometry is NOT NULL, so a district that cannot be assembled rolls back
    # the whole load instead of leaving a partial map.
    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO public.admin_boundaries
                (region_id, name, admin_level, osm_relation_id, wikidata_id, geometry)
            VALUES %s
            ON CONFLICT (region_id) DO UPDATE SET
                name = EXCLUDED.name,
                admin_level = EXCLUDED.admin_level,
                wikidata_id = EXCLUDED.wikidata_id,
                geometry = EXCLUDED.geometry
            """,
            rows,
            template=f"(%s, %s, %s, %s, %s, {AREA_FROM_WAYS_SQL})",
        )
    return len(rows)


with DAG(
    dag_id="admin_boundaries_ingest",
    default_args={**BASE_DEFAULT_ARGS, "owner": "angel"},
    description="Kerala district polygons from OSM into PostGIS",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["elt", "geospatial", "osm", "postgis", "angel"],
) as dag:
    extract = PythonOperator(
        task_id="extract_boundaries_to_bronze",
        python_callable=extract_boundaries_to_bronze,
    )
    load = PythonOperator(task_id="load_boundaries", python_callable=load_boundaries)
    quality_gate = PythonOperator(
        task_id="run_boundary_quality_gate",
        python_callable=run_admin_boundary_checks,
        outlets=[POSTGIS_BOUNDARIES],
    )

    extract >> load >> quality_gate
