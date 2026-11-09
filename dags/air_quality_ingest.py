"""
Airflow DAG: air_quality_ingest
===============================
Hourly OpenWeather air pollution readings for the 5 Kerala stations.

  fetch_to_bronze  raw API responses -> Bronze (JSONL), unchanged
  load_postgres    Bronze -> public.air_quality_observations (serves /air-quality)

Landing in Bronze publishes BRONZE_AIR_QUALITY, which triggers the Iceberg
Silver load. Writes are idempotent per (station_id, event_time).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import (
    BASE_DEFAULT_ARGS,
    BRONZE_AIR_QUALITY,
    BRONZE_BUCKET,
    KERALA_STATIONS,
    bronze_key,
    get_s3_client,
    pg_connection,
)

logger = logging.getLogger(__name__)

SOURCE = "air_quality"
URL = "https://api.openweathermap.org/data/2.5/air_pollution"
POLLUTANTS = ["pm2_5", "pm10", "no2", "o3", "so2", "co", "nh3", "no"]


def fetch_to_bronze(**context):
    """EXTRACT + LAND RAW: one air_pollution response per station to Bronze."""
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is not set.")

    fetched_at = datetime.now(timezone.utc).isoformat()
    items = []
    for station in KERALA_STATIONS:
        try:
            resp = requests.get(
                URL,
                params={"lat": station["lat"], "lon": station["lon"], "appid": api_key},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            # The exception text includes the URL, which carries the API key.
            logger.error(
                "Fetch failed for %s: %s", station["station_id"], type(e).__name__
            )
            continue
        items.append(
            {
                "station_id": station["station_id"],
                "city_name": station["name"],
                "latitude": station["lat"],
                "longitude": station["lon"],
                "raw_response": resp.json(),
                "fetched_at": fetched_at,
            }
        )

    if not items:
        raise RuntimeError("No air quality station fetched successfully.")

    key = bronze_key(SOURCE, context["data_interval_start"], "air_quality.jsonl")
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body="\n".join(json.dumps(it) for it in items).encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote %d stations to s3://%s/%s", len(items), BRONZE_BUCKET, key)
    context["ti"].xcom_push(key="bronze_key", value=key)


def to_row(item: dict):
    """Bronze item -> air_quality_observations row, or None without a reading."""
    readings = item.get("raw_response", {}).get("list") or []
    if not readings:
        return None
    reading = readings[0]
    parts = reading.get("components", {})
    return (
        item["station_id"],
        item["city_name"],
        datetime.fromtimestamp(reading["dt"], timezone.utc),
        item["latitude"],
        item["longitude"],
        reading["main"]["aqi"],
        *(parts.get(p) for p in POLLUTANTS),
    )


def load_postgres(**context):
    """LOAD: Bronze JSONL -> air_quality_observations."""
    key = context["ti"].xcom_pull(key="bronze_key", task_ids="fetch_to_bronze")
    body = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
    rows = [
        row
        for line in body.decode("utf-8").splitlines()
        if line.strip() and (row := to_row(json.loads(line)))
    ]

    from psycopg2.extras import execute_values

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO air_quality_observations
                (station_id, city_name, event_time, latitude, longitude, aqi,
                 pm2_5, pm10, no2, o3, so2, co, nh3, no)
            VALUES %s
            ON CONFLICT (station_id, event_time) DO NOTHING
            """,
            rows,
        )
    logger.info("Loaded %d air quality readings into PostgreSQL", len(rows))


with DAG(
    dag_id="air_quality_ingest",
    default_args=BASE_DEFAULT_ARGS,
    description="Hourly OpenWeather air quality: Bronze, then PostGIS",
    schedule="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["air_quality", "elt", "hourly"],
) as dag:
    task_fetch = PythonOperator(
        task_id="fetch_to_bronze",
        python_callable=fetch_to_bronze,
        outlets=[BRONZE_AIR_QUALITY],
    )
    task_load = PythonOperator(task_id="load_postgres", python_callable=load_postgres)

    task_fetch >> task_load
