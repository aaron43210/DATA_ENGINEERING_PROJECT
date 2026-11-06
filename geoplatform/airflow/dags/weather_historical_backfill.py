"""
Airflow DAG: weather_historical_backfill
=========================================
PURPOSE: Historical batch loading ONLY. Live/real-time weather is handled by
the Redpanda weather producer + Flink (weather.live topic).

Run this DAG:
  - on first platform setup (populate history)
  - to recover from a producer outage (fill gaps)
  - for compliance/audit needing a complete historical record

Pattern: ELT — Extract -> land raw JSON to MinIO Bronze -> transform in dbt.
Storage: MinIO only.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import (
    BRONZE_BUCKET,
    BRONZE_WEATHER,
    BASE_DEFAULT_ARGS,
    bronze_key,
    get_s3_client,
    pg_connection,
)

logger = logging.getLogger(__name__)

SOURCE = "weather"
# Same stations as the real-time producer so batch + stream align on station_id.
CITIES = [
    {
        "name": "Thiruvananthapuram",
        "lat": 8.5241,
        "lon": 76.9366,
        "station_id": "WX_TVM",
    },
    {"name": "Kochi", "lat": 9.9312, "lon": 76.2673, "station_id": "WX_COK"},
    {"name": "Kozhikode", "lat": 11.2588, "lon": 75.7804, "station_id": "WX_CCJ"},
    {"name": "Thrissur", "lat": 10.5276, "lon": 76.2144, "station_id": "WX_TCR"},
    {"name": "Kollam", "lat": 8.8932, "lon": 76.6141, "station_id": "WX_QLN"},
]


def fetch_to_bronze(**context):
    """EXTRACT + LAND RAW: snapshot current weather per station and write it
    unfiltered to Bronze as JSONL. Pushes only the Bronze object key."""
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key or api_key == "demo":
        raise ValueError(
            "PRODUCTION ERROR: OPENWEATHER_API_KEY is missing or invalid. "
            "Mock data fallback has been removed (No Mock Data Policy)."
        )

    observed_at = context["data_interval_start"].isoformat()
    items = []
    for city in CITIES:
        try:
            resp = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": api_key,
                    "units": "metric",
                },
                timeout=10,
            )
            resp.raise_for_status()
            items.append(
                {
                    "station_id": city["station_id"],
                    "city_name": city["name"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "raw_response": resp.json(),
                    "observed_at": observed_at,
                    "source": "airflow_backfill",
                }
            )
        except Exception as e:  # noqa: BLE001 - one city failing must not sink the run
            logger.error("Failed to fetch for %s: %s", city["name"], e)

    if not items:
        raise RuntimeError("No weather stations fetched successfully.")

    key = bronze_key(SOURCE, context["data_interval_start"], "weather.jsonl")
    body = "\n".join(json.dumps(it) for it in items)
    get_s3_client().put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote %d stations to s3://%s/%s", len(items), BRONZE_BUCKET, key)
    context["ti"].xcom_push(key="bronze_key", value=key)


def load_postgres(**context):
    """LOAD: read raw Bronze JSONL and upsert into weather_observations."""
    key = context["ti"].xcom_pull(key="bronze_key", task_ids="fetch_to_bronze")
    if not key:
        logger.info("No Bronze object produced; nothing to load.")
        return

    obj = get_s3_client().get_object(Bucket=BRONZE_BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()
    s3_path = f"s3://{BRONZE_BUCKET}/{key}"

    rows = []
    for line in lines:
        if not line.strip():
            continue
        item = json.loads(line)
        raw = item.get("raw_response", {})
        main, wind = raw.get("main", {}), raw.get("wind", {})
        clouds, rain = raw.get("clouds", {}), raw.get("rain", {})
        rows.append(
            (
                item["station_id"],
                item["observed_at"],
                main.get("temp"),
                main.get("humidity"),
                rain.get("1h", 0.0),
                wind.get("speed"),
                clouds.get("all"),
                item["longitude"],
                item["latitude"],
                s3_path,
            )
        )

    from psycopg2.extras import execute_values

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO weather_observations
                (station_id, observed_at, temperature_c, humidity_pct,
                 rainfall_mm, wind_speed_ms, cloud_cover_pct, location, raw_s3_path)
            VALUES %s
            ON CONFLICT (station_id, observed_at) DO NOTHING
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, "
            "ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)",
        )
    logger.info("Loaded %d historical weather records into PostgreSQL", len(rows))


with DAG(
    dag_id="weather_historical_backfill",
    default_args=BASE_DEFAULT_ARGS,
    description=(
        "Historical weather ELT backfill. Real-time weather is handled "
        "by the Redpanda weather producer + Flink."
    ),
    schedule="0 0 * * *",  # daily at midnight — historical record only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "weather", "elt", "historical"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_to_bronze",
        python_callable=fetch_to_bronze,
        outlets=[BRONZE_WEATHER],
    )
    task_load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres,
    )

    task_fetch >> task_load
