"""
Airflow DAG: weather_historical_backfill
=========================================
PURPOSE: Historical batch loading ONLY.
  - This DAG is for backfilling historical weather data into the data lake.
  - Live/real-time weather is now handled by the Redpanda weather producer
    + Flink unified stream processor (weather.live topic).

When to run this DAG:
  - On first platform setup (to populate historical data)
  - When recovering from a producer outage (to fill gaps)
  - For compliance/audit purposes requiring complete historical record

Pattern: ELT
  Extract → Load raw JSON to MinIO Bronze → Transform in dbt
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "demo")
BUCKET = "geoplatform-bronze"
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

# NOTE: These are the same cities as the real-time producer
# so historical and stream data align on station_id
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
    "max_retry_delay": timedelta(hours=1),
    "email_on_failure": False,
}


def fetch_historical_weather(**context):
    """
    ELT Step 1: Extract historical/current snapshot.
    For historical backfill, this uses the current endpoint.
    For true historical data, use OpenWeatherMap History API (paid).
    """
    execution_date = context["execution_date"]
    results = []

    if not API_KEY or API_KEY == "demo":
        raise ValueError(
            "PRODUCTION ERROR: OPENWEATHER_API_KEY is missing or invalid. "
            "Mock data fallback has been removed (No Mock Data Policy)."
        )

    for city in CITIES:
        try:
            resp = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": API_KEY,
                    "units": "metric",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            results.append(
                {
                    "station_id": city["station_id"],
                    "city_name": city["name"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "raw_response": data,
                    "fetched_at": execution_date.isoformat(),
                    "source": "airflow_backfill",  # distinguish from stream data
                }
            )

        except Exception as e:
            logger.error(f"Failed to fetch for {city['name']}: {e}")

    context["ti"].xcom_push(key="weather_data", value=results)
    logger.info(f"Fetched historical weather for {len(results)} stations")


def load_raw_to_s3(**context):
    """ELT Step 2: Dump raw JSON to MinIO Bronze — no transformation."""
    weather_data = context["ti"].xcom_pull(
        key="weather_data", task_ids="fetch_historical"
    )
    execution_date = context["execution_date"]
    date_prefix = execution_date.strftime("%Y/%m/%d/%H")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    for item in weather_data:
        key = f"historical/weather/{date_prefix}/{item['station_id']}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(item),
            ContentType="application/json",
        )
        item["s3_path"] = f"s3://{BUCKET}/{key}"
        logger.info(f"  → Uploaded {key}")

    context["ti"].xcom_push(key="weather_with_paths", value=weather_data)


def load_to_postgres(**context):
    """Load historical snapshot into weather_observations (the batch table)."""
    weather_data = context["ti"].xcom_pull(
        key="weather_with_paths", task_ids="load_raw_s3"
    )
    execution_date = context["execution_date"]

    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()

    for item in weather_data:
        raw = item["raw_response"]
        main = raw.get("main", {})
        wind = raw.get("wind", {})
        clouds = raw.get("clouds", {})
        rain = raw.get("rain", {})

        cur.execute(
            """
            INSERT INTO weather_observations
                (station_id, observed_at, temperature_c, humidity_pct,
                 rainfall_mm, wind_speed_ms, cloud_cover_pct, location, raw_s3_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            ON CONFLICT (station_id, observed_at) DO NOTHING
            """,
            (
                item["station_id"],
                execution_date,
                main.get("temp"),
                main.get("humidity"),
                rain.get("1h", 0.0),
                wind.get("speed"),
                clouds.get("all"),
                item["longitude"],
                item["latitude"],
                item.get("s3_path", ""),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Loaded {len(weather_data)} historical weather records to PostgreSQL")


with DAG(
    dag_id="weather_historical_backfill",
    default_args=default_args,
    description=(
        "Historical weather ELT backfill. "
        "Real-time weather is handled by the Redpanda weather producer + Flink."
    ),
    schedule="0 0 * * *",  # Daily at midnight — for historical record only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "weather", "elt", "historical"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_historical",
        python_callable=fetch_historical_weather,
    )

    task_s3 = PythonOperator(
        task_id="load_raw_s3",
        python_callable=load_raw_to_s3,
    )

    task_pg = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
    )

    task_fetch >> task_s3 >> task_pg
