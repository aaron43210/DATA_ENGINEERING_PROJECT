"""
Shared configuration and helpers for the geoplatform Airflow DAGs.

Secrets come only from the environment (docker-compose injects them from
`.env`). There are deliberately no in-code fallbacks: a missing secret fails
loudly instead of silently connecting with a default password.

Bronze layout (immutable, append-only):
    s3://<bucket>/source=<src>/ingest_date=<YYYY-MM-DD>/hour=<HH>/<file>.jsonl
"""

from __future__ import annotations

import contextlib
import logging
import os
from datetime import timedelta

from airflow.datasets import Dataset

logger = logging.getLogger(__name__)

BRONZE_BUCKET = os.environ.get("BUCKET_BRONZE", "bronze")


def require_env(name: str) -> str:
    """Return env var ``name`` or raise. No secret defaults in source."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set.")
    return value


def get_s3_client():
    """boto3 S3 client configured by the standard AWS SDK env vars
    (AWS_ENDPOINT_URL_S3 points it at MinIO locally). Imported lazily to keep DAG
    parsing fast."""
    import boto3

    return boto3.client("s3")


@contextlib.contextmanager
def pg_connection(**connect_kwargs):
    """psycopg2 connection: commits on success, rolls back on error, always closes."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "geoplatform"),
        user=os.environ.get("POSTGRES_USER", "geoplatform"),
        password=require_env("POSTGRES_PASSWORD"),
        sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
        **connect_kwargs,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bronze_key(source: str, logical_dt, filename: str) -> str:
    """Build ``source=<s>/ingest_date=<YYYY-MM-DD>/hour=<HH>/<filename>``."""
    return (
        f"source={source}"
        f"/ingest_date={logical_dt.strftime('%Y-%m-%d')}"
        f"/hour={logical_dt.strftime('%H')}"
        f"/{filename}"
    )


def alert_on_failure(context):
    """Structured failure log line for every failed task."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    logger.error(
        "DAG FAILURE dag=%s task=%s run=%s error=%s",
        getattr(dag, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        context.get("exception"),
    )


BASE_DEFAULT_ARGS = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "on_failure_callback": alert_on_failure,
}

# Raw data has landed in Bronze: triggers the Bronze -> Silver (Iceberg) load.
BRONZE_SENTINEL2 = Dataset(f"s3://{BRONZE_BUCKET}/source=sentinel-2")
BRONZE_OSM = Dataset(f"s3://{BRONZE_BUCKET}/source=osm")
BRONZE_WEATHER = Dataset(f"s3://{BRONZE_BUCKET}/source=weather")

# Loaded into PostGIS and past the quality gate: safe for dbt to read.
POSTGIS_OSM = Dataset("postgres://postgres/geoplatform/public/vector_features")
POSTGIS_BOUNDARIES = Dataset("postgres://postgres/geoplatform/public/admin_boundaries")
