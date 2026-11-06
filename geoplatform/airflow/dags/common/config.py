"""
Shared configuration & helpers for geoplatform Airflow DAGs.

SECRETS POLICY
--------------
Credentials are read from environment variables that docker-compose injects
from `.env` (each declared with `:?set in .env`, so a missing value already
fails `docker compose up`). `require_env()` additionally fails LOUD at task
runtime if a secret is absent. NO plaintext secret fallback is baked into DAG
source. Non-secret identifiers (host, port, db name, user) keep sane defaults
so local dev still works; only the actual secrets are mandatory.

BRONZE LAYOUT
-------------
s3://<bucket>/source=<src>/ingest_date=<YYYY-MM-DD>/hour=<HH>/<file>.jsonl
Immutable + append-only, partitioned by source / date / hour.
"""

from __future__ import annotations

import contextlib
import logging
import os
from datetime import timedelta

from airflow.datasets import Dataset

logger = logging.getLogger(__name__)

# ── Object storage (MinIO) ───────────────────────────────────────────────────
BRONZE_BUCKET = os.environ.get("BUCKET_BRONZE", "geoplatform-bronze")
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def require_env(name: str) -> str:
    """Return env var ``name`` or raise loudly. No secret defaults in source."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. Secrets must be "
            "supplied via the environment (.env / Airflow Connection / secrets "
            "backend); there is deliberately no in-code fallback."
        )
    return val


def get_s3_client():
    """boto3 S3 client for MinIO. Credentials from env; fails loud if absent."""
    import boto3  # imported lazily so DAG parsing never needs boto3

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=require_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=require_env("MINIO_SECRET_KEY"),
    )


@contextlib.contextmanager
def pg_connection():
    """
    Managed psycopg2 connection to the geoplatform app DB.

    Commits on clean exit, rolls back on exception, always closes — so a failed
    insert can never leak a server-side connection. Password is mandatory
    (fail loud); other params are non-secret identifiers with local defaults.
    """
    import psycopg2  # lazy import — not needed at DAG parse time

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "geoplatform"),
        user=os.environ.get("POSTGRES_USER", "geoplatform"),
        password=require_env("POSTGRES_PASSWORD"),
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
    """
    Build a partitioned Bronze object key:
    ``source=<source>/ingest_date=<YYYY-MM-DD>/hour=<HH>/<filename>``
    """
    return (
        f"source={source}"
        f"/ingest_date={logical_dt.strftime('%Y-%m-%d')}"
        f"/hour={logical_dt.strftime('%H')}"
        f"/{filename}"
    )


def alert_on_failure(context):
    """
    ``on_failure_callback`` hook. Emits a structured failure log line; wire a
    Slack / PagerDuty webhook here for real paging (kept dependency-free so it
    can never itself fail the callback).
    """
    ti = context.get("task_instance")
    dag = context.get("dag")
    logger.error(
        "DAG FAILURE dag=%s task=%s run=%s logical_date=%s error=%s",
        getattr(dag, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        context.get("logical_date"),
        context.get("exception"),
    )
    # TODO (production alerting): POST to Slack/PagerDuty, e.g.
    #   import requests
    #   requests.post(require_env("SLACK_WEBHOOK_URL"),
    #                 json={"text": f"Airflow failed: {getattr(dag,'dag_id','?')}"},
    #                 timeout=10)


# Base default_args shared by ingestion DAGs — includes failure alerting.
BASE_DEFAULT_ARGS = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "on_failure_callback": alert_on_failure,
}

# ── Bronze datasets (data-aware scheduling hooks for the dbt layer) ───────────
# A future dbt transformation DAG can `schedule=[BRONZE_SENTINEL2, ...]` to run
# automatically whenever one of these partitions is refreshed.
BRONZE_SENTINEL2 = Dataset(f"s3://{BRONZE_BUCKET}/source=sentinel-2")
BRONZE_OSM = Dataset(f"s3://{BRONZE_BUCKET}/source=osm")
BRONZE_WEATHER = Dataset(f"s3://{BRONZE_BUCKET}/source=weather")

# Emitted by the dbt_transform DAG once Gold marts are rebuilt — downstream
# DAGs (e.g. data-quality) can schedule on this instead of a fixed clock.
GOLD_READY = Dataset("postgres://geoplatform/gold_marts")
