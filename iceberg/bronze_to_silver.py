#!/usr/bin/env python3
"""
Bronze -> Silver loader (Owner: Angel)
======================================
Reads JSONL objects from MinIO Bronze, cleans them into typed rows and writes
them to Iceberg tables (Parquet on MinIO) through the REST catalog.

Each Bronze object is loaded once. Rows carry their `bronze_key`, so the loader
can see what is already in Silver, and each write replaces that key's rows, which
keeps a re-run after a crash from duplicating data.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Set

import boto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_to_silver")

NAMESPACE = "silver"
INGEST_DATE = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})/")


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set.")
    return value


def timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def osm_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tags = item.get("tags")
    if not tags or "id" not in item:
        return None
    return {
        "osm_type": item.get("type"),
        "osm_id": item["id"],
        "name": tags.get("name") or tags.get("name:en"),
        "amenity": tags.get("amenity"),
        "highway": tags.get("highway"),
        "tags_json": json.dumps(tags, sort_keys=True),
        "geometry_json": json.dumps(
            item.get("geometry") or {"lat": item.get("lat"), "lon": item.get("lon")}
        ),
    }


def sentinel_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = item.get("properties", {})
    acquired = timestamp(props.get("datetime"))
    if not item.get("id") or acquired is None:
        return None
    return {
        "scene_id": item["id"],
        "platform": props.get("platform"),
        "acquisition_time": acquired,
        "cloud_percentage": props.get("eo:cloud_cover"),
        "footprint_geojson": json.dumps(item.get("geometry")),
    }


def weather_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    observed = timestamp(item.get("observed_at"))
    if not item.get("station_id") or observed is None:
        return None
    raw = item.get("raw_response", {})
    main, wind = raw.get("main", {}), raw.get("wind", {})
    return {
        "station_id": item["station_id"],
        "observed_at": observed,
        "temperature_c": main.get("temp"),
        "humidity_pct": main.get("humidity"),
        "rainfall_mm": raw.get("rain", {}).get("1h", 0.0),
        "wind_speed_ms": wind.get("speed"),
        "cloud_cover_pct": raw.get("clouds", {}).get("all"),
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
    }


AIR_POLLUTANTS = ["pm2_5", "pm10", "no2", "o3", "so2", "co", "nh3", "no"]


def air_quality_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    readings = item.get("raw_response", {}).get("list") or []
    if not item.get("station_id") or not readings:
        return None
    reading = readings[0]
    parts = reading.get("components", {})
    return {
        "station_id": item["station_id"],
        "event_time": datetime.fromtimestamp(reading["dt"], timezone.utc),
        "aqi": reading.get("main", {}).get("aqi"),
        **{p: parts.get(p) for p in AIR_POLLUTANTS},
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
    }


class SilverTable(NamedTuple):
    name: str
    source: str  # Bronze `source=` partition
    columns: List[tuple]  # (name, iceberg type, arrow type)
    to_row: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]


STRING = (StringType(), pa.string())
DOUBLE = (DoubleType(), pa.float64())
LONG = (LongType(), pa.int64())
INSTANT = (TimestamptzType(), pa.timestamp("us", tz="UTC"))

TABLES = [
    SilverTable(
        "osm_features",
        "osm",
        [
            ("osm_type", *STRING),
            ("osm_id", *LONG),
            ("name", *STRING),
            ("amenity", *STRING),
            ("highway", *STRING),
            ("tags_json", *STRING),
            ("geometry_json", *STRING),
        ],
        osm_row,
    ),
    SilverTable(
        "sentinel2_scenes",
        "sentinel-2",
        [
            ("scene_id", *STRING),
            ("platform", *STRING),
            ("acquisition_time", *INSTANT),
            ("cloud_percentage", *DOUBLE),
            ("footprint_geojson", *STRING),
        ],
        sentinel_row,
    ),
    SilverTable(
        "weather_observations",
        "weather",
        [
            ("station_id", *STRING),
            ("observed_at", *INSTANT),
            ("temperature_c", *DOUBLE),
            ("humidity_pct", *DOUBLE),
            ("rainfall_mm", *DOUBLE),
            ("wind_speed_ms", *DOUBLE),
            ("cloud_cover_pct", *DOUBLE),
            ("latitude", *DOUBLE),
            ("longitude", *DOUBLE),
        ],
        weather_row,
    ),
    SilverTable(
        "air_quality_observations",
        "air_quality",
        [
            ("station_id", *STRING),
            ("event_time", *INSTANT),
            ("aqi", *LONG),
            *[(p, *DOUBLE) for p in AIR_POLLUTANTS],
            ("latitude", *DOUBLE),
            ("longitude", *DOUBLE),
        ],
        air_quality_row,
    ),
]

# Added to every table for lineage and partitioning.
COMMON_COLUMNS = [
    ("bronze_key", *STRING),
    ("ingest_date", DateType(), pa.date32()),
    ("loaded_at", *INSTANT),
]


def iceberg_schema(table: SilverTable) -> Schema:
    columns = table.columns + COMMON_COLUMNS
    return Schema(
        *[
            NestedField(field_id=i, name=name, field_type=ice, required=False)
            for i, (name, ice, _) in enumerate(columns, start=1)
        ]
    )


def arrow_schema(table: SilverTable) -> pa.Schema:
    return pa.schema(
        [(name, arrow) for name, _, arrow in table.columns + COMMON_COLUMNS]
    )


def get_or_create_table(catalog, table: SilverTable):
    identifier = f"{NAMESPACE}.{table.name}"
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        schema = iceberg_schema(table)
        spec = PartitionSpec(
            PartitionField(
                source_id=schema.find_field("ingest_date").field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="ingest_date",
            )
        )
        logger.info("Creating Iceberg table %s", identifier)
        return catalog.create_table(identifier, schema=schema, partition_spec=spec)


def list_keys(s3, bucket: str, source: str) -> Iterable[str]:
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"source={source}/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            yield obj["Key"]


def loaded_keys(iceberg_table) -> Set[str]:
    if iceberg_table.current_snapshot() is None:
        return set()
    rows = iceberg_table.scan(selected_fields=("bronze_key",)).to_arrow()
    return set(rows.column("bronze_key").to_pylist())


def read_rows(s3, bucket: str, key: str, table: SilverTable) -> List[Dict[str, Any]]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    match = INGEST_DATE.search(key)
    ingest_date = date.fromisoformat(match.group(1)) if match else None
    now = datetime.now(timezone.utc)

    rows = []
    for line in body.splitlines():
        if not line.strip():
            continue
        row = table.to_row(json.loads(line))
        if row is not None:
            row.update(bronze_key=key, ingest_date=ingest_date, loaded_at=now)
            rows.append(row)
    return rows


def load_table(catalog, s3, bucket: str, table: SilverTable) -> int:
    iceberg_table = get_or_create_table(catalog, table)
    done = loaded_keys(iceberg_table)
    written = 0
    for key in list_keys(s3, bucket, table.source):
        if key in done:
            continue
        rows = read_rows(s3, bucket, key, table)
        if not rows:
            logger.warning("No usable rows in s3://%s/%s", bucket, key)
            continue
        batch = pa.Table.from_pylist(rows, schema=arrow_schema(table))
        iceberg_table.overwrite(batch, overwrite_filter=EqualTo("bronze_key", key))
        written += len(rows)
        logger.info("%s: wrote %d rows from %s", table.name, len(rows), key)
    return written


def s3_client():
    """boto3 S3 client from the standard AWS SDK env vars (AWS_ENDPOINT_URL_S3 = MinIO)."""
    return boto3.client("s3")


def s3_properties() -> Dict[str, str]:
    """PyIceberg FileIO settings from the same AWS SDK env vars.

    Without AWS_ENDPOINT_URL_S3 and keys (on AWS) PyIceberg uses real S3 and the
    default credential chain, i.e. the IAM role.
    """
    props = {"s3.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
    if endpoint:
        props.update({"s3.endpoint": endpoint, "s3.path-style-access": "true"})
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        props["s3.access-key-id"] = os.environ["AWS_ACCESS_KEY_ID"]
        props["s3.secret-access-key"] = env("AWS_SECRET_ACCESS_KEY")
    return props


def silver_catalog():
    """Iceberg REST catalog for the Silver tables, with the Silver namespace present."""
    catalog = load_catalog(
        "geoplatform",
        **{"type": "rest", "uri": env("ICEBERG_REST_URI"), **s3_properties()},
    )
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog


def main() -> int:
    s3 = s3_client()
    catalog = silver_catalog()
    bucket = os.environ.get("BUCKET_BRONZE", "bronze")

    total = sum(load_table(catalog, s3, bucket, table) for table in TABLES)
    logger.info("Silver load finished: %d rows written", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
