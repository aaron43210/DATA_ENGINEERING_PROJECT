#!/usr/bin/env python3
"""
PostGIS streaming tables -> Iceberg Silver.

Flink writes live sensor and weather rows to PostGIS, which serves them with low
latency. This job copies those rows into Iceberg Silver so the streaming history is
versioned next to the batch Silver tables. Silver trails PostGIS by one schedule
interval plus COMMIT_GUARD.

The watermark is the highest PostGIS id already in Silver, read back from the Iceberg
table itself, so a crashed run resumes without gaps or duplicates.

Heavy imports (pyiceberg, pyarrow, psycopg2) stay inside functions so the pure
helpers can be tested without them.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, NamedTuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stream_to_silver")

BATCH_ROWS = int(os.environ.get("STREAM_SYNC_BATCH_ROWS", "10000"))
# Ids are assigned at insert, before commit: skipping very recent rows keeps a slower
# transaction with a lower id from committing after the watermark has passed it.
COMMIT_GUARD = "1 minute"


class StreamTable(NamedTuple):
    name: str  # Iceberg table in the silver namespace
    source: str  # PostGIS table written by Flink
    columns: List[tuple]  # (column, kind) copied as-is

    def source_columns(self) -> List[str]:
        return ["id"] + [name for name, _ in self.columns]


STREAM_TABLES = [
    StreamTable(
        "sensor_records",
        "sensor_observations",
        [
            ("source_topic", "string"),
            ("source_partition", "long"),
            ("source_offset", "long"),
            ("sensor_id", "string"),
            ("event_time", "timestamp"),
            ("latitude", "double"),
            ("longitude", "double"),
            ("measurement_type", "string"),
            ("value", "double"),
            ("unit", "string"),
            ("quality_flag", "string"),
            ("is_anomaly", "boolean"),
            ("nearest_weather_station", "string"),
            ("ambient_temperature_c", "double"),
            ("ambient_humidity_pct", "double"),
            ("processed_at", "timestamp"),
        ],
    ),
    StreamTable(
        "weather_stream_records",
        "weather_stream_observations",
        [
            ("source_topic", "string"),
            ("source_partition", "long"),
            ("source_offset", "long"),
            ("station_id", "string"),
            ("city_name", "string"),
            ("event_time", "timestamp"),
            ("latitude", "double"),
            ("longitude", "double"),
            ("temperature_c", "double"),
            ("humidity_pct", "double"),
            ("wind_speed_ms", "double"),
            ("cloud_cover_pct", "double"),
            ("rainfall_1h_mm", "double"),
            ("weather_condition", "string"),
            ("quality_flag", "string"),
            ("is_anomaly", "boolean"),
            ("processed_at", "timestamp"),
        ],
    ),
]


def silver_fields(table: StreamTable) -> List[tuple]:
    return (
        [("source_id", "long")]
        + table.columns
        + [("event_date", "date"), ("loaded_at", "timestamp")]
    )


def select_sql(table: StreamTable) -> str:
    return (
        f"SELECT {', '.join(table.source_columns())} FROM public.{table.source} "
        f"WHERE id > %s AND processed_at < now() - interval '{COMMIT_GUARD}' "
        "ORDER BY id LIMIT %s"
    )


def get_watermark(iceberg_table) -> int:
    """Highest PostGIS id already copied, or 0 for an empty table."""
    if iceberg_table.current_snapshot() is None:
        return 0
    # shortcut: scans the whole source_id column each run; fine at this data volume,
    # store the watermark in the snapshot summary once tables reach many millions of rows.
    ids = (
        iceberg_table.scan(selected_fields=("source_id",))
        .to_arrow()
        .column("source_id")
        .to_pylist()
    )
    return max(ids, default=0)


def to_silver_rows(
    pg_rows: Iterable[Dict[str, Any]], loaded_at: datetime
) -> List[Dict[str, Any]]:
    rows = []
    for pg_row in pg_rows:
        row = dict(pg_row)
        row["source_id"] = row.pop("id")
        row["event_date"] = row["event_time"].astimezone(timezone.utc).date()
        row["loaded_at"] = loaded_at
        rows.append(row)
    return rows


def _kinds():
    import pyarrow as pa
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        TimestamptzType,
    )

    return {
        "string": (StringType(), pa.string()),
        "long": (LongType(), pa.int64()),
        "double": (DoubleType(), pa.float64()),
        "boolean": (BooleanType(), pa.bool_()),
        "timestamp": (TimestamptzType(), pa.timestamp("us", tz="UTC")),
        "date": (DateType(), pa.date32()),
    }


def get_or_create_table(catalog, table: StreamTable, kinds):
    from bronze_to_silver import NAMESPACE
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import NestedField

    identifier = f"{NAMESPACE}.{table.name}"
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        schema = Schema(
            *[
                NestedField(
                    field_id=i, name=name, field_type=kinds[kind][0], required=False
                )
                for i, (name, kind) in enumerate(silver_fields(table), start=1)
            ]
        )
        spec = PartitionSpec(
            PartitionField(
                source_id=schema.find_field("event_date").field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="event_date",
            )
        )
        logger.info("Creating Iceberg table %s", identifier)
        return catalog.create_table(identifier, schema=schema, partition_spec=spec)


def sync_table(catalog, conn, table: StreamTable) -> int:
    import pyarrow as pa
    from psycopg2.extras import RealDictCursor

    kinds = _kinds()
    iceberg_table = get_or_create_table(catalog, table, kinds)
    arrow_schema = pa.schema(
        [(name, kinds[kind][1]) for name, kind in silver_fields(table)]
    )
    watermark = get_watermark(iceberg_table)
    written = 0
    while True:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(select_sql(table), (watermark, BATCH_ROWS))
            pg_rows = cur.fetchall()
        if not pg_rows:
            break
        rows = to_silver_rows(pg_rows, datetime.now(timezone.utc))
        iceberg_table.append(pa.Table.from_pylist(rows, schema=arrow_schema))
        watermark = rows[-1]["source_id"]
        written += len(rows)
        logger.info(
            "%s: appended %d rows up to id %d", table.name, len(rows), watermark
        )
        if len(pg_rows) < BATCH_ROWS:
            break
    return written


def main() -> int:
    import psycopg2
    from bronze_to_silver import env, silver_catalog

    catalog = silver_catalog()
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "geoplatform"),
        user=os.environ.get("POSTGRES_USER", "geoplatform"),
        password=env("POSTGRES_PASSWORD"),
        sslmode=os.environ.get("POSTGRES_SSLMODE", "require"),
    )
    try:
        total = sum(sync_table(catalog, conn, table) for table in STREAM_TABLES)
    finally:
        conn.close()
    logger.info("Stream Silver sync finished: %d rows written", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
