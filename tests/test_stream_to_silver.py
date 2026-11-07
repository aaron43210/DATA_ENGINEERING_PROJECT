"""Offline tests for the PostGIS streaming tables -> Iceberg Silver sync.

No PostgreSQL, Iceberg catalog or object store is contacted.
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "stream_to_silver", ROOT / "iceberg" / "stream_to_silver.py"
)
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)

SENSOR = next(t for t in sync.STREAM_TABLES if t.name == "sensor_records")


def fake_table(source_ids, has_snapshot=True):
    column = SimpleNamespace(to_pylist=lambda: list(source_ids))
    arrow = SimpleNamespace(column=lambda name: column)
    return SimpleNamespace(
        current_snapshot=lambda: object() if has_snapshot else None,
        scan=lambda selected_fields: SimpleNamespace(to_arrow=lambda: arrow),
    )


def test_watermark_is_zero_before_first_snapshot():
    assert sync.get_watermark(fake_table([], has_snapshot=False)) == 0


def test_watermark_is_highest_loaded_source_id():
    assert sync.get_watermark(fake_table([3, 41, 7])) == 41


def test_select_reads_only_committed_rows_after_watermark_in_id_order():
    sql = sync.select_sql(SENSOR)
    assert "FROM public.sensor_observations" in sql
    assert "WHERE id > %s" in sql
    assert "processed_at < now() - interval '1 minute'" in sql
    assert sql.rstrip().endswith("ORDER BY id LIMIT %s")


def test_rows_carry_source_id_and_utc_event_date():
    event_time = datetime(2026, 11, 7, 23, 30, tzinfo=timezone.utc)
    pg_row = {
        name: None for name in SENSOR.source_columns()
    } | {"id": 42, "sensor_id": "IOT_TVM_01", "event_time": event_time}
    loaded_at = datetime(2026, 11, 8, tzinfo=timezone.utc)

    [row] = sync.to_silver_rows([pg_row], loaded_at)

    assert row["source_id"] == 42
    assert "id" not in row
    assert row["sensor_id"] == "IOT_TVM_01"
    assert row["event_date"] == event_time.date()
    assert row["loaded_at"] == loaded_at
