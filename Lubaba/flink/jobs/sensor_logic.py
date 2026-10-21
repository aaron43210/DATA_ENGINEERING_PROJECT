"""Pure sensor decoding, validation, and scoring helpers used by the PyFlink job."""
import base64
import json
import math
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO

import requests


SUPPORTED_UNITS = {
    "temperature": "celsius",
    "humidity": "percent",
    "soil_moisture": "raw",
    "air_quality": "aqi",
}
SCHEMA_REGISTRY_URL = "http://redpanda:8081"


class TransientDependencyError(RuntimeError):
    """A temporary Schema Registry failure must fail/restart the Flink task."""


def decode_envelope(envelope, registry_url=None, get_schema=None):
    """Decode Java source envelope and Confluent wire-format Avro into a dict.

    The project's schema declares event_time as a plain Avro long, so fastavro
    returns an integer in epoch milliseconds. Datetime is also normalized for
    compatible future schemas that add timestamp-millis logicalType.
    """
    meta = json.loads(envelope) if isinstance(envelope, str) else envelope
    raw = base64.b64decode(meta["value_base64"], validate=True)
    if len(raw) < 5 or raw[0] != 0:
        raise ValueError("invalid Confluent Avro wire header")
    schema_id = int.from_bytes(raw[1:5], "big")
    import fastavro
    schema = _fetch_schema(schema_id, registry_url or SCHEMA_REGISTRY_URL, get_schema)
    record = fastavro.schemaless_reader(BytesIO(raw[5:]), schema)
    if not isinstance(record, dict):
        raise ValueError("Avro payload must decode to a record")
    return meta, record


@lru_cache(maxsize=256)
def _fetch_schema(schema_id, registry_url, get_schema=None):
    import fastavro
    if get_schema:
        response = get_schema(schema_id)
    else:
        try:
            response = requests.get(f"{registry_url.rstrip('/')}/schemas/ids/{schema_id}", timeout=5)
        except requests.RequestException as exc:
            raise TransientDependencyError("Schema Registry request failed") from exc
    status = getattr(response, "status_code", 200)
    if status == 429 or status >= 500:
        raise TransientDependencyError(f"Schema Registry returned HTTP {status}")
    response.raise_for_status()
    body = response.json()
    schema_text = body.get("schema")
    if not schema_text:
        raise ValueError(f"Schema Registry response has no schema for id {schema_id}")
    return fastavro.parse_schema(json.loads(schema_text))


def epoch_millis(value):
    if isinstance(value, bool):
        raise ValueError("event_time must be epoch milliseconds")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("logical timestamp must include a timezone")
        return int(value.astimezone(timezone.utc).timestamp() * 1000)
    if not isinstance(value, int) or value < 0:
        raise ValueError("event_time must be a non-negative epoch-millisecond integer")
    try:
        datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("event_time is outside the supported date range") from exc
    return value


def validate_sensor(record, meta):
    required = ("sensor_id", "event_time", "latitude", "longitude", "measurement_type", "value", "unit")
    missing = [field for field in required if field not in record or record[field] is None]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")
    sensor_id = record["sensor_id"]
    if not isinstance(sensor_id, str) or not sensor_id.strip() or len(sensor_id.strip()) > 100:
        raise ValueError("sensor_id must be a non-empty string of at most 100 characters")
    measurement = record["measurement_type"]
    if measurement not in SUPPORTED_UNITS:
        raise ValueError(f"unsupported measurement_type: {measurement!r}")
    if record["unit"] != SUPPORTED_UNITS[measurement]:
        raise ValueError(f"unsupported unit {record['unit']!r} for {measurement}")
    values = {}
    for name in ("latitude", "longitude", "value"):
        value = record[name]
        if isinstance(value, bool):
            raise ValueError(f"{name} must be numeric and finite")
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be numeric and finite") from exc
        if not math.isfinite(value):
            raise ValueError(f"{name} must be numeric and finite")
        values[name] = value
    if not -90 <= values["latitude"] <= 90 or not -180 <= values["longitude"] <= 180:
        raise ValueError("coordinates are outside latitude/longitude bounds")
    timestamp = epoch_millis(record["event_time"])
    return {
        "source_topic": str(meta["topic"]),
        "source_partition": int(meta["partition"]),
        "source_offset": int(meta["offset"]),
        "sensor_id": sensor_id.strip(),
        "event_time_ms": timestamp,
        "latitude": values["latitude"],
        "longitude": values["longitude"],
        "measurement_type": measurement,
        "value": values["value"],
        "unit": record["unit"],
        "quality_flag": record.get("quality_flag"),
    }


def score_reading(history, value, warmup_count=5, z_threshold=3.0):
    """Score against prior values, then return the bounded updated history.

    Constant baseline policy: repeated same values are normal; any different
    value after warm-up is anomalous because a z-score has zero denominator.
    """
    prior = [float(item) for item in history]
    reading = float(value)
    anomalous = False
    if len(prior) >= warmup_count:
        mean = sum(prior) / len(prior)
        variance = sum((item - mean) ** 2 for item in prior) / len(prior)
        stdev = math.sqrt(variance)
        if stdev <= 1e-12:
            anomalous = not math.isclose(reading, mean, rel_tol=0.0, abs_tol=1e-12)
        else:
            anomalous = abs((reading - mean) / stdev) > z_threshold
    updated = (prior + [reading])[-30:]
    return anomalous, updated


def is_late_event(event_time_ms, current_watermark_ms):
    """The policy drops events at/before the emitted watermark to the DLT."""
    return current_watermark_ms >= 0 and event_time_ms <= current_watermark_ms


def dead_letter(meta, reason):
    return json.dumps({
        "source_topic": meta.get("topic"),
        "source_partition": meta.get("partition"),
        "source_offset": meta.get("offset"),
        "source_key_base64": meta.get("key_base64"),
        "payload_base64": meta.get("value_base64"),
        "reason": str(reason)[:1000],
    }, separators=(",", ":"))
