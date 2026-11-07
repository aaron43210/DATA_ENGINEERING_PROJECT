"""Pure decoding, validation, scoring and join helpers used by the PyFlink stream job.

Kept free of PyFlink imports so it can be unit-tested offline.
"""
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
        **_source_fields(meta),
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


def _source_fields(meta):
    """Kafka coordinates plus original bytes, carried so late events can be quarantined intact."""
    return {
        "source_topic": str(meta["topic"]),
        "source_partition": int(meta["partition"]),
        "source_offset": int(meta["offset"]),
        "source_key_base64": meta.get("key_base64"),
        "source_value_base64": meta.get("value_base64"),
    }


def dead_letter(meta, reason):
    """Quarantine record. Accepts a raw source envelope or an already-validated event."""
    def pick(*names):
        return next((meta[name] for name in names if meta.get(name) is not None), None)
    return json.dumps({
        "source_topic": pick("topic", "source_topic"),
        "source_partition": pick("partition", "source_partition"),
        "source_offset": pick("offset", "source_offset"),
        "source_key_base64": pick("key_base64", "source_key_base64"),
        "payload_base64": pick("value_base64", "source_value_base64"),
        "reason": str(reason)[:1000],
    }, separators=(",", ":"))


# Reference weather stations; must match redpanda/weather_producer STATIONS.
WEATHER_STATIONS = {
    "WX_TVM": (8.5241, 76.9366),
    "WX_COK": (9.9312, 76.2673),
    "WX_CCJ": (11.2588, 75.7804),
    "WX_TCR": (10.5276, 76.2144),
    "WX_QLN": (8.8932, 76.6141),
}
# Ambient weather older than this (relative to the sensor reading) is not joined.
AMBIENT_MAX_AGE_MS = 2 * 60 * 60 * 1000


def haversine_km(lat1, lon1, lat2, lon2):
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def nearest_station(lat, lon, stations=None):
    stations = stations or WEATHER_STATIONS
    return min(stations, key=lambda sid: haversine_km(lat, lon, *stations[sid]))


def ambient_for(sensor_event_ms, weather):
    """Return (temperature, humidity) from a cached weather dict if fresh enough, else (None, None)."""
    if not weather or abs(sensor_event_ms - weather["event_time_ms"]) > AMBIENT_MAX_AGE_MS:
        return None, None
    return weather.get("temperature_c"), weather.get("humidity_pct")


# Producer quality flags. GOOD/STALE/ANOMALY carry a real observation; the rest are source errors.
WEATHER_OBSERVATION_FLAGS = {"GOOD", "STALE", "ANOMALY"}
WEATHER_SOURCE_ERROR_FLAGS = {"API_ERROR", "INVALID"}
WEATHER_NUMERIC = ("temperature_c", "humidity_pct", "wind_speed_ms", "cloud_cover_pct", "rainfall_1h_mm")


class WeatherSourceError(ValueError):
    """The producer reported an upstream API failure; not malformed input, but not an observation."""

    def __init__(self, station_id, flag, event_time_ms):
        super().__init__(f"producer reported {flag} for {station_id}")
        self.station_id, self.flag, self.event_time_ms = station_id, flag, event_time_ms


def validate_weather(record, meta):
    for field in ("station_id", "event_time", "latitude", "longitude", "api_poll_ms"):
        if record.get(field) is None:
            raise ValueError(f"missing required field: {field}")
    station_id = record["station_id"]
    if not isinstance(station_id, str) or not station_id.strip() or len(station_id.strip()) > 100:
        raise ValueError("station_id must be a non-empty string of at most 100 characters")
    flag = record.get("quality_flag")
    if flag in WEATHER_SOURCE_ERROR_FLAGS:
        raise WeatherSourceError(station_id.strip(), flag, epoch_millis(record["event_time"]))
    if flag not in WEATHER_OBSERVATION_FLAGS:
        raise ValueError(f"unsupported quality_flag: {flag!r}")
    coords = {}
    for name in ("latitude", "longitude"):
        value = record[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{name} must be numeric and finite")
        coords[name] = float(value)
    if not -90 <= coords["latitude"] <= 90 or not -180 <= coords["longitude"] <= 180:
        raise ValueError("coordinates are outside latitude/longitude bounds")
    numbers = {}
    for name in WEATHER_NUMERIC:
        value = record.get(name)
        if value is None:
            numbers[name] = None
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{name} must be numeric and finite")
        numbers[name] = float(value)
    if numbers["temperature_c"] is None:
        raise ValueError(f"{flag} weather observation has no temperature_c")
    poll_ms = record["api_poll_ms"]
    if isinstance(poll_ms, bool) or not isinstance(poll_ms, int) or not 0 <= poll_ms <= 2**31 - 1:
        raise ValueError("api_poll_ms must be a non-negative 32-bit integer")
    condition = record.get("weather_condition")
    city = record.get("city_name")
    return {
        **_source_fields(meta),
        "station_id": station_id.strip(),
        "city_name": city[:100] if isinstance(city, str) else None,
        "event_time_ms": epoch_millis(record["event_time"]),
        **coords,
        **numbers,
        "weather_condition": condition[:50] if isinstance(condition, str) else None,
        "api_poll_ms": poll_ms,
        "quality_flag": flag,
    }


def quality_alert(dataset, source_id, alert_type, value, event_time_ms, severity, **extra):
    """JSON payload for platform.quality.alerts. Deterministic so replays produce identical alerts."""
    return json.dumps({
        "alert_id": f"{dataset}:{source_id}:{alert_type}:{event_time_ms}",
        "dataset": dataset,
        "source_id": source_id,
        "alert_type": alert_type,
        "value": value,
        "event_time": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
        "severity": severity,
        **extra,
    }, separators=(",", ":"), default=str)
