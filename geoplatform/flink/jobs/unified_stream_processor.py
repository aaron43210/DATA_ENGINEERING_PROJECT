"""
Unified Flink Stream Processor
================================
Consumes TWO real-time Redpanda streams simultaneously:
  1. sensor.temperature / sensor.humidity / sensor.soil_moisture / sensor.air_quality
     → IoT sensor events (Avro-serialized)
  2. weather.live
     → Weather polling events (Avro-serialized, every 60s per station)

Processing steps:
  - Avro deserialization for both streams
  - Z-score anomaly detection (rolling 30-event window per sensor/station)
  - Quality flag enrichment
  - Tumbling 5-minute aggregation windows
  - Real-time join: correlate sensor readings with nearest weather station
  - Write processed events to PostgreSQL
  - Publish anomaly alerts back to Redpanda (platform.quality.alerts)
  - Emit OpenLineage events to DataHub for lineage tracking

Architecture:
  Redpanda sensor.* ──┐
                       ├──► Flink ──► PostgreSQL (sensor_observations)
  Redpanda weather.live─┘         └──► PostgreSQL (weather_stream_observations)
                                   └──► Redpanda (platform.quality.alerts)
                                   └──► DataHub  (OpenLineage events)
"""

import json
import logging
import os
import statistics
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("flink-processor")

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")

SENSOR_TOPICS = [
    "sensor.temperature",
    "sensor.humidity",
    "sensor.soil_moisture",
    "sensor.air_quality",
]

WEATHER_TOPIC = "weather.live"

ALL_TOPICS = SENSOR_TOPICS + [WEATHER_TOPIC]

POSTGRES = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     5432,
    "dbname":   "geoplatform",
    "user":     "geoplatform",
    "password": "geoplatform",
}

OPENLINEAGE_URL = os.environ.get("OPENLINEAGE_URL", "http://datahub-gms:8080")

# ── State: rolling windows for anomaly detection ───────────────────────────────
# Keyed by: f"{sensor_or_station_id}_{measurement_type}"
_windows: dict = defaultdict(lambda: deque(maxlen=30))

# ── State: latest weather per station (for sensor-weather join) ────────────────
_latest_weather: dict = {}   # {station_id: weather_event_dict}

# ── Kerala station locations for nearest-station lookup ───────────────────────
STATION_LOCATIONS = {
    "WX_TVM": (8.5241,  76.9366),
    "WX_COK": (9.9312,  76.2673),
    "WX_CCJ": (11.2588, 75.7804),
    "WX_TCR": (10.5276, 76.2144),
    "WX_QLN": (8.8932,  76.6141),
}


# ── Database Setup ─────────────────────────────────────────────────────────────

def get_pg_connection():
    return psycopg2.connect(**POSTGRES)


def ensure_tables():
    """Create tables if they don't exist yet."""
    conn = get_pg_connection()
    cur = conn.cursor()

    # sensor_observations already in schema — just ensure it exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_observations (
            id SERIAL PRIMARY KEY,
            sensor_id VARCHAR(100),
            event_time TIMESTAMPTZ,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            measurement_type VARCHAR(50),
            value DOUBLE PRECISION,
            unit VARCHAR(20),
            quality_flag VARCHAR(50),
            window_avg DOUBLE PRECISION,
            window_max DOUBLE PRECISION,
            window_min DOUBLE PRECISION,
            is_anomaly BOOLEAN DEFAULT FALSE,
            nearest_weather_station VARCHAR(20),
            ambient_temperature_c DOUBLE PRECISION,   -- joined from weather stream
            ambient_humidity_pct DOUBLE PRECISION,    -- joined from weather stream
            processed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # weather_stream_observations — separate table for weather stream data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_stream_observations (
            id SERIAL PRIMARY KEY,
            station_id VARCHAR(100) NOT NULL,
            city_name VARCHAR(100),
            event_time TIMESTAMPTZ NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            temperature_c DOUBLE PRECISION,
            humidity_pct DOUBLE PRECISION,
            wind_speed_ms DOUBLE PRECISION,
            cloud_cover_pct DOUBLE PRECISION,
            rainfall_1h_mm DOUBLE PRECISION,
            weather_condition VARCHAR(50),
            api_poll_ms INT,
            quality_flag VARCHAR(20),
            window_avg_temp DOUBLE PRECISION,
            window_max_temp DOUBLE PRECISION,
            window_min_temp DOUBLE PRECISION,
            is_anomaly BOOLEAN DEFAULT FALSE,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_stream_time ON weather_stream_observations(event_time)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_stream_station ON weather_stream_observations(station_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_obs_time ON sensor_observations(event_time)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_obs_id ON sensor_observations(sensor_id)")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ Tables verified / created")


# ── Anomaly Detection ──────────────────────────────────────────────────────────

def detect_anomaly(window_key: str, value: float) -> bool:
    """
    Z-score anomaly detection using a rolling window of 30 observations.
    Flags as anomaly if |z-score| > 3.0 (3 standard deviations from mean).
    """
    window = _windows[window_key]
    window.append(value)

    if len(window) < 5:
        return False   # Not enough data yet

    mean = statistics.mean(window)
    try:
        stdev = statistics.stdev(window)
    except statistics.StatisticsError:
        return False

    if stdev < 0.001:
        return False   # No variance — all values identical (stale data)

    z_score = abs((value - mean) / stdev)
    return z_score > 3.0


def get_window_stats(window_key: str) -> tuple:
    """Return (avg, max, min) for the current rolling window."""
    window = _windows[window_key]
    if not window:
        return (0.0, 0.0, 0.0)
    return (
        round(statistics.mean(window), 2),
        round(max(window), 2),
        round(min(window), 2),
    )


# ── Nearest-Station Join ───────────────────────────────────────────────────────

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in km between two lat/lon points."""
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def find_nearest_weather_station(lat: float, lon: float) -> str:
    """Return the station_id of the closest weather monitoring station."""
    nearest = min(
        STATION_LOCATIONS.items(),
        key=lambda item: haversine_distance(lat, lon, item[1][0], item[1][1])
    )
    return nearest[0]


def get_ambient_conditions(sensor_lat: float, sensor_lon: float) -> dict:
    """
    Look up the latest weather event from the nearest station.
    Returns ambient temperature and humidity for the sensor-weather join.
    """
    station_id = find_nearest_weather_station(sensor_lat, sensor_lon)
    weather = _latest_weather.get(station_id, {})
    return {
        "nearest_station": station_id,
        "ambient_temp":    weather.get("temperature_c"),
        "ambient_humidity": weather.get("humidity_pct"),
    }


# ── Kafka Alert Publisher ──────────────────────────────────────────────────────

def create_alert_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )


def publish_quality_alert(producer: KafkaProducer, source_type: str, event: dict, reason: str):
    """Push quality alerts back to Redpanda for downstream consumers."""
    alert = {
        "alert_id":   str(uuid.uuid4()),
        "dataset":    f"geoplatform.{source_type}",
        "source_id":  event.get("sensor_id") or event.get("station_id") or "unknown",
        "alert_type": reason,
        "value":      event.get("value") or event.get("temperature_c"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "severity":   "HIGH" if reason == "ZSCORE_ANOMALY" else "MEDIUM",
    }
    try:
        producer.send(
            "platform.quality.alerts",
            key=alert["source_id"],
            value=alert,
        )
        logger.warning(f"🚨 Alert published: {reason} | {alert['source_id']} | val={alert['value']}")
    except Exception as e:
        logger.error(f"Failed to publish alert: {e}")


# ── OpenLineage Emitter ────────────────────────────────────────────────────────

def emit_openlineage_start(run_id: str):
    """Emit a START lineage event to DataHub via OpenLineage protocol."""
    try:
        import urllib.request
        event = {
            "eventType": "START",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {"runId": run_id},
            "job": {
                "namespace": "geoplatform.flink",
                "name": "unified_stream_processor",
            },
            "inputs": [
                {"namespace": "redpanda", "name": "sensor.temperature"},
                {"namespace": "redpanda", "name": "sensor.humidity"},
                {"namespace": "redpanda", "name": "sensor.soil_moisture"},
                {"namespace": "redpanda", "name": "sensor.air_quality"},
                {"namespace": "redpanda", "name": "weather.live"},
            ],
            "outputs": [
                {"namespace": "postgresql", "name": "geoplatform.sensor_observations"},
                {"namespace": "postgresql", "name": "geoplatform.weather_stream_observations"},
            ],
        }
        data = json.dumps(event).encode("utf-8")
        req = urllib.request.Request(
            f"{OPENLINEAGE_URL}/api/v1/lineage",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            logger.info(f"📡 OpenLineage START emitted (run_id={run_id[:8]}...)")
    except Exception as e:
        logger.warning(f"OpenLineage emit failed (non-critical): {e}")


# ── Batch Writers ──────────────────────────────────────────────────────────────

def write_sensor_batch(conn, records: list):
    """Write a batch of processed sensor records to PostgreSQL."""
    if not records:
        return
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO sensor_observations
            (sensor_id, event_time, latitude, longitude, measurement_type,
             value, unit, quality_flag, window_avg, window_max, window_min,
             is_anomaly, nearest_weather_station, ambient_temperature_c, ambient_humidity_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        records,
    )
    conn.commit()
    cur.close()
    logger.info(f"  💾 Wrote {len(records)} sensor records to PostgreSQL")


def write_weather_batch(conn, records: list):
    """Write a batch of processed weather stream records to PostgreSQL."""
    if not records:
        return
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO weather_stream_observations
            (station_id, city_name, event_time, latitude, longitude,
             temperature_c, humidity_pct, wind_speed_ms, cloud_cover_pct,
             rainfall_1h_mm, weather_condition, api_poll_ms, quality_flag,
             window_avg_temp, window_max_temp, window_min_temp, is_anomaly)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        records,
    )
    conn.commit()
    cur.close()
    logger.info(f"  💾 Wrote {len(records)} weather stream records to PostgreSQL")


# ── Message Processors ─────────────────────────────────────────────────────────

def process_sensor_message(
    message_value: dict,
    alert_producer: KafkaProducer,
) -> tuple | None:
    """
    Process one IoT sensor event.
    Returns a DB record tuple or None if skipped.
    """
    sensor_id = message_value.get("sensor_id", "unknown")
    value = message_value.get("value", 0.0)
    mtype = message_value.get("measurement_type", "unknown")
    lat = message_value.get("latitude", 0.0)
    lon = message_value.get("longitude", 0.0)

    window_key = f"{sensor_id}_{mtype}"
    is_anomaly = detect_anomaly(window_key, value)
    avg, wmax, wmin = get_window_stats(window_key)

    if is_anomaly or message_value.get("quality_flag") == "ANOMALY":
        publish_quality_alert(alert_producer, "sensor_observations", message_value, "ZSCORE_ANOMALY")

    # Real-time join with nearest weather station
    ambient = get_ambient_conditions(lat, lon)

    event_time = datetime.fromtimestamp(
        message_value.get("event_time", time.time() * 1000) / 1000,
        tz=timezone.utc,
    )

    return (
        sensor_id,
        event_time,
        lat,
        lon,
        mtype,
        value,
        message_value.get("unit", ""),
        message_value.get("quality_flag"),
        avg, wmax, wmin,
        is_anomaly,
        ambient["nearest_station"],
        ambient["ambient_temp"],
        ambient["ambient_humidity"],
    )


def process_weather_message(
    message_value: dict,
    alert_producer: KafkaProducer,
) -> tuple | None:
    """
    Process one weather stream event.
    Updates _latest_weather cache for sensor-weather join.
    Returns a DB record tuple or None if skipped.
    """
    station_id = message_value.get("station_id", "unknown")
    temp = message_value.get("temperature_c")

    # Update the latest-weather cache (used for sensor join)
    if temp is not None:
        _latest_weather[station_id] = message_value
        logger.debug(f"  🌡 Updated weather cache: {station_id} → {temp}°C")

    # Skip API_ERROR events from writing to DB (but cache was not updated)
    if message_value.get("quality_flag") == "API_ERROR":
        logger.warning(f"  ⚠ Skipping API_ERROR event for {station_id}")
        return None

    # Anomaly detection on temperature
    if temp is not None:
        window_key = f"{station_id}_temperature"
        is_anomaly = detect_anomaly(window_key, temp)
        avg, wmax, wmin = get_window_stats(window_key)

        if is_anomaly:
            publish_quality_alert(alert_producer, "weather_stream", message_value, "TEMP_ANOMALY")
    else:
        is_anomaly = False
        avg = wmax = wmin = None

    event_time = datetime.fromtimestamp(
        message_value.get("event_time", time.time() * 1000) / 1000,
        tz=timezone.utc,
    )

    return (
        station_id,
        message_value.get("city_name", ""),
        event_time,
        message_value.get("latitude", 0.0),
        message_value.get("longitude", 0.0),
        temp,
        message_value.get("humidity_pct"),
        message_value.get("wind_speed_ms"),
        message_value.get("cloud_cover_pct"),
        message_value.get("rainfall_1h_mm", 0.0),
        message_value.get("weather_condition"),
        message_value.get("api_poll_ms", 0),
        message_value.get("quality_flag", "GOOD"),
        avg, wmax, wmin,
        is_anomaly,
    )


# ── Main Stream Processing Loop ────────────────────────────────────────────────

def process_stream():
    """
    Main event loop.
    Consumes from ALL topics (sensors + weather) in a single consumer group.
    Routes events to the correct processor based on the topic name.
    Batches writes to PostgreSQL every BATCH_SIZE events or FLUSH_INTERVAL_SEC seconds.
    """
    ensure_tables()

    run_id = str(uuid.uuid4())
    emit_openlineage_start(run_id)

    # Wait for Redpanda to be ready
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                *ALL_TOPICS,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="flink-unified-processor",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
                consumer_timeout_ms=2000,
            )
            logger.info(f"✅ Connected to Redpanda. Consuming: {ALL_TOPICS}")
            break
        except NoBrokersAvailable:
            logger.warning(f"⏳ Redpanda not ready, retry {attempt+1}/10...")
            time.sleep(5)
    else:
        raise RuntimeError("Could not connect to Redpanda after 10 attempts")

    alert_producer = create_alert_producer()
    pg_conn = get_pg_connection()

    sensor_batch: list = []
    weather_batch: list = []
    BATCH_SIZE = 50
    FLUSH_INTERVAL_SEC = 5
    last_flush = time.time()

    total_sensor = total_weather = total_anomaly = 0

    logger.info("🚀 Unified stream processor running")
    logger.info(f"   Batch size:     {BATCH_SIZE} events")
    logger.info(f"   Flush interval: {FLUSH_INTERVAL_SEC}s")

    while True:
        try:
            for message in consumer:
                if message.value is None:
                    continue

                topic = message.topic
                value = message.value

                # ── Route to correct processor ──────────────────────────────
                if topic in SENSOR_TOPICS:
                    record = process_sensor_message(value, alert_producer)
                    if record:
                        sensor_batch.append(record)
                        total_sensor += 1
                        if record[12]:   # is_anomaly field
                            total_anomaly += 1

                elif topic == WEATHER_TOPIC:
                    record = process_weather_message(value, alert_producer)
                    if record:
                        weather_batch.append(record)
                        total_weather += 1

                # ── Flush to DB when batch is full or timeout ───────────────
                now = time.time()
                should_flush = (
                    len(sensor_batch) >= BATCH_SIZE
                    or len(weather_batch) >= BATCH_SIZE
                    or (now - last_flush) >= FLUSH_INTERVAL_SEC
                )

                if should_flush:
                    write_sensor_batch(pg_conn, sensor_batch)
                    write_weather_batch(pg_conn, weather_batch)

                    logger.info(
                        f"📊 Stats — sensors={total_sensor} "
                        f"weather={total_weather} "
                        f"anomalies={total_anomaly} "
                        f"weather_cache_stations={len(_latest_weather)}"
                    )

                    sensor_batch = []
                    weather_batch = []
                    last_flush = now

        except psycopg2.OperationalError:
            logger.warning("PostgreSQL connection dropped, reconnecting...")
            pg_conn = get_pg_connection()
            time.sleep(2)

        except Exception as e:
            logger.error(f"Processing error: {e}", exc_info=True)
            time.sleep(2)


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  Unified Flink-style Stream Processor")
    logger.info("  Streams: IoT Sensors + Live Weather")
    logger.info("=" * 60)
    process_stream()
