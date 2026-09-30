"""
Weather Streaming Producer — Redpanda (Kafka-compatible)
=========================================================
Pattern: Polling-based Streaming
  - Polls OpenWeatherMap REST API every POLL_INTERVAL_SECONDS
  - Serializes each response as an Avro event
  - Pushes to Redpanda topic: weather.live
  - Schema is registered with Redpanda's built-in Schema Registry

This is the standard industry pattern for REST-based data sources
that don't offer WebSocket push (e.g., weather APIs, financial tickers).

Flow:
  OpenWeatherMap API → [this producer, every 60s] → Redpanda weather.live → Flink
"""

import os
import time
import logging
import random
from datetime import datetime, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("weather-producer")

# ── Configuration ──────────────────────────────────────────────────────────────
REDPANDA_BROKERS    = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "demo")
POLL_INTERVAL_SEC   = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))  # poll every 60s
TOPIC               = "weather.live"
OWM_API_URL         = "https://api.openweathermap.org/data/2.5/weather"

# ── Kerala monitoring stations ─────────────────────────────────────────────────
STATIONS = [
    {"station_id": "WX_TVM", "city_name": "Thiruvananthapuram", "lat": 8.5241,  "lon": 76.9366},
    {"station_id": "WX_COK", "city_name": "Kochi",              "lat": 9.9312,  "lon": 76.2673},
    {"station_id": "WX_CCJ", "city_name": "Kozhikode",          "lat": 11.2588, "lon": 75.7804},
    {"station_id": "WX_TCR", "city_name": "Thrissur",           "lat": 10.5276, "lon": 76.2144},
    {"station_id": "WX_QLN", "city_name": "Kollam",             "lat": 8.8932,  "lon": 76.6141},
]

# Load Avro schema from file
SCHEMA_STR = open(os.path.join(os.path.dirname(__file__), "weather_event.avsc")).read()

# ── Anomaly thresholds for quality flagging ────────────────────────────────────
ANOMALY_THRESHOLDS = {
    "temperature_c":   (-5.0,  50.0),   # Kerala extreme bounds
    "humidity_pct":    (0.0,   100.0),
    "wind_speed_ms":   (0.0,   60.0),   # 60 m/s = Category 5 hurricane
    "rainfall_1h_mm":  (0.0,   200.0),  # 200mm/hr is extreme cloudburst
}

# Rolling buffer to detect stale data (same temp twice = possible stale API response)
_last_temperature: dict = {}


def delivery_report(err, msg):
    """Kafka/Redpanda delivery callback."""
    if err:
        logger.error(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        logger.debug(f"✅ Delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def fetch_weather(station: dict) -> dict | None:
    """
    Poll OpenWeatherMap for one station.
    Returns parsed response dict, or None on failure.
    """
    if OPENWEATHER_API_KEY == "demo" or not OPENWEATHER_API_KEY:
        raise ValueError("PRODUCTION ERROR: OPENWEATHER_API_KEY is missing or invalid. Mock data fallback has been removed.")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _do_fetch():
        t0 = time.time()
        resp = requests.get(
            OWM_API_URL,
            params={"lat": station["lat"], "lon": station["lon"],
                    "appid": OPENWEATHER_API_KEY, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json(), int((time.time() - t0) * 1000)

    try:
        data, api_poll_ms = _do_fetch()
        return {
            "temperature_c":    data.get("main", {}).get("temp"),
            "humidity_pct":     data.get("main", {}).get("humidity"),
            "wind_speed_ms":    data.get("wind", {}).get("speed"),
            "cloud_cover_pct":  float(data.get("clouds", {}).get("all", 0)),
            "rainfall_1h_mm":   data.get("rain", {}).get("1h", 0.0),
            "weather_condition": data.get("weather", [{}])[0].get("main"),
            "api_poll_ms":      api_poll_ms,
        }
    except requests.exceptions.Timeout:
        logger.warning(f"⏱ API timeout for {station['station_id']}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"🌐 HTTP error for {station['station_id']}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error for {station['station_id']}: {e}")
        return None


def assign_quality_flag(station_id: str, data: dict) -> str:
    """
    Assign a quality flag to each event:
      GOOD    — values in expected range, not stale
      STALE   — same temperature as last reading (API likely returned cached data)
      ANOMALY — a value is outside physically plausible bounds
    """
    temp = data.get("temperature_c")

    # Check for stale data
    if temp is not None and _last_temperature.get(station_id) == temp:
        return "STALE"
    if temp is not None:
        _last_temperature[station_id] = temp

    # Check anomaly bounds
    for field, (low, high) in ANOMALY_THRESHOLDS.items():
        val = data.get(field)
        if val is not None and not (low <= val <= high):
            logger.warning(f"🚨 Anomaly detected: {station_id} {field}={val} (bounds: {low}–{high})")
            return "ANOMALY"

    return "GOOD"


def build_avro_event(station: dict, data: dict, quality_flag: str) -> dict:
    """Build the Avro-compatible dict matching weather_event.avsc schema."""
    return {
        "station_id":        station["station_id"],
        "city_name":         station["city_name"],
        "event_time":        int(datetime.now(timezone.utc).timestamp() * 1000),
        "latitude":          station["lat"],
        "longitude":         station["lon"],
        "temperature_c":     data.get("temperature_c"),
        "humidity_pct":      data.get("humidity_pct"),
        "wind_speed_ms":     data.get("wind_speed_ms"),
        "cloud_cover_pct":   data.get("cloud_cover_pct"),
        "rainfall_1h_mm":    data.get("rainfall_1h_mm", 0.0),
        "weather_condition": data.get("weather_condition"),
        "api_poll_ms":       data.get("api_poll_ms", 0),
        "quality_flag":      quality_flag,
    }


def create_producer():
    """Initialize Redpanda producer with Avro serializer connected to Schema Registry."""
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA_STR)

    producer = Producer({
        "bootstrap.servers": REDPANDA_BROKERS,
        "acks": "all",                    # wait for leader + replicas
        "enable.idempotence": "true",     # exactly-once producer semantics
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",     # compress Avro payloads
    })

    return producer, avro_serializer


def main():
    """
    Main loop:
      Every POLL_INTERVAL_SECONDS → poll all stations → push Avro events to Redpanda
    """
    logger.info(f"🌦  Weather streaming producer starting")
    logger.info(f"   Broker:   {REDPANDA_BROKERS}")
    logger.info(f"   Topic:    {TOPIC}")
    logger.info(f"   Interval: {POLL_INTERVAL_SEC}s")
    logger.info(f"   Mode:     LIVE (OpenWeatherMap API)")

    producer, avro_serializer = create_producer()

    poll_count = 0

    while True:
        poll_count += 1
        cycle_start = time.time()
        good_count = stale_count = anomaly_count = error_count = 0

        logger.info(f"─── Poll cycle #{poll_count} ───────────────────────────────")

        for station in STATIONS:
            raw_data = fetch_weather(station)

            if raw_data is None:
                # Failed to fetch — produce an error event so Flink knows
                error_event = {
                    "station_id":        station["station_id"],
                    "city_name":         station["city_name"],
                    "event_time":        int(datetime.now(timezone.utc).timestamp() * 1000),
                    "latitude":          station["lat"],
                    "longitude":         station["lon"],
                    "temperature_c":     None,
                    "humidity_pct":      None,
                    "wind_speed_ms":     None,
                    "cloud_cover_pct":   None,
                    "rainfall_1h_mm":    None,
                    "weather_condition": None,
                    "api_poll_ms":       -1,
                    "quality_flag":      "API_ERROR",
                }
                raw_data = error_event
                quality_flag = "API_ERROR"
                error_count += 1
            else:
                quality_flag = assign_quality_flag(station["station_id"], raw_data)
                if quality_flag == "GOOD":
                    good_count += 1
                elif quality_flag == "STALE":
                    stale_count += 1
                elif quality_flag == "ANOMALY":
                    anomaly_count += 1

            event = build_avro_event(station, raw_data, quality_flag)

            try:
                producer.produce(
                    topic=TOPIC,
                    key=station["station_id"],      # partition by station
                    value=avro_serializer(
                        event,
                        SerializationContext(TOPIC, MessageField.VALUE)
                    ),
                    on_delivery=delivery_report,
                )
                logger.info(
                    f"  → {station['station_id']} | {station['city_name']:<20} | "
                    f"temp={event.get('temperature_c')}°C | "
                    f"humidity={event.get('humidity_pct')}% | "
                    f"flag={quality_flag}"
                )
            except Exception as e:
                logger.error(f"  ✗ Failed to produce for {station['station_id']}: {e}")

        # Flush all queued messages
        producer.flush()

        cycle_duration = time.time() - cycle_start
        logger.info(
            f"─── Cycle #{poll_count} done in {cycle_duration:.1f}s | "
            f"good={good_count} stale={stale_count} anomaly={anomaly_count} error={error_count}"
        )

        # Sleep for the remainder of the polling interval
        sleep_time = max(0, POLL_INTERVAL_SEC - cycle_duration)
        logger.info(f"    Sleeping {sleep_time:.0f}s until next poll...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
