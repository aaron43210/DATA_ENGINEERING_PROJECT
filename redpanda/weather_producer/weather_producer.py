"""Poll OpenWeatherMap and publish validated observations as Avro events."""
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("weather-producer")
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
MAX_SOURCE_AGE_SEC = int(os.getenv("MAX_WEATHER_AGE_SECONDS", "900"))
TOPIC = "weather.live"
OWM_API_URL = "https://api.openweathermap.org/data/2.5/weather"
STATIONS = [
    {"station_id": "WX_TVM", "city_name": "Thiruvananthapuram", "lat": 8.5241, "lon": 76.9366},
    {"station_id": "WX_COK", "city_name": "Kochi", "lat": 9.9312, "lon": 76.2673},
    {"station_id": "WX_CCJ", "city_name": "Kozhikode", "lat": 11.2588, "lon": 75.7804},
    {"station_id": "WX_TCR", "city_name": "Thrissur", "lat": 10.5276, "lon": 76.2144},
    {"station_id": "WX_QLN", "city_name": "Kollam", "lat": 8.8932, "lon": 76.6141},
]
SCHEMA_STR = Path(__file__).with_name("weather_event.avsc").read_text(encoding="utf-8")
NUMERIC_FIELDS = ("temperature_c", "humidity_pct", "wind_speed_ms", "cloud_cover_pct", "rainfall_1h_mm")


def _number(value, name, low=None, high=None):
    if isinstance(value, bool):
        raise ValueError(f"{name} is not numeric")
    number = float(value)
    if not math.isfinite(number) or (low is not None and number < low) or (high is not None and number > high):
        raise ValueError(f"{name} is outside valid bounds")
    return number


def classify_weather(data, now=None, max_age_sec=MAX_SOURCE_AGE_SEC):
    """Return normalized fields and a quality flag based on source time and validity."""
    try:
        main, wind, clouds = data["main"], data["wind"], data["clouds"]
        weather = data["weather"]
        if not isinstance(main, dict) or not isinstance(wind, dict) or not isinstance(clouds, dict):
            raise ValueError("required API sections missing")
        rain = data.get("rain") or {}
        if not isinstance(rain, dict):
            raise ValueError("rain section must be an object when present")
        if not isinstance(weather, list) or not weather or not isinstance(weather[0].get("main"), str):
            raise ValueError("weather condition missing")
        normalized = {
            "temperature_c": _number(main.get("temp"), "temperature", -100, 70),
            "humidity_pct": _number(main.get("humidity"), "humidity", 0, 100),
            "wind_speed_ms": _number(wind.get("speed"), "wind speed", 0, 150),
            "cloud_cover_pct": _number(clouds.get("all"), "cloud cover", 0, 100),
            # Rain is optional in OWM's response. Absence means no reported rain.
            "rainfall_1h_mm": _number(rain.get("1h", 0.0), "rainfall", 0, 1000),
            "weather_condition": weather[0]["main"],
        }
        source_dt = data.get("dt")
        if isinstance(source_dt, bool) or not isinstance(source_dt, (int, float)) or not math.isfinite(source_dt) or source_dt <= 0:
            raise ValueError("valid source observation timestamp (dt) missing")
        source_time = datetime.fromtimestamp(source_dt, tz=timezone.utc)
        now = now or datetime.now(timezone.utc)
        age = (now - source_time).total_seconds()
        if age < -300:
            return normalized, source_time, "INVALID"
        if age > max_age_sec:
            return normalized, source_time, "STALE"
        if (
            not -5 <= normalized["temperature_c"] <= 50
            or not 0 <= normalized["humidity_pct"] <= 100
            or not 0 <= normalized["wind_speed_ms"] <= 60
            or not 0 <= normalized["rainfall_1h_mm"] <= 200
        ):
            return normalized, source_time, "ANOMALY"
        return normalized, source_time, "GOOD"
    except (KeyError, TypeError, ValueError, OverflowError):
        return {}, None, "INVALID"


def api_error_event(station, now=None):
    now = now or datetime.now(timezone.utc)
    return {
        "station_id": station["station_id"], "city_name": station["city_name"],
        "event_time": int(now.timestamp() * 1000), "latitude": station["lat"], "longitude": station["lon"],
        "temperature_c": None, "humidity_pct": None, "wind_speed_ms": None,
        "cloud_cover_pct": None, "rainfall_1h_mm": None, "weather_condition": None,
        "api_poll_ms": -1, "quality_flag": "API_ERROR",
    }


def fetch_weather(station, api_key=None, session=requests):
    key = (api_key if api_key is not None else OPENWEATHER_API_KEY).strip()
    if not key or key.lower() in {"demo", "changeme", "your_api_key"}:
        logger.error("OPENWEATHER_API_KEY is missing or still a placeholder; emitting API_ERROR")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def request():
        started = time.monotonic()
        response = session.get(OWM_API_URL, params={
            "lat": station["lat"], "lon": station["lon"], "appid": key, "units": "metric",
        }, timeout=10)
        response.raise_for_status()
        return response.json(), int((time.monotonic() - started) * 1000)

    try:
        data, poll_ms = request()
        fields, source_time, flag = classify_weather(data)
        if flag == "INVALID":
            logger.warning("Invalid weather response for %s", station["station_id"])
            return {
                **{name: None for name in NUMERIC_FIELDS}, "weather_condition": None,
                "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
                "api_poll_ms": poll_ms, "quality_flag": "INVALID",
            }
        return {
            **fields, "event_time": int(source_time.timestamp() * 1000),
            "api_poll_ms": poll_ms, "quality_flag": flag,
        }
    except (requests.RequestException, ValueError, TypeError) as exc:
        # Never log exception text: request URLs can contain appid.
        logger.error("Weather fetch failed for %s (%s)", station["station_id"], type(exc).__name__)
        return None


def build_event(station, data):
    if data is None:
        return api_error_event(station)
    return {
        "station_id": station["station_id"], "city_name": station["city_name"],
        "event_time": data["event_time"], "latitude": station["lat"], "longitude": station["lon"],
        **{key: data.get(key) for key in NUMERIC_FIELDS},
        "weather_condition": data.get("weather_condition"),
        "api_poll_ms": data["api_poll_ms"], "quality_flag": data["quality_flag"],
    }


def create_producer():
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(registry, SCHEMA_STR)
    producer = Producer({
        "bootstrap.servers": REDPANDA_BROKERS, "acks": "all",
        "enable.idempotence": True, "retries": 5, "retry.backoff.ms": 500,
        "compression.type": "snappy",
    })
    return producer, serializer


def main():
    if POLL_INTERVAL_SEC <= 0:
        raise SystemExit("POLL_INTERVAL_SECONDS must be greater than zero")
    if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY.lower() in {"demo", "changeme", "your_api_key"}:
        raise SystemExit("Set a valid OPENWEATHER_API_KEY before starting the weather producer")
    producer, serializer = create_producer()
    logger.info("Weather producer publishing to %s every %ss", TOPIC, POLL_INTERVAL_SEC)
    try:
        while True:
            started = time.monotonic()
            for station in STATIONS:
                data = fetch_weather(station)
                event = build_event(station, data)
                try:
                    producer.produce(
                        TOPIC, key=station["station_id"].encode(),
                        value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
                        on_delivery=lambda err, msg: logger.error("Delivery failed on %s: %s", msg.topic(), err) if err else logger.debug("Delivered %s offset %s", msg.topic(), msg.offset()),
                    )
                except BufferError:
                    logger.warning("Producer queue full; flushing before retry for %s", station["station_id"])
                    producer.flush(10)
                    try:
                        producer.produce(TOPIC, key=station["station_id"].encode(), value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)))
                    except Exception:
                        logger.exception("Unable to enqueue weather event for %s", station["station_id"])
                except Exception:
                    logger.exception("Unable to serialize or enqueue event for %s", station["station_id"])
                logger.info("Station %s quality=%s", station["station_id"], event["quality_flag"])
            if producer.flush(15):
                logger.error("Timed out flushing weather records")
            time.sleep(max(0, POLL_INTERVAL_SEC - (time.monotonic() - started)))
    except KeyboardInterrupt:
        logger.info("Stopping weather producer")
    finally:
        producer.flush(15)


if __name__ == "__main__":
    main()
