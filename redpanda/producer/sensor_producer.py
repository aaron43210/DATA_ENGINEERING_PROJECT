"""MQTT-to-Redpanda bridge for the documented GeoPlatform sensor payload."""
import json
import logging
import math
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sensor-producer")
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
TOPIC = "sensor.live"
DLT_TOPIC = "sensor.dead-letter"
MQTT_BROKER = os.getenv("MQTT_BROKER", "")
MQTT_TLS = os.getenv("MQTT_TLS", "true").lower() in {"1", "true", "yes"}
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883" if MQTT_TLS else "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "geoplatform/sensors/#")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "") or None
MQTT_CLIENT_CERT = os.getenv("MQTT_CLIENT_CERT", "") or None
MQTT_CLIENT_KEY = os.getenv("MQTT_CLIENT_KEY", "") or None
MQTT_KEY_PASSWORD = os.getenv("MQTT_KEY_PASSWORD", "") or None
DELIVERY_TIMEOUT = float(os.getenv("KAFKA_DELIVERY_TIMEOUT_SECONDS", "15"))
SCHEMA_STR = Path(__file__).with_name("sensor_event.avsc").read_text(encoding="utf-8")
MEASUREMENTS = {
    "temp": ("temperature", "celsius"),
    "hum": ("humidity", "percent"),
    "soil": ("soil_moisture", "raw"),
    "aqi": ("air_quality", "aqi"),
}
METADATA_KEYS = {"station_id", "sensor_id", "lat", "lon", "event_time", "timestamp", "units", "firmware", "device_status", "sequence", "battery"}


class InvalidPayload(ValueError):
    """Input does not follow the documented sensor payload contract."""


def _finite_number(value, field):
    if isinstance(value, bool):
        raise InvalidPayload(f"{field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidPayload(f"{field} must be numeric") from exc
    if not math.isfinite(number):
        raise InvalidPayload(f"{field} must be finite")
    return number


def _event_time(payload, now=None):
    """event_time is epoch milliseconds; timestamp is timezone-aware RFC3339."""
    if "event_time" in payload and "timestamp" in payload:
        raise InvalidPayload("provide only one of event_time (epoch ms) or timestamp (RFC3339)")
    value = payload.get("event_time", payload.get("timestamp"))
    if value is None:
        return int((now or datetime.now(timezone.utc)).timestamp() * 1000)
    if "event_time" in payload:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InvalidPayload("event_time must be a non-negative integer in epoch milliseconds")
        result = value
    elif not isinstance(value, str):
        raise InvalidPayload("timestamp must be timezone-aware RFC3339 text")
    else:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
            raise InvalidPayload("timestamp must be timezone-aware RFC3339 text")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise InvalidPayload("timestamp must be timezone-aware RFC3339 text") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise InvalidPayload("timestamp must include a timezone")
        result = int(parsed.timestamp() * 1000)
    try:
        datetime.fromtimestamp(result / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise InvalidPayload("timestamp is outside the supported UTC date range") from exc
    return result


def build_events(payload, now=None):
    """Validate and normalize a sensor object. Unusual finite readings are retained."""
    if not isinstance(payload, dict):
        raise InvalidPayload("JSON root must be an object")
    sensor_id = payload.get("station_id") or payload.get("sensor_id")
    if not isinstance(sensor_id, str) or not sensor_id.strip():
        raise InvalidPayload("station_id or sensor_id must be a non-empty string")
    if len(sensor_id.strip()) > 100:
        raise InvalidPayload("station_id or sensor_id must not exceed 100 characters")
    if payload.get("lat") is None or payload.get("lon") is None:
        raise InvalidPayload("lat and lon are required")
    latitude = _finite_number(payload["lat"], "lat")
    longitude = _finite_number(payload["lon"], "lon")
    if not -90 <= latitude <= 90:
        raise InvalidPayload("lat must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise InvalidPayload("lon must be between -180 and 180")
    unsupported = set(payload) - METADATA_KEYS - set(MEASUREMENTS)
    if unsupported:
        raise InvalidPayload(f"unsupported payload keys: {', '.join(sorted(unsupported))}")
    if not any(key in payload for key in MEASUREMENTS):
        raise InvalidPayload("payload has no supported measurement")
    units = payload.get("units", {})
    if not isinstance(units, dict):
        raise InvalidPayload("units must be an object keyed by temp, hum, soil, or aqi")
    unknown_units = set(units) - set(MEASUREMENTS)
    if unknown_units:
        raise InvalidPayload(f"unsupported unit keys: {', '.join(sorted(unknown_units))}")
    for key, (_, expected) in MEASUREMENTS.items():
        if key in units and units[key] != expected:
            raise InvalidPayload(f"{key} must be supplied in {expected}; conversion is not configured")
    event_time = _event_time(payload, now=now)
    events = []
    for key, (measurement_type, unit) in MEASUREMENTS.items():
        if key not in payload:
            continue
        value = _finite_number(payload[key], key)
        events.append({
            "sensor_id": sensor_id.strip(), "event_time": event_time,
            "latitude": latitude, "longitude": longitude,
            "measurement_type": measurement_type, "value": value, "unit": unit,
        })
    return events


def verify_schema_registry(registry, attempts=10, delay=2):
    """Require an actual Schema Registry response before reporting startup ready."""
    for attempt in range(attempts):
        try:
            registry.get_subjects()
            return
        except Exception:
            if attempt + 1 == attempts:
                raise
            logger.warning("Schema Registry not ready (attempt %d/%d)", attempt + 1, attempts)
            time.sleep(delay)


def verify_broker(producer, attempts=10, delay=2):
    for attempt in range(attempts):
        try:
            producer.list_topics(timeout=5)
            return
        except Exception:
            if attempt + 1 == attempts:
                raise
            logger.warning("Redpanda broker not ready (attempt %d/%d)", attempt + 1, attempts)
            time.sleep(delay)


def init_redpanda_producer():
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    verify_schema_registry(registry)
    serializer = AvroSerializer(registry, SCHEMA_STR)
    producer = Producer({
        "bootstrap.servers": REDPANDA_BROKERS, "acks": "all",
        "enable.idempotence": True, "retries": 5, "retry.backoff.ms": 500,
        "compression.type": "snappy", "queue.buffering.max.messages": 10000,
    })
    verify_broker(producer)
    return producer, serializer


def acknowledge_mqtt(client, msg):
    if msg.qos <= 0:
        return
    result = client.ack(msg.mid, msg.qos)
    if result != mqtt.MQTT_ERR_SUCCESS:
        logger.error("MQTT acknowledgement failed for mid=%s (result=%s); broker may redeliver", msg.mid, result)


def publish_payload(client, producer, serializer, msg):
    """Publish all events, then ack QoS 1. In-flight work is bounded to one MQTT callback."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        events = build_events(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, InvalidPayload) as exc:
        # Quarantine invalid raw input before ack; failed quarantine leaves QoS 1 for redelivery.
        try:
            dlt = json.dumps({"source": "mqtt", "mqtt_topic": msg.topic,
                              "mqtt_mid": msg.mid, "reason": str(exc),
                              "payload_utf8": msg.payload.decode("utf-8", errors="replace")})
            delivery = {"done": False, "error": None}
            def quarantined(err, _kafka_msg):
                delivery["done"], delivery["error"] = True, err
            producer.produce(DLT_TOPIC, key=str(msg.mid).encode(), value=dlt.encode(), on_delivery=quarantined)
            deadline = time.monotonic() + DELIVERY_TIMEOUT
            while not delivery["done"] and time.monotonic() < deadline:
                producer.poll(0.2)
            if not delivery["done"] or delivery["error"]:
                raise TimeoutError(f"dead-letter delivery failed: {delivery['error'] or 'timeout'}")
            acknowledge_mqtt(client, msg)
        except Exception:
            logger.exception("Invalid MQTT message quarantine failed; leaving it unacknowledged")
        return

    if msg.retain:
        # Retained state may be replayed on every subscription; ingest only live updates.
        logger.info("Ignoring retained MQTT message on %s (mid=%s)", msg.topic, msg.mid)
        acknowledge_mqtt(client, msg)
        return

    pending = {"count": len(events), "errors": []}

    def delivered(err, kafka_msg):
        if err:
            pending["errors"].append(str(err))
            logger.error("Redpanda delivery failed for key %r: %s", kafka_msg.key(), err)
        pending["count"] -= 1

    try:
        for event in events:
            key = f"{event['sensor_id']}_{event['measurement_type']}".encode("utf-8")
            value = serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=delivered)
            except BufferError:
                producer.poll(1)
                producer.produce(TOPIC, key=key, value=value, on_delivery=delivered)
        deadline = time.monotonic() + DELIVERY_TIMEOUT
        while pending["count"] and time.monotonic() < deadline:
            producer.poll(min(0.25, max(0, deadline - time.monotonic())))
        if pending["count"] or pending["errors"]:
            raise TimeoutError(f"delivery incomplete: pending={pending['count']}, errors={len(pending['errors'])}")
        logger.info("Delivered %d event(s) for %s", len(events), events[0]["sensor_id"])
        acknowledge_mqtt(client, msg)
    except Exception:
        logger.exception("Could not deliver all sensor events; leaving MQTT message unacknowledged")
        producer.poll(0)


def on_mqtt_connect(client, userdata, flags, rc):
    if rc != 0:
        logger.error("MQTT connection failed (return code %s); client will reconnect", rc)
        return
    result, mid = client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
    if result != mqtt.MQTT_ERR_SUCCESS:
        logger.error("MQTT subscription request failed: result=%s", result)
        client.disconnect()
        return
    logger.info("Subscribed to %s at requested QoS %s (mid=%s)", MQTT_TOPIC, MQTT_QOS, mid)


def on_mqtt_subscribe(client, userdata, mid, granted_qos):
    if not granted_qos or any(qos >= 128 for qos in granted_qos):
        logger.error("Broker rejected MQTT subscription (mid=%s, granted_qos=%s)", mid, granted_qos)
        client.disconnect()
        return
    logger.info("MQTT subscription accepted (mid=%s, granted_qos=%s)", mid, granted_qos)


def main():
    if not MQTT_BROKER:
        raise SystemExit("MQTT_BROKER is required; no public broker is configured by default")
    if MQTT_QOS not in (0, 1, 2):
        raise SystemExit("MQTT_QOS must be 0, 1, or 2")
    if bool(MQTT_CLIENT_CERT) != bool(MQTT_CLIENT_KEY):
        raise SystemExit("MQTT_CLIENT_CERT and MQTT_CLIENT_KEY must be configured together")
    producer, serializer = init_redpanda_producer()
    client_id = os.getenv("MQTT_CLIENT_ID") or f"lubaba-sensor-{uuid.uuid4().hex[:12]}"
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                         client_id=client_id, manual_ack=True)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)
    if MQTT_TLS:
        client.tls_set(ca_certs=MQTT_CA_CERT, certfile=MQTT_CLIENT_CERT,
                       keyfile=MQTT_CLIENT_KEY, keyfile_password=MQTT_KEY_PASSWORD)
    client.on_connect = on_mqtt_connect
    client.on_subscribe = on_mqtt_subscribe
    client.on_disconnect = lambda c, u, rc: logger.warning("MQTT disconnected (rc=%s); reconnect loop remains active", rc)
    client.on_message = lambda c, u, m: publish_payload(c, producer, serializer, m)
    try:
        # Async connect lets loop_forever retry even when the broker is initially unavailable.
        client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_forever(retry_first_connection=True)
        raise RuntimeError("MQTT network loop exited unexpectedly")
    except KeyboardInterrupt:
        logger.info("Stopping MQTT bridge")
    finally:
        try:
            client.disconnect()
        finally:
            remaining = producer.flush(DELIVERY_TIMEOUT)
            if remaining:
                logger.error("Shutdown timed out with %d Redpanda message(s) outstanding", remaining)


if __name__ == "__main__":
    main()
