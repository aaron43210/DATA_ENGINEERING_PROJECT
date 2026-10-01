"""
IoT Sensor Streaming Producer — Redpanda (Kafka-compatible)
===========================================================
Pattern: Push-based Streaming (MQTT to Kafka Bridge)
  - Subscribes to a real hardware MQTT broker (e.g., AWS IoT, Mosquitto)
  - Receives physical sensor payloads in real-time
  - Serializes each payload as an Avro event
  - Pushes to Redpanda topic: sensor.live
  - Schema is registered with Redpanda's built-in Schema Registry
"""

import os
import json
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("sensor-producer")

# ── Configuration ──────────────────────────────────────────────────────────────
REDPANDA_BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
TOPIC = "sensor.live"

# MQTT Configuration for physical sensors
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mqtt.eclipseprojects.io")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "geoplatform/sensors/#")

# Load Avro schema from file
SCHEMA_STR = open(os.path.join(os.path.dirname(__file__), "sensor_event.avsc")).read()

# Mapping from physical MQTT payload keys to (measurement_type, unit) pairs.
# Each physical reading is emitted as a separate SensorEvent so that the
# downstream stream processor can window/anomaly-detect per measurement_type.
MEASUREMENTS = {
    "temp": ("temperature", "celsius"),
    "hum": ("humidity", "percent"),
    "soil": ("soil_moisture", "raw"),
    "aqi": ("air_quality", "aqi"),
}

# ── Globals ────────────────────────────────────────────────────────────────────
redpanda_producer = None
avro_serializer = None


def delivery_report(err, msg):
    """Kafka/Redpanda delivery callback."""
    if err:
        logger.error(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        logger.debug(
            f"✅ Delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}"
        )


def init_redpanda_producer():
    """Initialize Redpanda producer with Avro serializer."""
    global redpanda_producer, avro_serializer

    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA_STR)

    redpanda_producer = Producer(
        {
            "bootstrap.servers": REDPANDA_BROKERS,
            "acks": "all",
            "enable.idempotence": "true",
            "retries": 5,
            "compression.type": "snappy",
        }
    )
    logger.info("✅ Connected to Redpanda Schema Registry & Broker.")


def on_mqtt_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT Broker."""
    if rc == 0:
        logger.info(
            f"✅ Connected to physical MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}"
        )
        client.subscribe(MQTT_TOPIC)
        logger.info(f"📡 Subscribed to MQTT topic: {MQTT_TOPIC}")
    else:
        logger.error(f"❌ Failed to connect to MQTT Broker, return code {rc}")
        raise ConnectionError("MQTT Connection failed")


def on_mqtt_message(client, userdata, msg):
    """Callback when a physical sensor publishes a message."""
    try:
        # Example physical payload:
        #   {"station_id": "SN_101", "lat": 9.93, "lon": 76.26,
        #    "temp": 28.5, "hum": 75, "soil": 400, "aqi": 35}
        payload = json.loads(msg.payload.decode("utf-8"))
        sensor_id = payload.get("station_id") or payload.get("sensor_id")

        if not sensor_id:
            logger.error("❌ Dropping payload with no station_id/sensor_id")
            return

        # Location is mandatory in the schema — do not fabricate coordinates.
        if payload.get("lat") is None or payload.get("lon") is None:
            logger.error(f"❌ Dropping {sensor_id}: payload missing lat/lon")
            return

        latitude = float(payload["lat"])
        longitude = float(payload["lon"])
        event_time = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Emit one Avro event per present measurement, matching sensor_event.avsc.
        emitted = 0
        for key, (measurement_type, unit) in MEASUREMENTS.items():
            if payload.get(key) is None:
                continue

            event = {
                "sensor_id": sensor_id,
                "event_time": event_time,
                "latitude": latitude,
                "longitude": longitude,
                "measurement_type": measurement_type,
                "value": float(payload[key]),
                "unit": unit,
            }

            redpanda_producer.produce(
                topic=TOPIC,
                key=f"{sensor_id}_{measurement_type}",
                value=avro_serializer(
                    event, SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
            emitted += 1

        redpanda_producer.poll(0)  # Trigger delivery callbacks
        logger.info(
            f"  → Bridged physical sensor {sensor_id} to Redpanda ({emitted} measurements)"
        )

    except json.JSONDecodeError:
        logger.error("❌ Received malformed MQTT payload (not JSON)")
    except Exception as e:
        logger.error(f"❌ Error bridging MQTT message to Redpanda: {e}")


def main():
    """Main process: Connect to Redpanda, then block on MQTT loop."""
    logger.info("🚀 Starting Physical IoT Sensor Bridge (MQTT → Redpanda)")

    # 1. Init Redpanda
    init_redpanda_producer()

    # 2. Init MQTT Client
    mqtt_client = mqtt.Client(client_id="geoplatform_bridge")
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message

    logger.info(f"Connecting to MQTT Broker {MQTT_BROKER}...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        logger.fatal(f"Could not connect to hardware MQTT broker: {e}")
        raise

    # 3. Block forever listening to physical hardware
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down bridge...")
    finally:
        mqtt_client.disconnect()
        redpanda_producer.flush()


if __name__ == "__main__":
    main()
