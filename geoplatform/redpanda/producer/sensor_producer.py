import os
import time
import random
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sensor-producer")

REDPANDA_BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")

# Sensor mock locations in Kerala
SENSORS = [
    {"id": "IOT_TVM_01", "lat": 8.5241, "lon": 76.9366},
    {"id": "IOT_COK_02", "lat": 9.9312, "lon": 76.2673},
    {"id": "IOT_CCJ_03", "lat": 11.2588, "lon": 75.7804},
]

MEASUREMENTS = ["temperature", "humidity", "soil_moisture", "air_quality"]

SCHEMA_STR = open(os.path.join(os.path.dirname(__file__), "sensor_event.avsc")).read()

def create_producer():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA_STR)

    producer = Producer({
        "bootstrap.servers": REDPANDA_BROKERS,
        "acks": "all",
        "compression.type": "snappy",
    })
    return producer, avro_serializer

def main():
    logger.info("Starting IoT Sensor Producer...")
    time.sleep(10) # wait for schema registry
    
    producer, avro_serializer = create_producer()
    
    while True:
        sensor = random.choice(SENSORS)
        m_type = random.choice(MEASUREMENTS)
        
        # Base values for simulation
        if m_type == "temperature": val, unit = random.uniform(20.0, 35.0), "C"
        elif m_type == "humidity": val, unit = random.uniform(60.0, 95.0), "%"
        elif m_type == "soil_moisture": val, unit = random.uniform(10.0, 40.0), "%"
        else: val, unit = random.uniform(20.0, 150.0), "AQI"
        
        # Inject occasional anomaly
        if random.random() > 0.98:
            val *= 3.0

        event = {
            "sensor_id": sensor["id"],
            "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latitude": sensor["lat"],
            "longitude": sensor["lon"],
            "measurement_type": m_type,
            "value": round(val, 2),
            "unit": unit
        }
        
        topic = f"sensor.{m_type}"
        
        try:
            producer.produce(
                topic=topic,
                key=sensor["id"],
                value=avro_serializer(event, SerializationContext(topic, MessageField.VALUE))
            )
            producer.poll(0)
            logger.info(f"Published to {topic}: {sensor['id']} = {round(val, 2)} {unit}")
        except Exception as e:
            logger.error(f"Failed to produce: {e}")
            
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    main()
