"""Offline tests: no MQTT, Kafka, PostgreSQL, or weather API service is contacted."""
import base64
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import yaml

ROOT = Path(__file__).resolve().parents[1]


def module(name, **attrs):
    result = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(result, key, value)
    return result


class TopicPartition:
    def __init__(self, topic, partition, offset=-1):
        self.topic, self.partition, self.offset = topic, partition, offset


class Producer: pass
class Consumer: pass
class PgError(Exception): pass

confluent = module("confluent_kafka", Producer=Producer, Consumer=Consumer, TopicPartition=TopicPartition)
registry = module("confluent_kafka.schema_registry", SchemaRegistryClient=object)
avro = module("confluent_kafka.schema_registry.avro", AvroSerializer=object, AvroDeserializer=object)
serialization = module("confluent_kafka.serialization", SerializationContext=lambda *a: a,
                       MessageField=types.SimpleNamespace(VALUE="value"))
paho = module("paho")
paho_mqtt = module("paho.mqtt")
paho_client = module("paho.mqtt.client", MQTT_ERR_SUCCESS=0, Client=object,
                     CallbackAPIVersion=types.SimpleNamespace(VERSION1="VERSION1"))
paho.mqtt, paho_mqtt.client = paho_mqtt, paho_client
sys.modules.update({"confluent_kafka": confluent, "confluent_kafka.schema_registry": registry,
                    "confluent_kafka.schema_registry.avro": avro, "confluent_kafka.serialization": serialization,
                    "paho": paho, "paho.mqtt": paho_mqtt, "paho.mqtt.client": paho_client})
pg = module("psycopg2", Error=PgError, OperationalError=PgError, connect=lambda **kw: None)
sys.modules["psycopg2"] = pg


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


sensor = load("lubaba_sensor", ROOT / "redpanda/producer/sensor_producer.py")
logic = load("lubaba_sensor_logic", ROOT / "flink/jobs/sensor_logic.py")


class SensorValidationTests(unittest.TestCase):
    def test_single_and_multiple_measurements(self):
        one = sensor.build_events({"sensor_id": "S1", "lat": 9.9, "lon": 76.2, "temp": 999})
        self.assertEqual(len(one), 1)
        self.assertEqual(one[0]["unit"], "celsius")
        self.assertEqual(one[0]["value"], 999)  # structural validation does not discard anomalies
        several = sensor.build_events({"station_id": "S2", "lat": 8, "lon": 77,
                                       "temp": 25, "hum": 40, "soil": 500, "aqi": 20})
        self.assertEqual([e["measurement_type"] for e in several],
                         ["temperature", "humidity", "soil_moisture", "air_quality"])

    def test_json_shape_and_required_fields(self):
        for raw in (b"{bad", b"[]", b"null"):
            with self.subTest(raw=raw):
                with self.assertRaises((json.JSONDecodeError, sensor.InvalidPayload)):
                    sensor.build_events(json.loads(raw))
        with self.assertRaises(sensor.InvalidPayload):
            sensor.build_events({"sensor_id": 3, "lat": 9, "lon": 76, "temp": 2})
        with self.assertRaises(sensor.InvalidPayload):
            sensor.build_events({"sensor_id": "", "lat": 9, "lon": 76, "temp": 2})
        with self.assertRaises(sensor.InvalidPayload):
            sensor.build_events({"sensor_id": "s" * 101, "lat": 9, "lon": 76, "temp": 2})
        for payload in ({"sensor_id": "s", "lon": 76, "temp": 1},
                        {"sensor_id": "s", "lat": 9, "temp": 1}):
            with self.assertRaises(sensor.InvalidPayload): sensor.build_events(payload)

    def test_coordinate_and_measurement_bounds_finiteness(self):
        for lat, lon, reading in ((91, 0, 1), (0, 181, 1), (float("nan"), 0, 1),
                                  (0, float("inf"), 1), (0, 0, float("nan")), (0, 0, float("-inf")),
                                  (0, 0, True), (0, 0, "bad")):
            with self.subTest(lat=lat, lon=lon, reading=reading), self.assertRaises(sensor.InvalidPayload):
                sensor.build_events({"sensor_id": "s", "lat": lat, "lon": lon, "temp": reading})

    def test_unknown_measurement_and_absent_reading_rejected(self):
        for payload in ({"sensor_id": "s", "lat": 9, "lon": 76, "temp": None},
                        {"sensor_id": "s", "lat": 9, "lon": 76, "pressure": 1000},
                        {"sensor_id": "s", "lat": 9, "lon": 76, "firmware": "v1"}):
            with self.assertRaises(sensor.InvalidPayload): sensor.build_events(payload)

    def test_unit_contract_and_timestamps(self):
        base = {"sensor_id": "s", "lat": 9, "lon": 76, "temp": 25}
        with self.assertRaises(sensor.InvalidPayload): sensor.build_events({**base, "units": {"temp": "fahrenheit"}})
        self.assertEqual(sensor.build_events({**base, "units": {"temp": "celsius"}})[0]["unit"], "celsius")
        self.assertEqual(sensor.build_events({**base, "event_time": 1700000000123})[0]["event_time"], 1700000000123)
        self.assertEqual(sensor.build_events({**base, "timestamp": "2023-11-14T22:13:20Z"})[0]["event_time"], 1700000000000)
        for timestamp in ("2024-01-01T00:00:00", "2024-01-01 00:00:00Z", 123, "not-time"):
            with self.subTest(timestamp=timestamp), self.assertRaises(sensor.InvalidPayload):
                sensor.build_events({**base, "timestamp": timestamp})
        with self.assertRaises(sensor.InvalidPayload):
            sensor.build_events({**base, "event_time": 10**30})


class ProducerReliabilityTests(unittest.TestCase):
    class Message:
        topic, mid, qos, retain = "sensors/a", 7, 1, False
        payload = json.dumps({"sensor_id": "S", "lat": 9, "lon": 76, "temp": 20, "hum": 40}).encode()

    class FakeProducer:
        def __init__(self, fail_at=None, full_once=False): self.callbacks, self.fail_at, self.full_once = [], fail_at, full_once
        def produce(self, topic, key=None, value=None, on_delivery=None):
            if self.full_once:
                self.full_once = False
                raise BufferError("full")
            self.callbacks.append((topic, key, value, on_delivery))
        def poll(self, _timeout):
            if self.callbacks:
                topic, key, value, callback = self.callbacks.pop(0)
                err = RuntimeError("delivery failed") if self.fail_at == key else None
                callback(err, SimpleNamespace(key=lambda: key))
            return 1
        def flush(self, _timeout): return len(self.callbacks)

    def test_delivery_success_acknowledges_after_every_event(self):
        client, producer = Mock(), self.FakeProducer()
        client.ack.return_value = 0
        sensor.publish_payload(client, producer, lambda event, ctx: event, self.Message())
        client.ack.assert_called_once_with(7, 1)

    def test_partial_delivery_failure_leaves_message_unacked(self):
        client, producer = Mock(), self.FakeProducer(fail_at=b"S_humidity")
        sensor.publish_payload(client, producer, lambda event, ctx: event, self.Message())
        client.ack.assert_not_called()

    def test_queue_full_retries_after_poll(self):
        client, producer = Mock(), self.FakeProducer(full_once=True)
        client.ack.return_value = 0
        sensor.publish_payload(client, producer, lambda event, ctx: event, self.Message())
        client.ack.assert_called_once_with(7, 1)

    def test_bad_input_is_quarantined_before_ack(self):
        client, producer = Mock(), self.FakeProducer()
        client.ack.return_value = 0
        msg = SimpleNamespace(payload=b"not-json", topic="sensors/a", mid=9, qos=1, retain=False)
        sensor.publish_payload(client, producer, lambda event, ctx: event, msg)
        client.ack.assert_called_once_with(9, 1)

    def test_quarantine_failure_leaves_invalid_message_unacknowledged(self):
        client, producer = Mock(), self.FakeProducer(fail_at=b"9")
        msg = SimpleNamespace(payload=b"not-json", topic="sensors/a", mid=9, qos=1, retain=False)
        sensor.publish_payload(client, producer, lambda event, ctx: event, msg)
        client.ack.assert_not_called()

    def test_retained_message_is_ignored_and_acknowledged(self):
        client, producer = Mock(), self.FakeProducer()
        client.ack.return_value = 0
        msg = SimpleNamespace(payload=self.Message.payload, topic="sensors/a", mid=10, qos=1, retain=True)
        sensor.publish_payload(client, producer, lambda event, ctx: event, msg)
        self.assertEqual(producer.callbacks, [])
        client.ack.assert_called_once_with(10, 1)

    def test_mqtt_ack_error_is_visible(self):
        client = Mock()
        client.ack.return_value = 1
        with patch.object(sensor.logger, "error") as log_error:
            sensor.acknowledge_mqtt(client, SimpleNamespace(mid=11, qos=1))
        log_error.assert_called_once()

    def test_subscription_failure_disconnects_and_connect_failure_is_visible(self):
        client = Mock()
        client.subscribe.return_value = (1, 11)
        sensor.on_mqtt_connect(client, None, None, 0)
        client.disconnect.assert_called_once()
        client.reset_mock()
        sensor.on_mqtt_connect(client, None, None, 5)
        client.subscribe.assert_not_called()
        client.reset_mock()
        sensor.on_mqtt_subscribe(client, None, 11, [128])
        client.disconnect.assert_called_once()

    def test_main_uses_async_startup_retry_and_generated_client_id(self):
        client, producer = Mock(), Mock()
        producer.flush.return_value = 0
        client.loop_forever.side_effect = KeyboardInterrupt
        with patch.object(sensor, "MQTT_BROKER", "private-broker"), \
             patch.object(sensor, "init_redpanda_producer", return_value=(producer, Mock())), \
             patch.object(sensor.mqtt, "Client", return_value=client) as make_client:
            sensor.main()
        self.assertTrue(make_client.call_args.kwargs["client_id"].startswith("lubaba-sensor-"))
        client.connect_async.assert_called_once_with("private-broker", sensor.MQTT_PORT, keepalive=60)
        client.loop_forever.assert_called_once_with(retry_first_connection=True)


class FlinkSensorLogicTests(unittest.TestCase):
    def setUp(self):
        self.meta = {"topic": "sensor.live", "partition": 2, "offset": 18,
                     "key_base64": "UzE=", "value_base64": "AA=="}
        self.record = {"sensor_id": "S", "event_time": 1700000000000, "latitude": 9.0,
                       "longitude": 76.0, "measurement_type": "temperature", "value": 20.0,
                       "unit": "celsius"}

    def test_validates_event_and_preserves_source_coordinates(self):
        event = logic.validate_sensor(self.record, self.meta)
        self.assertEqual((event["source_topic"], event["source_partition"], event["source_offset"]),
                         ("sensor.live", 2, 18))
        self.assertEqual(event["event_time_ms"], 1700000000000)

    def test_rejects_invalid_measurement_unit_number_and_coordinates(self):
        invalid = (
            ({**self.record, "sensor_id": ""}, "sensor_id"),
            ({**self.record, "measurement_type": "pressure"}, "measurement_type"),
            ({**self.record, "unit": "fahrenheit"}, "unit"),
            ({**self.record, "value": float("nan")}, "value"),
            ({**self.record, "value": float("inf")}, "value"),
            ({**self.record, "latitude": 91}, "latitude"),
            ({**self.record, "longitude": float("nan")}, "longitude"),
            ({**self.record, "event_time": -1}, "event_time"),
            ({key: value for key, value in self.record.items() if key != "value"}, "missing"),
        )
        for record, _ in invalid:
            with self.subTest(record=record), self.assertRaises(ValueError):
                logic.validate_sensor(record, self.meta)

    def test_timestamp_plain_long_and_logical_datetime(self):
        from datetime import datetime, timezone
        self.assertEqual(logic.epoch_millis(1700000000123), 1700000000123)
        self.assertEqual(logic.epoch_millis(datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)),
                         1700000000000)
        with self.assertRaises(ValueError):
            logic.epoch_millis(datetime(2023, 11, 14))

    def test_anomaly_warmup_prior_baseline_and_spike(self):
        history = []
        for reading in (10, 11, 9, 10):
            flagged, history = logic.score_reading(history, reading)
            self.assertFalse(flagged)
        flagged, history = logic.score_reading(history, 10)
        self.assertFalse(flagged)
        flagged, updated = logic.score_reading(history, 100)
        self.assertTrue(flagged)
        self.assertEqual(updated[-1], 100)
        self.assertNotIn(100, history)

    def test_constant_baseline_policy(self):
        history = [20.0] * 5
        self.assertEqual(logic.score_reading(history, 20.0)[0], False)
        self.assertEqual(logic.score_reading(history, 21.0)[0], True)

    def test_state_history_is_bounded(self):
        _, history = logic.score_reading(list(range(30)), 30)
        self.assertEqual(len(history), 30)
        self.assertEqual(history[0], 1)

    def test_watermark_late_event_policy(self):
        self.assertFalse(logic.is_late_event(10_001, 10_000))
        self.assertTrue(logic.is_late_event(10_000, 10_000))
        self.assertTrue(logic.is_late_event(8_000, 10_000))
        self.assertFalse(logic.is_late_event(10_000, -1))

    def test_dead_letter_contains_original_coordinates_and_payload(self):
        body = json.loads(logic.dead_letter(self.meta, "bad Avro"))
        self.assertEqual((body["source_topic"], body["source_partition"], body["source_offset"]),
                         ("sensor.live", 2, 18))
        self.assertEqual(base64.b64decode(body["payload_base64"]), b"\x00")
        self.assertIn("bad Avro", body["reason"])

    def test_malformed_confluent_wire_header_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "wire header"):
            logic.decode_envelope({**self.meta, "value_base64": base64.b64encode(b"bad").decode()})

    def test_confluent_avro_decoding_and_plain_epoch_millis(self):
        if not importlib.util.find_spec("fastavro"):
            self.skipTest("fastavro not installed; install requirements-test.txt")
        from io import BytesIO
        import fastavro
        schema = json.loads((ROOT / "redpanda/producer/sensor_event.avsc").read_text())
        parsed = fastavro.parse_schema(schema)
        record = {"sensor_id": "S", "event_time": 1700000000123, "latitude": 9.0,
                  "longitude": 76.0, "measurement_type": "temperature", "value": 25.0,
                  "unit": "celsius"}
        payload = BytesIO()
        fastavro.schemaless_writer(payload, parsed, record)
        wire = b"\x00" + (7).to_bytes(4, "big") + payload.getvalue()
        meta = {**self.meta, "value_base64": base64.b64encode(wire).decode()}

        class Response:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"schema": json.dumps(schema)}

        decoded_meta, decoded = logic.decode_envelope(meta, get_schema=lambda _schema_id: Response())
        self.assertEqual(decoded_meta["offset"], 18)
        self.assertEqual(decoded["event_time"], 1700000000123)
        self.assertIsInstance(decoded["event_time"], int)


class SchemaAndConfigTests(unittest.TestCase):
    def test_schemas_parse_with_fastavro_when_installed(self):
        if not importlib.util.find_spec("fastavro"):
            self.skipTest("fastavro not installed; install requirements-test.txt")
        from io import BytesIO
        import fastavro
        schema = fastavro.parse_schema(json.loads((ROOT / "redpanda/producer/sensor_event.avsc").read_text()))
        record = {"sensor_id": "S1", "event_time": 1700000000123, "latitude": 9.9, "longitude": 76.2,
                  "measurement_type": "temperature", "value": 24.5, "unit": "celsius"}
        stream = BytesIO()
        fastavro.schemaless_writer(stream, schema, record)
        stream.seek(0)
        self.assertEqual(fastavro.schemaless_reader(stream, schema), record)
        weather_schema = json.loads((ROOT / "redpanda/weather_producer/weather_event.avsc").read_text())
        fastavro.parse_schema(weather_schema)

    def test_sensor_schema_plain_epoch_millisecond_long(self):
        schema = json.loads((ROOT / "redpanda/producer/sensor_event.avsc").read_text())
        time_field = next(field for field in schema["fields"] if field["name"] == "event_time")
        self.assertEqual(time_field["type"], "long")
        self.assertNotIn("logicalType", time_field)

    def test_sensor_only_configuration_has_no_weather_key_requirement(self):
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        services = compose["services"]
        self.assertEqual(services["sensor-producer"]["profiles"], ["sensor"])
        self.assertEqual(services["flink-jobmanager"]["profiles"], ["pipeline"])
        self.assertEqual(services["flink-taskmanager"]["profiles"], ["pipeline"])
        self.assertEqual(services["flink-job-submit"]["profiles"], ["pipeline"])
        self.assertIn("submit_and_monitor.sh", " ".join(services["flink-job-submit"]["entrypoint"]))
        self.assertIn("flink_checkpoints", str(services["flink-jobmanager"]["volumes"]))
        self.assertNotIn("OPENWEATHER_API_KEY", services["sensor-producer"]["environment"])
        self.assertIn("OPENWEATHER_API_KEY", services["weather-producer"]["environment"])
        self.assertIn("sensor.dead-letter", services["topic-setup"]["command"][0])
        sql = (ROOT / "infra/001_sensor_pipeline.sql").read_text()
        self.assertIn("uq_sensor_observations_source", sql)
        job = (ROOT / "flink/jobs/sensor_flink_job.py").read_text()
        self.assertIn("ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING", job)


if __name__ == "__main__":
    unittest.main(verbosity=2)
