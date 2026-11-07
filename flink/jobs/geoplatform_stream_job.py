"""Genuine PyFlink streaming job for Confluent-Avro sensor.live events."""
import json
import logging
import os
from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
    DeliveryGuarantee,
)
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, ProcessFunction
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.util.java_utils import get_gateway
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.output_tag import OutputTag

from sensor_logic import (
    TransientDependencyError,
    decode_envelope,
    dead_letter,
    is_late_event,
    score_reading,
    validate_sensor,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
LOG = logging.getLogger("lubaba-sensor-flink")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor.live")
DLT_TOPIC = os.getenv("DLT_TOPIC", "sensor.dead-letter")
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
LATE_TAG = OutputTag("late-or-invalid", Types.STRING())
DECODE_ERROR_TAG = OutputTag("decode-errors", Types.STRING())


class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event["event_time_ms"]


class DecodeAndValidate(ProcessFunction):
    def __init__(self):
        self.registry_url = SCHEMA_REGISTRY

    def process_element(self, raw, ctx):
        meta = {}
        try:
            meta = json.loads(raw)
            meta, record = decode_envelope(meta, registry_url=self.registry_url)
            yield validate_sensor(record, meta)
        except TransientDependencyError:
            # Do not quarantine valid Kafka records just because the registry is down.
            raise
        except Exception as exc:
            yield DECODE_ERROR_TAG, dead_letter(meta, f"decode/validation: {type(exc).__name__}: {exc}")


class RollingSensorAnomaly(KeyedProcessFunction):
    """Per sensor/measurement Flink-managed 30-reading arrival-order baseline."""
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("rolling_history_json", Types.STRING())
        ttl = (StateTtlConfig.new_builder(Time.days(30))
               .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
               .build())
        descriptor.enable_time_to_live(ttl)
        self.history = runtime_context.get_state(descriptor)

    def process_element(self, event, ctx):
        event_time = event["event_time_ms"]
        watermark = ctx.timer_service().current_watermark()
        if is_late_event(event_time, watermark):
            yield LATE_TAG, dead_letter(event, "event timestamp is at or behind the current Flink watermark")
            return
        raw_history = self.history.value()
        prior = json.loads(raw_history) if raw_history else []
        anomalous, updated = score_reading(prior, event["value"])
        event["is_anomaly"] = anomalous
        event["window_avg"] = sum(updated) / len(updated)
        event["window_min"] = min(updated)
        event["window_max"] = max(updated)
        self.history.update(json.dumps(updated, separators=(",", ":")))
        yield event


class ToJdbcRow(MapFunction):
    """Serialize enriched Python events into the typed row expected by Flink JDBC."""
    def map(self, event):
        from pyflink.common import Row
        return Row(
            event["source_topic"], event["source_partition"], event["source_offset"],
            event["sensor_id"], event["event_time_utc"], event["latitude"], event["longitude"],
            event["measurement_type"], event["value"], event["unit"], event.get("quality_flag"),
            event["window_avg"], event["window_max"], event["window_min"], event["is_anomaly"],
        )


class AddUtcTimestamp(ProcessFunction):
    def process_element(self, event, ctx):
        # Passing UTC datetime to JDBC ensures timestamp interpretation is explicit.
        from datetime import datetime, timezone
        event["event_time_utc"] = datetime.fromtimestamp(event["event_time_ms"] / 1000, tz=timezone.utc)
        yield event


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    env.enable_checkpointing(int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")), CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(5000)
    checkpoint_config.set_checkpoint_timeout(60000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(10, 5000))

    builder = (KafkaSource.builder()
               .set_bootstrap_servers(BROKERS)
               .set_group_id(os.getenv("KAFKA_GROUP_ID", "lubaba-sensor-flink"))
               .set_topics(SENSOR_TOPIC)
               .set_starting_offsets(KafkaOffsetsInitializer.earliest()))
    # The helper is a Java KafkaRecordDeserializationSchema bundled in the image.
    # It emits JSON containing key/value bytes plus topic, partition and offset.
    java_schema = get_gateway().jvm.com.lubaba.flink.SensorRecordEnvelopeSchema()
    builder._j_builder.setDeserializer(java_schema)
    source = builder.build()
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "redpanda-sensor-source",
                          type_info=Types.STRING())

    decoded = raw.process(DecodeAndValidate(), output_type=Types.PICKLED_BYTE_ARRAY())
    dlt_records = decoded.get_side_output(DECODE_ERROR_TAG)
    watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(
        int(os.getenv("WATERMARK_OUT_OF_ORDER_SECONDS", "10"))))
        .with_timestamp_assigner(SensorTimestampAssigner()))
    event_time_stream = decoded.assign_timestamps_and_watermarks(watermark_strategy)
    scored = (event_time_stream
              .key_by(lambda event: f"{event['sensor_id']}:{event['measurement_type']}", key_type=Types.STRING())
              .process(RollingSensorAnomaly(), output_type=Types.PICKLED_BYTE_ARRAY()))
    late_records = scored.get_side_output(LATE_TAG)
    results = scored.process(AddUtcTimestamp(), output_type=Types.PICKLED_BYTE_ARRAY())
    jdbc_row_type = Types.ROW_NAMED(
        ["source_topic", "source_partition", "source_offset", "sensor_id", "event_time",
         "latitude", "longitude", "measurement_type", "value", "unit", "quality_flag",
         "window_avg", "window_max", "window_min", "is_anomaly"],
        [Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.SQL_TIMESTAMP(),
         Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
         Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.BOOLEAN()],
    )
    jdbc_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder()\
        .with_url("jdbc:postgresql://%s:%s/%s" % (
            os.getenv("POSTGRES_HOST", "postgres"), os.getenv("POSTGRES_PORT", "5432"),
            os.getenv("POSTGRES_DB", "geoplatform")))\
        .with_driver_name("org.postgresql.Driver")\
        .with_user_name(os.getenv("POSTGRES_USER", "geoplatform"))\
        .with_password(os.environ["POSTGRES_PASSWORD"]).build()
    jdbc_execution = (JdbcExecutionOptions.builder().with_batch_size(1)
                      .with_batch_interval_ms(0).with_max_retries(5).build())
    jdbc_sql = """INSERT INTO sensor_observations
      (source_topic, source_partition, source_offset, sensor_id, event_time, latitude, longitude,
       measurement_type, value, unit, quality_flag, window_avg, window_max, window_min, is_anomaly)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING"""
    jdbc_sink = JdbcSink.sink(jdbc_sql, jdbc_row_type, jdbc_options, jdbc_execution)
    results.map(ToJdbcRow(), output_type=jdbc_row_type).add_sink(jdbc_sink).name(
        "idempotent-postgres-sensor-sink")

    dlt_stream = dlt_records.union(late_records)
    dlt_sink = (KafkaSink.builder()
                .set_bootstrap_servers(BROKERS)
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                    .set_topic(DLT_TOPIC)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build())
                .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build())
    dlt_stream.sink_to(dlt_sink).name("sensor-dead-letter-sink")
    return env


if __name__ == "__main__":
    LOG.info("Submitting genuine PyFlink sensor job for %s", SENSOR_TOPIC)
    build_job().execute("lubaba-sensor-stream")
