"""PyFlink streaming job for the GeoPlatform Redpanda topics.

Originally Lubaba's sensor job (sensor.live). Extended to process weather.live,
join each sensor reading with ambient weather from its nearest station, and
publish anomalies to platform.quality.alerts.

    sensor.live ─► decode+validate ─► event-time watermark ─► keyed z-score ─┐
                        │                                       │ late       │
                        └──────────── sensor.dead-letter ◄──────┘            ▼
    weather.live ─► decode+validate ─► keyed dedupe + z-score ─► broadcast ─► ambient join
                        │ malformed → weather.dead-letter        │               │
                        │ API_ERROR → alerts                     ▼               ▼
                        │                        weather_stream_observations  sensor_observations
                        └──────────────► platform.quality.alerts ◄── anomalies (both streams)

All PostgreSQL writes are idempotent (ON CONFLICT DO NOTHING) so checkpoint
replays never duplicate rows.
"""
import json
import logging
import os

from pyflink.common import Duration, Row, Time, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import BroadcastProcessFunction, KeyedProcessFunction, ProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig, ValueStateDescriptor
from pyflink.util.java_utils import get_gateway

from stream_logic import (
    TransientDependencyError,
    WeatherSourceError,
    ambient_for,
    dead_letter,
    decode_envelope,
    is_late_event,
    nearest_station,
    quality_alert,
    score_reading,
    validate_sensor,
    validate_weather,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
LOG = logging.getLogger("geoplatform-stream")

JOB_NAME = os.getenv("FLINK_JOB_NAME", "geoplatform-stream")
SENSOR_TOPIC = os.getenv("TOPIC_SENSOR_LIVE", "sensor.live")
WEATHER_TOPIC = os.getenv("TOPIC_WEATHER_LIVE", "weather.live")
SENSOR_DLT_TOPIC = os.getenv("TOPIC_SENSOR_DLT", "sensor.dead-letter")
WEATHER_DLT_TOPIC = os.getenv("TOPIC_WEATHER_DLT", "weather.dead-letter")
ALERTS_TOPIC = os.getenv("TOPIC_QUALITY_ALERTS", "platform.quality.alerts")
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
GROUP_PREFIX = os.getenv("KAFKA_GROUP_PREFIX", "geoplatform-flink")

LATE_TAG = OutputTag("late-sensor", Types.STRING())
DECODE_ERROR_TAG = OutputTag("decode-errors", Types.STRING())
ALERT_TAG = OutputTag("quality-alerts", Types.STRING())
AMBIENT_STATE = MapStateDescriptor("latest_weather_by_station", Types.STRING(), Types.STRING())


# ── Sensor branch ─────────────────────────────────────────────────────────────

class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event["event_time_ms"]


class DecodeAndValidate(ProcessFunction):
    """Confluent-Avro decode + contract validation. Bad records go to the DLT side output."""

    def __init__(self, validator):
        self.validator = validator

    def process_element(self, raw, ctx):
        meta = {}
        try:
            meta = json.loads(raw)
            meta, record = decode_envelope(meta, registry_url=SCHEMA_REGISTRY)
            yield self.validator(record, meta)
        except TransientDependencyError:
            # Registry outage: fail the task and replay from checkpoint instead of quarantining.
            raise
        except WeatherSourceError as exc:
            yield ALERT_TAG, quality_alert(
                "weather_stream_observations", exc.station_id, "WEATHER_SOURCE_ERROR", None,
                exc.event_time_ms, "MEDIUM", reason=exc.flag)
        except Exception as exc:
            yield DECODE_ERROR_TAG, dead_letter(meta, f"decode/validation: {type(exc).__name__}: {exc}")


def _history_state(runtime_context, name):
    descriptor = ValueStateDescriptor(name, Types.STRING())
    ttl = (StateTtlConfig.new_builder(Time.days(30))
           .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
           .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
           .build())
    descriptor.enable_time_to_live(ttl)
    return runtime_context.get_state(descriptor)


class RollingSensorAnomaly(KeyedProcessFunction):
    """Per sensor/measurement Flink-managed 30-reading baseline (Lubaba's algorithm)."""

    def open(self, runtime_context):
        self.history = _history_state(runtime_context, "rolling_history_json")

    def process_element(self, event, ctx):
        if is_late_event(event["event_time_ms"], ctx.timer_service().current_watermark()):
            yield LATE_TAG, dead_letter(event, "event timestamp is at or behind the current Flink watermark")
            return
        raw_history = self.history.value()
        prior = json.loads(raw_history) if raw_history else []
        anomalous, updated = score_reading(prior, event["value"])
        event["is_anomaly"] = anomalous
        event["quality_flag"] = "ANOMALY" if anomalous else "GOOD"
        event["window_avg"] = sum(updated) / len(updated)
        event["window_min"] = min(updated)
        event["window_max"] = max(updated)
        self.history.update(json.dumps(updated, separators=(",", ":")))
        if anomalous:
            yield ALERT_TAG, quality_alert(
                "sensor_observations", event["sensor_id"], "ZSCORE_ANOMALY", event["value"],
                event["event_time_ms"], "HIGH", measurement_type=event["measurement_type"],
                rolling_mean=event["window_avg"])
        yield event


class AmbientWeatherJoin(BroadcastProcessFunction):
    """Enrich sensor readings with the latest fresh observation from the nearest weather station."""

    def process_element(self, event, ctx):
        station = nearest_station(event["latitude"], event["longitude"])
        cached = ctx.get_broadcast_state(AMBIENT_STATE).get(station)
        temperature, humidity = ambient_for(event["event_time_ms"], json.loads(cached) if cached else None)
        event["nearest_weather_station"] = station
        event["ambient_temperature_c"] = temperature
        event["ambient_humidity_pct"] = humidity
        yield event

    def process_broadcast_element(self, weather, ctx):
        state = ctx.get_broadcast_state(AMBIENT_STATE)
        current = state.get(weather["station_id"])
        if current and json.loads(current)["event_time_ms"] >= weather["event_time_ms"]:
            return
        state.put(weather["station_id"], json.dumps({
            "event_time_ms": weather["event_time_ms"],
            "temperature_c": weather["temperature_c"],
            "humidity_pct": weather["humidity_pct"],
        }))


# ── Weather branch ────────────────────────────────────────────────────────────

class WeatherDedupeAndScore(KeyedProcessFunction):
    """Per station: drop repeat polls of the same observation, then z-score temperature."""

    def open(self, runtime_context):
        self.history = _history_state(runtime_context, "weather_temp_history_json")
        self.last_seen = runtime_context.get_state(ValueStateDescriptor("last_event_time_ms", Types.LONG()))

    def process_element(self, event, ctx):
        last = self.last_seen.value()
        if last is not None and event["event_time_ms"] <= last:
            return  # OpenWeatherMap returns the same `dt` until the station reports again.
        self.last_seen.update(event["event_time_ms"])
        raw_history = self.history.value()
        prior = json.loads(raw_history) if raw_history else []
        anomalous, updated = score_reading(prior, event["temperature_c"])
        self.history.update(json.dumps(updated, separators=(",", ":")))
        event["is_anomaly"] = anomalous
        event["window_avg_temp"] = sum(updated) / len(updated)
        event["window_min_temp"] = min(updated)
        event["window_max_temp"] = max(updated)
        if anomalous or event["quality_flag"] == "ANOMALY":
            yield ALERT_TAG, quality_alert(
                "weather_stream_observations", event["station_id"],
                "ZSCORE_ANOMALY" if anomalous else "RANGE_ANOMALY", event["temperature_c"],
                event["event_time_ms"], "HIGH" if anomalous else "MEDIUM", rolling_mean=event["window_avg_temp"])
        yield event


# ── Sinks ─────────────────────────────────────────────────────────────────────

def _utc(event_ms):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc)


SENSOR_COLUMNS = [
    ("source_topic", Types.STRING()), ("source_partition", Types.INT()), ("source_offset", Types.LONG()),
    ("sensor_id", Types.STRING()), ("event_time", Types.SQL_TIMESTAMP()),
    ("latitude", Types.DOUBLE()), ("longitude", Types.DOUBLE()), ("measurement_type", Types.STRING()),
    ("value", Types.DOUBLE()), ("unit", Types.STRING()), ("quality_flag", Types.STRING()),
    ("window_avg", Types.DOUBLE()), ("window_max", Types.DOUBLE()), ("window_min", Types.DOUBLE()),
    ("is_anomaly", Types.BOOLEAN()), ("nearest_weather_station", Types.STRING()),
    ("ambient_temperature_c", Types.DOUBLE()), ("ambient_humidity_pct", Types.DOUBLE()),
]
WEATHER_COLUMNS = [
    ("source_topic", Types.STRING()), ("source_partition", Types.INT()), ("source_offset", Types.LONG()),
    ("station_id", Types.STRING()), ("city_name", Types.STRING()), ("event_time", Types.SQL_TIMESTAMP()),
    ("latitude", Types.DOUBLE()), ("longitude", Types.DOUBLE()), ("temperature_c", Types.DOUBLE()),
    ("humidity_pct", Types.DOUBLE()), ("wind_speed_ms", Types.DOUBLE()), ("cloud_cover_pct", Types.DOUBLE()),
    ("rainfall_1h_mm", Types.DOUBLE()), ("weather_condition", Types.STRING()), ("api_poll_ms", Types.INT()),
    ("quality_flag", Types.STRING()), ("window_avg_temp", Types.DOUBLE()), ("window_max_temp", Types.DOUBLE()),
    ("window_min_temp", Types.DOUBLE()), ("is_anomaly", Types.BOOLEAN()),
]


def _row_type(columns):
    return Types.ROW_NAMED([name for name, _ in columns], [kind for _, kind in columns])


def _to_row(columns):
    names = [name for name, _ in columns]

    def convert(event):
        values = dict(event, event_time=_utc(event["event_time_ms"]))
        return Row(*[values.get(name) for name in names])
    return convert


def _insert_sql(table, columns, conflict):
    names = [name for name, _ in columns]
    return (f"INSERT INTO public.{table} ({', '.join(names)}) VALUES ({', '.join('?' * len(names))}) "
            f"ON CONFLICT {conflict} DO NOTHING")


def _jdbc_sink(table, columns, conflict):
    options = (JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
               .with_url("jdbc:postgresql://%s:%s/%s?sslmode=%s" % (
                   os.getenv("POSTGRES_HOST", "postgres"), os.getenv("POSTGRES_PORT", "5432"),
                   os.getenv("POSTGRES_DB", "geoplatform"), os.getenv("POSTGRES_SSLMODE", "require")))
               .with_driver_name("org.postgresql.Driver")
               .with_user_name(os.getenv("POSTGRES_USER", "geoplatform"))
               .with_password(os.environ["POSTGRES_PASSWORD"])
               .build())
    execution = (JdbcExecutionOptions.builder().with_batch_size(int(os.getenv("JDBC_BATCH_SIZE", "1")))
                 .with_batch_interval_ms(int(os.getenv("JDBC_BATCH_INTERVAL_MS", "0")))
                 .with_max_retries(5).build())
    return JdbcSink.sink(_insert_sql(table, columns, conflict), _row_type(columns), options, execution)


def _string_sink(topic):
    return (KafkaSink.builder()
            .set_bootstrap_servers(BROKERS)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())


def _committed_or_earliest():
    """Resume from committed group offsets; earliest only for a brand-new group.

    Built through the Java API because PyFlink's KafkaOffsetResetStrategy assumes the
    shaded flink-sql-connector-kafka jar, while this image ships flink-connector-kafka.
    """
    jvm = get_gateway().jvm
    initializer = jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
    return KafkaOffsetsInitializer(
        initializer.committedOffsets(jvm.org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))


def _source(env, topic, name):
    builder = (KafkaSource.builder()
               .set_bootstrap_servers(BROKERS)
               .set_group_id(f"{GROUP_PREFIX}-{name}")
               .set_topics(topic)
               .set_starting_offsets(_committed_or_earliest()))
    # Java KafkaRecordDeserializationSchema bundled in the image (flink/connectors/):
    # emits JSON with topic/partition/offset and base64 key/value bytes.
    builder._j_builder.setDeserializer(get_gateway().jvm.com.lubaba.flink.SensorRecordEnvelopeSchema())
    return (env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), f"redpanda-{name}-source",
                            type_info=Types.STRING())
            .uid(f"redpanda-{name}-source"))


def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    env.enable_checkpointing(int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")), CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(5000)
    checkpoint_config.set_checkpoint_timeout(60000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.set_externalized_checkpoint_cleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(10, 5000))
    pickled = Types.PICKLED_BYTE_ARRAY()

    # Weather: decode → dedupe/score per station → PostGIS + broadcast for the ambient join.
    weather_decoded = (_source(env, WEATHER_TOPIC, "weather")
                       .process(DecodeAndValidate(validate_weather), output_type=pickled)
                       .uid("weather-decode").name("weather-decode-validate"))
    weather_scored = (weather_decoded
                      .key_by(lambda event: event["station_id"], key_type=Types.STRING())
                      .process(WeatherDedupeAndScore(), output_type=pickled)
                      .uid("weather-score").name("weather-dedupe-zscore"))
    weather_scored.map(_to_row(WEATHER_COLUMNS), output_type=_row_type(WEATHER_COLUMNS)) \
        .add_sink(_jdbc_sink("weather_stream_observations", WEATHER_COLUMNS, "(station_id, event_time)")) \
        .uid("weather-postgres-sink").name("idempotent-postgres-weather-sink")

    # Sensor: decode → event-time watermark → keyed z-score → ambient join → PostGIS.
    watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(int(os.getenv("WATERMARK_OUT_OF_ORDER_SECONDS", "10"))))
        .with_timestamp_assigner(SensorTimestampAssigner())
        # Subtasks without partitions/traffic must not hold the watermark back forever.
        .with_idleness(Duration.of_seconds(int(os.getenv("WATERMARK_IDLE_SECONDS", "60")))))
    sensor_decoded = (_source(env, SENSOR_TOPIC, "sensor")
                      .process(DecodeAndValidate(validate_sensor), output_type=pickled)
                      .uid("sensor-decode").name("sensor-decode-validate"))
    sensor_scored = (sensor_decoded.assign_timestamps_and_watermarks(watermarks).uid("sensor-watermarks")
                     .key_by(lambda event: f"{event['sensor_id']}:{event['measurement_type']}",
                             key_type=Types.STRING())
                     .process(RollingSensorAnomaly(), output_type=pickled)
                     .uid("sensor-score").name("sensor-zscore"))
    sensor_enriched = (sensor_scored.connect(weather_scored.broadcast(AMBIENT_STATE))
                       .process(AmbientWeatherJoin(), output_type=pickled)
                       .uid("sensor-ambient-join").name("sensor-nearest-station-join"))
    sensor_enriched.map(_to_row(SENSOR_COLUMNS), output_type=_row_type(SENSOR_COLUMNS)) \
        .add_sink(_jdbc_sink("sensor_observations", SENSOR_COLUMNS,
                             "(source_topic, source_partition, source_offset)")) \
        .uid("sensor-postgres-sink").name("idempotent-postgres-sensor-sink")

    # Dead letters and alerts (at-least-once; consumers dedupe on source coordinates / alert_id).
    sensor_decoded.get_side_output(DECODE_ERROR_TAG).union(sensor_scored.get_side_output(LATE_TAG)) \
        .sink_to(_string_sink(SENSOR_DLT_TOPIC)).uid("sensor-dlt-sink").name("sensor-dead-letter-sink")
    weather_decoded.get_side_output(DECODE_ERROR_TAG) \
        .sink_to(_string_sink(WEATHER_DLT_TOPIC)).uid("weather-dlt-sink").name("weather-dead-letter-sink")
    sensor_scored.get_side_output(ALERT_TAG) \
        .union(weather_scored.get_side_output(ALERT_TAG), weather_decoded.get_side_output(ALERT_TAG)) \
        .sink_to(_string_sink(ALERTS_TOPIC)).uid("alerts-sink").name("quality-alerts-sink")
    return env


if __name__ == "__main__":
    LOG.info("Submitting %s for %s and %s", JOB_NAME, SENSOR_TOPIC, WEATHER_TOPIC)
    build_job().execute(JOB_NAME)
