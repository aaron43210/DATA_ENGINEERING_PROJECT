# Lubaba IoT sensor pipeline

This isolated project consumes sensor measurements through a genuine PyFlink DataStream job. The copied weather producer remains optional and is outside this sensor pipeline.

## Architecture

```text
MQTT sensor producer → Redpanda sensor.live → Flink KafkaSource
                                             → Confluent Avro decode + validation
                                             → event-time watermark + keyed anomaly state
                                             ├→ PostgreSQL sensor_observations
                                             └→ Redpanda sensor.dead-letter
```

`flink/jobs/sensor_flink_job.py` builds a PyFlink `StreamExecutionEnvironment`, reads `sensor.live` with Flink's Kafka source connector, applies Flink DataStream operators and keyed `ValueState`, and executes as the Flink job `lubaba-sensor-stream`. The small Java class in `flink/connectors/` preserves Kafka topic, partition, offset, key and original bytes while handing Confluent Avro values to Python. The Python decoder fetches writer schemas by the schema ID in the Confluent wire header. The existing sensor schema's `event_time` is a plain Avro `long`, so it decodes as epoch milliseconds; the decoder also normalizes timezone-aware datetimes should a compatible schema use `timestamp-millis` later.

The job validates required fields, sensor ID, measurement type and unit, finite numeric values, coordinates and timestamp before state or database operations. Supported type/unit pairs are temperature/celsius, humidity/percent, soil_moisture/raw and air_quality/aqi. Missing fields are rejected; no values are silently replaced by zero.

Event timestamps drive bounded-out-of-orderness watermarks (10 seconds by default). A record arriving after its timestamp is at or behind the current watermark goes to `sensor.dead-letter` with the original payload and source coordinates. Records within the watermark allowance are processed in arrival order. Per sensor and measurement, Flink-managed state holds the most recent 30 values; state expires after 30 days without an update. The job scores each value against prior values, then updates state. It warms up for five prior readings. A constant baseline accepts the same value and marks any different value anomalous. Otherwise it flags an absolute z-score above 3.

PostgreSQL writes use one transaction per row and a unique `(source_topic, source_partition, source_offset)` key. A replay is safely ignored by `ON CONFLICT DO NOTHING`. Database errors are retried up to five times; an exhausted retry fails the task so Flink can restore from the last completed checkpoint. This gives checkpointed Kafka source/state recovery and idempotent database effects, **not a single atomic transaction across Kafka and PostgreSQL**. The dead-letter Kafka sink is at-least-once and can contain duplicate quarantine records after a failure; use source coordinates to deduplicate them.

## Versions and components

- Flink runtime image: `flink:1.18.1-scala_2.12-java11`
- PyFlink: `apache-flink==1.18.1`
- Kafka connector: `org.apache.flink:flink-connector-kafka:3.2.0-1.18`, compiled against the Flink 1.18 line and added to the Flink runtime classpath.
- Python runtime dependencies are pinned in `flink/requirements.txt`; the connector helper build is pinned in `flink/connectors/pom.xml`.

## Configuration and startup

Copy the placeholders and set a local database password. A private MQTT broker is required only when starting the producer.

```bash
cp .env.example .env
# Set POSTGRES_PASSWORD. Set MQTT_BROKER and broker/TLS credentials to use sensor-producer.
docker compose --profile pipeline up --build -d redpanda topic-setup postgres flink-jobmanager flink-taskmanager flink-job-submit
```

To include the MQTT producer, enable its separate profile:

```bash
docker compose --profile pipeline --profile sensor up --build -d
```

The `flink-job-submit` service waits for the Flink REST API, Redpanda Schema Registry and required Compose dependencies, then submits the Python job. It restores from the newest retained local checkpoint when one exists. Checkpoint files live in the named `flink_checkpoints` volume. Local Docker volumes are suitable for development on this machine; they are not highly available storage.

Important settings (with defaults):

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `REDPANDA_BROKERS=redpanda:9092`, `SCHEMA_REGISTRY_URL=http://redpanda:8081`
- `SENSOR_TOPIC=sensor.live`, `DLT_TOPIC=sensor.dead-letter`, `KAFKA_GROUP_ID=lubaba-sensor-flink`
- `WATERMARK_OUT_OF_ORDER_SECONDS=10`, `CHECKPOINT_INTERVAL_MS=10000`, `FLINK_PARALLELISM=1`
- `MQTT_BROKER`, `MQTT_PORT`, `MQTT_TOPIC`, `MQTT_QOS`, TLS and certificate variables for the producer

## Verify the actual job

The dashboard/API being available proves only that the JobManager is running. Check that the submitted job itself is `RUNNING`:

```bash
curl -fsS http://localhost:8083/jobs/overview
docker compose --profile pipeline logs flink-job-submit
```

Query persisted sensor rows:

```bash
docker compose --profile pipeline exec postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c 'SELECT source_topic, source_partition, source_offset, sensor_id, event_time, measurement_type, value, is_anomaly FROM sensor_observations ORDER BY id DESC LIMIT 20;'
```

Inspect malformed/late events through Redpanda Console at `http://localhost:8088` when the optional `console` profile is running, or consume `sensor.dead-letter` with `rpk` inside the Redpanda container. Schema Registry outages fail/restart the processing task rather than misclassifying otherwise valid messages as malformed. If dead-letter publication fails, the sink fails and Flink retries from a completed checkpoint; malformed input is not silently acknowledged by a standalone consumer.

## Database migration

Fresh PostgreSQL volumes apply `infra/001_sensor_pipeline.sql` automatically. That migration adds nullable source-coordinate columns and a unique index without fabricating identities for existing rows. For an existing Lubaba volume, apply it manually because PostgreSQL does not replay initialization scripts:

```bash
docker compose --profile pipeline exec -T postgres \
  sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/001_sensor_pipeline.sql
```

Existing rows remain intact. The unique index permits legacy rows with null coordinates while enforcing identity for new source events.

## Tests and limitations

Offline unit tests do not contact MQTT, Redpanda, Schema Registry or PostgreSQL:

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile redpanda/producer/sensor_producer.py flink/jobs/sensor_logic.py flink/jobs/sensor_flink_job.py
docker compose config --quiet
```

Build and start the actual runtime separately with the startup command above. Verify the job via Flink REST and rows in PostgreSQL; a running container alone is not an end-to-end test. Flink checkpoint recovery depends on preserving `flink_checkpoints`; deleting that volume loses the local recovery point. A deliberate job graph change may require a fresh group ID or an explicit compatible restore plan. PostgreSQL and the Kafka dead-letter sink are not one two-phase-commit transaction.

Weather producer files and behavior are unchanged.
