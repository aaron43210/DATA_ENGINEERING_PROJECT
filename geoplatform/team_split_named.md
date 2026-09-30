# 🌍 Cloud-Native Geospatial Data Platform
## Team Plan — Aaron · Lubaba · Angel · Fidal · Adhityan

---

## Architecture & Data Flow

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           DATA SOURCES                                       ║
╠═══════════════╦══════════════════╦════════════════╦═══════════════════════════╣
║ Sentinel-2    ║ OpenWeatherMap   ║ OSM Overpass   ║ IoT Sensors (simulated)   ║
║ STAC API      ║ REST API         ║ REST API       ║ 3 stations — Kerala       ║
║ GeoJSON       ║ JSON — every 60s ║ JSON — weekly  ║ Avro — sub-second         ║
║ [Batch ETL]   ║ [Stream+Batch]   ║ [Batch ELT]    ║ [Streaming]               ║
╚══════╤════════╩═════════╤════════╩═══════╤════════╩══════════════╤════════════╝
       │                  │                │                       │
       ▼                  ▼                ▼                       ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                        INGESTION LAYER                                       ║
║  ┌─────────────────────────────────────┐   ┌──────────────────────────────┐  ║
║  │   AIRFLOW (Batch Orchestration)     │   │   REDPANDA (Message Broker)  │  ║
║  │   Aaron: sentinel2_ingest.py (ETL)  │   │   Aaron: sensor_producer.py  │  ║
║  │   Aaron: weather_backfill.py (ELT)  │   │   Aaron: weather_producer.py │  ║
║  │   Angel: osm_vector_ingest.py (ELT) │   │   Topics:                    │  ║
║  │   Angel: data_quality_dag.py        │   │   • sensor.temperature        │  ║
║  └──────────────────┬──────────────────┘   │   • sensor.humidity           │  ║
║                     │                       │   • sensor.soil_moisture      │  ║
║                     │                       │   • sensor.air_quality        │  ║
║                     │                       │   • weather.live              │  ║
║                     │                       │   • platform.quality.alerts   │  ║
║                     │                       └──────────────┬───────────────┘  ║
╚═════════════════════╪═══════════════════════════════════════╪══════════════════╝
                      │                                       │
                      ▼                                       ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                        PROCESSING LAYER                                      ║
║  ┌────────────────────────────┐     ┌──────────────────────────────────────┐  ║
║  │  MinIO + LakeFS            │     │  APACHE FLINK                        │  ║
║  │  Angel: docker-compose +   │     │  Lubaba: detect_anomaly()            │  ║
║  │         bucket setup       │     │  Lubaba: haversine + spatial join    │  ║
║  │  Bronze: raw JSON/Avro     │     │  Lubaba: process_sensor_event()      │  ║
║  │  Silver: Iceberg Parquet   │     │  Lubaba: process_weather_event()     │  ║
║  │  Gold:   ORC archive       │     │  Angel:  get_pg_connection()         │  ║
║  │  LakeFS: git-like branches │     │  Angel:  publish_quality_alert()     │  ║
║  └────────────────────────────┘     │  Angel:  main() consumer loop        │  ║
║                                     └──────────────────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                       STORAGE LAYER (PostgreSQL + PostGIS)                   ║
║  Angel creates schema (01_init_postgis.sql) — everyone writes to it          ║
║  ┌─────────────────────────────────────────────────────────────────────────┐ ║
║  │  satellite_observations      ← Aaron (sentinel2 ETL DAG)                │ ║
║  │  weather_observations        ← Aaron (weather backfill ELT DAG)         │ ║
║  │  weather_stream_observations ← Lubaba (Flink stream)                    │ ║
║  │  sensor_observations         ← Lubaba (Flink stream)                    │ ║
║  │  vector_features             ← Angel (osm ELT DAG)                      │ ║
║  │  admin_boundaries            ← Angel (must seed Kerala polygons)         │ ║
║  │  quality_runs                ← Angel (data_quality_dag)                 │ ║
║  │  data_products               ← Angel (seeded in init_postgis.sql)       │ ║
║  │  gdpr_field_registry         ← Angel (seeded in init_postgis.sql)       │ ║
║  │  lineage_events              ← Lubaba (Flink OpenLineage emitter)        │ ║
║  └─────────────────────────────────────────────────────────────────────────┘ ║
║  MongoDB: geoplatform_raw.weather_raw ← Aaron (weather backfill ELT, raw)    ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                      TRANSFORMATION LAYER (dbt)                              ║
║                                                                              ║
║  Aaron → stg_sentinel2.sql (view)  ─────────────────────────────────────►   ║
║           FROM satellite_observations                        satellite_       ║
║                                                              observation.sql  ║
║                                                              (Gold TABLE)     ║
║  Aaron → stg_weather.sql (view)                                              ║
║           FROM weather_stream_observations                                   ║
║                │                                                             ║
║  Angel → int_weather_spatial.sql (ephemeral CTE)  ──────────────────────►   ║
║           PostGIS ST_Contains join with admin_boundaries     environmental_   ║
║                                                              conditions.sql   ║
║                                                              (Gold TABLE)     ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                        SERVING LAYER (FastAPI + GraphQL)                     ║
║  Aaron + Lubaba                                                              ║
║  ┌──────────────────────────────────────────────────────────────────────┐    ║
║  │  Aaron:  GET /health         GET /datasets   GET /imagery            │    ║
║  │          GET /sensors        GET /weather    Auth: JWT + API Key     │    ║
║  │          GDPR: mask_pii() on lat/lon for non-admin users             │    ║
║  │                                                                      │    ║
║  │  Lubaba: GET /vector/features   GET /lineage/{job}                   │    ║
║  │          POST /graphql  (Strawberry schema — imagery + weather)      │    ║
║  └──────────────────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
╔═══════════════════════════════╗   ╔══════════════════════════════════════════╗
║  OBSERVABILITY                ║   ║  TESTING + CI/CD                         ║
║  Fidal + Adhityan             ║   ║  Lubaba + Adhityan                        ║
║  Prometheus — metrics scrape  ║   ║  pytest API test suite                   ║
║  Grafana — dashboards         ║   ║  GitHub Actions pipeline                 ║
║  Alert rules: APIDown,        ║   ║  dbt compile · docker build · lint       ║
║    HighErrorRate, Stale stream║   ║  OpenLineage schema validation           ║
║  DataHub — lineage graph      ║   ╚══════════════════════════════════════════╝
╚═══════════════════════════════╝
```

---

## Service → Pair + Leader

| Service | Pair | Leader within pair |
|---------|------|--------------------|
| **FastAPI + GraphQL** | Aaron + **Lubaba** | Aaron leads |
| **Redpanda (Producers + Schemas)** | Aaron + **Lubaba** | Aaron leads |
| **dbt Models** | Aaron + **Angel** | Aaron leads |
| **Airflow ETL + ELT DAGs** | Aaron + **Angel** | Aaron leads |
| **Apache Flink** | **Lubaba** + Angel | Lubaba leads |
| **Infrastructure (Docker, DB, MinIO, LakeFS)** | Angel + **Fidal** | Angel leads |
| **Monitoring (Prometheus + Grafana)** | **Fidal** + Adhityan | Fidal leads |
| **Testing + CI/CD** | **Lubaba** + Adhityan | Lubaba leads |

---

## Per-Person at a Glance

| Name | Pairs With | Services Involved |
|------|-----------|-------------------|
| **Aaron** | Lubaba · Angel | FastAPI · Redpanda · dbt · Airflow |
| **Lubaba** | Aaron · Angel · Adhityan | FastAPI · Redpanda · Flink · Testing/CI |
| **Angel** | Aaron · Lubaba · Fidal | dbt · Airflow · Flink · Infrastructure |
| **Fidal** | Angel · Adhityan | Infrastructure · Monitoring |
| **Adhityan** | Fidal · Lubaba | Monitoring · Testing/CI |

---

## All 51 Files — Owner Table

| # | File | Owner | Service |
|---|------|-------|---------|
| 1 | `.env` | Fidal | Infrastructure |
| 2 | `.github/workflows/ci.yml` | Lubaba | CI/CD |
| 3 | `airflow/dags/data_quality_dag.py` | Angel | Airflow |
| 4 | `airflow/dags/osm_vector_ingest.py` | Angel | Airflow |
| 5 | `airflow/dags/sentinel2_ingest.py` | Aaron | Airflow |
| 6 | `airflow/dags/weather_historical_backfill.py` | Aaron | Airflow |
| 7 | `api/Dockerfile` | Aaron | FastAPI |
| 8 | `api/app/__init__.py` | Aaron | FastAPI |
| 9 | `api/app/auth.py` | Aaron | FastAPI |
| 10 | `api/app/db/__init__.py` | Aaron | FastAPI |
| 11 | `api/app/db/postgis.py` | Aaron | FastAPI |
| 12 | `api/app/graphql_schema.py` | Lubaba | FastAPI |
| 13 | `api/app/main.py` | Aaron | FastAPI |
| 14 | `api/app/routers/__init__.py` | Aaron | FastAPI |
| 15 | `api/app/routers/datasets.py` | Aaron | FastAPI |
| 16 | `api/app/routers/imagery.py` | Aaron | FastAPI |
| 17 | `api/app/routers/lineage.py` | Lubaba | FastAPI |
| 18 | `api/app/routers/sensors.py` | Aaron | FastAPI |
| 19 | `api/app/routers/vector.py` | Lubaba | FastAPI |
| 20 | `api/app/routers/weather.py` | Aaron | FastAPI |
| 21 | `api/requirements.txt` | Aaron | FastAPI |
| 22 | `api/tests/test_main.py` | Adhityan | Testing |
| 23 | `dbt/geoplatform/dbt_project.yml` | Aaron | dbt |
| 24 | `dbt/geoplatform/models/intermediate/int_weather_spatial.sql` | Angel | dbt |
| 25 | `dbt/geoplatform/models/marts/data_products.yml` | Angel | dbt |
| 26 | `dbt/geoplatform/models/marts/environmental_conditions.sql` | Angel | dbt |
| 27 | `dbt/geoplatform/models/marts/satellite_observation.sql` | Aaron | dbt |
| 28 | `dbt/geoplatform/models/staging/sources.yml` | Aaron | dbt |
| 29 | `dbt/geoplatform/models/staging/stg_sentinel2.sql` | Aaron | dbt |
| 30 | `dbt/geoplatform/models/staging/stg_weather.sql` | Aaron | dbt |
| 31 | `dbt/geoplatform/profiles.yml` | Aaron | dbt |
| 32 | `docker-compose.yml` | Angel | Infrastructure |
| 33 | `flink/jobs/unified_stream_processor.py` | Lubaba (algorithms) + Angel (infra/loop) | Flink |
| 34 | `infra/init_db/00_create_databases.sh` | Angel | Infrastructure |
| 35 | `infra/init_db/01_init_postgis.sql` | Angel | Infrastructure |
| 36 | `lakefs/setup_repos.sh` | Angel | Infrastructure |
| 37 | `monitoring/alert_rules.yml` | Fidal | Monitoring |
| 38 | `monitoring/grafana/provisioning/dashboards/dashboards.yml` | Fidal | Monitoring |
| 39 | `monitoring/grafana/provisioning/dashboards/platform_overview.json` | Adhityan | Monitoring |
| 40 | `monitoring/grafana/provisioning/datasources/prometheus.yml` | Fidal | Monitoring |
| 41 | `monitoring/prometheus.yml` | Fidal | Monitoring |
| 42 | `openlineage/config/openlineage.yml` | Fidal | Infrastructure |
| 43 | `redpanda/console-config.yml` | Lubaba | Redpanda |
| 44 | `redpanda/producer/Dockerfile` | Aaron | Redpanda |
| 45 | `redpanda/producer/sensor_event.avsc` | Aaron | Redpanda |
| 46 | `redpanda/producer/sensor_producer.py` | Aaron | Redpanda |
| 47 | `redpanda/schemas/weather_event.avsc` | Lubaba | Redpanda |
| 48 | `redpanda/weather_producer/Dockerfile` | Aaron | Redpanda |
| 49 | `redpanda/weather_producer/weather_event.avsc` | Aaron | Redpanda |
| 50 | `redpanda/weather_producer/weather_producer.py` | Aaron | Redpanda |
| 51 | `setup.sh` | Angel | Infrastructure |

---

## 📘 AARON — Files + Functions
*Pairs with: Lubaba (FastAPI + Redpanda) · Angel (dbt + Airflow)*

---

### SERVICE: FastAPI + GraphQL (Aaron + Lubaba)

#### `api/app/main.py`

**`lifespan(app: FastAPI)`**
| | |
|---|---|
| Purpose | Creates asyncpg DB pool on startup; closes it on shutdown |
| Input | `app: FastAPI` instance |
| Output | Sets `app.state.pool`; yields control |
| Called by | FastAPI automatically on boot/shutdown |

**`get_context(request, user_info)`**
| | |
|---|---|
| Purpose | Injects DB connection + auth info into every GraphQL resolver |
| Input | `request: Request`, `user_info: dict` from `verify_access` |
| Output | yields `{"request": Request, "db": asyncpg.Connection}` |
| Called by | Strawberry GraphQL router per request |

**`GET /health`**
| | |
|---|---|
| Purpose | Docker/K8s liveness probe |
| Input | None — no auth required |
| Output | `{"status": "healthy"}` HTTP 200 |

#### `api/app/auth.py`

**`verify_access(api_key, credentials)`**
| | |
|---|---|
| Purpose | Dual-mode auth: API Key or JWT Bearer |
| Input | `api_key: str` (X-API-Key header) OR `credentials: HTTPAuthorizationCredentials` |
| Output | `{"user": str, "role": str, "privileged": bool}` |
| Failure | Raises `HTTPException(401)` |
| Called by | Every router via `Depends(verify_access)` |

**`mask_pii(data, table, is_privileged)`**
| | |
|---|---|
| Purpose | GDPR — truncates GPS coordinates to 3dp for non-admin users |
| Input | `data: dict` (one DB row), `table: str`, `is_privileged: bool` |
| Output | Same `dict` with lat/lon truncated to 3dp or `"***MASKED***"` |
| Called by | `weather.py`, `sensors.py`, `graphql_schema.py` |

#### `api/app/db/postgis.py`

**`get_db_pool()`**
| | |
|---|---|
| Purpose | Creates asyncpg connection pool at startup |
| Input | `POSTGRES_DSN` env var |
| Output | `asyncpg.Pool` |

**`get_db(request)`**
| | |
|---|---|
| Purpose | FastAPI dependency — provides one DB connection per HTTP request |
| Input | `request: Request` |
| Output | yields `asyncpg.Connection` |
| Called by | All routers via `Depends(get_db)` |

#### `api/app/routers/datasets.py` — `GET /datasets`
| | |
|---|---|
| DB | `SELECT * FROM data_products WHERE status='active'` |
| Output | `List[DataProduct]` — `product_id, name, domain, owner_team, description, sla_freshness_hours, output_port_rest, output_port_graphql` |

#### `api/app/routers/imagery.py` — `GET /imagery`
| | |
|---|---|
| Params | `max_cloud_pct: float = 50.0`, `limit: int = 50` |
| DB | `marts.satellite_observation WHERE cloud_percentage <= $1 ORDER BY acquisition_time DESC` |
| Output | `List[SatelliteRecord]` — `scene_id, satellite, acquisition_time, cloud_percentage, image_quality` |

#### `api/app/routers/sensors.py` — `GET /sensors`
| | |
|---|---|
| Params | `sensor_id`, `measurement_type`, `anomalies_only: bool`, `limit: int = 100` |
| DB | `sensor_observations` with dynamic WHERE |
| Output | `List[SensorRecord]` — 12 fields; GDPR: lat/lon masked |

#### `api/app/routers/weather.py` — `GET /weather`
| | |
|---|---|
| Params | `station_id`, `anomalies_only: bool`, `limit: int = 100` |
| DB | `weather_stream_observations` with dynamic WHERE |
| Output | `List[WeatherRecord]` — 13 fields; GDPR: lat/lon masked |

---

### SERVICE: Redpanda — Producers (Aaron + Lubaba)

#### `sensor_producer.py`

**`create_producer()`**
| | |
|---|---|
| Purpose | Init Confluent Kafka producer + Avro serializer |
| Input | `REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL` env vars; reads `sensor_event.avsc` |
| Output | `(confluent_kafka.Producer, AvroSerializer)` — snappy compression, acks=all |

**`main()`**
| | |
|---|---|
| Purpose | Infinite loop producing IoT sensor events |
| Output topics | `sensor.temperature`, `sensor.humidity`, `sensor.soil_moisture`, `sensor.air_quality` |
| Event format | `{sensor_id, event_time(ms), latitude, longitude, measurement_type, value, unit}` (Avro) |
| Rate | Every 0.5–2.0 seconds per sensor; 2% chance 3× anomaly injection |
| Key | `sensor_id` |

#### `weather_producer.py`

**`fetch_weather_data(session, station)`**
| | |
|---|---|
| Input | `aiohttp.ClientSession`, `station: {id, city, lat, lon}` |
| Output | Raw OWM API `dict` or `None` on timeout/error |

**`validate_weather(raw, station_id)`**
| | |
|---|---|
| Input | OWM response dict, station_id string |
| Output | `"GOOD"` / `"STALE"` / `"ANOMALY"` / `"API_ERROR"` |

**`create_event(raw, station, quality_flag, poll_ms)`**
| | |
|---|---|
| Input | OWM dict + station config + quality flag + poll duration |
| Output | `dict` with 13 fields matching `weather_event.avsc` |

**`main()`**
| | |
|---|---|
| Purpose | Async infinite loop — polls 5 Kerala weather stations every 60s concurrently |
| Output topic | `weather.live` — key: `station_id` |

---

### SERVICE: dbt Models (Aaron + Angel)

**`stg_sentinel2.sql`**
| | |
|---|---|
| Source | `public.satellite_observations` (Aaron's Airflow ETL writes here) |
| Materialization | `view` — renames columns only, no logic |
| Output | `staging.stg_sentinel2` |
| Consumed by | Aaron's `satellite_observation.sql` mart |

**`stg_weather.sql`**
| | |
|---|---|
| Source | `public.weather_stream_observations` (Lubaba's Flink writes here) |
| Filter | `WHERE quality_flag != 'API_ERROR'` |
| Materialization | `view` |
| Consumed by | Angel's `int_weather_spatial.sql` |

**`marts/satellite_observation.sql`**
| | |
|---|---|
| Input | `{{ ref('stg_sentinel2') }}` |
| New column | `image_quality`: `'Excellent'`(<10%), `'Good'`(<30%), `'Poor'`(≥30%) |
| Filter | `WHERE cloud_percentage <= 50` |
| Materialization | `table` + GiST index on geometry + btree on `acquisition_time` |
| Read by | Aaron's `routers/imagery.py`, Lubaba's `graphql_schema.py` |

---

### SERVICE: Airflow ETL + ELT (Aaron + Angel)

**`sentinel2_ingest.py`** — ETL, daily 2 AM

**`extract_stac_data(**context)`**
| Input | Airflow `context` (reads `execution_date`) |
| Output | XCom `"raw_scenes"` → `List[dict]` GeoJSON features from STAC API |

**`transform_and_load_bronze(**context)`**
| Input | XCom `"raw_scenes"` |
| Transform | Drop `cloud_cover > 80%` |
| Output | S3 `s3://geoplatform-bronze/sentinel2/YYYY/MM/DD/{scene_id}.json` + XCom `"clean_scenes"` |

**`load_to_postgis_and_lakefs(**context)`**
| Input | XCom `"clean_scenes"` |
| Output | Rows in `public.satellite_observations`; LakeFS commit on `bronze` branch |
| PostGIS | `ST_GeomFromGeoJSON(bbox_json)` |

**`weather_historical_backfill.py`** — ELT, daily midnight

**`fetch_weather_for_station(station, date)`**
| Input | station dict + datetime |
| Output | OWM API `dict` or `None` |

**`load_raw_to_bronze`** → `s3://geoplatform-bronze/historical/weather/YYYY/MM/DD/WX_XXX.json`

**`load_to_mongodb`** → `geoplatform_raw.weather_raw` (schemaless ELT landing)

**`load_to_postgres`** → `public.weather_observations` with `ON CONFLICT DO NOTHING`

---

## 📗 LUBABA — Files + Functions
*Pairs with: Aaron (FastAPI + Redpanda) · Angel (Flink) · Adhityan (Testing/CI)*

---

### SERVICE: FastAPI + GraphQL (Aaron + Lubaba)

#### `api/app/routers/vector.py` — `GET /vector/features`
| | |
|---|---|
| Params | `feature_type: str (optional)`, `limit: int = 100` |
| DB | `vector_features` table |
| Output | `List[VectorRecord]` — `id, osm_id, feature_type, name, tags (dict)` |
| GDPR | None — OSM data is public |

#### `api/app/routers/lineage.py` — `GET /lineage/{job_name}`
| | |
|---|---|
| Params | `job_name: str` (path, ILIKE partial match), `limit: int = 20` |
| DB | `lineage_events WHERE job_name ILIKE '%{job_name}%' ORDER BY event_time DESC` |
| Output | `List[LineageEvent]` — `run_id, job_name, job_namespace, event_type, event_time, input_datasets, output_datasets` |

#### `api/app/graphql_schema.py`

**`Query.imagery(max_cloud_pct, limit)`**
| | |
|---|---|
| Input | `max_cloud_pct: float = 100.0`, `limit: int = 50` |
| DB | `marts.satellite_observation` |
| Output | `List[SatelliteObservation]` — 5 fields |

**`Query.weather(station_id, limit)`**
| | |
|---|---|
| Input | `station_id: Optional[str]`, `limit: int = 50` |
| DB | `weather_stream_observations` |
| Output | `List[WeatherObservation]` — 8 fields; GDPR masked via `mask_pii()` |

**`get_auth_status(info)`**
| | |
|---|---|
| Input | `info: strawberry.types.Info` |
| Output | `{"user": str, "role": str, "privileged": bool}` from request context |

---

### SERVICE: Redpanda — Schema + Console (Aaron + Lubaba)

**`redpanda/console-config.yml`**
| | |
|---|---|
| Purpose | Configures Redpanda Console UI — broker + Schema Registry connection |
| Critical | Missing file → Console container crashes on boot |

**`redpanda/schemas/weather_event.avsc`** (canonical schema reference)
| | |
|---|---|
| Purpose | Shared Avro schema for `weather.live` topic; Lubaba validates it's registered in Schema Registry |
| Fields | `station_id, city_name, event_time_ms, latitude, longitude, temperature_c, humidity_pct, wind_speed_ms, cloud_cover_pct, rainfall_1h_mm, weather_condition, api_poll_ms, quality_flag` |

---

### SERVICE: Flink — Algorithms (Lubaba + Angel, Lubaba leads)

**`detect_anomaly(window_key, value)`**
| | |
|---|---|
| Input | `window_key: str` e.g. `"IOT_TVM_01_temperature"`, `value: float` |
| State | updates `_windows[window_key]` — `deque(maxlen=30)` |
| Output | `bool` — `True` if `|z-score| > 3.0`; `False` if < 5 readings |

**`get_window_stats(window_key)`**
| | |
|---|---|
| Input | `window_key: str` |
| Output | `(avg, max, min)` as floats — rounded to 2dp |

**`haversine_distance(lat1, lon1, lat2, lon2)`**
| | |
|---|---|
| Input | 4 × `float` in degrees |
| Output | `float` — distance in kilometres |

**`find_nearest_weather_station(lat, lon)`**
| | |
|---|---|
| Input | sensor GPS floats |
| Output | `str` — nearest station ID e.g. `"WX_TVM"` |

**`get_ambient_conditions(sensor_lat, sensor_lon)`** — real-time join
| | |
|---|---|
| Input | sensor lat, lon |
| State reads | `_latest_weather[station_id]` — updated by every `weather.live` message |
| Output | `{"nearest_station": str, "ambient_temp": float\|None, "ambient_humidity": float\|None}` |

**`process_sensor_event(msg, pg_cur, alert_producer)`**
| | |
|---|---|
| Input | raw Kafka message (`sensor.*`), open DB cursor, alert producer |
| Steps | parse JSON → Z-score → window stats → spatial join → INSERT |
| DB INSERT | `sensor_observations` — 15 fields incl. `is_anomaly`, `nearest_weather_station`, `ambient_temperature_c` |
| Side effect | anomaly → calls Angel's `publish_quality_alert()` |

**`process_weather_event(msg, pg_cur, alert_producer)`**
| | |
|---|---|
| Input | raw Kafka message (`weather.live`), open DB cursor, alert producer |
| Steps | parse JSON → update `_latest_weather` cache → Z-score → window stats → INSERT |
| DB INSERT | `weather_stream_observations` — 17 fields incl. `window_avg_temp`, `is_anomaly` |

**`emit_lineage_start / emit_lineage_complete / emit_lineage_fail(job, run_id)`**
| | |
|---|---|
| Input | `job: str`, `run_id: str` UUID |
| Output | HTTP POST to DataHub GMS — Redpanda → PostgreSQL lineage declared |

---

### SERVICE: Testing + CI/CD (Lubaba + Adhityan, Lubaba leads)

**`.github/workflows/ci.yml`** — Lubaba writes this

| Step | What it validates |
|------|------------------|
| `flake8` | Python lint — max-line-length 120 |
| `black --check` | Code format |
| `dbt parse && dbt compile` | All SQL models compile |
| `docker compose config` | docker-compose.yml valid |
| `docker build ./api` | API Dockerfile builds |
| OpenLineage test | Schema importable + valid |

---

## 📙 ANGEL — Files + Functions
*Pairs with: Aaron (dbt + Airflow) · Lubaba (Flink) · Fidal (Infrastructure)*

---

### SERVICE: dbt Models (Aaron + Angel)

**`int_weather_spatial.sql`**
| | |
|---|---|
| Input 1 | `{{ ref('stg_weather') }}` — Aaron's staging view |
| Input 2 | `{{ source('geoplatform', 'admin_boundaries') }}` — Kerala district polygons |
| Join | `LEFT JOIN ON ST_Contains(boundary.geometry, ST_SetSRID(ST_MakePoint(lon, lat), 4326))` |
| Materialization | `ephemeral` (CTE only, not persisted) |
| New columns | `admin_region_name, admin_level, data_domain` |
| ⚠️ Requires | Angel to seed `admin_boundaries` with Kerala GeoJSON |

**`marts/environmental_conditions.sql`**
| | |
|---|---|
| Input | `{{ ref('int_weather_spatial') }}` |
| New columns | `heat_index_category`: Extreme Heat/High Heat/Warm/Comfortable/Cool; `rainfall_intensity`: No Rain/Light/Moderate/Heavy/Extreme |
| Filter | `WHERE quality_flag = 'GOOD'` |
| Materialization | `table` + btree on `event_time`, `station_id` |

**`marts/data_products.yml`**
| | |
|---|---|
| Purpose | Data Mesh metadata — domain ownership, SLA, output ports per Gold Data Product |

---

### SERVICE: Airflow (Aaron + Angel)

**`osm_vector_ingest.py`** — ELT, weekly Sunday 4 AM

**`fetch_osm_data(**context)`**
| Input | Airflow context |
| API | Overpass QL → hospitals + trunk roads in Kerala bbox |
| Output | XCom `"osm_elements"` → `List[dict]` |

**`load_raw_to_bronze(**context)`**
| Output | `s3://geoplatform-bronze/osm/YYYY/MM/DD/kerala_extract.json` |

**`load_to_postgis(**context)`**
| Geometry | `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` for hospital nodes |
| Output | rows in `public.vector_features` |

**`data_quality_dag.py`** — daily 6 AM

**`run_satellite_quality_check(**context)`**
| Output | INSERT into `quality_runs` — `{dataset="satellite_observation", pass_rate, total, passed, failed, details}` |

**`run_weather_quality_check(**context)`**
| Output | INSERT into `quality_runs` — `{dataset="environmental_conditions", ...}` |

---

### SERVICE: Flink — Infra + Loop (Lubaba + Angel)

**`get_pg_connection()`**
| Input | `POSTGRES` config dict |
| Output | `psycopg2.connection` |

**`ensure_tables()`**
| Input | None |
| Output | `CREATE TABLE IF NOT EXISTS` — `sensor_observations` + `weather_stream_observations` + 4 indexes |

**`create_alert_producer()`**
| Input | `KAFKA_SERVERS` env var |
| Output | `KafkaProducer` — JSON serializer, acks=all |

**`publish_quality_alert(producer, source_type, event, reason)`**
| Input | `producer`, `source_type: "sensor"/"weather"`, `event: dict`, `reason: str` |
| Output | message to `platform.quality.alerts` — `{alert_id: UUID, dataset, source_id, alert_type, value, timestamp}` |

**`main()`**
| Startup | Retry Redpanda 10× (5s delay); `ensure_tables()`; emit lineage START |
| Consumer | `KafkaConsumer` on all 5 topics simultaneously |
| Routing | `weather.live` → `process_weather_event`; else → `process_sensor_event` |
| Fault | `try/except` per message; reconnects on DB drop |

---

### SERVICE: Infrastructure (Angel + Fidal, Angel leads)

**`00_create_databases.sh`**
| Output | `DATABASE airflow` + `USER airflow` — runs once on first Postgres boot |

**`01_init_postgis.sql`**
| Output | `postgis` + `postgis_topology` extensions; 10 tables; 8 indexes; `data_products` + `gdpr_field_registry` seeded |
| ⚠️ | `admin_boundaries` starts empty — Angel seeds Kerala polygons |

**`lakefs/setup_repos.sh`**
| Output | LakeFS repo `geoplatform` + branches: `bronze, silver, gold, staging, main` |
| Run | Manually once: `bash lakefs/setup_repos.sh` |

**`docker-compose.yml`** — 16 services, boot order:
```
Tier 1: postgres  minio  mongodb  redpanda
Tier 2: minio-setup  lakefs  iceberg-rest
Tier 3: redpanda-console  airflow-init  datahub-quickstart
Tier 4: airflow-webserver  airflow-scheduler
Tier 5: sensor-producer  weather-producer  flink-jobmanager  flink-taskmanager
Tier 6: api  prometheus  grafana
```

---

## 📕 FIDAL — Files + Functions
*Pairs with: Angel (Infrastructure) · Adhityan (Monitoring)*

---

### SERVICE: Infrastructure (Angel + Fidal)

**`.env`**
| Purpose | Env var template for docker-compose |
| Critical var | `OPENWEATHER_API_KEY` — real OWM key or `demo` |

**`openlineage/config/openlineage.yml`**
| Purpose | Airflow → DataHub lineage transport config |
| Transport | HTTP POST to `http://datahub-gms:8080` |
| Namespace | `geoplatform.airflow` |

---

### SERVICE: Monitoring (Fidal + Adhityan, Fidal leads)

**`monitoring/prometheus.yml`**
| Targets | `prometheus:9090` (self), `api:8000/metrics` |
| Interval | 15s |
| To add | `redpanda:9644/metrics`, `flink-jobmanager:8081/metrics` |

**`monitoring/alert_rules.yml`**

`APIDown`:
| Condition | `up{job="geoplatform-api"} == 0` for 2+ minutes |
| Severity | `critical` |

`HighErrorRate`:
| Condition | 5xx rate > 5% for 3+ minutes |
| Severity | `warning` |

*Fidal to add:* `WeatherStreamStale` (no new row in `weather_stream_observations` > 10 min), `SensorStreamStale` (no new row in `sensor_observations` > 5 min)

**`grafana/provisioning/datasources/prometheus.yml`**
| Purpose | Auto-connects Grafana → Prometheus on first boot |

**`grafana/provisioning/dashboards/dashboards.yml`**
| Purpose | Grafana dashboard auto-provisioning config |

---

## 📓 ADHITYAN — Files + Functions
*Pairs with: Fidal (Monitoring) · Lubaba (Testing/CI)*

---

### SERVICE: Monitoring (Fidal + Adhityan)

**`grafana/provisioning/dashboards/platform_overview.json`**
| Current | API liveness timeseries panel |
| Adhityan to add | Redpanda msg/sec, sensor anomaly rate %, weather stream freshness, Flink lag |

---

### SERVICE: Testing (Lubaba + Adhityan)

**`api/tests/test_main.py`**

**`test_health_check()`**
| Input | `GET /health` — no auth |
| Expected | `200`, `{"status": "healthy"}` |

**`test_datasets_unauthorized()`**
| Input | `GET /datasets` — no headers |
| Expected | `401` |

**`test_datasets_authorized()`**
| Input | `GET /datasets` with `X-API-Key: supersecretapikey123` |
| Expected | `200` |

*Adhityan to add:*
- `test_imagery_cloud_filter()` — assert all `cloud_percentage <= max_cloud_pct`
- `test_sensors_anomaly_only()` — assert all `is_anomaly == true`
- `test_weather_gdpr_masking()` — reader JWT returns lat/lon ≤ 3dp
- `test_graphql_imagery()` — POST `/graphql`, assert valid response shape

---

## Cross-Team Contracts

| # | From | To | Contract |
|---|------|----|---------|
| C1 | Lubaba (Flink) | Aaron (dbt) | `weather_stream_observations` columns match `stg_weather.sql` exactly |
| C2 | Aaron (producers) | Lubaba (Flink) | Topic names `sensor.*` + `weather.live` must not change |
| C3 | Aaron (dbt mart) | Aaron (API) | `satellite_observation.satellite_name` aliased as `satellite` in router |
| C4 | Angel (infra) | Everyone | Hostnames: `postgres:5432` `redpanda:9092` `minio:9000` `lakefs:8000` |
| C5 | Angel (init_db) | Aaron + Lubaba | Tables must exist before any DAG or Flink write |
| C6 | Angel (admin_boundaries seed) | Angel (dbt) | `int_weather_spatial.sql` returns NULLs until Kerala polygons seeded |

---

## Quick-Start Per Person

```bash
# ANGEL — Boot platform (do this FIRST) ─────────────────────────────────
docker compose up -d postgres minio mongodb redpanda
sleep 20 && docker compose up minio-setup
docker compose up -d lakefs iceberg-rest redpanda-console
bash lakefs/setup_repos.sh
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler datahub-quickstart

# FIDAL — Verify .env + start monitoring ─────────────────────────────────
cat .env   # ensure OPENWEATHER_API_KEY is set
docker compose up -d prometheus grafana
curl http://localhost:8081/subjects   # check Schema Registry

# AARON — Start producers + trigger his DAGs ──────────────────────────────
docker compose up -d sensor-producer weather-producer
docker compose exec airflow-webserver airflow dags trigger sentinel2_ingest
docker compose exec airflow-webserver airflow dags trigger weather_historical_backfill

# LUBABA — Start Flink + trigger schema validation ─────────────────────────
docker compose up -d flink-jobmanager flink-taskmanager
docker compose logs -f flink-jobmanager   # watch anomaly detection live

# ANGEL — Trigger remaining Airflow DAGs ───────────────────────────────────
docker compose exec airflow-webserver airflow dags trigger osm_vector_ingest
docker compose exec airflow-webserver airflow dags trigger data_quality_dag

# AARON — Run dbt + start API ─────────────────────────────────────────────
cd dbt/geoplatform && dbt run --profiles-dir . && cd ../..
docker compose up -d api
curl -H "X-API-Key: supersecretapikey123" http://localhost:8000/datasets
open http://localhost:8000/docs      # Swagger — all 7 endpoints
open http://localhost:8000/graphql   # GraphQL playground

# ADHITYAN — Run tests ────────────────────────────────────────────────────
cd api && pip install pytest httpx && pytest tests/test_main.py -v
```

---

## File Count

| Name | Files | Services |
|------|-------|---------|
| **Aaron** | 22 | FastAPI (core) · dbt (staging + satellite mart) · Redpanda (producers) · Airflow (ETL + ELT) |
| **Lubaba** | 10 | FastAPI (GraphQL + 2 routers) · Redpanda (console + schema ref) · Flink (algorithms) · CI/CD |
| **Angel** | 13 | dbt (spatial + climate mart) · Airflow (OSM + quality DAGs) · Flink (infra + loop) · Infrastructure |
| **Fidal** | 6 | Infrastructure (`.env`, openlineage) · Monitoring (prometheus, alerts, grafana config) |
| **Adhityan** | 2 | Monitoring (Grafana dashboard) · Testing (pytest suite) |
| **TOTAL** | **51** | All services covered ✅ |
