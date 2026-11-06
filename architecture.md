# Cloud-Native Geospatial Data Platform — v2

## 1. Data Sources

| Source | Type | Frequency | Owner |
|---|---|---:|---|
| Sentinel-2 | Satellite imagery (STAC/GeoJSON) | Daily batch ETL | Aaron |
| OpenWeatherMap | Weather (REST JSON) | Stream + batch (60s) | Aaron |
| OSM Overpass | Vector data (REST JSON) | Weekly batch ELT | Angel |
| IoT Sensors | 3 stations, Kerala (Avro) | Streaming (sub-second) | Aaron |

---

## 2. Ingestion Layer

- Apache Airflow — orchestrates batch ETL/ELT for Sentinel-2, weather history, and OSM; also runs data-quality checks. (Aaron + Angel)
- Redpanda — Kafka-compatible broker for real-time sensor and weather streams, plus quality-alert topics. (Aaron + Lubaba)

---

## 3. Processing Layer

- MinIO — object storage and the single raw landing zone (Bronze). Holds Bronze/Silver/Gold layers and absorbs the former MongoDB role for raw JSON landing, schemaless storage, and historical backfill. (Angel)
- Layout: `s3://bronze/source=<x>/ingest_date=<d>/hour=<h>/*.jsonl`, immutable and append-only, partitioned by source/date/hour, with retention/GDPR lifecycle policies.
- Apache Flink — real-time stream processing, anomaly detection, and spatial joins; writes processed data to Postgres and publishes quality + lineage events. (Lubaba + Angel)

> Removed: LakeFS — Iceberg snapshots and time-travel cover versioning needs.

---

## 4. Storage Layer

- PostgreSQL + PostGIS — main relational and spatial store for processed and curated tables, with spatial indexes. (Angel)
- Apache Iceberg (on MinIO) — open table format for analytical data; the versioning and time-travel backbone for Silver, Parquet-based analytical data, served via a REST catalog. (Angel)

> Removed: MongoDB — its landing-zone and backfill responsibilities are now handled by MinIO Bronze.

---

## 5. Transformation Layer

- dbt — staging → intermediate → mart models, including spatial joins and business logic; builds Gold data products such as `satellite_observation` and `environmental_conditions`. (Aaron + Angel)

---

## 6. Serving Layer — hybrid with a clean boundary

- FastAPI (REST) — tiles, raster access, bulk exports, downloads, health checks, webhooks, OGC-style and cacheable endpoints optimized for CDN friendliness. (Aaron + Lubaba)
- GraphQL — flexible multi-resource queries for the frontend and dashboards only.
- Rule: no operation is duplicated across both; REST owns heavy/binary/standardized interfaces, while GraphQL owns flexible queries. Auth and GDPR data masking are enforced at this layer. (Aaron + Lubaba)

---

## 7. Observability Layer

- OpenMetadata — data catalog, lineage, and GDPR governance/PII tagging, fed by OpenLineage events from Airflow, Flink, and dbt. Replaces DataHub with a lighter operational footprint and stronger governance support. (Fidal)
- Prometheus — metrics collection and alerting. (Fidal + Adhityan)
- Grafana — dashboards and platform/pipeline health monitoring. (Fidal + Adhityan)

> Note: OpenLineage is the event standard emitted by your tools; OpenMetadata is the platform that stores and visualizes it. They are complementary layers, not alternatives.

---

## Data flow (end to end)

```mermaid
graph TD
    Sources[Sources\nSentinel-2 / Weather / OSM / IoT]
    Airflow[Airflow\n(batch ETL/ELT)]
    Redpanda[Redpanda\nKafka-compatible broker]
    MinIO[(MinIO\nBronze / Silver / Gold)]
    Flink[Flink\nreal-time processing\nspatial joins + anomalies]
    Postgres[(PostgreSQL + PostGIS)]
    Iceberg[(Iceberg on MinIO\nversioning + time-travel)]
    dbt[dbt\nGold product builds]
    API[FastAPI REST\n+ GraphQL]
    OpenMetadata[OpenMetadata\nCatalog + governance]
    Prometheus[Prometheus]
    Grafana[Grafana]
    OpenLineage[OpenLineage\nEvents]

    Sources --> Airflow
    Sources --> Redpanda

    Airflow --> MinIO
    Redpanda --> Flink
    Flink --> Postgres
    Flink --> OpenLineage

    MinIO --> Iceberg
    Postgres --> dbt
    Iceberg --> dbt

    dbt --> API
    API --> OpenMetadata
    Airflow --> OpenLineage
    Flink --> OpenLineage
    dbt --> OpenLineage

    Prometheus --> Grafana
    OpenMetadata --> Grafana
    OpenLineage --> OpenMetadata
```

---

## What changed vs v1

| Change | Before | After | Why |
|---|---|---|---|
| MongoDB | Separate document store for raw/backfill | MinIO Bronze | No overlap; object storage is better for immutable raw and replay workflows |
| LakeFS | LakeFS + Iceberg | Iceberg only | Iceberg is the industry standard; snapshots already provide versioning and time-travel |
| API boundary | REST + GraphQL overlapping | REST for binary/standardized APIs + GraphQL for flexible queries | Clear ownership and no duplicated operations |
| Metadata stack | DataHub | OpenMetadata + OpenLineage | Lighter operational footprint and stronger GDPR/PII governance |

Net result: three heavy components were removed, and governance capability was added, creating a leaner platform that is easier to defend in a production review.

---

## Architecture Summary

```text
Sources (Sentinel-2 / Weather / OSM / IoT)
        │
   ┌────┴────┐
 Airflow   Redpanda                         ← Ingestion
 (batch)   (streaming)
        │         │
        ▼         ▼
      MinIO  ◄──►  Flink                     ← Processing
   (Bronze/Silver/Gold,              (real-time, spatial joins,
    raw landing + backfill)           anomaly detection)
        │
   ┌────┴─────────────┐
 PostgreSQL+PostGIS  Iceberg (on MinIO)      ← Storage
        │
       dbt (Gold data products)              ← Transformation
        │
   FastAPI REST  +  GraphQL                  ← Serving (auth + GDPR masking)
        │
 OpenMetadata ◄─(OpenLineage)─ Airflow/Flink/dbt
 Prometheus + Grafana                        ← Observability
```

---

## Final Notes

This v2 design reflects the current production-oriented direction of the platform:

- MinIO is now the unified raw landing zone and historical replay layer.
- Iceberg provides the versioning backbone instead of LakeFS.
- OpenMetadata + OpenLineage replaces DataHub with a governance-first metadata workflow.
- FastAPI and GraphQL are split by responsibility instead of overlapping in the same workload.

The Airflow folder is the correct first major milestone in this architecture, and the rest of the platform can now be built in the same pattern around the v2 boundary definitions.
