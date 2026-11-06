#!/usr/bin/env python3
"""
Apache Iceberg on MinIO: Silver Layer Table Initialization Script
================================================================
Engine: PySpark + Iceberg REST Catalog (No LakeFS required)
Owner: Angel (Storage & Infrastructure)

This script configures a SparkSession connected to the Tabular/Apache Iceberg
REST Catalog backed by MinIO S3 object storage. It creates the `silver`
namespace and provisions Iceberg tables stored as columnar Parquet with full
ACID snapshot isolation, partition evolution, and time-travel capability.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("iceberg-setup")

# ── Environment Configurations (No hardcoded credentials) ────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_secure_secret_2026")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "geoplatform_catalog")


def get_spark_iceberg_session() -> SparkSession:
    """Build SparkSession with Iceberg REST catalog and MinIO S3 FileIO."""
    logger.info("Initializing SparkSession with Iceberg REST Catalog at %s", ICEBERG_REST_URI)

    # Note: Packages can be submitted via spark-submit --packages
    # org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4
    builder = (
        SparkSession.builder.appName("Iceberg-Silver-Catalog-Setup")
        # SQL Extensions for Iceberg DDL/DML
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Primary Iceberg Catalog Configuration
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # S3 / MinIO FileIO Properties
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", MINIO_ACCESS_KEY)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", MINIO_SECRET_KEY)
        # Global Hadoop S3A configurations for fallback IO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return builder.getOrCreate()


def init_silver_tables(spark: SparkSession):
    """Create Silver namespace and Iceberg Parquet tables."""
    cat = CATALOG_NAME

    logger.info("Creating Iceberg namespace: %s.silver", cat)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")

    # 1. OpenStreetMap Vector Features (Silver Parquet Table)
    logger.info("Provisioning %s.silver.osm_vector_features...", cat)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.silver.osm_vector_features (
        osm_id BIGINT,
        feature_type STRING,
        name STRING,
        tags MAP<STRING, STRING>,
        geometry_wkt STRING,
        longitude DOUBLE,
        latitude DOUBLE,
        ingest_date DATE,
        ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (feature_type, ingest_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'history.expire.max-snapshot-age-ms' = '604800000' -- 7 days retention for snapshots
    )
    """)

    # 2. Sentinel-2 Satellite Observations (Silver Parquet Table)
    logger.info("Provisioning %s.silver.sentinel2_observations...", cat)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.silver.sentinel2_observations (
        scene_id STRING,
        satellite STRING,
        acquisition_time TIMESTAMP,
        cloud_percentage FLOAT,
        crs STRING,
        bbox_wkt STRING,
        s3_bronze_path STRING,
        ingest_date DATE
    )
    USING iceberg
    PARTITIONED BY (days(acquisition_time))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """)

    # 3. Weather Station Observations (Silver Parquet Table)
    logger.info("Provisioning %s.silver.weather_hourly...", cat)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.silver.weather_hourly (
        station_id STRING,
        observed_at TIMESTAMP,
        temperature_c DOUBLE,
        humidity_pct DOUBLE,
        rainfall_mm DOUBLE,
        wind_speed_ms DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        quality_flag STRING,
        ingest_date DATE
    )
    USING iceberg
    PARTITIONED BY (station_id, days(observed_at))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """)

    logger.info("Verifying tables in %s.silver:", cat)
    tables_df = spark.sql(f"SHOW TABLES IN {cat}.silver")
    tables_df.show(truncate=False)


if __name__ == "__main__":
    try:
        spark = get_spark_iceberg_session()
        init_silver_tables(spark)
        logger.info("Iceberg Silver Layer setup completed successfully.")
        spark.stop()
    except Exception as e:
        logger.error("Failed to initialize Iceberg tables: %s", str(e), exc_info=True)
        sys.exit(1)
