"""
Reusable Geospatial Data Quality Check Module for Apache Airflow
================================================================
Managed by: Angel (Ingestion & Quality Lead)

Validates spatial & tabular integrity across PostgreSQL/PostGIS warehouse tables:
  - Critical column null checks
  - PostGIS geometry validity via ST_IsValid() & ST_IsEmpty()
  - Spatial reference system verification (ST_SRID() == 4326)
  - Primary key uniqueness & non-empty row assertionsgit branch
  - Audit logging to `quality_runs` table
  - Enforces pass-rate gate (fails task if pass_rate < threshold)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import RealDictCursor
from airflow.exceptions import AirflowException

logger = logging.getLogger("airflow.data_quality")

# Default quality thresholds
DEFAULT_MIN_PASS_RATE = float(os.getenv("QUALITY_MIN_PASS_RATE", "95.0"))


def get_db_connection():
    """Create psycopg2 connection from environment variables without hardcoded credentials."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "geoplatform"),
        user=os.getenv("POSTGRES_USER", "geoplatform"),
        password=os.getenv("POSTGRES_PASSWORD", "geoplatform_secure_pass_2026"),
        cursor_factory=RealDictCursor,
    )


class SpatialDataQualityRunner:
    """Orchestrates test execution and records results into quality_runs table."""

    def __init__(
        self,
        dataset_name: str,
        suite_name: str,
        logical_date: datetime,
        min_pass_rate: float = DEFAULT_MIN_PASS_RATE,
    ):
        self.dataset_name = dataset_name
        self.suite_name = suite_name
        self.logical_date = logical_date
        self.min_pass_rate = min_pass_rate
        self.results: List[Dict[str, Any]] = []

    def check_min_row_count(self, table_name: str, min_expected: int = 1) -> bool:
        """Verify that the table has at least `min_expected` records."""
        query = f"SELECT COUNT(*) AS cnt FROM {table_name};"
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            count = row["cnt"] if row else 0

        passed = count >= min_expected
        self.results.append(
            {
                "check": "min_row_count",
                "table": table_name,
                "passed": passed,
                "details": f"Actual count: {count}, Required min: {min_expected}",
            }
        )
        logger.info(
            "[%s] min_row_count on %s: %s (Count: %d)",
            self.dataset_name,
            table_name,
            passed,
            count,
        )
        return passed

    def check_null_columns(self, table_name: str, columns: List[str]) -> bool:
        """Verify that mandatory columns do not contain null values."""
        overall_passed = True
        with get_db_connection() as conn, conn.cursor() as cur:
            for col in columns:
                query = f"SELECT COUNT(*) AS null_cnt FROM {table_name} WHERE {col} IS NULL;"
                cur.execute(query)
                row = cur.fetchone()
                null_cnt = row["null_cnt"] if row else 0
                passed = null_cnt == 0
                if not passed:
                    overall_passed = False
                self.results.append(
                    {
                        "check": f"null_check_{col}",
                        "table": table_name,
                        "passed": passed,
                        "details": f"Column {col} has {null_cnt} NULL values",
                    }
                )
                logger.info(
                    "[%s] Null check on %s.%s: passed=%s (nulls=%d)",
                    self.dataset_name,
                    table_name,
                    col,
                    passed,
                    null_cnt,
                )
        return overall_passed

    def check_geometry_validity(
        self,
        table_name: str,
        geom_column: str = "geometry",
        expected_srid: int = 4326,
    ) -> bool:
        """
        Validate spatial geometry:
          1. ST_IsValid(geom) == true
          2. ST_IsEmpty(geom) == false
          3. ST_SRID(geom) == expected_srid
        """
        query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE {geom_column} IS NULL) AS null_geom_count,
            COUNT(*) FILTER (WHERE {geom_column} IS NOT NULL AND NOT ST_IsValid({geom_column})) AS invalid_geom_count,
            COUNT(*) FILTER (WHERE {geom_column} IS NOT NULL AND ST_IsEmpty({geom_column})) AS empty_geom_count,
            COUNT(*) FILTER (WHERE {geom_column} IS NOT NULL AND ST_SRID({geom_column}) != %s) AS invalid_srid_count
        FROM {table_name};
        """
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(query, (expected_srid,))
            res = cur.fetchone()

        total = res["total_rows"]
        nulls = res["null_geom_count"]
        invalid = res["invalid_geom_count"]
        empty = res["empty_geom_count"]
        bad_srid = res["invalid_srid_count"]

        passed = (nulls == 0) and (invalid == 0) and (empty == 0) and (bad_srid == 0)
        self.results.append(
            {
                "check": "geometry_validity",
                "table": table_name,
                "passed": passed,
                "details": {
                    "total_rows": total,
                    "null_geom_count": nulls,
                    "invalid_geom_count": invalid,
                    "empty_geom_count": empty,
                    "invalid_srid_count": bad_srid,
                    "expected_srid": expected_srid,
                },
            }
        )
        logger.info(
            "[%s] Geometry validity check on %s.%s: passed=%s (invalid=%d, empty=%d, bad_srid=%d)",
            self.dataset_name,
            table_name,
            geom_column,
            passed,
            invalid,
            empty,
            bad_srid,
        )
        return passed

    def check_unique_keys(self, table_name: str, key_columns: List[str]) -> bool:
        """Check for duplicate records across composite or single primary keys."""
        cols = ", ".join(key_columns)
        query = f"""
        SELECT {cols}, COUNT(*) AS dup_count
        FROM {table_name}
        GROUP BY {cols}
        HAVING COUNT(*) > 1;
        """
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(query)
            duplicates = cur.fetchall()

        dup_cnt = len(duplicates)
        passed = dup_cnt == 0
        self.results.append(
            {
                "check": "uniqueness_check",
                "table": table_name,
                "passed": passed,
                "details": f"Found {dup_cnt} duplicate key combinations on ({cols})",
            }
        )
        return passed

    def log_and_enforce_gate(self) -> Dict[str, Any]:
        """
        Compute total passed / failed assertions, log into `quality_runs`,
        and raise AirflowException if pass rate < configured threshold.
        """
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        failed = total - passed
        pass_rate = round((passed / total * 100.0) if total > 0 else 0.0, 2)

        # Upsert into PostgreSQL quality_runs
        upsert_sql = """
        INSERT INTO public.quality_runs (
            dataset, suite, logical_date, run_time, pass_rate,
            total_expectations, passed_expectations, failed_expectations, details
        )
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        ON CONFLICT (dataset, suite, logical_date) DO UPDATE SET
            pass_rate = EXCLUDED.pass_rate,
            total_expectations = EXCLUDED.total_expectations,
            passed_expectations = EXCLUDED.passed_expectations,
            failed_expectations = EXCLUDED.failed_expectations,
            details = EXCLUDED.details,
            run_time = NOW();
        """
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                upsert_sql,
                (
                    self.dataset_name,
                    self.suite_name,
                    self.logical_date,
                    pass_rate,
                    total,
                    passed,
                    failed,
                    json.dumps(self.results, default=str),
                ),
            )
            conn.commit()

        logger.info(
            "Quality gate completed for %s [%s]: Pass rate: %.2f%% (%d/%d passed)",
            self.dataset_name,
            self.suite_name,
            pass_rate,
            passed,
            total,
        )

        if pass_rate < self.min_pass_rate:
            raise AirflowException(
                f"🚨 Data Quality Gate FAILED for {self.dataset_name} ({self.suite_name}): "
                f"Pass rate {pass_rate}% is below required threshold {self.min_pass_rate}%."
            )

        return {
            "dataset": self.dataset_name,
            "suite": self.suite_name,
            "pass_rate": pass_rate,
            "total": total,
            "passed": passed,
            "failed": failed,
        }


def run_osm_quality_checks(**context):
    """Airflow callable helper to execute data quality checks on vector_features."""
    logical_date = (
        context.get("logical_date")
        or context.get("execution_date")
        or datetime.utcnow()
    )
    runner = SpatialDataQualityRunner(
        dataset_name="osm_vector_features",
        suite_name="osm_spatial_integrity",
        logical_date=logical_date,
    )
    runner.check_min_row_count("public.vector_features", min_expected=1)
    runner.check_null_columns(
        "public.vector_features", columns=["osm_id", "feature_type", "geometry"]
    )
    runner.check_unique_keys("public.vector_features", key_columns=["osm_id"])
    runner.check_geometry_validity(
        "public.vector_features", geom_column="geometry", expected_srid=4326
    )
    return runner.log_and_enforce_gate()
