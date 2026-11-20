"""Delta snapshot replication orchestration."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, MapType, StructType

from app.config import Settings, load_settings
from app.postgres import (
    acquire_table_lock,
    apply_retention,
    cleanup_load_table,
    create_load_table,
    is_version_published,
    load_dataframe_partitions,
    postgres_type,
    publish_drop_create,
    publish_table_swap,
    publish_truncate,
    publish_versioned_view,
    release_table_lock,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplicationResult:
    source: str
    target_schema: str
    target_table: str
    publication_strategy: str
    versioning_enabled: bool
    delta_version: int | None
    physical_table: str | None
    rows_written: int | None
    status: str
    started_at: str
    completed_at: str
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _physical_table_name(logical_table: str, delta_version: int) -> str:
    suffix = f"_v{delta_version}"
    if len(logical_table.encode("utf-8")) + len(suffix.encode("utf-8")) > 63:
        raise ValueError(
            f"Target table name '{logical_table}' is too long to append version suffix '{suffix}' "
            "within PostgreSQL's 63-byte identifier limit"
        )
    return f"{logical_table}{suffix}"


def _get_delta_version(spark: SparkSession, source: str) -> int:
    delta_table = DeltaTable.forPath(spark, source)
    row = delta_table.history(1).select("version").first()
    if row is None:
        raise RuntimeError(f"Could not determine the latest Delta version for {source}")
    return int(row["version"])


def _validate_schema(schema: StructType) -> None:
    for field in schema.fields:
        postgres_type(field.dataType)


def _read_snapshot(spark: SparkSession, source: str, version: int):
    return spark.read.format("delta").option("versionAsOf", version).load(source)


def replicate(
    source: str,
    target_schema: str,
    target_table: str,
    config_path: str | None = None,
    publication_strategy: str | None = None,
    spark: SparkSession | None = None,
) -> ReplicationResult:
    """Replicate one complete Delta snapshot into PostgreSQL.

    The optional SparkSession is useful when the host Airflow environment manages
    Spark session lifecycle itself. When omitted, the function obtains a session
    using SparkSession.builder.getOrCreate().
    """
    started_clock = time.monotonic()
    started_at = datetime.now(timezone.utc)
    settings: Settings = load_settings(config_path, publication_strategy)
    logging.getLogger().setLevel(settings.log_level)
    connection_kwargs = settings.postgres.connection_kwargs()
    spark_session = spark or SparkSession.builder.appName("delta-to-sql").getOrCreate()
    owns_spark_session = spark is None
    load_table: str | None = None
    lock_connection: psycopg.Connection | None = None
    delta_version: int | None = None
    physical_table: str | None = None
    rows_written: int | None = None
    status = "FAILED"

    try:
        lock_connection = psycopg.connect(**connection_kwargs, autocommit=True)
        acquire_table_lock(lock_connection, target_schema, target_table)

        delta_version = _get_delta_version(spark_session, source)
        dataframe = _read_snapshot(spark_session, source, delta_version)
        _validate_schema(dataframe.schema)

        if settings.versioning.enabled:
            physical_table = _physical_table_name(target_table, delta_version)
            if is_version_published(connection_kwargs, target_schema, target_table, physical_table):
                # A previous run may have published successfully but failed during retention.
                apply_retention(
                    connection_kwargs,
                    target_schema,
                    target_table,
                    physical_table,
                    settings.retention.keep_previous_versions,
                    settings.retention.max_versions,
                )
                status = "SKIPPED"
                logger.info("Delta version %s is already published as %s.%s", delta_version, target_schema, physical_table)
                return _result(
                    source, target_schema, target_table, settings.publication_strategy, settings.versioning.enabled, delta_version,
                    physical_table, rows_written, status, started_at, started_clock,
                )

        load_table = create_load_table(connection_kwargs, target_schema, target_table, dataframe.schema)
        logger.info(
            "Loading Delta snapshot source=%s version=%s target=%s.%s strategy=%s partitions=%s",
            source, delta_version, target_schema, target_table, settings.publication_strategy,
            settings.performance.partitions,
        )
        load_dataframe_partitions(
            dataframe,
            connection_kwargs,
            target_schema,
            load_table,
            settings.performance.partitions,
        )

        publication_table = physical_table if settings.versioning.enabled else target_table
        if settings.publication_strategy == "truncate":
            publish_truncate(connection_kwargs, target_schema, publication_table, load_table, dataframe.schema)
        elif settings.publication_strategy == "table_swap":
            publish_table_swap(connection_kwargs, target_schema, publication_table, load_table)
        elif settings.publication_strategy == "drop_create":
            publish_drop_create(connection_kwargs, target_schema, publication_table, load_table)
        else:
            raise ValueError(f"Unsupported publication strategy: {settings.publication_strategy}")
        load_table = None

        if settings.versioning.enabled:
            publish_versioned_view(connection_kwargs, target_schema, target_table, publication_table)
            apply_retention(
                connection_kwargs,
                target_schema,
                target_table,
                publication_table,
                settings.retention.keep_previous_versions,
                settings.retention.max_versions,
            )

        status = "PUBLISHED"
        logger.info("Replication published successfully: %s.%s", target_schema, target_table)
        return _result(
            source, target_schema, target_table, settings.publication_strategy, settings.versioning.enabled, delta_version,
            physical_table, rows_written, status, started_at, started_clock,
        )
    except Exception:
        logger.exception("Delta-to-SQL replication failed for %s -> %s.%s", source, target_schema, target_table)
        raise
    finally:
        if load_table is not None:
            try:
                cleanup_load_table(connection_kwargs, target_schema, load_table)
            except Exception:
                logger.exception("Could not clean up temporary load table %s.%s", target_schema, load_table)
        if lock_connection is not None:
            try:
                release_table_lock(lock_connection, target_schema, target_table)
            finally:
                lock_connection.close()
        if owns_spark_session:
            spark_session.stop()


def _result(
    source: str,
    target_schema: str,
    target_table: str,
    publication_strategy: str,
    versioning_enabled: bool,
    delta_version: int | None,
    physical_table: str | None,
    rows_written: int | None,
    status: str,
    started_at: datetime,
    started_clock: float,
) -> ReplicationResult:
    completed_at = datetime.now(timezone.utc)
    return ReplicationResult(
        source=source,
        target_schema=target_schema,
        target_table=target_table,
        publication_strategy=publication_strategy,
        versioning_enabled=versioning_enabled,
        delta_version=delta_version,
        physical_table=physical_table,
        rows_written=rows_written,
        status=status,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        duration_seconds=round(time.monotonic() - started_clock, 3),
    )
