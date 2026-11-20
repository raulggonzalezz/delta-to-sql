"""PostgreSQL DDL, bulk loading, publication, locking, and retention."""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

import psycopg
from psycopg import sql
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from pyspark.sql.types import TimestampNTZType
except ImportError:  # Spark versions before TimestampNTZType
    TimestampNTZType = ()  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class SchemaMappingError(ValueError):
    """Raised when a Spark type cannot be represented in PostgreSQL."""


def postgres_type(data_type: Any) -> str:
    if isinstance(data_type, StringType):
        return "TEXT"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, (ByteType, ShortType)):
        return "SMALLINT"
    if isinstance(data_type, IntegerType):
        return "INTEGER"
    if isinstance(data_type, LongType):
        return "BIGINT"
    if isinstance(data_type, FloatType):
        return "REAL"
    if isinstance(data_type, DoubleType):
        return "DOUBLE PRECISION"
    if isinstance(data_type, DecimalType):
        return f"NUMERIC({data_type.precision},{data_type.scale})"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType) or (TimestampNTZType and isinstance(data_type, TimestampNTZType)):
        return "TIMESTAMP"
    if isinstance(data_type, BinaryType):
        return "BYTEA"
    if isinstance(data_type, (ArrayType, MapType, StructType)):
        return "JSONB"
    raise SchemaMappingError(f"Unsupported Spark type: {data_type.simpleString()}")


def _column_definitions(schema: StructType) -> list[sql.Composed]:
    definitions = []
    for field in schema.fields:
        definition = sql.SQL("{} {}{}").format(
            sql.Identifier(field.name),
            sql.SQL(postgres_type(field.dataType)),
            sql.SQL("" if field.nullable else " NOT NULL"),
        )
        definitions.append(definition)
    return definitions


def _qualified(schema_name: str, table_name: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))


def _create_table(cur: psycopg.Cursor, schema_name: str, table_name: str, schema: StructType) -> None:
    cur.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            _qualified(schema_name, table_name),
            sql.SQL(", ").join(_column_definitions(schema)),
        )
    )


def _safe_table_name(prefix: str, logical_name: str) -> str:
    suffix = uuid.uuid4().hex[:12]
    base = re.sub(r"[^a-zA-Z0-9_]+", "_", logical_name).strip("_").lower() or "table"
    # PostgreSQL identifiers are limited to 63 bytes.
    return f"_{prefix}_{base[:30]}_{suffix}"[:63]


def _lock_key(schema_name: str, table_name: str) -> int:
    # Stable signed 64-bit key for PostgreSQL advisory locks.
    import hashlib

    digest = hashlib.sha256(f"{schema_name}.{table_name}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def acquire_table_lock(connection: psycopg.Connection, schema_name: str, table_name: str) -> None:
    connection.execute("SELECT pg_advisory_lock(%s)", (_lock_key(schema_name, table_name),))


def release_table_lock(connection: psycopg.Connection, schema_name: str, table_name: str) -> None:
    connection.execute("SELECT pg_advisory_unlock(%s)", (_lock_key(schema_name, table_name),))


def create_load_table(
    connection_kwargs: dict[str, Any],
    target_schema: str,
    target_table: str,
    spark_schema: StructType,
) -> str:
    load_table = _safe_table_name("d2s_load", target_table)
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(target_schema)))
            _create_table(cursor, target_schema, load_table, spark_schema)
    return load_table


def _json_compatible(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Row):
        return {key: _json_compatible(item) for key, item in value.asDict(recursive=True).items()}
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value


def write_partition(
    rows: Iterable[Row],
    connection_kwargs: dict[str, Any],
    target_schema: str,
    load_table: str,
    columns: list[str],
    complex_columns: set[str],
) -> None:
    """Write one Spark partition to PostgreSQL using COPY FROM STDIN."""
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            copy_statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
                _qualified(target_schema, load_table),
                sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            )
            with cursor.copy(copy_statement) as copy:
                for row in rows:
                    values = []
                    for column, value in zip(columns, row):
                        if column in complex_columns and value is not None:
                            value = json.dumps(
                                _json_compatible(value),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        values.append(value)
                    copy.write_row(values)


def load_dataframe_partitions(
    dataframe,
    connection_kwargs: dict[str, Any],
    target_schema: str,
    load_table: str,
    partitions: int,
) -> None:
    fields = dataframe.schema.fields
    columns = [field.name for field in fields]
    complex_columns = {
        field.name
        for field in fields
        if isinstance(field.dataType, (ArrayType, MapType, StructType))
    }
    load_df = dataframe.repartition(partitions)
    load_df.foreachPartition(
        lambda rows: write_partition(
            rows,
            connection_kwargs,
            target_schema,
            load_table,
            columns,
            complex_columns,
        )
    )


def _table_exists(cursor: psycopg.Cursor, schema_name: str, table_name: str) -> bool:
    cursor.execute(
        """SELECT EXISTS (
               SELECT 1 FROM information_schema.tables
               WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
           )""",
        (schema_name, table_name),
    )
    return bool(cursor.fetchone()[0])


def _view_exists(cursor: psycopg.Cursor, schema_name: str, view_name: str) -> bool:
    cursor.execute(
        """SELECT EXISTS (
               SELECT 1 FROM information_schema.views
               WHERE table_schema = %s AND table_name = %s
           )""",
        (schema_name, view_name),
    )
    return bool(cursor.fetchone()[0])


def _view_target(cursor: psycopg.Cursor, schema_name: str, view_name: str) -> str | None:
    cursor.execute(
        """SELECT pg_get_viewdef(c.oid, true)
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'v'""",
        (schema_name, view_name),
    )
    row = cursor.fetchone()
    if not row:
        return None
    definition = row[0]
    match = re.search(r'FROM\s+(?:"?[\w]+"?\.)?"?([\w]+)"?', definition, re.IGNORECASE)
    return match.group(1) if match else None


def is_version_published(connection_kwargs: dict[str, Any], schema_name: str, logical_table: str, physical_table: str) -> bool:
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            return _view_target(cursor, schema_name, logical_table) == physical_table


def _ensure_evolved_target(cursor: psycopg.Cursor, schema_name: str, target_table: str, schema: StructType) -> None:
    if not _table_exists(cursor, schema_name, target_table):
        _create_table(cursor, schema_name, target_table, schema)
        return

    cursor.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema = %s AND table_name = %s""",
        (schema_name, target_table),
    )
    existing_columns = {row[0] for row in cursor.fetchall()}
    for field in schema.fields:
        if field.name not in existing_columns:
            # New columns are initially nullable so existing rows remain valid.
            cursor.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    _qualified(schema_name, target_table),
                    sql.Identifier(field.name),
                    sql.SQL(postgres_type(field.dataType)),
                )
            )


def publish_versioned_view(
    connection_kwargs: dict[str, Any],
    schema_name: str,
    logical_table: str,
    physical_table: str,
) -> None:
    """Create or repoint the stable logical view to a published physical version."""
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            if _view_exists(cursor, schema_name, logical_table):
                cursor.execute(sql.SQL("DROP VIEW {} RESTRICT").format(_qualified(schema_name, logical_table)))
            elif _table_exists(cursor, schema_name, logical_table):
                raise RuntimeError(
                    f"Logical name {schema_name}.{logical_table} is a physical table, not a view; "
                    "versioned publication will not replace it automatically"
                )

            cursor.execute(
                sql.SQL("CREATE VIEW {} AS SELECT * FROM {}").format(
                    _qualified(schema_name, logical_table), _qualified(schema_name, physical_table)
                )
            )


def publish_truncate(
    connection_kwargs: dict[str, Any],
    schema_name: str,
    target_table: str,
    load_table: str,
    spark_schema: StructType,
) -> None:
    columns = [field.name for field in spark_schema.fields]
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            _ensure_evolved_target(cursor, schema_name, target_table, spark_schema)
            cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTRICT").format(_qualified(schema_name, target_table)))
            cursor.execute(
                sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    _qualified(schema_name, target_table),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    _qualified(schema_name, load_table),
                )
            )
            cursor.execute(sql.SQL("DROP TABLE {}").format(_qualified(schema_name, load_table)))


def publish_table_swap(
    connection_kwargs: dict[str, Any], schema_name: str, target_table: str, load_table: str
) -> None:
    backup_table = _safe_table_name("d2s_old", target_table)
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            if _table_exists(cursor, schema_name, target_table):
                cursor.execute(
                    sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                        _qualified(schema_name, target_table), sql.Identifier(backup_table)
                    )
                )
            cursor.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    _qualified(schema_name, load_table), sql.Identifier(target_table)
                )
            )
            if _table_exists(cursor, schema_name, backup_table):
                cursor.execute(sql.SQL("DROP TABLE {} RESTRICT").format(_qualified(schema_name, backup_table)))


def publish_drop_create(
    connection_kwargs: dict[str, Any], schema_name: str, target_table: str, load_table: str
) -> None:
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            if _table_exists(cursor, schema_name, target_table):
                cursor.execute(sql.SQL("DROP TABLE {} RESTRICT").format(_qualified(schema_name, target_table)))
            cursor.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    _qualified(schema_name, load_table), sql.Identifier(target_table)
                )
            )


def cleanup_load_table(connection_kwargs: dict[str, Any], schema_name: str, load_table: str) -> None:
    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} RESTRICT").format(_qualified(schema_name, load_table)))


def apply_retention(
    connection_kwargs: dict[str, Any],
    schema_name: str,
    logical_table: str,
    active_table: str,
    keep_previous_versions: bool,
    max_versions: int,
) -> None:
    if keep_previous_versions and max_versions < 1:
        raise ValueError("max_versions must be >= 1")

    with psycopg.connect(**connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = %s AND table_type = 'BASE TABLE'""",
                (schema_name,),
            )
            pattern = re.compile(rf"^{re.escape(logical_table)}_v(\d+)$")
            versions = []
            for (table_name,) in cursor.fetchall():
                match = pattern.match(table_name)
                if match:
                    versions.append((int(match.group(1)), table_name))

            versions.sort(reverse=True)
            if keep_previous_versions:
                retained = {name for _, name in versions[:max_versions]}
                retained.add(active_table)
            else:
                retained = {active_table}

            for _, table_name in versions:
                if table_name not in retained:
                    logger.info("Removing expired physical version %s.%s", schema_name, table_name)
                    cursor.execute(sql.SQL("DROP TABLE {} RESTRICT").format(_qualified(schema_name, table_name)))
