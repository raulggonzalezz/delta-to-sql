# delta-to-sql

A reusable Python component that publishes complete Delta Lake snapshots from S3 into PostgreSQL.

Airflow invokes the `replicate()` function through `PythonOperator`. Spark reads the pinned Delta snapshot, and PostgreSQL stores the replicated data.

## Project layout

```text
delta-to-sql/
├── src/app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── replicator.py
│   └── postgres.py
├── config/config.yaml
├── pyproject.toml
├── requirements.txt
├── .python-version
├── .env.example
├── .gitignore
└── README.md
```

## Airflow usage

```python
from airflow.operators.python import PythonOperator
from app.replicator import replicate

replicate_customers = PythonOperator(
    task_id="replicate_customers",
    python_callable=replicate,
    op_kwargs={
        "source": "s3a://data-products/customers",
        "target_schema": "analytics",
        "target_table": "customers",
    },
)
```

The Python package must be importable by the Airflow worker. Spark code and dependencies must also be available to the Spark executors according to the cluster deployment.

An existing `SparkSession` can be passed explicitly using the `spark` argument. If omitted, `replicate()` obtains a session through `SparkSession.builder.getOrCreate()` and stops it when the call finishes.

To run it from command line:

python -m app.main \
  --source "s3a://data-products/customers" \
  --target-schema analytics \
  --target-table customers

## Configuration

The default configuration path is `config/config.yaml`. A different path can be passed using `config_path`.

Credentials are supplied through environment variables. The S3 credentials are not configured here: access is delegated to the Spark runtime, such as an IAM role or Hadoop credential provider.

Example:

```yaml
postgres:
  host: ${POSTGRES_HOST}
  port: ${POSTGRES_PORT}
  database: ${POSTGRES_DATABASE}
  username: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
  sslmode: ${POSTGRES_SSLMODE}

replication:
  publication_strategy: drop_create
  versioning:
    enabled: true
    retention:
      keep_previous_versions: true
      max_versions: 3
  performance:
    partitions: 8

logging:
  level: INFO
```

Publication strategies: `truncate`, `table_swap`, and `drop_create`. These strategies define how a completed staging load replaces the publication target.

Versioning is an independent setting. When `versioning.enabled` is true, the publication target is a physical table named with the Delta version suffix (for example, `customers_v33`) and a stable logical view (`customers`) points to the active physical version. When versioning is false, the publication target is the logical table itself and no historical versions are retained.

The Delta version is read once and pinned using `versionAsOf`. The component loads into a uniquely named temporary table, applies the selected publication strategy to the target, and then updates the logical view when versioning is enabled.

If the logical view already points to the requested physical version, the execution returns `SKIPPED`.

Retention is independent of publication strategy and applies only when versioning is enabled. `max_versions` includes the active version. The active physical table is never removed.

Examples:

- `versioning.enabled: false` + `publication_strategy: truncate`: maintain one logical table and replace its contents.
- `versioning.enabled: true` + `publication_strategy: drop_create`: maintain one physical table per Delta version and repoint the logical view.
- `versioning.enabled: true` + `publication_strategy: truncate`: maintain versioned physical tables, truncating/reloading the physical table if that Delta version is republished.

## PostgreSQL behavior and limitations

- Data is transferred using `psycopg` 3 `COPY FROM STDIN`, one connection per Spark partition.
- Each executor writes to a temporary load table. Publication is performed separately by the driver.
- A PostgreSQL advisory lock serializes executions targeting the same schema and logical table.
- When versioning is enabled, the logical table is a normal PostgreSQL view, not a materialized view.
- Updating a view with a changed schema may require dropping and recreating it. Existing dependent objects or grants can prevent this; arbitrary dependency migration is outside this initial implementation.
- `table_swap` and `drop_create` can fail when dependent PostgreSQL objects prevent dropping the previous table. `CASCADE` is never used implicitly.
- The `truncate` strategy uses a staging table and a single transaction for `TRUNCATE` plus `INSERT ... SELECT`.
- Index management is intentionally not included in this first version.

## Schema mapping

Supported initial mappings:

| Spark type | PostgreSQL type |
|---|---|
| StringType | TEXT |
| BooleanType | BOOLEAN |
| ByteType / ShortType | SMALLINT |
| IntegerType | INTEGER |
| LongType | BIGINT |
| FloatType | REAL |
| DoubleType | DOUBLE PRECISION |
| DecimalType | NUMERIC |
| DateType | DATE |
| TimestampType / TimestampNTZType | TIMESTAMP |
| BinaryType | BYTEA |
| ArrayType / MapType / StructType | JSONB |

Unsupported types fail before loading. Complex values are serialized as JSON.

## Development

```bash
uv sync
```

The exact Spark and Delta versions must be aligned with the Spark runtime used by the deployment. Do not install a different Spark/Delta combination into an existing managed cluster without checking compatibility.

`uv.lock` should be generated and committed after the target Spark runtime versions have been confirmed:

```bash
uv lock
```

## Environment responsibilities

This project does not provide a Dockerfile or install Airflow. The deployment environment is responsible for:

- Making this Python package available to the Airflow worker.
- Providing compatible Spark and Delta runtime dependencies.
- Making the application code and required Python dependencies available to Spark executors.
- Providing S3 access to the Spark runtime.
- Providing network access from Spark executors to PostgreSQL.
- Providing PostgreSQL permissions to create schemas/tables/views and acquire advisory locks.
