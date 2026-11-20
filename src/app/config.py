"""Global configuration loading and validation."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ALLOWED_PUBLICATION_STRATEGIES = {"truncate", "table_swap", "drop_create"}


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "prefer"

    def connection_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "sslmode": self.sslmode,
            "application_name": "delta-to-sql",
        }


@dataclass(frozen=True)
class RetentionSettings:
    keep_previous_versions: bool = True
    max_versions: int = 3


@dataclass(frozen=True)
class PerformanceSettings:
    partitions: int = 8


@dataclass(frozen=True)
class VersioningSettings:
    enabled: bool = True


@dataclass(frozen=True)
class Settings:
    postgres: PostgresSettings
    publication_strategy: str = "drop_create"
    versioning: VersioningSettings = VersioningSettings()
    retention: RetentionSettings = RetentionSettings()
    performance: PerformanceSettings = PerformanceSettings()
    log_level: str = "INFO"


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            variable = match.group(1)
            if variable not in os.environ:
                raise ValueError(f"Environment variable '{variable}' is not set")
            return os.environ[variable]

        return _ENV_PATTERN.sub(replace, value)
    return value


def load_settings(config_path: str | None = None, publication_strategy_override: str | None = None) -> Settings:
    """Load global YAML configuration and resolve environment placeholders."""
    path = Path(config_path) if config_path else Path("config/config.yaml")
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path.resolve()}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw = _expand_environment(raw)
    postgres = raw.get("postgres", {})
    replication = raw.get("replication", {})
    versioning = replication.get("versioning", {})
    retention = versioning.get("retention", {})
    performance = replication.get("performance", {})

    required = ("host", "database", "username", "password")
    missing = [key for key in required if not postgres.get(key)]
    if missing:
        raise ValueError(f"Missing PostgreSQL configuration values: {', '.join(missing)}")

    publication_strategy = publication_strategy_override or replication.get("publication_strategy", "drop_create")
    if publication_strategy not in _ALLOWED_PUBLICATION_STRATEGIES:
        raise ValueError(
            f"Unsupported publication strategy '{publication_strategy}'. "
            f"Expected one of: {', '.join(sorted(_ALLOWED_PUBLICATION_STRATEGIES))}"
        )

    max_versions = int(retention.get("max_versions", 3))
    partitions = int(performance.get("partitions", 8))
    if max_versions < 1:
        raise ValueError("replication.versioning.retention.max_versions must be >= 1")
    if partitions < 1:
        raise ValueError("replication.performance.partitions must be >= 1")

    return Settings(
        postgres=PostgresSettings(
            host=str(postgres["host"]),
            port=int(postgres.get("port", 5432)),
            database=str(postgres["database"]),
            username=str(postgres["username"]),
            password=str(postgres["password"]),
            sslmode=str(postgres.get("sslmode", "prefer")),
        ),
        publication_strategy=publication_strategy,
        versioning=VersioningSettings(enabled=bool(versioning.get("enabled", True))),
        retention=RetentionSettings(
            keep_previous_versions=bool(retention.get("keep_previous_versions", True)),
            max_versions=max_versions,
        ),
        performance=PerformanceSettings(partitions=partitions),
        log_level=str(raw.get("logging", {}).get("level", "INFO")).upper(),
    )
