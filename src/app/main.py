"""Optional command-line entry point for manual execution."""

from __future__ import annotations

import argparse
import json
import logging

from app.replicator import replicate


def main() -> None:
    parser = argparse.ArgumentParser(description="Replicate a Delta table into PostgreSQL.")
    parser.add_argument("--source", required=True, help="Delta path, e.g. s3a://bucket/path")
    parser.add_argument("--target-schema", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--config", default=None)
    parser.add_argument("--publication-strategy", "--strategy", dest="publication_strategy", choices=["truncate", "table_swap", "drop_create"])
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = replicate(
        source=args.source,
        target_schema=args.target_schema,
        target_table=args.target_table,
        config_path=args.config,
        publication_strategy=args.publication_strategy,
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))


if __name__ == "__main__":
    main()
