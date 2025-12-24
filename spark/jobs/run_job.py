# CLI entrypoint to run PySpark jobs with shared config/session wiring.
from __future__ import annotations

import argparse
import importlib
from typing import Callable

from spark.config.loader import JobConfig, load_config
from spark.lib.session import build_spark_session


def _load_callable(job_name: str) -> Callable:
    module = importlib.import_module(f"spark.jobs.{job_name}")
    if not hasattr(module, "main"):
        raise AttributeError(f"Job module {job_name} must expose a main() function")
    return getattr(module, "main")


def run_job(job_fn: Callable, app_name: str, config_path: str | None = None) -> None:
    cfg = load_config(config_path)
    spark_session = build_spark_session(app_name=app_name, config=cfg)
    job_fn(spark_session, cfg)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument("--job", required=True, help="Job module name under spark.jobs")
    parser.add_argument("--config", help="Optional path to JSON config file")
    return parser.parse_args()


def main() -> None:  # pragma: no cover - thin wrapper
    args = parse_args()
    job_callable = _load_callable(args.job)
    run_job(job_callable, app_name=args.job, config_path=args.config)


if __name__ == "__main__":
    main()
