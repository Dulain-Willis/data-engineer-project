# Simple configuration loader for PySpark jobs.
#
# This module centralizes reading runtime configuration from environment
# variables and optional JSON files. Jobs can consume the resulting
# `JobConfig` dataclass to avoid duplicating config parsing logic and to
# keep storage targets/configuration flexible per job.
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


@dataclass
class JobConfig:
    # Container for job settings.
    #
    # minio_endpoint: Endpoint for MinIO/S3A connections
    # minio_access_key: Access key for MinIO
    # minio_secret_key: Secret key for MinIO
    # extras: Any additional, job-specific settings loaded from config files
    #         or environment variables

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    extras: Dict[str, Any] = field(default_factory=dict)


def _load_from_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def load_config(config_path: str | None = None) -> JobConfig:
    # Load configuration from env vars and an optional JSON file.
    #
    # Parameters
    # ----------
    # config_path: str | None
    #     Path to a JSON config file with overrides. If omitted, only
    #     environment variables are used.

    file_values: Dict[str, Any] = {}
    if config_path:
        file_values = _load_from_json(Path(config_path))

    env_defaults = {
        "minio_endpoint": os.getenv("MINIO_ENDPOINT", JobConfig.minio_endpoint),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY", JobConfig.minio_access_key),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY", JobConfig.minio_secret_key),
    }

    merged = env_defaults | {k: v for k, v in file_values.items() if k in env_defaults}
    extras = {k: v for k, v in file_values.items() if k not in env_defaults}

    return JobConfig(**merged, extras=extras)
