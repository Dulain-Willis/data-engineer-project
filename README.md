# Steam Data Platform

An end-to-end batch data pipeline that ingests Steam game metadata, processes it through a multi-layer warehouse, and surfaces analytics-ready models. Fully containerized with Docker Compose and orchestrated by Airflow.

## Overview

Python extracts data from the SteamSpy API and lands it in MinIO backed by Apache Iceberg. PySpark jobs process data through bronze and silver Iceberg layers, with silver replicated into ClickHouse. dbt transforms the ClickHouse silver tables into gold-layer analytics models.

## Architecture

                  SteamSpy API
                       |
                       v
              Python Extract Scripts
                       |
                       v
          MinIO + Apache Iceberg
                       |
                       v
                 PySpark Jobs
              /               \
        Bronze Layer        Silver Layer
        (Iceberg/MinIO)     (Iceberg/MinIO)
                                  |
                                  v
                     ClickHouse Replication
                       (Silver mirror in CH)
                                  |
                                  v
                          dbt Gold Models
                                  |
                                  v
                          Analytics Layer

        Airflow orchestrates the full pipeline

## How the Pipeline Works

### 1. Extract
Python scripts call the SteamSpy API and write partitioned JSON files to MinIO (S3-compatible object storage).

### 2. Replicate
PySpark jobs process data through bronze (raw) and silver (cleaned, typed) Iceberg tables stored in MinIO. Silver is then replicated into ClickHouse for fast analytical queries.

### 3. Transform
dbt runs gold-layer models on top of ClickHouse silver tables, producing analytics-ready outputs including hybrid-ranked game scores.

### 4. Orchestration
Airflow DAGs coordinate extraction, Spark replication, and dbt runs end-to-end.

## Tech Stack

Python · PySpark · Apache Iceberg · Airflow · MinIO · ClickHouse · dbt · Docker Compose · Terraform

## Setup

### Prerequisites

- everything is dockerized, even cli binaries :)

### Steps

1. Clone the repository
2. Run `docker compose up -d`
3. Initialize Airflow
4. Apply Terraform to create MinIO buckets
5. Trigger the pipeline in Airflow

## Local Development

### Setup

Dependencies are declared in `pyproject.toml` using optional-dependency groups instead of a `requirements.txt`. The `-e` flag installs the project in editable mode (changes to source take effect immediately without reinstalling). The `[group]` suffix installs that optional group on top of the core dependencies.

Create a virtual environment and install the package with dev dependencies:

    python3 -m venv venv
    venv/bin/pip install -e ".[dev]"

For dbt development:

    python3 -m venv .venv-dbt
    source .venv-dbt/bin/activate
    pip install -e ".[dbt]"

### Running Tests

    venv/bin/pytest tests/ -v

To run a specific test file:

    venv/bin/pytest tests/test_steamspy_extract.py -v

## Contributing

See [docs/contributing.md](docs/contributing.md).

## Contact

Created by **Dulain Willis**.
Reach out on LinkedIn if you want to discuss data engineering or the project.
