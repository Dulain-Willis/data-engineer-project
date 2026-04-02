# Steam Data Platform

A fully automated & containerized, end-to-end batch data pipeline that ingests data from the Steam ecosystem, stores it in S3-compatible object storage, loads it into a warehouse, and transforms it using dbt. Everything is orchestrated with Airflow and the infrastructure is managed with Docker and Terraform.

## Overview

This project pulls Steam game metadata, lands the raw JSON in MinIO, moves it through Airbyte into a warehouse (Postgres or Snowflake), and transforms it into analytics-ready models using dbt. Airflow handles the full ELT workflow from extraction through transformation.

## Architecture

                Steam APIs 
                    |
                    v
             Python Extract Scripts
                    |
                    v
           MinIO (Raw JSON Buckets)
                    |
                    v
            Airbyte (ELT Ingestion)
                    |
                    v
            Postgres Warehouse
                    |
                    v
                   dbt
     (staging → intermediate → mart)
                    |
                    v
              Analytics Layer

## Features

- Extraction from Steam APIs using Python  
- Raw JSON storage in MinIO  
- Airbyte ingestion into Postgres (analytics warehouse)  
- dbt transformation layer following a bronze/silver/gold model  
- Incremental models, schema tests, documentation, and Jinja macros  
- Airflow DAG orchestrating extract → load → transform  
- Fully containerized using Docker Compose  
- Terraform provisioning of MinIO buckets  
- Modular design to expand to additional APIs or structured processing (PySpark)

# Repository Structure

    .
    ├── airflow/
    │   ├── logs/
    │   └── plugins/
    ├── dags/
    │   ├── src/
    │   │   ├── extract/
    │   │   ├── load/
    │   │   └── transform/
    │   ├── connectors/
    │   ├── <dag_1>.py
    │   └── <dag_2>.py
    ├── transform/
    │   └── dbt/
    │       ├── models/
    │       ├── macros/
    │       ├── snapshots/
    │       └── tests/
    ├── infra/
    │   ├── docker/
    │   └── terraform/
    └── README.md

## How the Pipeline Works

### 1. Extract
Python scripts call multiple Steam endpoints, fetch JSON metadata, and write the files to MinIO. Objects are written using a partitioned folder structure (such as by date or app ID).

### 2. Load
Airbyte reads from MinIO using an S3-compatible connector and loads the raw data into a warehouse Postgres.

### 3. Transform
dbt organizes transformations across three layers:

- Staging: raw ingested objects  
- Intermediate: cleaned, typed, standardized tables  
- Mart: analytics-ready fact and dimension models  

### 4. Orchestration
Airflow coordinates extraction, triggers Airbyte syncs, runs dbt models and tests, and manages retries, logging, and scheduling.

## Tech Stack

- Python  
- Airflow  
- MinIO  
- Airbyte  
- Postgres or Snowflake  
- dbt  
- Docker / Docker Compose  
- Terraform  
- PySpark

## Setup

### Prerequisites

- abctl (airbyte)
- eveything else is dockerized, even cli binaries :) 

### Steps

1. Clone the repository  
2. Run `docker compose up -d`  
3. Initialize Airflow  
4. Apply Terraform to create MinIO buckets  
5. Configure Airbyte source and destination  
6. Trigger the pipeline in Airflow  

## DAGs

### steamspy

Ingests Steam game metadata through the full bronze → silver → ClickHouse pipeline.

| Parameter | Default | Description |
|---|---|---|
| `force_refresh` | `false` | Re-extract from the Steam API (~1 hr). If false, skips extraction and runs transforms only (~5 min). |

## Common Commands

Trigger with extraction (full run, ~1 hour):

    docker exec airflow-scheduler airflow dags trigger steamspy --conf '{"force_refresh": true}'

Trigger transforms only (~5 minutes, uses existing landing data):

    docker exec airflow-scheduler airflow dags trigger steamspy

Re-run bronze/silver against a specific existing landing partition (e.g. if extract succeeded but bronze failed):

    docker exec airflow-scheduler airflow dags trigger steamspy --logical-date 2026-03-28 --conf '{"force_refresh": false}'

Replace `2026-03-28` with the date of the landing partition you want to process. This skips extraction and reads directly from `landing/steamspy/raw/request=all/dt=<date>/`.

Unpause the DAG if it is paused:

    docker exec airflow-scheduler airflow dags unpause steamspy

Check run status:

    docker exec airflow-scheduler airflow dags list-runs -d steamspy

Check task states for a specific run:

    docker exec airflow-scheduler airflow tasks states-for-dag-run steamspy <run_id>

View logs for a specific task:

    docker exec airflow-scheduler airflow tasks logs steamspy <task_id> <run_id>

## Data Models

- staging: raw game data from both APIs  
- int: normalized and cleaned tables  
- mart: analytics marts including dimensions and facts  
 
## Contributing

### Commit Message Format

All commits follow this format:

    [Issue #N]
    type: short imperative description

      One or two sentences explaining the motivation — what problem this
      solves or what goal it achieves, not just what changed.

      - Bullet per logical change group (file or subsystem level)
      - Start with a verb: Add, Delete, Revert, Update, Fix, etc.
      - Include the why when it is not obvious from the bullet itself

**Types:** `feat`, `fix`, `refactor`, `docs`, `chore`

**Example:**

    [Issue #17]
    fix: repair steamspy bronze/silver pipeline end-to-end

      Bronze writer was emitting Parquet with a mismatched schema after the
      landing bucket rename, causing the silver job to fail on read.

      - Fix partition path construction in bronze writer to use new bucket name
      - Update silver reader schema to match bronze output column order
      - Add end-to-end smoke test that runs both stages against fixture data

## Contact

Created by **Dulain Willis**.
Reach out on LinkedIn if you want to discuss data engineering or the project.


