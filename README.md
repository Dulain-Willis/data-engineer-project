# Steam Data Platform

A fully automated, scalable, & containerized, end-to-end batch data pipeline that ingests data from the Steam ecosystem, stores it in S3-compatible object storage, loads it into a warehouse, and transforms it using dbt. Everything is orchestrated with Airflow and the infrastructure is managed with Docker and Terraform.

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
          Postgres / Snowflake Warehouse
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

## Running the Pipeline

To trigger the full ELT flow in Airflow:

    airflow dags trigger steam_data_pipeline

To run dbt manually:

    dbt run
    dbt test

## Data Models

- staging: raw game data from both APIs  
- int: normalized and cleaned tables  
- mart: analytics marts including dimensions and facts  

## Roadmap

- Expand API coverage  
- Add Great Expectations or Soda for data quality  
- Add GitHub Actions CI/CD for dbt  
- Add dashboards with Superset or Metabase  
- Optional streaming ingestion using Kafka  

## Contact

Created by **Dulain Willis**.  
Reach out on LinkedIn if you want to discuss data engineering or the project.


