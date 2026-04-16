# CLAUDE.md — Steam Data Platform

## Project Overview

This is a **Steam game analytics data platform** that ingests SteamSpy API data and processes it through a multi-layer warehouse pipeline.

**Data flow:**
```
SteamSpy API → MinIO (landing JSON) → PySpark (bronze Iceberg) → PySpark (silver Iceberg) → ClickHouse (replication) → dbt (gold models)
```

Orchestrated by Apache Airflow. All services run via Docker Compose.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.10.3 |
| Processing | PySpark 3.5.3 |
| Table format | Apache Iceberg (REST catalog) |
| Object storage | MinIO (S3-compatible) |
| OLAP warehouse | ClickHouse |
| Transformations | dbt-core + dbt-clickhouse |
| Language | Python 3.10+ |
| Infrastructure | Docker Compose, Terraform |

---

## Key Directories

```
src/pipelines/         # Shared Python package (imported by Spark jobs & Airflow)
  steamspy/            # SteamSpy API extraction with retry/backoff
  common/spark/        # SparkSession factory + Iceberg/S3A config
  common/storage/      # MinIO client/loader
  common/clickhouse/   # ClickHouse connection factory
  replication/         # Snapshot state tracking for CDC
airflow/dags/          # DAG definitions (steamspy.py, replication.py)
spark/spark_jobs/      # PySpark job scripts (bronze/, staging/stg, clickhouse_replication/)
dbt/models/intermediate/ # Developer/publisher dimension models (ClickHouse SQL)
dbt/models/gold/       # Analytics SQL models (ranking, word cloud)
infra/terraform/       # MinIO bucket provisioning
infra/clickhouse/init/ # ClickHouse schema init SQL
docs/decisions/        # ADRs for key architectural choices
tests/                 # pytest unit tests (mock-based, no live services)
```

---

## Commit Message Format

Follow `docs/contributing.md` exactly. Every commit must use:

```
[Issue #N]
type: short imperative description

  One or two sentences explaining the motivation — what problem this
  solves or what goal it achieves.

  - Bullet per logical change group
  - Start verbs: Add, Delete, Revert, Update, Fix, etc.
  - Include the why when not obvious
```

**Types:** `feat`, `fix`, `refactor`, `docs`, `chore`

**Do NOT add Co-Authored-By lines.** The user has explicitly requested this.

---

## Docstring Conventions

PEP 257 + Google style. See `docs/contributing.md` for full rules.

- Every module needs a module-level docstring (before imports)
- Functions use Google sections (`Args:`, `Returns:`, `Raises:`) only when the info is non-obvious
- Do **not** repeat types inside `Args:` when type annotations are present

---

## Architecture Notes

### Iceberg layers
- **Bronze**: raw JSON payload + metadata, partitioned by `dt`
- **Silver stg**: parsed, typed, deduplicated games table
- **Silver int**: denormalized developer/publisher dimension tables
- All silver jobs use **OVERWRITE PARTITION** (not append) — important for replication logic

### Replication (Iceberg → ClickHouse)
Silver jobs overwrite partitions, so standard incremental Iceberg APIs don't work. The pipeline uses **snapshot-based CDC**:
1. `analytics.replication_state` tracks the last replicated snapshot ID per table
2. Iceberg `.entries` metadata is queried to find partitions written after that snapshot
3. Affected `dt` partitions are truncated in ClickHouse and reloaded
4. State is updated to the new snapshot ID

See `docs/decisions/replication_explanation.md` for a full explanation.

### ClickHouse table engines
All silver tables use `ReplacingMergeTree` keyed on `updated_at` for CDC deduplication.

### dbt gold models
Gold models run on ClickHouse (target: `steam-pipeline` profile).
- `gold_top_games_ranked`: Hybrid ranking — Wilson score quality gate (top 500) then rank by raw positive volume
- `gold_top_games_wilson`: Pure Wilson 95% CI lower-bound ranking
- `gold_worst_games_ranked`: Inverse ranking

### Spark resource constraints
The cluster is tuned for ~1.5 GB footprint to coexist with Airflow + MinIO + ClickHouse in Docker. See `docs/decisions/oom_memory_tuning.md` before changing Spark resource config.

---

## Running Things

```bash
# Start all services
docker compose up -d

# Provision MinIO buckets
terraform apply -chdir=infra/terraform

# Query or modify Iceberg tables via spark-sql — always use the Makefile target
make spark q="SELECT * FROM iceberg.steamspy.silver_stg_games LIMIT 10"
make spark q="DROP TABLE IF EXISTS iceberg.steamspy.some_table PURGE"

# Query MinIO Parquet via DuckDB
make duck q="SELECT * FROM 's3://landing/...'"

# Run dbt models
cd dbt && ../.dbt_venv/bin/dbt run

# Run tests
venv/bin/pytest tests/ -v
```

---

## Environment Variables

Copy `.env.example` to `.env`. Key vars:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MINIO_ENDPOINT` | `minio:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO credentials |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO credentials |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | Spark cluster |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` | Iceberg catalog |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse host |

Credentials are dev-only defaults — insecure by design for local Docker.

---

## DAGs

**`steamspy`** — extract → bronze → silver pipeline
- Triggered manually (no schedule)
- `force_refresh` param: bypass short-circuit to re-run extraction
- Task order: `should_extract` → `extract` → `resolve_partition` → `bronze` → `[silver_stg || silver_int_developers || silver_int_publishers]`

**`replication`** — Iceberg silver → ClickHouse sync
- Triggered manually
- `full_load` param: ignore state, reload all data
- `from_snapshot` param: override start snapshot for backfills

---

## Testing

Tests are mock-based unit tests in `tests/`. No live services required.

```bash
python3 -m venv venv
venv/bin/pip install -e ".[dev]"
venv/bin/pytest tests/ -v
```

Tests cover: SteamSpy pagination termination, retry/backoff behavior, partial-data guards.

---

## Dependency Groups (pyproject.toml)

- `airflow_dockerfile` — installed in the Airflow Docker image
- `spark_dockerfile` — installed in the Spark Docker image
- `dev` — pytest + MinIO client for local testing
- `notebooks` — Jupyter, matplotlib, wordcloud, numpy
- `dbt` — dbt-core + dbt-clickhouse (use `.dbt_venv/`)

To add a dependency that runs inside a container, add it to the correct optional group and rebuild the image — don't just `pip install` inside the container.
