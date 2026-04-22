# Steam Data Platform — Monorepo → Multi-Repo Migration Plan

## Context

The current monorepo lives at:
```
/Users/dulainslaptop/projects/data-engineer-project/
```

**This repo must not be modified or deleted.** All operations below use `cp` to copy files into new locations. The original remains fully intact as a reference.

The target layout is 5 repos inside a new `steam/` directory:

```
/Users/dulainslaptop/projects/steam/
  steam-data-platform/     ← meta-repo: compose, setup, cross-cutting docs
  steam-infra/             ← Terraform, ClickHouse schema, MinIO config
  steam-pipelines/         ← Spark jobs + shared Python library
  steam-orchestration/     ← Airflow DAGs only
  steam-analytics/         ← dbt models + notebooks
```

---

## Final File Trees

### `steam-data-platform/`
```
steam-data-platform/
├── compose.yml              ← adapted from root compose.yml (paths updated, see Phase 3)
├── Makefile                 ← adapted from root Makefile (paths updated, see Phase 3)
├── .env.example             ← copied from root .env.example
├── .gitignore
├── setup.sh                 ← NEW: clones all 4 sibling repos
├── README.md                ← NEW: architecture overview + links to all repos
├── for_later.md             ← copied from root: deferred production improvements
└── docs/
    ├── contributing.md      ← copied from docs/contributing.md
    ├── links.md             ← copied from docs/links.md
    └── decisions/           ← ALL decision docs live here, centralised
        ├── docker_compose.md        ← copied from docs/decisions/docker_compose.md
        ├── query_targets.md         ← copied from docs/decisions/query_targets.md
        ├── oom_memory_tuning.md     ← copied from docs/decisions/oom_memory_tuning.md
        ├── replication_explanation.md ← copied from docs/decisions/replication_explanaion.md
        ├── dbt.md                   ← copied from docs/decisions/dbt.md
        └── dbt_trick.md             ← copied from docs/decisions/dbt_trick.md
```

### `steam-infra/`
```
steam-infra/
├── .gitignore
├── README.md                ← NEW
├── terraform/               ← copied from infra/terraform/
│   ├── versions.tf
│   ├── providers.tf
│   ├── variables.tf
│   ├── main.tf
│   ├── outputs.tf
│   ├── terraform.tfvars
│   ├── terraform.tfstate
│   ├── terraform.tfstate.backup
│   ├── .terraform.lock.hcl
│   └── modules/
│       └── minio-bucket/
│           ├── main.tf
│           ├── variables.tf
│           ├── outputs.tf
│           └── providers.tf
├── clickhouse/
│   └── init/                ← copied from infra/clickhouse/init/
│       ├── 01_create_analytics_db.sql
│       ├── 02_create_steamspy_silver.sql
│       ├── 03_create_replication_state.sql
│       ├── 04_create_steamspy_silver_int_developers.sql
│       └── 05_create_steamspy_silver_int_publishers.sql
├── minio/
│   └── minio.sql            ← copied from infra/minio/minio.sql
└── docs/
    ├── terraform/
    │   └── commands.md      ← copied from docs/terraform/commands.md
    ├── docker/
    │   └── commands.md      ← copied from docs/docker/commands.md
    └── minio/
        └── querying.md      ← copied from docs/minio/querying.md
```

### `steam-pipelines/`
```
steam-pipelines/
├── pyproject.toml           ← adapted from root pyproject.toml (see Phase 3)
├── .gitignore
├── README.md                ← NEW
├── .github/
│   └── workflows/
│       └── docker-publish.yml   ← NEW: builds & pushes image to ghcr.io on merge to main
├── src/
│   └── pipelines/           ← copied from src/pipelines/
│       ├── __init__.py
│       ├── steamspy/
│       │   ├── __init__.py
│       │   └── extract.py
│       ├── common/
│       │   ├── __init__.py
│       │   ├── text.py
│       │   ├── spark/
│       │   │   ├── __init__.py
│       │   │   ├── config.py
│       │   │   └── session.py
│       │   ├── storage/
│       │   │   ├── __init__.py
│       │   │   ├── minio_client.py
│       │   │   └── minio_loader.py
│       │   └── clickhouse/
│       │       ├── __init__.py
│       │       └── client.py
│       └── replication/
│           ├── __init__.py
│           └── state.py
├── jobs/
│   ├── bronze/              ← copied from spark/spark_jobs/bronze/
│   │   └── bronze_steamspy_games.py
│   ├── staging/             ← copied from spark/spark_jobs/staging/
│   │   └── stg/
│   │       └── silver_stg_steamspy_games.py
│   └── replication/         ← copied from spark/spark_jobs/clickhouse_replication/
│       └── steamspy_replication.py
├── docker/
│   └── Dockerfile           ← copied from spark/Dockerfile
├── tests/                   ← copied from tests/
│   ├── test_imports.py
│   └── test_steamspy_extract.py
└── docs/
    └── spark/
        └── commands.md      ← copied from docs/spark/commands.md
```

### `steam-orchestration/`
```
steam-orchestration/
├── .gitignore
├── README.md                ← NEW
├── .github/
│   └── workflows/
│       └── docker-publish.yml   ← NEW: builds & pushes image to ghcr.io on merge to main
├── dags/                    ← copied from airflow/dags/
│   ├── steamspy.py
│   └── replication.py
├── plugins/                 ← copied from airflow/plugins/
└── docker/
    └── Dockerfile           ← copied from airflow/Dockerfile
```

### `steam-analytics/`
```
steam-analytics/
├── .gitignore
├── README.md                ← NEW
├── dbt/                     ← copied from dbt/
│   ├── dbt_project.yml
│   ├── dbt_style_guide.md
│   ├── README.md
│   ├── .gitignore
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── marts/
│   │   └── metrics/
│   ├── analyses/
│   ├── macros/
│   ├── tests/
│   ├── seeds/
│   └── snapshots/
└── notebooks/               ← copied from notebooks/
    ├── steamspy_analysis.ipynb
    ├── others.ipynb
    └── *.png
```

---

## Phase 0 — Prerequisites

- [ ] Confirm `duckdb` is installed on host (`duckdb --version`)
- [ ] Confirm Docker Desktop is running
- [ ] **Stop the running stack before copying `.mnt/`.** Copying live MinIO or ClickHouse volume files while services are writing risks corruption.

```bash
cd /Users/dulainslaptop/projects/data-engineer-project
docker compose down
```

- [ ] Confirm the source repo is clean and untouched before starting

```bash
ls /Users/dulainslaptop/projects/data-engineer-project/
```

---

## Phase 1 — Create Directory Structure

- [ ] Create the parent `steam/` directory and all 5 repo directories

```bash
mkdir -p /Users/dulainslaptop/projects/steam/steam-data-platform/docs/decisions
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/terraform
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/clickhouse/init
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/minio
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/docs/terraform
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/docs/docker
mkdir -p /Users/dulainslaptop/projects/steam/steam-infra/docs/minio
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/src
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/jobs
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/docker
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/tests
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/docs/spark
mkdir -p /Users/dulainslaptop/projects/steam/steam-pipelines/.github/workflows
mkdir -p /Users/dulainslaptop/projects/steam/steam-orchestration/dags
mkdir -p /Users/dulainslaptop/projects/steam/steam-orchestration/plugins
mkdir -p /Users/dulainslaptop/projects/steam/steam-orchestration/docker
mkdir -p /Users/dulainslaptop/projects/steam/steam-orchestration/.github/workflows
mkdir -p /Users/dulainslaptop/projects/steam/steam-analytics/dbt
mkdir -p /Users/dulainslaptop/projects/steam/steam-analytics/notebooks
```

---

## Phase 2 — Copy Files

Set a variable for the source repo to keep commands short:

```bash
SRC=/Users/dulainslaptop/projects/data-engineer-project
DST=/Users/dulainslaptop/projects/steam
```

### 2a — `steam-data-platform`

- [ ] Copy `.env.example`
```bash
cp $SRC/.env.example $DST/steam-data-platform/.env.example
```

- [ ] Copy `.mnt/` (MinIO landing data, ClickHouse tables, mc config — all Docker volume state)
```bash
cp -r $SRC/.mnt $DST/steam-data-platform/.mnt
```

> **Why here:** `compose.yml` mounts `./.mnt/` relative to its own location. Since `compose.yml` lives in `steam-data-platform/`, `.mnt/` must live there too. Containers write to MinIO and ClickHouse via their network APIs — they never reference `.mnt/` directly, so nothing else in the codebase needs to change. Your existing landing data and ClickHouse silver tables are fully preserved. You will not need to run a full refresh.

- [ ] Copy cross-cutting docs
```bash
cp $SRC/docs/contributing.md $DST/steam-data-platform/docs/contributing.md
cp $SRC/docs/links.md $DST/steam-data-platform/docs/links.md
cp $SRC/docs/decisions/docker_compose.md $DST/steam-data-platform/docs/decisions/docker_compose.md
cp $SRC/docs/decisions/query_targets.md $DST/steam-data-platform/docs/decisions/query_targets.md
```

- [ ] Copy `for_later.md`
```bash
cp $SRC/for_later.md $DST/steam-data-platform/for_later.md
```

- [ ] Copy `compose.yml` (will be edited in Phase 3)
```bash
cp $SRC/compose.yml $DST/steam-data-platform/compose.yml
```

- [ ] Copy `Makefile` (will be edited in Phase 3)
```bash
cp $SRC/Makefile $DST/steam-data-platform/Makefile
```

### 2b — `steam-infra`

- [ ] Copy Terraform
```bash
cp -r $SRC/infra/terraform/. $DST/steam-infra/terraform/
```

- [ ] Copy ClickHouse init scripts
```bash
cp -r $SRC/infra/clickhouse/init/. $DST/steam-infra/clickhouse/init/
```

- [ ] Copy MinIO DuckDB config
```bash
cp $SRC/infra/minio/minio.sql $DST/steam-infra/minio/minio.sql
```

- [ ] Copy docs
```bash
cp $SRC/docs/terraform/commands.md $DST/steam-infra/docs/terraform/commands.md
cp $SRC/docs/docker/commands.md $DST/steam-infra/docs/docker/commands.md
cp $SRC/docs/minio/querying.md $DST/steam-infra/docs/minio/querying.md
```

### 2c — `steam-pipelines`

- [ ] Copy shared Python library
```bash
cp -r $SRC/src/pipelines $DST/steam-pipelines/src/pipelines
```

- [ ] Copy Spark jobs (note: directory names change here)
```bash
cp -r $SRC/spark/spark_jobs/bronze/. $DST/steam-pipelines/jobs/bronze/
cp -r $SRC/spark/spark_jobs/staging/. $DST/steam-pipelines/jobs/staging/
cp -r $SRC/spark/spark_jobs/clickhouse_replication/. $DST/steam-pipelines/jobs/replication/
```

- [ ] Copy Spark Dockerfile
```bash
cp $SRC/spark/Dockerfile $DST/steam-pipelines/docker/Dockerfile
```

- [ ] Copy tests
```bash
cp -r $SRC/tests/. $DST/steam-pipelines/tests/
```

- [ ] Copy `pyproject.toml` (will be edited in Phase 3)
```bash
cp $SRC/pyproject.toml $DST/steam-pipelines/pyproject.toml
```

- [ ] Copy docs
```bash
cp $SRC/docs/spark/commands.md $DST/steam-pipelines/docs/spark/commands.md
cp $SRC/docs/decisions/oom_memory_tuning.md $DST/steam-data-platform/docs/decisions/oom_memory_tuning.md
cp $SRC/docs/decisions/replication_explanaion.md $DST/steam-data-platform/docs/decisions/replication_explanation.md
```

### 2d — `steam-orchestration`

- [ ] Copy DAGs
```bash
cp -r $SRC/airflow/dags/. $DST/steam-orchestration/dags/
```

- [ ] Copy plugins
```bash
cp -r $SRC/airflow/plugins/. $DST/steam-orchestration/plugins/
```

- [ ] Copy Airflow Dockerfile
```bash
cp $SRC/airflow/Dockerfile $DST/steam-orchestration/docker/Dockerfile
```

### 2e — `steam-analytics`

- [ ] Copy dbt project
```bash
cp -r $SRC/dbt/. $DST/steam-analytics/dbt/
```

- [ ] Copy notebooks
```bash
cp -r $SRC/notebooks/. $DST/steam-analytics/notebooks/
```

- [ ] Copy dbt ADRs (centralised in steam-data-platform with all other decisions)
```bash
cp $SRC/docs/decisions/dbt.md $DST/steam-data-platform/docs/decisions/dbt.md
cp $SRC/docs/decisions/dbt_trick.md $DST/steam-data-platform/docs/decisions/dbt_trick.md
```

---

## Phase 3 — Post-Copy File Edits

### 3a — Update `steam-data-platform/compose.yml`

The compose file has build contexts and volume paths that pointed to the old monorepo layout. These must be updated to reference sibling repos. All paths are relative to where `compose.yml` lives (`steam-data-platform/`).

> **`.mnt/` volume paths need no changes.** They are already relative (`./mnt/minio:/data`, etc.) and resolve correctly since `compose.yml` and `.mnt/` both now live in `steam-data-platform/`.

**Change 1 — Airflow volume mounts in the `x-airflow-common` anchor (lines 16–19):**

| Old path | New path |
|---|---|
| `./airflow/dags` | `../steam-orchestration/dags` |
| `./airflow/logs` | `./logs/airflow` |
| `./airflow/plugins` | `../steam-orchestration/plugins` |
| `./spark` | `../steam-pipelines/jobs` |

- [ ] Apply the above 4 volume path changes in the `x-airflow-common` block

**Change 2 — ClickHouse init volume (line 61):**

| Old | New |
|---|---|
| `./infra/clickhouse/init:/docker-entrypoint-initdb.d/` | `../steam-infra/clickhouse/init:/docker-entrypoint-initdb.d/` |

- [ ] Apply this volume path change in the `clickhouse` service

**Change 3 — Terraform volume (line 120):**

| Old | New |
|---|---|
| `./infra/terraform:/workspace` | `../steam-infra/terraform:/workspace` |

- [ ] Apply this volume path change in the `terraform` service

**Change 4 — Replace Spark `build:` blocks with registry image references (lines 123–125 and 140–142):**

Both `spark-master` and `spark-worker` currently have:
```yaml
build:
  context: .
  dockerfile: spark/Dockerfile
```

Remove the `build:` block entirely from both services and replace with:
```yaml
image: ghcr.io/YOUR_GITHUB_USERNAME/steam-pipelines:latest
```

Images are built and pushed to the registry by CI (see Phase 4d). The compose file never builds images — it only pulls them.

- [ ] Apply to `spark-master`
- [ ] Apply to `spark-worker`

**Change 5 — Spark volume mounts (lines 137 and 153):**

Both `spark-master` and `spark-worker` mount `./spark:/opt/spark/jobs`.

Change both to: `../steam-pipelines/jobs:/opt/spark/jobs`

The image is pulled from the registry but job files are still mounted from the local `steam-pipelines` checkout at runtime. This is correct — job files are not baked into the image.

- [ ] Apply to `spark-master`
- [ ] Apply to `spark-worker`

**Change 6 — Replace Airflow image and remove `build:` blocks:**

The `x-airflow-common` anchor at the top of the file sets `image: airflow-custom`. Change this to:
```yaml
image: ghcr.io/YOUR_GITHUB_USERNAME/steam-orchestration:latest
```

Then remove the `build:` blocks from both `airflow-webserver` and `airflow-scheduler` entirely. Because they inherit from the anchor via `<<: *airflow-common`, the `image:` field is already set — no `build:` needed.

- [ ] Update `image:` in the `x-airflow-common` anchor
- [ ] Remove `build:` block from `airflow-webserver`
- [ ] Remove `build:` block from `airflow-scheduler`

---

### 3b — Update `steam-data-platform/Makefile`

The `duck` target references `infra/minio/minio.sql`. Update to point to `steam-infra`:

| Old | New |
|---|---|
| `MINIO_INIT := infra/minio/minio.sql` | `MINIO_INIT := ../steam-infra/minio/minio.sql` |

- [ ] Apply this change

---

### 3c — Update `steam-pipelines/pyproject.toml`

Remove the `dbt` and `notebooks` optional dependency groups — those belong to `steam-analytics`, not this repo.

- [ ] Delete the `notebooks` group (lines 28–35 in the original)
- [ ] Delete the `dbt` group (line 36 in the original)

The remaining optional groups should be: `airflow_dockerfile`, `spark_dockerfile`, `dev`.

---

### 3d — Update Spark job paths referenced in Airflow DAGs

The Airflow DAGs in `steam-orchestration/dags/` reference Spark job file paths mounted at `/opt/spark/jobs`. The job files moved:

| Old container path | New container path |
|---|---|
| `/opt/spark/jobs/spark_jobs/bronze/bronze_steamspy_games.py` | `/opt/spark/jobs/bronze/bronze_steamspy_games.py` |
| `/opt/spark/jobs/spark_jobs/staging/stg/silver_stg_steamspy_games.py` | `/opt/spark/jobs/staging/stg/silver_stg_steamspy_games.py` |
| `/opt/spark/jobs/clickhouse_replication/steamspy_replication.py` | `/opt/spark/jobs/replication/steamspy_replication.py` |

- [ ] Open `steam-orchestration/dags/steamspy.py` and update any job file path strings to match the new paths above
- [ ] Open `steam-orchestration/dags/replication.py` and update any job file path strings to match the new paths above

---

## Phase 4 — Create New Files

### 4a — `steam-data-platform/setup.sh`

- [ ] Create this file with the following content:

```bash
#!/bin/bash
# setup.sh — Clone all Steam Data Platform repos into the current directory's parent.
# Run this from inside steam-data-platform/ after cloning it.

set -e

PARENT="$(cd "$(dirname "$0")/.." && pwd)"
REPOS=(
  "steam-infra"
  "steam-pipelines"
  "steam-orchestration"
  "steam-analytics"
)

echo "Cloning sibling repos into $PARENT ..."

for REPO in "${REPOS[@]}"; do
  TARGET="$PARENT/$REPO"
  if [ -d "$TARGET" ]; then
    echo "  $REPO already exists, skipping"
  else
    # Replace the URL below with the actual GitHub remote for each repo
    git clone "https://github.com/YOUR_USERNAME/$REPO.git" "$TARGET"
    echo "  Cloned $REPO"
  fi
done

echo ""
echo "Done. To start the full stack:"
echo "  cd $PARENT/steam-data-platform"
echo "  docker compose up -d"
```

- [ ] Make it executable: `chmod +x /Users/dulainslaptop/projects/steam/steam-data-platform/setup.sh`

---

### 4b — `.gitignore` files

- [ ] Create `steam-data-platform/.gitignore`:
```
.env
.mnt/
logs/
```

- [ ] Create `steam-infra/.gitignore`:
```
infra/terraform/.terraform/
infra/terraform/terraform.tfstate
infra/terraform/terraform.tfstate.backup
terraform/.terraform/
terraform/terraform.tfstate
terraform/terraform.tfstate.backup
```

- [ ] Create `steam-pipelines/.gitignore`:
```
.env
venv/
.venv/
__pycache__/
*.egg-info/
dist/
.pytest_cache/
```

- [ ] Create `steam-orchestration/.gitignore`:
```
.env
logs/
```

- [ ] Create `steam-analytics/.gitignore`:
```
.env
dbt/target/
dbt/logs/
dbt/dbt_packages/
.dbt_venv/
.nb_venv/
notebooks/.ipynb_checkpoints/
```

---

### 4c — `README.md` files

Each repo needs a scoped README. The meta-repo README is the most important — it is the front door.

- [ ] Create `steam-data-platform/README.md` — should include:
  - What this project is (Steam analytics data platform)
  - Architecture diagram / data flow: `SteamSpy API → MinIO → Spark/Iceberg → ClickHouse → dbt`
  - Links to all 4 sibling repos with one-line descriptions
  - Local dev quickstart: clone this repo, run `./setup.sh`, run `docker compose up -d`
  - Link to `docs/decisions/` for all architecture rationale (centralised here)

- [ ] Create `steam-infra/README.md` — covers Terraform usage, ClickHouse init, what buckets/schemas are provisioned; note that ADRs live in `steam-data-platform/docs/decisions/`
- [ ] Create `steam-pipelines/README.md` — covers how to run Spark jobs, how to run tests, the shared `pipelines` package; note that ADRs live in `steam-data-platform/docs/decisions/`
- [ ] Create `steam-orchestration/README.md` — covers DAG overview, how to trigger, DAG parameters
- [ ] Create `steam-analytics/README.md` — covers dbt model layers, how to run dbt, notebook usage; note that ADRs live in `steam-data-platform/docs/decisions/`

---

### 4d — GitHub Actions Docker publish workflows

Each repo that has a `Dockerfile` (`steam-pipelines`, `steam-orchestration`) needs a workflow that builds and pushes its image to ghcr.io on every merge to `main`. The workflow uses `GITHUB_TOKEN` — no secrets to configure.

- [ ] Create `steam-pipelines/.github/workflows/docker-publish.yml`:

```yaml
name: Build and publish Docker image

on:
  push:
    branches: [main]
  workflow_dispatch:

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository_owner }}/steam-pipelines

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Extract image metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=sha,prefix=,format=short
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: ./docker
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

- [ ] Create `steam-orchestration/.github/workflows/docker-publish.yml` with identical content except change `IMAGE_NAME` to:
```yaml
  IMAGE_NAME: ${{ github.repository_owner }}/steam-orchestration
```

---

## Phase 5 — Initialize Git Repositories

- [ ] Initialize each repo as a git repo and make an initial commit:

```bash
DST=/Users/dulainslaptop/projects/steam

for REPO in steam-data-platform steam-infra steam-pipelines steam-orchestration steam-analytics; do
  cd $DST/$REPO
  git init
  git add .
  git commit -m "[Issue #1] chore: initial repo structure from monorepo migration"
done
```

---

## Phase 6 — Verify

- [ ] Check all 5 repo directories exist and are non-empty:
```bash
ls /Users/dulainslaptop/projects/steam/
```

- [ ] Confirm source repo is untouched:
```bash
ls /Users/dulainslaptop/projects/data-engineer-project/
```

- [ ] Push `steam-pipelines` and `steam-orchestration` to GitHub and confirm their Actions workflows ran successfully and images appear at `https://github.com/YOUR_GITHUB_USERNAME?tab=packages`

- [ ] Pull the images before bringing the stack up:
```bash
cd /Users/dulainslaptop/projects/steam/steam-data-platform
docker compose pull
```

> **Bootstrapping note:** `docker compose pull` will fail if CI has not yet built the images. If you are running the stack for the first time before any CI run, temporarily add `build:` blocks back to spark and airflow services pointing to the local Dockerfile paths, run `docker compose up -d` once to build, then remove the `build:` blocks and rely on the registry going forward.

- [ ] Bring the stack up:
```bash
docker compose up -d
```

- [ ] Confirm Airflow UI is reachable at `http://localhost:8080`
- [ ] Confirm Spark UI is reachable at `http://localhost:8081`
- [ ] Confirm MinIO UI is reachable at `http://localhost:9001`
- [ ] Confirm ClickHouse responds:
```bash
make clickhouse q="SELECT 1"
```

- [ ] Run tests from `steam-pipelines`:
```bash
cd /Users/dulainslaptop/projects/steam/steam-pipelines
python3 -m venv venv
venv/bin/pip install -e ".[dev]"
venv/bin/pytest tests/ -v
```

---

## Notes for the Executing LLM

1. **Never use `mv`** — always `cp` or `cp -r`. The source repo `data-engineer-project` must remain fully intact.

2. **The `compose.yml` edits in Phase 3 are the most critical step.** There must be no `build:` blocks for Spark or Airflow services — only `image:` references pointing to ghcr.io. Volume mounts for job files and DAGs are still required and must use `../steam-pipelines/jobs` and `../steam-orchestration/dags` paths.

3. **Terraform state files** (`terraform.tfstate`, `terraform.tfstate.backup`) are copied intentionally. They represent the current provisioned state and are needed to manage existing MinIO buckets. Do not add them to `.gitignore` in a way that excludes them from the initial copy — just don't commit them after the initial commit if they contain sensitive values.

4. **The `dbt/` directory has its own `.gitignore`** that excludes `target/` and `logs/`. This is already in the source. The `steam-analytics/.gitignore` at the repo root is a separate file covering the outer repo level.

5. **The `pyproject.toml` in `steam-pipelines`** only needs the `airflow_dockerfile`, `spark_dockerfile`, and `dev` optional groups. The `notebooks` and `dbt` groups were only in the monorepo for convenience — they are no longer relevant here.

6. **DAG path updates (Phase 3d) require reading the actual DAG files** to find the exact strings used for Spark job paths before editing.
