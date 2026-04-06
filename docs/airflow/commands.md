# Airflow Commands

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
