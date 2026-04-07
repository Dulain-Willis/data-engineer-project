Monitor an Airflow DAG run every 5 minutes, investigating failures automatically.

## Step 1 — Identify the DAG

If the user did not specify a DAG ID, ask them which DAG they want to monitor before proceeding.

## Step 2 — Trigger the DAG, then unpause if stuck in queued

Trigger the DAG:
```
docker exec airflow-scheduler airflow dags trigger <dag_id>
```

Wait a few seconds, then check if the run is stuck in a queued state:
```
docker exec airflow-scheduler airflow dags list-runs -d <dag_id> --limit 1
```

If the state is `queued`, unpause the DAG to allow it to run:
```
docker exec airflow-scheduler airflow dags unpause <dag_id>
```

## Step 3 — Get the run ID

Use the run ID from the `list-runs` output above for all subsequent commands.

## Step 4 — Set up the monitoring loop

Use `/loop 5m` to check status every 5 minutes. Each loop iteration should:

1. Check task states for the run:
   ```
   docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
   ```

2. Based on the result:

   **Still running or queued** — Report which task is currently active and how long the run has been going. Do nothing else — the loop will check again in 5 minutes.

   **All tasks succeeded** — Report success with a summary of which tasks completed. Stop the loop.

   **Any task failed** — Investigate immediately:
   - Identify which task failed
   - Fetch its logs:
     ```
     docker exec airflow-scheduler airflow tasks logs <dag_id> <task_id> <run_id>
     ```
   - Read the log output and identify the root cause
   - Also check the local log file at `./airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/` if more context is needed
   - Report: which task failed, the error message, likely cause, and suggested fix
   - Stop the loop

Run the first check immediately before the loop kicks in.
