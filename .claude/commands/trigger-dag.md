Trigger an Airflow DAG, then investigate and report on any issues that arise.

## Step 1 — Identify the DAG

If the user did not specify a DAG ID, ask:
> "Which DAG would you like to trigger?"

Wait for their answer before proceeding.

## Step 2 — Trigger the DAG

```
docker exec airflow-scheduler airflow dags trigger <dag_id>
```

Confirm to the user that the DAG has been triggered and note the run ID.

## Step 3 — Investigate any failures

If any task fails, investigate thoroughly:

1. Fetch logs for the failed task:
   ```
   docker exec airflow-scheduler airflow tasks logs <dag_id> <task_id> <run_id>
   ```

2. If logs are incomplete, check local log files at:
   ```
   ./airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/
   ```

3. Look for upstream failures that may have cascaded downstream.

4. Check for environment or infrastructure issues (e.g. Spark executor down, MinIO unreachable, schema mismatches, missing dependencies).

## Step 4 — Report

Deliver a structured diagnosis:

---

**DAG Run Summary**
- DAG: `<dag_id>`
- Run ID: `<run_id>`
- Status: Failed
- Failed task(s): `<task_id(s)>`

**What Happened**
Plain-language explanation of the failure based on the logs.

**Root Cause**
The specific error or condition that caused the failure.

**Possible Solutions**
Numbered list of actionable fixes, ordered from most to least likely:
1. ...
2. ...
3. ...

**Next Steps**
Whether to re-trigger, fix and re-trigger, or escalate.
