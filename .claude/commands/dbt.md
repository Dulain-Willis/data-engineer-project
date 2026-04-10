Run a dbt command against the project's ClickHouse target.

## Step 1 — Ask which command to run

Ask the user:
> "Which dbt command would you like to run? Options: `build`, `run`, `test`, `compile`, `docs generate`, `docs serve`, `snapshot`, `seed`, or a custom command."

Wait for their answer before proceeding.

## Step 2 — Run the command

All dbt commands must be run from the `dbt/` directory using the project venv:

```
cd /Users/dulainslaptop/projects/data-engineer-project/dbt && /Users/dulainslaptop/projects/data-engineer-project/.dbt_venv/bin/dbt <command>
```

## Step 3 — Report results

- If successful, confirm what ran and summarise any key output (models built, tests passed, etc.)
- If it failed, investigate the error and suggest a fix
