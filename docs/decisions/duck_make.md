# DuckDB + MinIO: Project-Local Query Access via Makefile

## Context

During development we often need to quickly inspect Parquet files stored in MinIO (our local S3-compatible object store). DuckDB's `httpfs` extension can query them directly over S3 — no file download needed — but requires several credentials and settings to be configured on every session.

The question was: how do we avoid typing those credentials every time without polluting global config?

## Options Considered

| Option | Scope | Notes |
|---|---|---|
| `~/.duckdbrc` | Global | Affects all DuckDB sessions on the machine |
| Global alias + `minio.sql` | Alias global, file local | Alias lives in `~/.zshrc`, not the project |
| Script in `.venv/bin/` | Local when venv active | Hacky — venv bin is for packages, not scripts |
| `direnv` + `.envrc` | Local per directory | Clean, but requires system-level `brew install direnv` + shell hook setup |
| **Makefile target** | **Fully project-local** | **Chosen** |

## Decision

Use a `Makefile` target (`make duck`) backed by a project-local `minio.sql` init file.

**Why Makefile:**
- Already a standard project tool — no new dependencies or system setup
- Entirely contained within the repo; nothing written to `~/`
- Supports both interactive and one-shot query modes
- Trivially discoverable by other contributors via `make duck`

## Usage

```bash
# interactive shell (MinIO pre-configured)
make duck

# one-shot query
make duck q="SELECT * FROM read_parquet('s3://warehouse/steamspy/silver/data/**/*.parquet') LIMIT 10"
```

## Files

- `Makefile` — defines the `duck` target
- `infra/minio/minio.sql` — DuckDB init script with MinIO connection settings; lives alongside other infra config rather than at the project root
