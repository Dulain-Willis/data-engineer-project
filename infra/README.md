# Infra

## Terraform

### Delete a bucket

```bash
docker compose run --rm --entrypoint="" mc sh -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && \
   mc rb --force local/<bucket-name>"
```

### Create a bucket

```bash
docker compose run --rm --entrypoint="" mc sh -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && \
   mc mb local/<bucket-name>"
```
