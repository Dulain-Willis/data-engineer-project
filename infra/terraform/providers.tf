provider "minio" {
  minio_server   = var.minio_endpoint
  access_key     = var.minio_access_key
  secret_key     = var.minio_secret_key
  ssl            = false
}
