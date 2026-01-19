module "raw_bucket" {
  source = "./modules/minio-bucket"

  bucket_name = "${var.project}-${var.environment}-raw"
}

module "silver_bucket" {
  source      = "./modules/minio-bucket"
  bucket_name = "${var.project}-${var.environment}-silver"
}
