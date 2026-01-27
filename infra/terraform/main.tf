module "bronze_bucket" {
  source = "./modules/minio-bucket"

  bucket_name = "bronze"
}

module "silver_bucket" {
  source      = "./modules/minio-bucket"
  bucket_name = "silver"
}
