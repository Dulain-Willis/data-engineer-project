module "steam_bucket" {
  source = "./modules/minio-bucket"

  bucket_name = "steam"
}
