output "bronze_bucket_name" {
  description = "Name of the bronze data bucket."
  value       = module.bronze_bucket.bucket_name
}

output "silver_bucket_name" {
  description = "Name of the silver data bucket."
  value       = module.silver_bucket.bucket_name
}
