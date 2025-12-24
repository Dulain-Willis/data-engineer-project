output "steam_bucket_name" {
  description = "Name of the shared Steam object storage bucket."
  value       = module.steam_bucket.bucket_name
}
