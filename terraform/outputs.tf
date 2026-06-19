output "bucket_name" {
  value = google_storage_bucket.weather_bucket.name
}

output "dataset_name" {
  value = google_bigquery_dataset.weather_dataset.dataset_id
}

output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}