terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------
# Enable APIs
# -----------------------

resource "google_project_service" "bigquery" {
  service = "bigquery.googleapis.com"
}

resource "google_project_service" "storage" {
  service = "storage.googleapis.com"
}

# -----------------------
# GCS Bucket
# -----------------------

resource "google_storage_bucket" "weather_bucket" {
  name     = "${var.project_id}-weather-data-${var.hash}"
  location = var.region

  uniform_bucket_level_access = true

  depends_on = [
    google_project_service.storage
  ]
}

# -----------------------
# BigQuery Dataset
# -----------------------

resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id = "weather_data"
  location   = var.region

  depends_on = [
    google_project_service.bigquery
  ]
}