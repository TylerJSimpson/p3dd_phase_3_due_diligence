provider "google" {
  version = "3.74.0"
  region  = var.region
}

resource "google_project" "project" {
  name            = var.project_id
  project_id      = var.project_id
  billing_account = var.billing_account
}

resource "google_service_account" "service_account" {
  account_id   = var.service_account_name
  display_name = var.service_account_name
  project      = google_project.project.project_id
}

resource "google_project_iam_binding" "iam_binding" {
  project = google_project.project.project_id
  role    = "roles/owner"

  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}

resource "google_project_iam_member" "bigquery_admin_member" {
  project = google_project.project.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_iam_member" "storage_admin_member" {
  project = google_project.project.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_iam_member" "storage_object_admin_member" {
  project = google_project.project.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_storage_bucket" "gcs_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true
  project       = google_project.project.project_id
}

resource "google_bigquery_dataset" "bigquery_dataset" {
  dataset_id   = var.bigquery_dataset_name
  location     = var.region
  project      = google_project.project.project_id
  description  = "A dataset for storing sample data in BigQuery"
  default_table_expiration_ms = "3600000"
}

output "service_account_email" {
  value = google_service_account.service_account.email
}
