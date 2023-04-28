provider "google" {
  region = var.region
}

resource "google_project" "project" {
  name            = "prj-p3dd-data-${var.environment}-${var.increment}"
  project_id      = "prj-p3dd-data-${var.environment}-${var.increment}"
  billing_account = var.billing_account
}

resource "google_service_account" "service_account" {
  account_id   = "svc-p3dd-data-${var.environment}-${var.increment}"
  display_name = "svc-p3dd-data-${var.environment}-${var.increment}"
  project      = google_project.project.project_id
}

resource "google_project_iam_binding" "iam_binding" {
  project = google_project.project.project_id
  role    = "roles/editor"

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
  name          = "gcs-p3dd-data-${var.environment}-${var.increment}"
  location      = var.region
  force_destroy = true
  project       = google_project.project.project_id

  labels = {
    env = "${var.environment}"
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
}

resource "google_project_service" "bigquery" {
  service                    = "bigquery.googleapis.com"
  project                    = google_project.project.project_id
  disable_dependent_services = true
}

resource "google_bigquery_dataset" "bigquery_dataset_bronze" {
  dataset_id                  = "bronze"
  location                    = var.region
  project                     = google_project.project.project_id
  description                 = "p3dd BigQuery Bronze Schema ${var.environment}-${var.increment}"
  default_table_expiration_ms = "3600000"
}

module "bigquery_bronze" {
  source = "./bigquery"

  environment = var.environment
  increment   = var.increment

  project_id        = google_project.project.project_id
  dataset_id_bronze = google_bigquery_dataset.bigquery_dataset_bronze.dataset_id
}

output "service_account_email" {
  value = google_service_account.service_account.email
}
