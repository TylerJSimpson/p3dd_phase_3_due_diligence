resource "google_bigquery_table" "bronze_jobs" {
  dataset_id = google_bigquery_dataset.bigquery_dataset_bronze.dataset_id
  table_id   = "jobs"
  project    = google_project.project.project_id

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
  {
    "name": "figi_primary_key",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Placeholder"
  },
  {
    "name": "linkedin_source_name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Placeholder"
  },
  {
    "name": "num_jobs",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Placeholder"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Placeholder"
  }    
]
EOF
}