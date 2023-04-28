resource "google_bigquery_table" "bronze_figi" {
  dataset_id = google_bigquery_dataset.bigquery_dataset.dataset_id
  table_id   = "figi"
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
        "name": "figi_id",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "figi_source_name",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "ticker",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "exchange_code",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "security_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "market_sector",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "figi_composite",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "share_class",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "start_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "end_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "nct_source_name",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    },
    {
        "name": "linkedin_source_name",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Placeholder"
    }
    ]
EOF
}