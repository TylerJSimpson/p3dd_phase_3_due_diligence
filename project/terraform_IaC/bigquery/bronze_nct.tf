resource "google_bigquery_table" "bronze_nct" {
  dataset_id = google_bigquery_dataset.bigquery_dataset_bronze.dataset_id
  table_id   = "nct"
  project    = google_project.project.project_id

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
    {
        "name": "nct_id",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "nlm_download_date_description",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "study_first_submitted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "results_first_submitted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "disposition_first_submitted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "last_update_submitted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "study_first_submitted_qc_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "study_first_posted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "study_first_posted_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "results_first_submitted_qc_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "results_first_posted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "results_first_posted_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "disposition_first_submitted_qc_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "disposition_first_posted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "disposition_first_posted_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "last_update_submitted_qc_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "last_update_posted_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "last_update_posted_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "start_month_year",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "start_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "start_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "verification_month_year",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "verification_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "completion_month_year",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "completion_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "completion_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "primary_completion_month_year",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "primary_completion_date_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "primary_completion_date",
        "type": "DATE",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "target_duration",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "study_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "acronym",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "baseline_population",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "brief_title",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "official_title",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "overall_status",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "last_known_status",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "phase",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "enrollment",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "enrollment_type",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "source",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "limitations_and_caveats",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "number_of_arms",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "number_of_groups",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "why_stopped",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "has_expanded_access",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "expanded_access_type_individual",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "expanded_access_type_intermediate",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "expanded_access_type_treatment",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "has_dmc",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "is_fda_regulated_drug",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "is_fda_regulated_device",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "is_unapproved_device",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "is_ppsd",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "is_us_export",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "biospec_retention",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "biospec_description",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "ipd_time_frame",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "ipd_access_criteria",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "ipd_url",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "plan_to_share_ipd",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "plan_to_share_ipd_description",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "created_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "updated_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "source_class",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "delayed_posting",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "expanded_access_nctid",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "expanded_access_status_for_nctid",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "fdaaa801_violation",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": null
    },
    {
        "name": "baseline_type_units_analyzed",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": null
    }
]
EOF
}

