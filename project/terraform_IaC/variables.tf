variable "environment" {
  description = "The environment of the GCP project ie 'prd', 'uat', 'dev'"
  type        = string
}

variable "increment" {
  type        = string
  description = "The increment of the GCP project ie '001', '002', '003'"
}

variable "project_id" {
  type        = string
  description = "The ID of the GCP project to create"
}

variable "billing_account" {
  type        = string
  description = "The ID of the billing account to associate with the new project"
}

variable "region" {
  type        = string
  default     = "us-east4"
  description = "The region to create resources in"
}

variable "gcs_bucket_name" {
  type        = string
  description = "The name of the GCS bucket to create"
}

variable "bigquery_dataset_name" {
  type        = string
  description = "The name of the BigQuery dataset to create"
}

variable "service_account_name" {
  type        = string
  description = "The name of the service account to create"
}
