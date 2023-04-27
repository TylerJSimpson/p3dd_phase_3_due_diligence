variable "environment" {
  description = "The environment of the GCP project ie 'prd', 'uat', 'dev'"
  type        = string
}

variable "increment" {
  type        = string
  description = "The increment of the GCP project ie '001', '002', '003'"
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
