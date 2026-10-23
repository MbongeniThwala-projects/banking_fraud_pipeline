terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  type        = string
  description = "GCP Project ID"
  default     = "banking-fraud-pipeline-dev"
}

variable "region" {
  type        = string
  description = "GCP Region for BigQuery datasets"
  default     = "africa-south1" # Johannesburg
}

provider "google" {
  project = var.project_id
  region  = var.region
}

#Raw landing dataset for ingestion
resource "google_bigquery_dataset" "raw_banking" {
  dataset_id                 = "raw_banking"
  friendly_name              = "Raw Banking Data"
  description                = "Landing zone for mock transaction API streams"
  location                   = var.region
  delete_contents_on_destroy = true
}

# Transformed dataset target for dbt analytics models
resource "google_bigquery_dataset" "analytics_banking" {
  dataset_id                 = "analytics_banking"
  friendly_name              = "Analytics Banking Fraud"
  description                = "Gold-layer transformed tables for fraud analysis"
  location                   = var.region
  delete_contents_on_destroy = true
}