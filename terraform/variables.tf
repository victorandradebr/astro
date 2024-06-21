variable "project_id" {
  default     = "etl-teste-karhub"
  description = "The project ID"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Bucket to code Cloud Functions"
  default     = "gcp-etl-code-364758"
}

variable "services" {
  type = list(string)
  default = [
    "iam.googleapis.com",
    "cloudfunctions.googleapis.com",
    "bigquery.googleapis.com"
  ]
}