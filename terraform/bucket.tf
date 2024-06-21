resource "google_storage_bucket" "bucket" {
  name     = var.bucket_name
  location = var.region
  project  = var.project_id

  depends_on = [google_project_service.project]
}