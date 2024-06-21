resource "google_storage_bucket_object" "archive" {
  name   = "main.zip"
  bucket = google_storage_bucket.bucket.name
  source = "../cloud-function/main.zip"

  depends_on = [google_project_service.project]
}

resource "google_cloudfunctions_function" "function" {
  name                  = "etl_gcp_function"
  description           = "Function ETL."
  runtime               = "python39"
  timeout               = 300
  region                = var.region
  project               = var.project_id
  available_memory_mb   = 256
  source_archive_bucket = var.bucket_name
  source_archive_object = google_storage_bucket_object.archive.name
  trigger_http          = true
  entry_point           = "execute_gcf"

  depends_on = [google_project_service.project]
}

resource "google_cloudfunctions_function_iam_member" "etlprocess" {
  project        = var.project_id
  region         = var.region
  cloud_function = google_cloudfunctions_function.function.name
  role           = "roles/cloudfunctions.invoker"
  member         = "allUsers"

  depends_on = [google_project_service.project]
}
