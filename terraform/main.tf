terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "gcp-etl-tfstate"
    prefix = "terraform/state"
  }
}

provider "google" {
  project     = "etl-teste-karhub"
  credentials = file("../etl.json")
  region      = "us-central1"
  zone        = "us-central1-c"
}
