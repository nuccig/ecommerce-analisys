terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.4.0"
    }
  }

  backend "s3" {
    bucket = "nuccig-data-analysis"
    key    = "terraform/state"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Environment = "prod"
      Project     = "data-analysis"
      ManagedBy   = "Terraform"
      Owner       = "Gustavo Nucci"
    }
  }
}
