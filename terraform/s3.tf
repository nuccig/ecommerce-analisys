resource "aws_s3_bucket" "bronze-bucket" {
  bucket = "nuccig-data-analysis-bronze"
}

resource "aws_s3_bucket" "silver-bucket" {
  bucket = "nuccig-data-analysis-silver"
}

resource "aws_s3_bucket" "gold-bucket" {
  bucket = "nuccig-data-analysis-gold"
}
