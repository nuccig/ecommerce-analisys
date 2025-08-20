resource "aws_s3_bucket" "ecommerce" {
  bucket = "nuccig-data-analysis-ecommerce"
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket       = aws_s3_bucket.ecommerce.bucket
  key          = "glue-scripts/bronze_to_silver.py"
  content      = file("${path.module}/scripts/bronze_to_silver.py")
  content_type = "text/plain"

  tags = {
    Name        = "bronze-to-silver-script"
    Environment = "production"
    Layer       = "silver"
  }
}
