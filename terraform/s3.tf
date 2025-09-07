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

resource "aws_s3_object" "silver_to_gold_script" {
  bucket       = aws_s3_bucket.ecommerce.bucket
  key          = "glue-scripts/silver_to_gold.py"
  content      = file("${path.module}/scripts/silver_to_gold.py")
  content_type = "text/plain"

  tags = {
    Name        = "silver_to_gold-script"
    Environment = "production"
    Layer       = "gold"
  }
}
