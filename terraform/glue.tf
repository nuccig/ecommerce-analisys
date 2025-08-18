# Glue Database
resource "aws_glue_catalog_database" "ecommerce_database" {
  name = "${var.project_name}-database"

  description = "Database para análise de dados de ecommerce"

  tags = {
    Name        = "${var.project_name}-database"
    Environment = "production"
    Layer       = "bronze"
  }
}

# Glue Crawler para dados de ecommerce
resource "aws_glue_crawler" "ecommerce_crawler" {
  database_name = aws_glue_catalog_database.ecommerce_database.name
  name          = "${var.project_name}-crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.ecommerce.bucket}/"

    exclusions = [
      "**/_SUCCESS",
      "**/_reports"
    ]
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name        = "${var.project_name}-crawler"
    Environment = "production"
  }
}

output "glue_database_name" {
  description = "Nome do database Glue criado"
  value       = aws_glue_catalog_database.ecommerce_database.name
}

# Output do nome do crawler
output "glue_crawler_name" {
  description = "Nome do crawler Glue criado"
  value       = aws_glue_crawler.ecommerce_crawler.name
}
