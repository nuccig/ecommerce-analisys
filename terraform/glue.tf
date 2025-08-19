resource "aws_glue_catalog_database" "ecommerce_bronze" {
  name = "bronze"

  description = "Bronze layer - Dados brutos do ecommerce"

  tags = {
    Name        = "bronze"
    Environment = "production"
    Layer       = "bronze"
  }
}

resource "aws_glue_catalog_database" "ecommerce_silver" {
  name = "silver"

  description = "Silver layer - Dados limpos e padronizados do ecommerce"

  tags = {
    Name        = "silver"
    Environment = "production"
    Layer       = "silver"
  }
}

resource "aws_glue_catalog_database" "ecommerce_gold" {
  name = "gold"

  description = "Gold layer - Dados agregados e prontos para BI do ecommerce"

  tags = {
    Name        = "gold"
    Environment = "production"
    Layer       = "gold"
  }
}

# Glue Crawler - Bronze Layer
resource "aws_glue_crawler" "ecommerce_bronze_crawler" {
  database_name = aws_glue_catalog_database.ecommerce_bronze.name
  name          = "${var.project_name}-bronze-crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.ecommerce.bucket}/bronze/"

    exclusions = [
      "**/_SUCCESS",
      "/_reports/**"
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
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name        = "${var.project_name}-bronze-crawler"
    Environment = "production"
    Layer       = "bronze"
  }
}

resource "aws_glue_crawler" "ecommerce_silver_crawler" {
  database_name = aws_glue_catalog_database.ecommerce_silver.name
  name          = "${var.project_name}-silver-crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.ecommerce.bucket}/silver/"

    exclusions = [
      "**/_SUCCESS",
      "**/_temporary/**",
      "/_reports/**"
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
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name        = "${var.project_name}-silver-crawler"
    Environment = "production"
    Layer       = "silver"
  }
}

# Glue Crawler - Gold Layer
resource "aws_glue_crawler" "ecommerce_gold_crawler" {
  database_name = aws_glue_catalog_database.ecommerce_gold.name
  name          = "${var.project_name}-gold-crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.ecommerce.bucket}/gold/"

    exclusions = [
      "**/_SUCCESS",
      "**/_temporary/**",
      "/_reports/**"
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
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Name        = "${var.project_name}-gold-crawler"
    Environment = "production"
    Layer       = "gold"
  }
}
