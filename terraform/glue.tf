resource "aws_glue_catalog_database" "ecommerce_bronze" {
  name = "bronze"

  description = "Bronze layer - Dados brutos do ecommerce"

  tags = {
    Name        = "${var.project_name}-bronze"
    Environment = "production"
    Layer       = "bronze"
  }
}

resource "aws_glue_catalog_database" "ecommerce_silver" {
  name = "silver"

  description = "Silver layer - Dados limpos e padronizados do ecommerce"

  tags = {
    Name        = "${var.project_name}-silver"
    Environment = "production"
    Layer       = "silver"
  }
}

resource "aws_glue_catalog_database" "ecommerce_gold" {
  name = "gold"

  description = "Gold layer - Dados agregados e prontos para BI do ecommerce"

  tags = {
    Name        = "${var.project_name}-gold"
    Environment = "production"
    Layer       = "gold"
  }
}

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
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
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
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
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
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
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

resource "aws_glue_job" "bronze_to_silver_full" {
  name         = "${var.project_name}-bronze-to-silver-full"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.0"

  command {
    script_location = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-logs/spark-events/"
    "--TempDir"                          = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-temp/"
    "--enable-glue-datacatalog"          = "true"
    "--S3_BUCKET"                        = aws_s3_bucket.ecommerce.bucket
    "--BRONZE_DATABASE"                  = "bronze"
    "--SILVER_DATABASE"                  = "silver"
    "--incremental"                      = "false"
    "--full_refresh"                     = "true"
    "--triggered_by"                     = "manual"
  }

  max_retries = 0
  timeout     = 60

  execution_property {
    max_concurrent_runs = 1
  }

  worker_type       = "G.1X"
  number_of_workers = 5

  tags = {
    Name        = "${var.project_name}-bronze-to-silver"
    Environment = "production"
    Layer       = "silver"
    Type        = "etl"
  }
}

resource "aws_glue_job" "bronze_to_silver_incremental" {
  name         = "${var.project_name}-bronze-to-silver-incremental"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.0"

  command {
    script_location = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-logs/spark-events/"
    "--TempDir"                          = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-temp/"
    "--enable-glue-datacatalog"          = "true"
    "--S3_BUCKET"                        = aws_s3_bucket.ecommerce.bucket
    "--BRONZE_DATABASE"                  = "bronze"
    "--SILVER_DATABASE"                  = "silver"
    "--incremental"                      = "true"
    "--full_refresh"                     = "false"
    "--triggered_by"                     = "scheduled"
  }

  max_retries = 0
  timeout     = 30

  execution_property {
    max_concurrent_runs = 2
  }

  worker_type       = "G.1X"
  number_of_workers = 3

  tags = {
    Name        = "${var.project_name}-bronze-to-silver-incremental"
    Environment = "production"
    Layer       = "silver"
    Type        = "etl-incremental"
  }
}

resource "aws_glue_trigger" "bronze_to_silver_daily_3am" {
  name = "${var.project_name}-bronze-to-silver-daily-3am"
  type = "SCHEDULED"

  schedule = "cron(0 6 * * ? *)"

  actions {
    job_name = aws_glue_job.bronze_to_silver_incremental.name
    arguments = {
      "--incremental"  = "true"
      "--full_refresh" = "false"
      "--triggered_by" = "daily_schedule"
    }
  }

  tags = {
    Name        = "${var.project_name}-bronze-to-silver-daily-3am"
    Environment = "production"
    Schedule    = "daily-3am-brazil"
  }
}

resource "aws_glue_trigger" "bronze_to_silver_on_demand" {
  name = "${var.project_name}-bronze-to-silver-on-demand"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.bronze_to_silver_full.name
    arguments = {
      "--incremental"  = "false"
      "--full_refresh" = "true"
      "--triggered_by" = "manual_execution"
    }
  }

  tags = {
    Name        = "${var.project_name}-bronze-to-silver-on-demand"
    Environment = "production"
    Type        = "manual"
  }
}

resource "aws_glue_job" "silver_to_gold_full" {
  name         = "${var.project_name}-silver-to-gold-full"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.0"

  command {
    script_location = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-logs/spark-events/"
    "--TempDir"                          = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-temp/"
    "--enable-glue-datacatalog"          = "true"
    "--S3_BUCKET"                        = aws_s3_bucket.ecommerce.bucket
    "--SILVER_DATABASE"                  = "silver"
    "--GOLD_DATABASE"                    = "gold"
    "--incremental"                      = "false"
    "--triggered_by"                     = "manual"
  }

  max_retries = 0
  timeout     = 45

  execution_property {
    max_concurrent_runs = 1
  }

  worker_type       = "G.1X"
  number_of_workers = 3

  tags = {
    Name        = "${var.project_name}-silver-to-gold"
    Environment = "production"
    Layer       = "gold"
    Type        = "analytics"
  }
}

resource "aws_glue_job" "silver_to_gold_incremental" {
  name         = "${var.project_name}-silver-to-gold-incremental"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.0"

  command {
    script_location = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-logs/spark-events/"
    "--TempDir"                          = "s3://${aws_s3_bucket.ecommerce.bucket}/glue-temp/"
    "--enable-glue-datacatalog"          = "true"
    "--S3_BUCKET"                        = aws_s3_bucket.ecommerce.bucket
    "--SILVER_DATABASE"                  = "silver"
    "--GOLD_DATABASE"                    = "gold"
    "--incremental"                      = "true"
    "--triggered_by"                     = "scheduled"
  }

  max_retries = 0
  timeout     = 20

  execution_property {
    max_concurrent_runs = 2
  }

  worker_type       = "G.1X"
  number_of_workers = 2

  tags = {
    Name        = "${var.project_name}-silver-to-gold-incremental"
    Environment = "production"
    Layer       = "gold"
    Type        = "analytics-incremental"
  }
}

resource "aws_glue_trigger" "silver_to_gold_after_silver" {
  name = "${var.project_name}-silver-to-gold-after-silver"
  type = "CONDITIONAL"

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_to_silver_incremental.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_to_gold_incremental.name
    arguments = {
      "--incremental"  = "true"
      "--triggered_by" = "auto_after_silver"
    }
  }

  tags = {
    Name        = "${var.project_name}-silver-to-gold-after-silver"
    Environment = "production"
    Type        = "conditional"
  }
}

resource "aws_glue_trigger" "silver_to_gold_daily_6am" {
  name = "${var.project_name}-silver-to-gold-daily-6am"
  type = "SCHEDULED"

  schedule = "cron(0 9 * * ? *)"

  actions {
    job_name = aws_glue_job.silver_to_gold_incremental.name
    arguments = {
      "--incremental"  = "true"
      "--triggered_by" = "daily_analytics_schedule"
    }
  }

  tags = {
    Name        = "${var.project_name}-silver-to-gold-daily-6am"
    Environment = "production"
    Schedule    = "daily-6am-brazil"
  }
}

resource "aws_glue_trigger" "silver_to_gold_on_demand" {
  name = "${var.project_name}-silver-to-gold-on-demand"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.silver_to_gold_full.name
    arguments = {
      "--incremental"  = "false"
      "--triggered_by" = "manual_analytics"
    }
  }

  tags = {
    Name        = "${var.project_name}-silver-to-gold-on-demand"
    Environment = "production"
    Type        = "manual"
  }
}
