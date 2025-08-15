# IAM Role para EC2 Airflow
resource "aws_iam_role" "airflow_ec2_role" {
  name = "${var.project_name}-airflow-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-airflow-ec2-role"
  }
}

# IAM Policy para EC2 acessar S3
resource "aws_iam_role_policy" "airflow_ec2_s3_policy" {
  name = "${var.project_name}-airflow-ec2-s3-policy"
  role = aws_iam_role.airflow_ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.ecommerce.arn,
          "${aws_s3_bucket.ecommerce.arn}/*"
        ]
      }
    ]
  })
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "airflow_ec2_profile" {
  name = "${var.project_name}-airflow-ec2-profile"
  role = aws_iam_role.airflow_ec2_role.name
}
