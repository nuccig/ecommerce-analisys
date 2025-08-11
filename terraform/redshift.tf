resource "aws_redshift_cluster" "main" {
  cluster_identifier = "${var.project_name}-cluster"
  database_name      = var.database_name
  master_username    = var.master_username
  master_password    = var.master_password

  node_type           = "ra3.large"
  cluster_type        = "single-node"
  publicly_accessible = true

  vpc_security_group_ids    = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name

  iam_roles = [aws_iam_role.redshift_role.arn]

  tags = {
    Name = "${var.project_name}-cluster"
  }
}
