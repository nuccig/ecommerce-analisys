# Security Group para EC2 Airflow
resource "aws_security_group" "airflow_ec2_sg" {
  name_prefix = "${var.project_name}-airflow-ec2-sg"
  vpc_id      = aws_vpc.main.id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrinja conforme necessário
    description = "SSH access"
  }

  # Airflow Web UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrinja conforme necessário
    description = "Airflow Web UI"
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = {
    Name = "${var.project_name}-airflow-ec2-sg"
  }

  lifecycle {
    create_before_destroy = true
  }
}
