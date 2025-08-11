data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_key_pair" "airflow_key" {
  key_name = var.ec2_ssh_key_name
}

locals {
  user_data_base64 = templatefile("${path.module}/user-data.sh", {
    project_name = var.project_name
  })
}

# EC2 Instance para Airflow
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.ec2_instance_type
  key_name               = data.aws_key_pair.airflow_key.key_name
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.airflow_ec2_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_ec2_profile.name

  associate_public_ip_address = true

  user_data = local.user_data_base64

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-airflow"
  }
}
