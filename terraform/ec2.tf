data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
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
  user_data_base64 = base64encode(<<-EOF
    #!/bin/bash

    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    echo "Iniciando setup do Docker e Airflow..."

    apt-get update -y
    apt-get install -y docker.io git curl
    systemctl start docker
    systemctl enable docker
    usermod -a -G docker ubuntu

    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

    mkdir -p /opt/airflow
    cd /opt/airflow
    mkdir -p dags logs plugins config
    chown -R 50000:0 logs
    chmod -R 777 logs
    chmod -R 777 config
    chmod -R 777 plugins
    chmod -R 777 dags

    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml'

    docker-compose run airflow-cli airflow config list

    sudo sed -i 's/load_examples = True/load_examples = False/' config/airflow.cfg
    
    cat > .env << 'ENVEOF'
      AIRFLOW_UID=50000
      AIRFLOW__CORE__LOAD_EXAMPLES=false
    ENVEOF


    docker-compose up airflow-init
    docker-compose up -d

    EOF
  )
}


# EC2 Instance para Airflow
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.ec2_instance_type
  key_name               = data.aws_key_pair.airflow_key.key_name
  subnet_id              = data.aws_subnets.public.ids[0]
  vpc_security_group_ids = [aws_security_group.airflow_ec2_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_ec2_profile.name

  associate_public_ip_address = true

  user_data = local.user_data_base64

  user_data_replace_on_change = true

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-airflow"
  }
}
