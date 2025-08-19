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
    newgrp docker

    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

    mkdir -p /opt/airflow
    cd /opt/airflow
    mkdir -p dags logs plugins config

    # Configurar permissões corretas
    chown -R 50000:0 /opt/airflow
    chmod -R 755 /opt/airflow
    chmod -R 777 logs

    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml'

    cat > requirements.txt << 'REQEOF'
boto3>=1.40.11
pandas>=2.3.1
pyarrow>=21.0.0
pymysql>=1.1.1
requests>=2.32.4
sqlalchemy>=1.4.54
    REQEOF

    cat > .env << 'ENVEOF'
AIRFLOW_UID=50000
AIRFLOW_GID=0

AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__PARALLELISM=4
AIRFLOW__CORE__DAG_CONCURRENCY=2
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1

AIRFLOW__LOGGING__REMOTE_LOGGING=False
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__LOGGING__WORKER_LOG_SERVER_HOST=airflow-scheduler

AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60
AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
AIRFLOW__SCHEDULER__PARSING_PROCESSES=2

_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

_PIP_ADDITIONAL_REQUIREMENTS=boto3>=1.40.11 pandas>=2.3.1 pyarrow>=21.0.0 pymysql>=1.1.1 requests>=2.32.4 sqlalchemy>=1.4.54
    ENVEOF

    echo "Inicializando Airflow..."

    docker-compose up airflow-init

    sleep 15

    docker-compose up -d --remove-orphans

    sleep 300

    echo "Status dos containers:"
    docker-compose ps

    echo "Setup concluído em $(date)"

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

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-airflow"
  }
}
