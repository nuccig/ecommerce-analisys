#!/bin/bash

exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Iniciando setup do Docker e Airflow..."

yum update -y

yum install -y docker git
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

mkdir -p /opt/ecommerce-airflow
cd /opt/ecommerce-airflow

mkdir -p dags logs plugins config

cat <<'EOF' > Dockerfile
FROM apache/airflow:2.7.0-python3.10

USER root
RUN apt-get update && apt-get install -y \
    pkg-config \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    mysqlclient==2.2.0 \
    pymysql==1.1.0 \
    sqlalchemy==1.4.49 \
    boto3==1.28.57 \
    pandas==2.1.0 \
    requests==2.31.0

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
EOF

# Criar entrypoint básico
cat <<'EOF' > entrypoint.sh
#!/bin/bash
set -e

airflow db init

airflow users create \
    --username admin \
    --password admin123 \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

airflow webserver --port 8080 &
airflow scheduler &

wait
EOF

chmod +x entrypoint.sh

echo "Construindo imagem do Airflow..."
docker build -t ecommerce-airflow .

echo "Iniciando container do Airflow..."
docker run -d \
    --name airflow-container \
    -p 8080:8080 \
    -v $(pwd)/dags:/opt/airflow/dags \
    -v $(pwd)/logs:/opt/airflow/logs \
    -v $(pwd)/plugins:/opt/airflow/plugins \
    -e AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    -e AIRFLOW__CORE__LOGS_FOLDER=/opt/airflow/logs \
    -e AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins \
    -e AWS_DEFAULT_REGION=us-east-1 \
    --restart unless-stopped \
    ecommerce-airflow

# Change ownership para ec2-user
chown -R ec2-user:ec2-user /opt/ecommerce-airflow