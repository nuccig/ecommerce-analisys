# 🏗️ Infraestrutura AWS - Data Warehouse

## 📋 Visão Geral

Infraestrutura completa na AWS para suportar um data warehouse moderno com arquitetura Medallion, provisionada integralmente via Terraform. A solução inclui todos os componentes necessários para ingestão, processamento, armazenamento e consulta de dados em escala.

## ⚡ Componentes da Infraestrutura

### 🌐 Rede e Segurança

- **VPC Isolada**
  - CIDR customizável com DNS habilitado
  - Subnets públicas e privadas em múltiplas AZs
  - Internet Gateway para acesso externo
  - Route tables configuradas para isolamento

- **Security Groups Específicos**
  - Airflow: HTTP (8080) + SSH (22)
  - Glue Jobs: Acesso interno VPC
  - S3: Políticas granulares por bucket
  - RDS: Acesso apenas de recursos autorizados

### 🖥️ Computação e Orquestração

- **EC2 para Airflow**
  - Instance type: t3.medium (2 vCPU, 4GB RAM)
  - User data script para setup automatizado
  - Apache Airflow 3.x instalado e configurado
  - Systemd services para auto-restart
  - Python 3.9+ com dependências necessárias

- **IAM Roles e Políticas**
  - Role específica para EC2 Airflow
  - Permissões granulares para S3, Glue, CloudWatch

### 🗄️ Data Lake (Amazon S3)

- **Bucket Principal**: `nuccig-data-analysis-ecommerce`
  - Versionamento habilitado
  - Particionamento hierárquico

- **Estrutura Medallion**
  ```
  s3://nuccig-data-analysis-ecommerce/
  ├── bronze/          # Dados brutos da API
  ├── silver/          # Dados limpos e padronizados  
  ├── gold/            # Dados analíticos agregados
  ├── glue-scripts/         # Scripts Glue
  ```

### 🔄 Processamento de Dados (AWS Glue)

- **Glue Catalog**
  - Database: `bronze`, `silver`, `gold`
  - Tables descobertas automaticamente

- **Glue Jobs**
  - `bronze-to-silver-job`: Limpeza e padronização
  - `silver-to-gold-job`: Agregações e métricas

### 📊 Consultas e Analytics (Amazon Athena)

- **Configuração Athena**
  - Integração com Glue Catalog
  - Otimizações de performance habilitadas

## 📁 Estrutura dos Arquivos Terraform

```
terraform/
├── main.tf                 # Providers e configuração backend
├── variables.tf            # Variáveis de entrada
├── outputs.tf              # Outputs da infraestrutura
├── network.tf              # VPC, subnets, gateways
├── ec2.tf                  # Instância EC2 para Airflow
├── s3.tf                   # Buckets S3 para Data Lake
├── glue.tf                 # Glue Catalog, databases e jobs
├── iam.tf                  # Roles, políticas e permissões
├── sgs.tf                  # Security Groups
└── scripts/                # Scripts para Glue Jobs
    ├── bronze_to_silver.py
    └── silver_to_gold.py
``` 
