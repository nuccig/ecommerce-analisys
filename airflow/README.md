# 🔄 Pipeline de Dados - Apache Airflow

## 📋 Visão Geral
O Airflow orquestra toda a pipeline de dados do projeto, implementando a arquitetura Medallion através de DAGs que automatizam a extração, transformação e carregamento dos dados da API de e-commerce para o data warehouse na AWS.

## ⚡ Funcionalidades

### 🔄 Orquestração Automatizada
- **DAG Principal**: `ecommerce_ingestion.py`
- **Frequência**: Execução diária automática
- **Extração Incremental**: Dados novos baseados em timestamp
- **Retry Logic**: Reexecução automática em caso de falha

### 📊 Ingestão de Dados
- **Fonte**: Fake E-commerce API (api.gustavonucci.dev)
- **Entidades**: Clientes, Produtos, Vendas, Fornecedores, Categorias, Endereços
- **Formato de Saída**: Parquet otimizado
- **Particionamento**: Por data de extração (ano/mês/dia)
- **Destino**: S3 Bucket (camada Bronze)

### 🔧 Tarefas Principais

#### 1. **Extração de Dados (Extract Tasks)**
- **Função**: Conecta com API e extrai dados por entidade
- **Processamento**: Paginação automática para grandes volumes
- **Validação**: Verificação de esquema e qualidade dos dados
- **Armazenamento**: Upload direto para S3 (Bronze layer)

#### 2. **Transformação Bronze → Silver**
- **Trigger**: AWS Glue Job via Airflow
- **Script**: `bronze_to_silver.py`
- **Processamento**: Limpeza, deduplicação, padronização
- **Catálogo**: Atualização automática do Glue Catalog

#### 3. **Transformação Silver → Gold**
- **Trigger**: AWS Glue Job via Airflow  
- **Script**: `silver_to_gold.py`
- **Processamento**: Agregações, métricas de negócio, joins
- **Otimização**: Compactação e particionamento para analytics