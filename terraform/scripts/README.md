# 🛠️ Scripts de Transformação - AWS Glue

## 📋 Visão Geral

Scripts PySpark desenvolvidos para AWS Glue que implementam as transformações da arquitetura Medallion, processando dados desde a camada Bronze (dados brutos) até a camada Gold (dados analíticos), garantindo qualidade, consistência e otimização para consultas analíticas.

## ⚡ Scripts Disponíveis

### 🥈 bronze_to_silver.py - Limpeza e Padronização

**Função**: Transforma dados brutos da camada Bronze em dados limpos e padronizados na camada Silver.

**Processamentos Principais**:

- **Deduplicação de Registros**
  - Remoção de duplicatas baseada em chaves primárias
  - Preservação do registro mais recente em caso de conflito
  - Validação de integridade referencial

- **Padronização de Dados**
  - Conversão de tipos de dados (strings → timestamp, numeric)
  - Normalização de formatos (datas, CPF/CNPJ, telefones)
  - Limpeza de caracteres especiais e espaços

- **Validação de Qualidade**
  - Verificação de campos obrigatórios
  - Validação de formatos (email, CPF, CNPJ)
  - Detecção e tratamento de outliers

- **Enriquecimento de Dados**
  - Cálculo de campos derivados (idade, categoria de cliente)

### 🥇 silver_to_gold.py - Agregações e Métricas

**Função**: Cria dados analíticos otimizados na camada Gold através de agregações, joins e cálculo de métricas de negócio.

**Processamentos Principais**:

- **Métricas de Vendas**
  - Vendas por período (diário, semanal, mensal, anual)
  - Ticket médio por cliente e categoria
  - Taxa de conversão e performance de produtos
  - Análise de sazonalidade e tendências

- **Análise de Clientes**
  - Segmentação por valor e frequência (RFM)
  - Lifetime Value (CLV) calculation
  - Análise demográfica e geográfica

- **Performance de Produtos**
  - Ranking de produtos mais vendidos
  - Análise de margem por categoria
  - Gestão de estoque e rotatividade

- **KPIs Operacionais**
  - Métricas de fornecedores
  - Performance regional
  - Eficiência logística
  - Indicadores financeiros

## 🏗️ Arquitetura de Processamento

### 📊 Fluxo Bronze → Silver

```text
📥 Bronze S3 (Raw Data)
    │
    ▼ 
🔄 Glue Job: bronze_to_silver.py
    │
    ├── 🧹 Data Cleaning
    ├── 🔍 Deduplication  
    ├── ✅ Validation
    ├── 📊 Type Conversion
    └── 🏷️ Schema Standardization
    │
    ▼
💾 Silver S3 (Clean Data) + Glue Catalog
```

### 📈 Fluxo Silver → Gold

```text
📥 Silver S3 (Clean Data)
    │
    ▼
🔄 Glue Job: silver_to_gold.py
    │
    ├── 🔗 Data Joins
    ├── 📊 Aggregations
    ├── 📈 KPI Calculations
    ├── 🎯 Business Metrics
    └── 🚀 Performance Optimization
    │
    ▼
💎 Gold S3 (Analytics Data) + Optimized Partitions
```