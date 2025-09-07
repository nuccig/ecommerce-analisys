# Bronze to Silver ETL - Simplified Version

## Overview
This simplified ETL script transforms data from the Bronze layer to Silver layer in the data lakehouse architecture. The script has been optimized for simplicity and maintainability while preserving all essential functionality.

## Key Features

### 1. Incremental Loading Support
- **Parameter**: `incremental` (boolean)
- **Behavior**: 
  - `true`: Appends new data to `facts_vendas`, overwrites dimension tables
  - `false`: Full refresh - overwrites all tables

### 2. Timestamp Conversion
- **Source**: Bronze layer timestamps are stored as bigint nanoseconds
- **Target**: Silver layer timestamps are converted to standard timestamp format
- **Implementation**: `convert_nano_to_timestamp()` function divides nanoseconds by 1,000,000,000

### 3. Simplified Architecture
- Removed excessive logging and comments
- Streamlined error handling
- Consolidated transformation logic
- Reduced from 1,004 lines (original) to 240 lines (simplified)

## Arguments
- `JOB_NAME`: AWS Glue job name
- `S3_BUCKET`: S3 bucket for data storage
- `BRONZE_DATABASE`: Glue catalog database for bronze tables
- `SILVER_DATABASE`: Glue catalog database for silver tables
- `incremental`: Boolean flag for incremental vs full load
- `triggered_by`: Execution trigger identifier

## Tables Processed

### Bronze Input Tables
- `vendas`
- `itens_venda`
- `produtos`
- `clientes`
- `categorias`
- `fornecedores`
- `enderecos`

### Silver Output Tables
- `dim_fornecedores`
- `dim_categorias`
- `dim_produtos`
- `dim_clientes`
- `dim_enderecos`
- `dim_tempo`
- `facts_vendas`
- `_execution_reports`

## Partitioning Strategy
- `dim_tempo`: Partitioned by `ano`, `mes`
- `facts_vendas`: Partitioned by `ano`, `mes`, `dia`
- Other dimensions: No partitioning

## Key Functions

### `convert_nano_to_timestamp(df, timestamp_cols)`
Converts timestamp columns from nanoseconds to timestamp format for columns:
- `criado_em`
- `atualizado_em`
- `data_venda`
- `data_nascimento`

### `add_silver_metadata(df)`
Adds standard metadata columns:
- `silver_created_at`: Current timestamp
- `silver_execution_date`: Execution date
- `triggered_by`: Execution trigger

### `load_bronze_table(table_name)`
Loads bronze table and applies timestamp conversion.

### `save_to_silver(df, table_name, partition_keys=[])`
Saves DataFrame to Silver layer with appropriate write mode based on incremental flag.

## Execution Report
The script generates an execution report with:
- Total records processed
- Total sales value
- Total items sold
- Last processed timestamp
- Execution metadata

This simplified version maintains all essential functionality while being significantly more maintainable and easier to understand.