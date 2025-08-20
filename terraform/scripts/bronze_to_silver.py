import sys
import logging
from datetime import datetime, date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, coalesce, 
    to_date, year, month, dayofmonth, quarter, weekofyear, dayofweek,
    when, count, sum as sql_sum, max as sql_max, min as sql_min,
    concat, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType, LongType
)
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_BUCKET', 
    'BRONZE_DATABASE', 
    'SILVER_DATABASE',
    'incremental',
    'full_refresh',
    'triggered_by'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = args['S3_BUCKET']
S3_BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
S3_SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
BRONZE_DATABASE = args['BRONZE_DATABASE']
SILVER_DATABASE = args['SILVER_DATABASE']

EXECUTION_DATE = datetime.now().strftime('%Y-%m-%d')
IS_INCREMENTAL = args.get('incremental', 'false').lower() == 'true'
IS_FULL_REFRESH = args.get('full_refresh', 'false').lower() == 'true'
TRIGGERED_BY = args.get('triggered_by', 'manual')

print(f"Execution Date: {EXECUTION_DATE}")
print(f"Incremental: {IS_INCREMENTAL}")
print(f"Full Refresh: {IS_FULL_REFRESH}")
print(f"Triggered By: {TRIGGERED_BY}")

def add_silver_metadata(df):
    """Adiciona metadados padrão da camada Silver"""
    return df.withColumn("silver_created_at", current_timestamp()) \
             .withColumn("silver_execution_date", lit(EXECUTION_DATE)) \
             .withColumn("data_source", lit("silver")) \
             .withColumn("triggered_by", lit(TRIGGERED_BY))

def apply_basic_filters(df, table_name):
    """Aplica filtros básicos por tabela"""
    if table_name == "facts_vendas":
        return df.filter(
            (col("quantidade") > 0) & 
            (col("preco_unitario") > 0) & 
            (col("subtotal") > 0) & 
            (col("dim_cliente_id").isNotNull()) & 
            (col("dim_produto_id").isNotNull())
        )
    else:
        # Filtros básicos gerais
        return df.filter(col("id").isNotNull())

def get_last_processed_timestamp(table_name):
    """Recupera o último timestamp processado para uma tabela específica"""
    try:
        last_report = glueContext.create_dynamic_frame.from_catalog(
            database=SILVER_DATABASE,
            table_name="_execution_reports",
            transformation_ctx=f"load_last_report_{table_name}"
        ).toDF()
        
        last_successful = last_report.filter(
            (col("is_incremental") == True) & 
            (col("total_records") > 0)
        ).orderBy(col("processed_at").desc()).limit(1)
        
        if last_successful.count() > 0:
            last_timestamp = last_successful.select("last_processed_timestamp").collect()[0][0]
            print(f"Último timestamp processado para {table_name}: {last_timestamp}")
            return last_timestamp
        else:
            print(f"Nenhuma execução anterior encontrada para {table_name}, processando últimos 7 dias")
            return None
            
    except Exception as e:
        print(f"Erro ao recuperar último timestamp para {table_name}: {str(e)}")
        print("Processando todos os dados como fallback")
        return None

def get_incremental_filter_by_timestamp(table_name, timestamp_column="criado_em"):
    """Retorna filtro baseado no último timestamp processado"""
    if IS_INCREMENTAL:
        last_timestamp = get_last_processed_timestamp(table_name)
        
        if last_timestamp:
            timestamp_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            return f"{timestamp_column} > '{timestamp_str}'"
        else:
            return f"{timestamp_column} >= date_sub(current_date(), 365)"
    
    return None

def load_bronze_table_with_timestamp(table_name, transformation_ctx, timestamp_column="criado_em", apply_incremental=False):
    """Carrega tabela bronze com filtro baseado em timestamp"""
    try:
        if IS_INCREMENTAL and apply_incremental:
            timestamp_filter = get_incremental_filter_by_timestamp(table_name, timestamp_column)
            
            if timestamp_filter:
                print(f"Carregando {table_name} com filtro incremental: {timestamp_filter}")
                df = glueContext.create_dynamic_frame.from_catalog(
                    database=BRONZE_DATABASE,
                    table_name=table_name,
                    push_down_predicate=timestamp_filter,
                    transformation_ctx=transformation_ctx
                ).toDF()
            else:
                df = glueContext.create_dynamic_frame.from_catalog(
                    database=BRONZE_DATABASE,
                    table_name=table_name,
                    transformation_ctx=transformation_ctx
                ).toDF()
        else:
            print(f"Carregando {table_name} completa")
            df = glueContext.create_dynamic_frame.from_catalog(
                database=BRONZE_DATABASE,
                table_name=table_name,
                transformation_ctx=transformation_ctx
            ).toDF()
        
        return df
        
    except Exception as e:
        print(f"Erro ao carregar {table_name}: {str(e)}")
        raise

def bulk_load_bronze_tables(table_configs):
    """Carrega múltiplas tabelas bronze de uma vez"""
    loaded_tables = {}
    
    for table_name, config in table_configs.items():
        timestamp_col = config.get('timestamp_column', 'criado_em')
        apply_incremental = config.get('apply_incremental', True)
        
        print(f"Carregando {table_name}...")
        loaded_tables[table_name] = load_bronze_table_with_timestamp(
            table_name, 
            f"{table_name}_bronze",
            timestamp_column=timestamp_col,
            apply_incremental=apply_incremental
        )
    
    return loaded_tables

def create_dimension_with_mapping(df, table_name, column_mappings, join_tables=None):
    """Cria dimensão aplicando mapeamento de colunas e joins"""
    
    if join_tables:
        for join_config in join_tables:
            join_df = join_config['df']
            join_condition = join_config['condition']
            join_type = join_config.get('type', 'left')
            alias = join_config.get('alias')
            
            if alias:
                join_df = join_df.alias(alias)
            
            df = df.join(join_df, join_condition, join_type)
    
    select_cols = [col(old_col).alias(new_col) for old_col, new_col in column_mappings.items()]
    
    dim_df = df.select(*select_cols).distinct()
    
    dim_df = add_silver_metadata(apply_basic_filters(dim_df, table_name))
    
    print(f"{table_name.upper()} criada: {dim_df.count()} registros")
    return dim_df

def get_max_timestamp_from_multiple_tables(table_timestamp_mapping):
    """Calcula o maior timestamp de múltiplas tabelas"""
    max_timestamps = {}
    
    for table_name, (df, timestamp_col) in table_timestamp_mapping.items():
        if df.count() > 0:
            max_val = df.agg(sql_max(timestamp_col)).collect()[0][0]
            if max_val:
                max_timestamps[table_name] = max_val
    
    if max_timestamps:
        overall_max = max(max_timestamps.values())
        print(f"Maior timestamp processado: {overall_max}")
        print(f"Detalhes por tabela: {max_timestamps}")
        return overall_max
    
    return datetime.now()

def calculate_data_quality_metrics(tables_dict):
    """Calcula métricas de qualidade de dados de forma otimizada"""
    metrics = {}
    
    if 'dim_produtos' in tables_dict:
        produtos_df = tables_dict['dim_produtos']
        metrics['produtos_sem_categoria'] = produtos_df.filter(col("categoria_nome").isNull()).count()
        metrics['produtos_sem_fornecedor'] = produtos_df.filter(col("fornecedor_nome").isNull()).count()
    
    if 'facts_vendas' in tables_dict:
        vendas_df = tables_dict['facts_vendas']
        metrics['vendas_sem_cliente'] = vendas_df.filter(col("dim_cliente_id").isNull()).count()
        metrics['vendas_sem_produto'] = vendas_df.filter(col("dim_produto_id").isNull()).count()
        
        aggregations = vendas_df.agg(
            count("*").alias("total_records"),
            sql_sum("total_venda").alias("total_value"),
            sql_sum("quantidade").alias("total_items")
        ).collect()[0]
        
        metrics.update({
            'facts_vendas_count': aggregations['total_records'],
            'facts_vendas_total_value': aggregations['total_value'] or 0,
            'facts_vendas_total_items': aggregations['total_items'] or 0
        })
    
    return metrics

try:
    bronze_tables_config = {
        'vendas': {
            'timestamp_column': 'data_venda',
            'apply_incremental': True
        },
        'itens_venda': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        },
        'produtos': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        },
        'clientes': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        },
        'categorias': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        },
        'fornecedores': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        },
        'enderecos': {
            'timestamp_column': 'criado_em',
            'apply_incremental': True
        }
    }
    
    bronze_data = bulk_load_bronze_tables(bronze_tables_config)
    
    vendas_bronze = bronze_data['vendas']
    itens_venda_bronze = bronze_data['itens_venda']
    produtos_bronze = bronze_data['produtos']
    clientes_bronze = bronze_data['clientes']
    categorias_bronze = bronze_data['categorias']
    fornecedores_bronze = bronze_data['fornecedores']
    enderecos_bronze = bronze_data['enderecos']

    print(f"Carregamento concluído: {vendas_bronze.count()} vendas, {produtos_bronze.count()} produtos")

except Exception as e:
    print(f"Erro ao carregar dados Bronze: {str(e)}")
    raise

# DIM_FORNECEDORES
dim_fornecedores_mapping = {
    "id": "fornecedor_id",
    "nome": "fornecedor_nome",
    "email": "fornecedor_email",
    "telefone": "fornecedor_telefone",
    "cnpj": "fornecedor_cnpj",
    "endereco": "fornecedor_endereco",
    "cidade": "fornecedor_cidade",
    "estado": "fornecedor_estado",
    "cep": "fornecedor_cep",
    "ativo": "fornecedor_ativo",
    "criado_em": "fornecedor_criado_em"
}

dim_fornecedores = create_dimension_with_mapping(
    fornecedores_bronze, 
    "dim_fornecedores", 
    dim_fornecedores_mapping
)

# DIM_CATEGORIAS
dim_categorias_mapping = {
    "id": "categoria_id",
    "nome": "categoria_nome",
    "descricao": "categoria_descricao",
    "ativa": "categoria_ativa",
    "criado_em": "categoria_criado_em"
}

dim_categorias = create_dimension_with_mapping(
    categorias_bronze, 
    "dim_categorias", 
    dim_categorias_mapping
)

# DIM_PRODUTOS
dim_produtos_mapping = {
    "id": "produto_id",
    "nome": "produto_nome",
    "descricao": "produto_descricao",
    "categoria_id": "categoria_id",
    "fornecedor_id": "fornecedor_id",
    "preco": "produto_preco",
    "custo": "produto_custo",
    "peso": "produto_peso",
    "quantidade_estoque": "produto_quantidade_estoque",
    "em_estoque": "produto_em_estoque",
    "ativo": "produto_ativo",
    "criado_em": "produto_criado_em",
    "cat.nome": "categoria_nome",
    "cat.descricao": "categoria_descricao",
    "forn.nome": "fornecedor_nome",
    "forn.email": "fornecedor_email",
    "forn.telefone": "fornecedor_telefone"
}

dim_produtos_joins = [
    {
        'df': categorias_bronze,
        'condition': col("categoria_id") == col("cat.id"),
        'type': 'left',
        'alias': 'cat'
    },
    {
        'df': fornecedores_bronze,
        'condition': col("fornecedor_id") == col("forn.id"),
        'type': 'left',
        'alias': 'forn'
    }
]

dim_produtos = create_dimension_with_mapping(
    produtos_bronze,
    "dim_produtos",
    dim_produtos_mapping,
    join_tables=dim_produtos_joins
)

# DIM_CLIENTES - Com colunas calculadas
dim_clientes = clientes_bronze.select(
    col("id").alias("cliente_id"),
    col("nome").alias("cliente_nome"),
    col("sobrenome").alias("cliente_sobrenome"),
    concat(col("nome"), lit(" "), col("sobrenome")).alias("cliente_nome_completo"),
    col("email").alias("cliente_email"),
    col("telefone").alias("cliente_telefone"),
    col("cpf").alias("cliente_cpf"),
    col("data_nascimento").alias("cliente_data_nascimento"),
    year(current_date()) - year(col("data_nascimento")).alias("cliente_idade"),
    col("genero").alias("cliente_genero"),
    col("criado_em").alias("cliente_criado_em")
).distinct()

dim_clientes = add_silver_metadata(apply_basic_filters(dim_clientes, "dim_clientes"))
print(f"DIM_CLIENTES criada: {dim_clientes.count()} registros")

# DIM_ENDERECOS
dim_enderecos_mapping = {
    "id": "endereco_id",
    "cliente_id": "cliente_id",
    "cep": "endereco_cep",
    "logradouro": "endereco_logradouro",
    "numero": "endereco_numero",
    "complemento": "endereco_complemento",
    "bairro": "endereco_bairro",
    "cidade": "endereco_cidade",
    "estado": "endereco_estado",
    "endereco_principal": "endereco_principal",
    "criado_em": "endereco_criado_em"
}

dim_enderecos = create_dimension_with_mapping(
    enderecos_bronze,
    "dim_enderecos",
    dim_enderecos_mapping
)

# DIM_TEMPO
dim_tempo = vendas_bronze.select(
    to_date(col("data_venda")).alias("data"),
    year(col("data_venda")).alias("ano"),
    month(col("data_venda")).alias("mes"),
    dayofmonth(col("data_venda")).alias("dia"),
    dayofweek(col("data_venda")).alias("dia_semana"),
    weekofyear(col("data_venda")).alias("semana_ano"),
    quarter(col("data_venda")).alias("trimestre"),
    date_format(col("data_venda"), "MMMM").alias("mes_nome"),
    date_format(col("data_venda"), "EEEE").alias("dia_semana_nome"),
    when(dayofweek(col("data_venda")).isin([1, 7]), "Final de Semana").otherwise("Dia Útil").alias("tipo_dia"),
).distinct()

dim_tempo = add_silver_metadata(dim_tempo)
print(f"DIM_TEMPO criada: {dim_tempo.count()} registros")

# Fato

# FACTS_VENDAS - Tabela de Fatos de Vendas
facts_vendas = vendas_bronze.alias("v").join(
    itens_venda_bronze.alias("iv"), 
    col("v.id") == col("iv.venda_id"), 
    "inner"
).select(
    col("v.id").alias("venda_id"),
    col("iv.id").alias("item_venda_id"),
    
    col("v.cliente_id").alias("dim_cliente_id"),
    col("iv.produto_id").alias("dim_produto_id"),
    col("v.endereco_entrega_id").alias("dim_endereco_id"),
    to_date(col("v.data_venda")).alias("dim_tempo_data"),
    
    col("iv.quantidade").alias("quantidade"),
    col("iv.preco_unitario").alias("preco_unitario"),
    col("iv.subtotal").alias("subtotal"),
    col("v.subtotal").alias("subtotal_venda"),
    col("v.frete").alias("frete"),
    col("v.total").alias("total_venda"),
    
    col("v.status").alias("status_venda"),
    col("v.metodo_pagamento").alias("metodo_pagamento"),
    col("v.status_pagamento").alias("status_pagamento"),
    col("v.data_venda").alias("data_venda_timestamp"),
    
    # Partições
    year(col("v.data_venda")).alias("ano"),
    month(col("v.data_venda")).alias("mes"),
    dayofmonth(col("v.data_venda")).alias("dia")
)

facts_vendas = add_silver_metadata(apply_basic_filters(facts_vendas, "facts_vendas"))
print(f"FACTS_VENDAS criada: {facts_vendas.count()} registros")

def save_to_silver(df, table_name, partition_keys=[]):
    """Salva DataFrame na camada Silver"""
    try:
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
        
        write_options = {
            "path": f"{S3_SILVER_PATH}{table_name}/",
            "partitionKeys": partition_keys
        }
        
        if IS_INCREMENTAL and table_name == "facts_vendas":

            print(f"Salvando {table_name} em modo incremental (partition overwrite)")
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options=write_options,
                format="parquet",
                format_options={
                    "writeMode": "append",
                    "partitionOverwriteMode": "dynamic"
                },
                transformation_ctx=f"write_{table_name}"
            )
        elif IS_INCREMENTAL and table_name.startswith("dim_"):

            print(f"Salvando {table_name} em modo incremental (overwrite completo)")
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options=write_options,
                format="parquet",
                format_options={"writeMode": "overwrite"},
                transformation_ctx=f"write_{table_name}"
            )
        else:

            print(f"Salvando {table_name} em modo completo (overwrite)")
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options=write_options,
                format="parquet",
                format_options={"writeMode": "overwrite"},
                transformation_ctx=f"write_{table_name}"
            )
        
        mode = "incremental" if IS_INCREMENTAL else "full"
        print(f"{table_name} salva com sucesso - {df.count()} registros ({mode})")
        
    except Exception as e:
        print(f"Erro ao salvar {table_name}: {str(e)}")
        raise

def bulk_save_to_silver(tables_dict, partition_configs=None):
    """Salva múltiplas tabelas para Silver"""
    if partition_configs is None:
        partition_configs = {}
    
    print(f"Iniciando salvamento bulk de {len(tables_dict)} tabelas...")
    
    saved_tables = {}
    
    for table_name, df in tables_dict.items():
        partition_keys = partition_configs.get(table_name, [])
        
        try:
            print(f"Salvando {table_name}...")
            save_to_silver(df, table_name, partition_keys)
            saved_tables[table_name] = {
                'status': 'success',
                'records': df.count()
            }
        except Exception as e:
            print(f"Erro ao salvar {table_name}: {str(e)}")
            saved_tables[table_name] = {
                'status': 'failed',
                'error': str(e)
            }
            raise
    
    total_records = sum([info['records'] for info in saved_tables.values() if info['status'] == 'success'])
    print(f"Bulk save concluído: {len(saved_tables)} tabelas, {total_records:,} registros totais")
    
    return saved_tables

silver_tables = {
    'dim_fornecedores': dim_fornecedores,
    'dim_categorias': dim_categorias,
    'dim_produtos': dim_produtos,
    'dim_clientes': dim_clientes,
    'dim_enderecos': dim_enderecos,
    'dim_tempo': dim_tempo,
    'facts_vendas': facts_vendas
}

partition_configs = {
    'dim_tempo': ['ano', 'mes'],
    'facts_vendas': ['ano', 'mes', 'dia']
}

save_results = bulk_save_to_silver(silver_tables, partition_configs)

timestamp_mapping = {
    'vendas': (vendas_bronze, 'data_venda'),
    'clientes': (clientes_bronze, 'criado_em'),
    'produtos': (produtos_bronze, 'criado_em'),
    'categorias': (categorias_bronze, 'criado_em'),
    'fornecedores': (fornecedores_bronze, 'criado_em'),
    'enderecos': (enderecos_bronze, 'criado_em')
}

max_processed_timestamp = get_max_timestamp_from_multiple_tables(timestamp_mapping)

quality_metrics = calculate_data_quality_metrics(silver_tables)

stats = {
    "execution_date": EXECUTION_DATE,
    "triggered_by": TRIGGERED_BY,
    "is_incremental": IS_INCREMENTAL,
    "dim_fornecedores_count": dim_fornecedores.count(),
    "dim_categorias_count": dim_categorias.count(),
    "dim_produtos_count": dim_produtos.count(),
    "dim_clientes_count": dim_clientes.count(), 
    "dim_enderecos_count": dim_enderecos.count(),
    "dim_tempo_count": dim_tempo.count(),
    **quality_metrics
}

# Log das estatísticas
for key, value in stats.items():
    print(f"{key}: {value}")

report_schema = StructType([
    StructField("execution_date", StringType(), True),
    StructField("triggered_by", StringType(), True),
    StructField("is_incremental", BooleanType(), True),
    StructField("total_records", LongType(), True),
    StructField("total_value", DoubleType(), True),
    StructField("total_items", LongType(), True),
    StructField("produtos_sem_categoria", LongType(), True),
    StructField("produtos_sem_fornecedor", LongType(), True),
    StructField("vendas_sem_cliente", LongType(), True),
    StructField("vendas_sem_produto", LongType(), True),
    StructField("last_processed_timestamp", TimestampType(), True),
    StructField("processed_at", TimestampType(), True)
])

report_data = [(
    EXECUTION_DATE,
    TRIGGERED_BY,
    IS_INCREMENTAL,
    quality_metrics.get('facts_vendas_count', 0),
    float(quality_metrics.get('facts_vendas_total_value', 0.0)),
    quality_metrics.get('facts_vendas_total_items', 0),
    quality_metrics.get('produtos_sem_categoria', 0),
    quality_metrics.get('produtos_sem_fornecedor', 0),
    quality_metrics.get('vendas_sem_cliente', 0),
    quality_metrics.get('vendas_sem_produto', 0),
    max_processed_timestamp,
    datetime.now()
)]

report_df = spark.createDataFrame(report_data, report_schema)

save_to_silver(report_df, "_execution_reports", ["execution_date"])

job.commit()
