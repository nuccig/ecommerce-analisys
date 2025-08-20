import sys
import logging
from datetime import datetime, date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_BUCKET', 
    'BRONZE_DATABASE', 
    'SILVER_DATABASE',
    'execution_date',
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

EXECUTION_DATE = args.get('execution_date', datetime.now().strftime('%Y-%m-%d'))
IS_INCREMENTAL = args.get('incremental', 'false').lower() == 'true'
IS_FULL_REFRESH = args.get('full_refresh', 'false').lower() == 'true'
TRIGGERED_BY = args.get('triggered_by', 'manual')

print(f"Execution Date: {EXECUTION_DATE}")
print(f"Incremental: {IS_INCREMENTAL}")
print(f"Full Refresh: {IS_FULL_REFRESH}")
print(f"Triggered By: {TRIGGERED_BY}")

def convert_bigint_timestamps(df, timestamp_cols):
    """Converte colunas bigint para timestamp (nanossegundos)"""
    for col_name in timestamp_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                from_unixtime(col(col_name) / 1000000000).cast(TimestampType())
            )
    return df

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

try:
    # Carregar todas as tabelas bronze
    vendas_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="vendas",
        transformation_ctx="vendas_bronze"
    ).toDF()

    produtos_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="produtos",
        transformation_ctx="produtos_bronze"
    ).toDF()

    clientes_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="clientes",
        transformation_ctx="clientes_bronze"
    ).toDF()

    categorias_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="categorias",
        transformation_ctx="categorias_bronze"
    ).toDF()

    fornecedores_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="fornecedores",
        transformation_ctx="fornecedores_bronze"
    ).toDF()

    enderecos_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="enderecos",
        transformation_ctx="enderecos_bronze"
    ).toDF()

    itens_venda_bronze = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name="itens_venda",
        transformation_ctx="itens_venda_bronze"
    ).toDF()

    print(f"Dados carregados: {vendas_bronze.count()} vendas, {produtos_bronze.count()} produtos")

except Exception as e:
    print(f"Erro ao carregar dados Bronze: {str(e)}")
    raise

# Converter timestamps
timestamp_columns = ["data_venda", "criado_em", "data_nascimento"]

vendas_bronze = convert_bigint_timestamps(vendas_bronze, ["data_venda"])
clientes_bronze = convert_bigint_timestamps(clientes_bronze, ["data_nascimento", "criado_em"])
produtos_bronze = convert_bigint_timestamps(produtos_bronze, ["criado_em"])
categorias_bronze = convert_bigint_timestamps(categorias_bronze, ["criado_em"])
fornecedores_bronze = convert_bigint_timestamps(fornecedores_bronze, ["criado_em"])
enderecos_bronze = convert_bigint_timestamps(enderecos_bronze, ["criado_em"])

# Dimensões

# DIM_FORNECEDORES - Dimensão de Fornecedores
dim_fornecedores = fornecedores_bronze.select(
    col("id").alias("fornecedor_id"),
    col("nome").alias("fornecedor_nome"),
    col("email").alias("fornecedor_email"),
    col("telefone").alias("fornecedor_telefone"),
    col("cnpj").alias("fornecedor_cnpj"),
    col("endereco").alias("fornecedor_endereco"),
    col("cidade").alias("fornecedor_cidade"),
    col("estado").alias("fornecedor_estado"),
    col("cep").alias("fornecedor_cep"),
    col("ativo").alias("fornecedor_ativo"),
    col("criado_em").alias("fornecedor_criado_em")
).distinct()

dim_fornecedores = add_silver_metadata(apply_basic_filters(dim_fornecedores, "dim_fornecedores"))
print(f"DIM_FORNECEDORES criada: {dim_fornecedores.count()} registros")

# DIM_CATEGORIAS - Dimensão de Categorias
dim_categorias = categorias_bronze.select(
    col("id").alias("categoria_id"),
    col("nome").alias("categoria_nome"),
    col("descricao").alias("categoria_descricao"),
    col("ativa").alias("categoria_ativa"),
    col("criado_em").alias("categoria_criado_em")
).distinct()

dim_categorias = add_silver_metadata(apply_basic_filters(dim_categorias, "dim_categorias"))
print(f"DIM_CATEGORIAS criada: {dim_categorias.count()} registros")

# DIM_PRODUTOS - Dimensão de Produtos
dim_produtos = produtos_bronze.join(
    categorias_bronze.alias("cat"), 
    col("categoria_id") == col("cat.id"), 
    "left"
).join(
    fornecedores_bronze.alias("forn"), 
    col("fornecedor_id") == col("forn.id"), 
    "left"
).select(
    col("id").alias("produto_id"),
    col("nome").alias("produto_nome"),
    col("descricao").alias("produto_descricao"),
    col("categoria_id").alias("categoria_id"),
    col("fornecedor_id").alias("fornecedor_id"),
    col("preco").alias("produto_preco"),
    col("custo").alias("produto_custo"),
    col("peso").alias("produto_peso"),
    col("quantidade_estoque").alias("produto_quantidade_estoque"),
    col("em_estoque").alias("produto_em_estoque"),
    col("ativo").alias("produto_ativo"),
    col("criado_em").alias("produto_criado_em"),
    col("cat.nome").alias("categoria_nome"),
    col("cat.descricao").alias("categoria_descricao"),
    col("forn.nome").alias("fornecedor_nome"),
    col("forn.email").alias("fornecedor_email"),
    col("forn.telefone").alias("fornecedor_telefone")
).distinct()

dim_produtos = add_silver_metadata(apply_basic_filters(dim_produtos, "dim_produtos"))
print(f"DIM_PRODUTOS criada: {dim_produtos.count()} registros")

# DIM_CLIENTES - Dimensão de Clientes
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

# DIM_ENDERECOS - Dimensão de Endereços
dim_enderecos = enderecos_bronze.select(
    col("id").alias("endereco_id"),
    col("cliente_id").alias("cliente_id"),
    col("cep").alias("endereco_cep"),
    col("logradouro").alias("endereco_logradouro"),
    col("numero").alias("endereco_numero"),
    col("complemento").alias("endereco_complemento"),
    col("bairro").alias("endereco_bairro"),
    col("cidade").alias("endereco_cidade"),
    col("estado").alias("endereco_estado"),
    col("endereco_principal").alias("endereco_principal"),
    col("criado_em").alias("endereco_criado_em")
).distinct()

dim_enderecos = add_silver_metadata(apply_basic_filters(dim_enderecos, "dim_enderecos"))
print(f"DIM_ENDERECOS criada: {dim_enderecos.count()} registros")

# DIM_TEMPO - Dimensão de Tempo
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
        
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="s3",
            connection_options=write_options,
            format="parquet",
            transformation_ctx=f"write_{table_name}"
        )
        
        print(f"{table_name} salva com sucesso - {df.count()} registros")
        
    except Exception as e:
        print(f"Erro ao salvar {table_name}: {str(e)}")
        raise

save_to_silver(dim_fornecedores, "dim_fornecedores")
save_to_silver(dim_categorias, "dim_categorias")
save_to_silver(dim_produtos, "dim_produtos")
save_to_silver(dim_clientes, "dim_clientes")
save_to_silver(dim_enderecos, "dim_enderecos")
save_to_silver(dim_tempo, "dim_tempo", ["ano", "mes"])

save_to_silver(facts_vendas, "facts_vendas", ["ano", "mes", "dia"])

# Calcular estatísticas
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
    "facts_vendas_count": facts_vendas.count(),
    "facts_vendas_total_value": facts_vendas.agg(sum("total_venda")).collect()[0][0] or 0,
    "facts_vendas_total_items": facts_vendas.agg(sum("quantidade")).collect()[0][0] or 0
}

# Integridade
for key, value in stats.items():
    print(f"{key}: {value}")

produtos_sem_categoria = dim_produtos.filter(col("categoria_nome").isNull()).count()
produtos_sem_fornecedor = dim_produtos.filter(col("fornecedor_nome").isNull()).count()
vendas_sem_cliente = facts_vendas.filter(col("dim_cliente_id").isNull()).count()
vendas_sem_produto = facts_vendas.filter(col("dim_produto_id").isNull()).count()

print(f"Produtos sem categoria: {produtos_sem_categoria}")
print(f"Produtos sem fornecedor: {produtos_sem_fornecedor}")
print(f"Vendas sem cliente: {vendas_sem_cliente}")
print(f"Vendas sem produto: {vendas_sem_produto}")

# Salvar relatório de execução
report_data = [(
    EXECUTION_DATE,
    TRIGGERED_BY,
    IS_INCREMENTAL,
    stats["facts_vendas_count"],
    stats["facts_vendas_total_value"],
    stats["facts_vendas_total_items"],
    produtos_sem_categoria,
    produtos_sem_fornecedor,
    vendas_sem_cliente,
    vendas_sem_produto,
    datetime.now()
)]

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
    StructField("processed_at", TimestampType(), True)
])

report_df = spark.createDataFrame(report_data, report_schema)
save_to_silver(report_df, "_execution_reports", ["execution_date"])

job.commit()
