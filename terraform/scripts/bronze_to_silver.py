import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    concat,
    count,
    current_date,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
)
from pyspark.sql.functions import max as sql_max
from pyspark.sql.functions import month, quarter
from pyspark.sql.functions import sum as sql_sum
from pyspark.sql.functions import to_date, to_timestamp, weekofyear, when, year
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "BRONZE_DATABASE",
        "SILVER_DATABASE",
        "incremental",
        "triggered_by",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
S3_SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
BRONZE_DATABASE = args["BRONZE_DATABASE"]
SILVER_DATABASE = args["SILVER_DATABASE"]
EXECUTION_DATE = datetime.now().strftime("%Y-%m-%d")
IS_INCREMENTAL = args.get("incremental", "false").lower() == "true"
TRIGGERED_BY = args.get("triggered_by", "manual")

spark.conf.set("spark.sql.parquet.timestampType", "TIMESTAMP_MILLIS")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")


def convert_nano_to_timestamp(df, timestamp_cols):
    for col_name in timestamp_cols:
        if col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            if col_type in ["bigint", "long"]:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNotNull() & (col(col_name) != 0),
                        to_timestamp((col(col_name) / 1000000000).cast("double"))
                    ).otherwise(lit(None)),
                )
            elif col_type == "date":
                df = df.withColumn(col_name, col(col_name).cast("timestamp"))
    return df


def add_silver_metadata(df):
    return (
        df.withColumn("silver_created_at", current_timestamp())
        .withColumn("silver_execution_date", lit(EXECUTION_DATE))
        .withColumn("triggered_by", lit(TRIGGERED_BY))
    )


def load_bronze_table(table_name):
    bronze_path = f"s3://{S3_BUCKET}/bronze/{table_name}/"
    
    df = spark.read \
        .option("mergeSchema", "false") \
        .format("parquet") \
        .load(bronze_path)
    
    timestamp_cols = ["criado_em", "atualizado_em", "data_venda", "data_nascimento"]
    df = convert_nano_to_timestamp(df, timestamp_cols)
    
    if IS_INCREMENTAL and table_name in ["vendas", "itens_venda"]:
        if spark.catalog.tableExists(f"{SILVER_DATABASE}.facts_vendas"):
            max_date_query = f"""
                SELECT COALESCE(MAX(data_venda), '1900-01-01') as max_date 
                FROM {SILVER_DATABASE}.facts_vendas
            """
            max_date = spark.sql(max_date_query).collect()[0]["max_date"]
            df = df.filter(col("data_venda") > lit(max_date))
    
    return df


def save_to_silver(df, table_name, partition_keys=[]):
    if df.count() == 0:
        print(f"Pulando {table_name} - sem dados")
        return

    dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    
    if IS_INCREMENTAL and table_name == "facts_vendas":
        mode = "append"
    elif IS_INCREMENTAL and table_name.startswith("dim_"):
        mode = "overwrite"
    else:
        mode = "overwrite"

    print(f"Salvando {table_name}: {df.count()} registros ({mode})")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": f"{S3_SILVER_PATH}{table_name}/",
            "partitionKeys": partition_keys,
        },
        format="parquet",
        format_options={"writeMode": mode},
        transformation_ctx=f"write_{table_name}",
    )


# Carregamento das tabelas bronze
print(f"Modo de execução: {'Incremental' if IS_INCREMENTAL else 'Full Refresh'}")
vendas_bronze = load_bronze_table("vendas")
itens_venda_bronze = load_bronze_table("itens_venda")
produtos_bronze = load_bronze_table("produtos")
clientes_bronze = load_bronze_table("clientes")
categorias_bronze = load_bronze_table("categorias")
fornecedores_bronze = load_bronze_table("fornecedores")
enderecos_bronze = load_bronze_table("enderecos")
print(f"Registros carregados - Vendas: {vendas_bronze.count()}, Itens: {itens_venda_bronze.count()}")

# DIM_FORNECEDORES
dim_fornecedores = add_silver_metadata(
    fornecedores_bronze.select(
        col("id").alias("fornecedor_id"),
        col("nome").alias("fornecedor_nome"),
        col("email").alias("fornecedor_email"),
        col("telefone").alias("fornecedor_telefone"),
        col("cnpj").alias("fornecedor_cnpj"),
        col("ativo").alias("fornecedor_ativo"),
        col("criado_em").alias("fornecedor_criado_em"),
    ).distinct()
)

# DIM_CATEGORIAS
dim_categorias = add_silver_metadata(
    categorias_bronze.select(
        col("id").alias("categoria_id"),
        col("nome").alias("categoria_nome"),
        col("descricao").alias("categoria_descricao"),
        col("ativa").alias("categoria_ativa"),
        col("criado_em").alias("categoria_criado_em"),
    ).distinct()
)

# DIM_PRODUTOS
dim_produtos = add_silver_metadata(
    produtos_bronze.alias("p")
    .join(categorias_bronze.alias("c"), col("p.categoria_id") == col("c.id"), "left")
    .join(fornecedores_bronze.alias("f"), col("p.fornecedor_id") == col("f.id"), "left")
    .select(
        col("p.id").alias("produto_id"),
        col("p.nome").alias("produto_nome"),
        col("p.descricao").alias("produto_descricao"),
        col("p.categoria_id"),
        col("c.nome").alias("categoria_nome"),
        col("p.fornecedor_id"),
        col("f.nome").alias("fornecedor_nome"),
        col("p.preco").alias("produto_preco"),
        col("p.custo").alias("produto_custo"),
        col("p.peso").alias("produto_peso"),
        col("p.quantidade_estoque").alias("produto_quantidade_estoque"),
        col("p.em_estoque").alias("produto_em_estoque"),
        col("p.ativo").alias("produto_ativo"),
        col("p.criado_em").alias("produto_criado_em"),
    )
    .distinct()
)

# DIM_CLIENTES
dim_clientes = add_silver_metadata(
    clientes_bronze.select(
        col("id").alias("cliente_id"),
        col("nome").alias("cliente_nome"),
        col("sobrenome").alias("cliente_sobrenome"),
        concat(col("nome"), lit(" "), col("sobrenome")).alias("cliente_nome_completo"),
        col("email").alias("cliente_email"),
        col("telefone").alias("cliente_telefone"),
        col("cpf").alias("cliente_cpf"),
        col("data_nascimento").alias("cliente_data_nascimento"),
        when(
            col("data_nascimento").isNotNull(),
            year(current_date()) - year(col("data_nascimento")),
        )
        .otherwise(lit(None))
        .alias("cliente_idade"),
        col("genero").alias("cliente_genero"),
        col("criado_em").alias("cliente_criado_em"),
    ).distinct()
)

# DIM_ENDERECOS
dim_enderecos = add_silver_metadata(
    enderecos_bronze.select(
        col("id").alias("endereco_id"),
        col("cliente_id"),
        col("cep").alias("endereco_cep"),
        col("logradouro").alias("endereco_logradouro"),
        col("numero").alias("endereco_numero"),
        col("complemento").alias("endereco_complemento"),
        col("bairro").alias("endereco_bairro"),
        col("cidade").alias("endereco_cidade"),
        col("estado").alias("endereco_estado"),
        col("endereco_principal"),
        col("criado_em").alias("endereco_criado_em"),
    ).distinct()
)

# DIM_TEMPO
dim_tempo = add_silver_metadata(
    vendas_bronze.select(
        to_date(col("data_venda")).alias("data"),
        year(col("data_venda")).alias("ano"),
        month(col("data_venda")).alias("mes"),
        dayofmonth(col("data_venda")).alias("dia"),
        dayofweek(col("data_venda")).alias("dia_semana"),
        weekofyear(col("data_venda")).alias("semana_ano"),
        quarter(col("data_venda")).alias("trimestre"),
        date_format(col("data_venda"), "MMMM").alias("mes_nome"),
        date_format(col("data_venda"), "EEEE").alias("dia_semana_nome"),
        when(dayofweek(col("data_venda")).isin([1, 7]), "Final de Semana")
        .otherwise("Dia Útil")
        .alias("tipo_dia"),
    )
    .filter(col("data").isNotNull())
    .distinct()
)

# FACTS_VENDAS
facts_vendas = add_silver_metadata(
    vendas_bronze.alias("v")
    .join(itens_venda_bronze.alias("iv"), col("v.id") == col("iv.venda_id"), "inner")
    .select(
        col("v.id").alias("venda_id"),
        col("iv.id").alias("item_venda_id"),
        col("v.cliente_id").alias("dim_cliente_id"),
        col("iv.produto_id").alias("dim_produto_id"),
        col("v.endereco_entrega_id").alias("dim_endereco_id"),
        to_date(col("v.data_venda")).alias("dim_tempo_data"),
        col("iv.quantidade"),
        col("iv.preco_unitario"),
        col("iv.subtotal"),
        col("v.subtotal").alias("subtotal_venda"),
        col("v.frete"),
        col("v.total").alias("total_venda"),
        col("v.status").alias("status_venda"),
        col("v.metodo_pagamento"),
        col("v.status_pagamento"),
        col("v.data_venda"),
        year(col("v.data_venda")).alias("ano"),
        month(col("v.data_venda")).alias("mes"),
        dayofmonth(col("v.data_venda")).alias("dia"),
    )
    .filter(
        (col("quantidade") > 0)
        & (col("preco_unitario") > 0)
        & (col("subtotal") > 0)
        & (col("dim_cliente_id").isNotNull())
        & (col("dim_produto_id").isNotNull())
    )
)

# Salvamento
save_to_silver(dim_fornecedores, "dim_fornecedores")
save_to_silver(dim_categorias, "dim_categorias")
save_to_silver(dim_produtos, "dim_produtos")
save_to_silver(dim_clientes, "dim_clientes")
save_to_silver(dim_enderecos, "dim_enderecos")
save_to_silver(dim_tempo, "dim_tempo", ["ano", "mes"])
save_to_silver(facts_vendas, "facts_vendas", ["ano", "mes", "dia"])

# Métricas e relatório de execução
total_vendas = facts_vendas.agg(
    count("*").alias("total_records"),
    sql_sum("total_venda").alias("total_value"),
    sql_sum("quantidade").alias("total_items"),
).collect()[0]

max_timestamp = vendas_bronze.agg(sql_max("data_venda")).collect()[0][0]

report_data = [
    (
        EXECUTION_DATE,
        TRIGGERED_BY,
        IS_INCREMENTAL,
        total_vendas["total_records"] or 0,
        float(total_vendas["total_value"] or 0.0),
        total_vendas["total_items"] or 0,
        max_timestamp.strftime("%Y-%m-%d %H:%M:%S") if max_timestamp else None,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
]

report_schema = StructType(
    [
        StructField("execution_date", StringType(), True),
        StructField("triggered_by", StringType(), True),
        StructField("is_incremental", BooleanType(), True),
        StructField("total_records", LongType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("total_items", LongType(), True),
        StructField("last_processed_timestamp", StringType(), True),
        StructField("processed_at", StringType(), True),
    ]
)

report_df = spark.createDataFrame(report_data, report_schema)
save_to_silver(report_df, "_execution_reports", ["execution_date"])

job.commit()
