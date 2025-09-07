import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    desc,
    first,
    lit,
)
from pyspark.sql.functions import max as sql_max
from pyspark.sql.functions import min as sql_min
from pyspark.sql.functions import month
from pyspark.sql.functions import round as sql_round
from pyspark.sql.functions import sum as sql_sum
from pyspark.sql.functions import when, year
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
        "SILVER_DATABASE",
        "GOLD_DATABASE",
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
S3_GOLD_PATH = f"s3://{S3_BUCKET}/gold/"
SILVER_DATABASE = args["SILVER_DATABASE"]
GOLD_DATABASE = args["GOLD_DATABASE"]
EXECUTION_DATE = datetime.now().strftime("%Y-%m-%d")
IS_INCREMENTAL = args.get("incremental", "false").lower() == "true"
TRIGGERED_BY = args.get("triggered_by", "manual")

spark.conf.set("spark.sql.parquet.timestampType", "TIMESTAMP_MILLIS")
spark.conf.set("spark.sql.adaptive.enabled", "true")


def add_gold_metadata(df):
    return (
        df.withColumn("gold_created_at", current_timestamp())
        .withColumn("gold_execution_date", lit(EXECUTION_DATE))
        .withColumn("triggered_by", lit(TRIGGERED_BY))
    )


def load_silver_table(table_name):
    try:
        df = glueContext.create_dynamic_frame.from_catalog(
            database=SILVER_DATABASE,
            table_name=table_name,
            transformation_ctx=f"load_{table_name}",
        ).toDF()
        return df
    except Exception as e:
        print(f"Erro carregando {table_name}: {str(e)}")
        return spark.createDataFrame([], StructType([]))


def save_to_gold(df, table_name, partition_keys=[]):
    if df.count() == 0:
        print(f"Pulando {table_name} - sem dados")
        return

    dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    mode = "overwrite"

    print(f"Salvando {table_name}: {df.count()} registros ({mode})")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": f"{S3_GOLD_PATH}{table_name}/",
            "partitionKeys": partition_keys,
        },
        format="parquet",
        format_options={"writeMode": mode},
        transformation_ctx=f"write_{table_name}",
    )


print(f"Modo de execução: {'Incremental' if IS_INCREMENTAL else 'Full Refresh'}")

facts_vendas = load_silver_table("facts_vendas")
dim_produtos = load_silver_table("dim_produtos")
dim_clientes = load_silver_table("dim_clientes")
dim_categorias = load_silver_table("dim_categorias")
dim_tempo = load_silver_table("dim_tempo")

if IS_INCREMENTAL:
    current_year = datetime.now().year
    current_month = datetime.now().month
    facts_vendas = facts_vendas.filter(
        (col("ano") >= current_year - 1)
        | ((col("ano") == current_year) & (col("mes") >= current_month - 3))
    )

print(f"Registros carregados - Facts: {facts_vendas.count()}")

vendas_mensais = add_gold_metadata(
    facts_vendas.groupBy("ano", "mes")
    .agg(
        count("*").alias("total_vendas"),
        sql_sum("total_venda").alias("receita_total"),
        sql_sum("quantidade").alias("itens_vendidos"),
        avg("total_venda").alias("ticket_medio"),
        count("dim_cliente_id").alias("clientes_unicos"),
    )
    .withColumn("receita_total", sql_round(col("receita_total"), 2))
    .withColumn("ticket_medio", sql_round(col("ticket_medio"), 2))
    .orderBy("ano", "mes")
)

top_produtos = add_gold_metadata(
    facts_vendas.alias("f")
    .join(dim_produtos.alias("p"), col("f.dim_produto_id") == col("p.produto_id"))
    .join(dim_categorias.alias("c"), col("p.categoria_id") == col("c.categoria_id"))
    .groupBy("p.produto_id", "p.produto_nome", "c.categoria_nome")
    .agg(
        sql_sum("f.quantidade").alias("quantidade_vendida"),
        sql_sum("f.total_venda").alias("receita_produto"),
        count("*").alias("numero_vendas"),
        avg("f.preco_unitario").alias("preco_medio"),
    )
    .withColumn("receita_produto", sql_round(col("receita_produto"), 2))
    .withColumn("preco_medio", sql_round(col("preco_medio"), 2))
    .orderBy(desc("receita_produto"))
    .limit(100)
)

clientes_vip = add_gold_metadata(
    facts_vendas.alias("f")
    .join(dim_clientes.alias("c"), col("f.dim_cliente_id") == col("c.cliente_id"))
    .groupBy("c.cliente_id", "c.cliente_nome_completo", "c.cliente_email")
    .agg(
        sql_sum("f.total_venda").alias("valor_total_compras"),
        count("*").alias("numero_compras"),
        avg("f.total_venda").alias("ticket_medio_cliente"),
        sql_max("f.data_venda").alias("ultima_compra"),
        sql_min("f.data_venda").alias("primeira_compra"),
    )
    .withColumn("valor_total_compras", sql_round(col("valor_total_compras"), 2))
    .withColumn("ticket_medio_cliente", sql_round(col("ticket_medio_cliente"), 2))
    .filter(col("valor_total_compras") > 1000)
    .orderBy(desc("valor_total_compras"))
    .limit(500)
)

performance_categorias = add_gold_metadata(
    facts_vendas.alias("f")
    .join(dim_produtos.alias("p"), col("f.dim_produto_id") == col("p.produto_id"))
    .join(dim_categorias.alias("c"), col("p.categoria_id") == col("c.categoria_id"))
    .groupBy("c.categoria_id", "c.categoria_nome")
    .agg(
        sql_sum("f.total_venda").alias("receita_categoria"),
        sql_sum("f.quantidade").alias("itens_vendidos"),
        count("*").alias("numero_transacoes"),
        avg("f.preco_unitario").alias("preco_medio_categoria"),
        count("f.dim_cliente_id").alias("clientes_unicos"),
    )
    .withColumn("receita_categoria", sql_round(col("receita_categoria"), 2))
    .withColumn("preco_medio_categoria", sql_round(col("preco_medio_categoria"), 2))
    .orderBy(desc("receita_categoria"))
)

vendas_diarias = add_gold_metadata(
    facts_vendas.alias("f")
    .join(dim_tempo.alias("t"), col("f.dim_tempo_data") == col("t.data"))
    .groupBy("t.data", "t.ano", "t.mes", "t.dia", "t.dia_semana_nome", "t.tipo_dia")
    .agg(
        count("*").alias("vendas_dia"),
        sql_sum("f.total_venda").alias("receita_dia"),
        sql_sum("f.quantidade").alias("itens_dia"),
        avg("f.total_venda").alias("ticket_medio_dia"),
    )
    .withColumn("receita_dia", sql_round(col("receita_dia"), 2))
    .withColumn("ticket_medio_dia", sql_round(col("ticket_medio_dia"), 2))
    .orderBy(desc("data"))
)

resumo_executivo = add_gold_metadata(
    facts_vendas.agg(
        count("*").alias("total_transacoes"),
        sql_sum("total_venda").alias("receita_total_periodo"),
        sql_sum("quantidade").alias("total_itens_vendidos"),
        avg("total_venda").alias("ticket_medio_geral"),
        count("dim_cliente_id").alias("total_clientes"),
        sql_max("data_venda").alias("data_ultima_venda"),
        sql_min("data_venda").alias("data_primeira_venda"),
    )
    .withColumn("receita_total_periodo", sql_round(col("receita_total_periodo"), 2))
    .withColumn("ticket_medio_geral", sql_round(col("ticket_medio_geral"), 2))
    .withColumn("periodo_analise", lit(EXECUTION_DATE))
)

save_to_gold(vendas_mensais, "vendas_mensais", ["ano", "mes"])
save_to_gold(top_produtos, "top_produtos")
save_to_gold(clientes_vip, "clientes_vip")
save_to_gold(performance_categorias, "performance_categorias")
save_to_gold(vendas_diarias, "vendas_diarias", ["ano", "mes"])
save_to_gold(resumo_executivo, "resumo_executivo")

total_records = (
    vendas_mensais.count()
    + top_produtos.count()
    + clientes_vip.count()
    + performance_categorias.count()
    + vendas_diarias.count()
    + resumo_executivo.count()
)

report_data = [
    (
        EXECUTION_DATE,
        TRIGGERED_BY,
        IS_INCREMENTAL,
        total_records,
        facts_vendas.count(),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
]

report_schema = StructType(
    [
        StructField("execution_date", StringType(), True),
        StructField("triggered_by", StringType(), True),
        StructField("is_incremental", BooleanType(), True),
        StructField("total_gold_records", LongType(), True),
        StructField("source_facts_records", LongType(), True),
        StructField("processed_at", StringType(), True),
    ]
)

report_df = spark.createDataFrame(report_data, report_schema)
save_to_gold(report_df, "_gold_execution_reports", ["execution_date"])

print(f"ETL Gold Concluído - Total registros criados: {total_records}")

job.commit()
