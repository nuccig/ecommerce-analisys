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
    month,
    quarter,
    to_date,
    to_timestamp,
    weekofyear,
    when,
    year,
)
from pyspark.sql.functions import max as sql_max
from pyspark.sql.functions import sum as sql_sum
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "BRONZE_DATABASE",
        "SILVER_DATABASE",
        "incremental",
        "full_refresh",
        "triggered_by",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = args["S3_BUCKET"]
S3_BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
S3_SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
BRONZE_DATABASE = args["BRONZE_DATABASE"]
SILVER_DATABASE = args["SILVER_DATABASE"]

EXECUTION_DATE = datetime.now().strftime("%Y-%m-%d")
IS_INCREMENTAL = args.get("incremental", "false").lower() == "true"
IS_FULL_REFRESH = args.get("full_refresh", "false").lower() == "true"
TRIGGERED_BY = args.get("triggered_by", "manual")

print(f"Execution Date: {EXECUTION_DATE}")
print(f"Incremental: {IS_INCREMENTAL}")
print(f"Full Refresh: {IS_FULL_REFRESH}")
print(f"Triggered By: {TRIGGERED_BY}")

# Configurações para lidar com timestamps problemáticos
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "false")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.parquet.timestampType", "TIMESTAMP_MILLIS")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# Configuração adicional para controlar precisão de timestamp
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")


def add_silver_metadata(df):
    """Adiciona metadados padrão da camada Silver"""
    return (
        df.withColumn("silver_created_at", current_timestamp())
        .withColumn("silver_execution_date", lit(EXECUTION_DATE))
        .withColumn("data_source", lit("silver"))
        .withColumn("triggered_by", lit(TRIGGERED_BY))
    )


def apply_basic_filters(df, table_name):
    """Aplica filtros básicos por tabela"""
    if table_name == "facts_vendas":
        return df.filter(
            (col("quantidade") > 0)
            & (col("preco_unitario") > 0)
            & (col("subtotal") > 0)
            & (col("dim_cliente_id").isNotNull())
            & (col("dim_produto_id").isNotNull())
        )
    elif table_name == "dim_produtos":
        # Para dim_produtos, o ID agora é produto_id
        return df.filter(col("produto_id").isNotNull())
    elif table_name == "dim_clientes":
        return df.filter(col("cliente_id").isNotNull())
    elif table_name == "dim_fornecedores":
        return df.filter(col("fornecedor_id").isNotNull())
    elif table_name == "dim_categorias":
        return df.filter(col("categoria_id").isNotNull())
    elif table_name == "dim_enderecos":
        return df.filter(col("endereco_id").isNotNull())
    else:
        # Para tabelas que ainda usam 'id' como nome da coluna
        id_columns = [col_name for col_name in df.columns if col_name.endswith("_id")]
        if id_columns:
            # Usar a primeira coluna que termina com _id
            return df.filter(col(id_columns[0]).isNotNull())
        elif "id" in df.columns:
            return df.filter(col("id").isNotNull())
        else:
            # Se não encontrar nenhuma coluna de ID, retornar o DataFrame sem filtro
            return df


def get_last_processed_timestamp(table_name):
    """Recupera o último timestamp processado para uma tabela específica"""
    try:
        last_report = glueContext.create_dynamic_frame.from_catalog(
            database=SILVER_DATABASE,
            table_name="_execution_reports",
            transformation_ctx=f"load_last_report_{table_name}",
        ).toDF()

        last_successful = (
            last_report.filter(
                (col("is_incremental") == True) & (col("total_records") > 0)
            )
            .orderBy(col("processed_at").desc())
            .limit(1)
        )

        if last_successful.count() > 0:
            last_timestamp = last_successful.select(
                "last_processed_timestamp"
            ).collect()[0][0]
            print(f"Último timestamp processado para {table_name}: {last_timestamp}")
            return last_timestamp
        else:
            print(
                f"Nenhuma execução anterior encontrada para {table_name}, processando últimos 7 dias"
            )
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
            timestamp_str = last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            return f"{timestamp_column} > '{timestamp_str}'"
        else:
            return f"{timestamp_column} >= date_sub(current_date(), 365)"

    return None


def load_bronze_table_with_timestamp(
    table_name,
    transformation_ctx,
    timestamp_column="criado_em",
    apply_incremental=False,
):
    """Carrega tabela bronze com filtro baseado em timestamp"""
    try:
        print(f"Carregando {table_name} com timestamps como string...")

        # Sempre carregar timestamps como string para evitar problemas de compatibilidade
        additional_options = {
            "timestampAsString": "true",
            "parquet.datetime.as.string": "true",
        }

        if IS_INCREMENTAL and apply_incremental:
            timestamp_filter = get_incremental_filter_by_timestamp(
                table_name, timestamp_column
            )

            if timestamp_filter:
                print(
                    f"Carregando {table_name} com filtro incremental: {timestamp_filter}"
                )
                df = glueContext.create_dynamic_frame.from_catalog(
                    database=BRONZE_DATABASE,
                    table_name=table_name,
                    push_down_predicate=timestamp_filter,
                    additional_options=additional_options,
                    transformation_ctx=transformation_ctx,
                ).toDF()
            else:
                df = glueContext.create_dynamic_frame.from_catalog(
                    database=BRONZE_DATABASE,
                    table_name=table_name,
                    additional_options=additional_options,
                    transformation_ctx=transformation_ctx,
                ).toDF()
        else:
            print(f"Carregando {table_name} completa")
            df = glueContext.create_dynamic_frame.from_catalog(
                database=BRONZE_DATABASE,
                table_name=table_name,
                additional_options=additional_options,
                transformation_ctx=transformation_ctx,
            ).toDF()

        # Converter colunas de timestamp que vieram como string de volta para timestamp
        timestamp_columns = [
            "criado_em",
            "atualizado_em",
            "data_venda",
            "data_nascimento",
        ]

        for col_name in timestamp_columns:
            if col_name in df.columns:
                try:
                    print(
                        f"Convertendo coluna {col_name} para timestamp em {table_name}"
                    )
                    # Converter para timestamp com precisão limitada
                    df = df.withColumn(
                        col_name,
                        coalesce(
                            to_timestamp(col(col_name)),
                            to_date(col(col_name)).cast(TimestampType()),
                        ),
                    )
                except Exception as e:
                    print(
                        f"Aviso: Não foi possível converter coluna {col_name}: {str(e)}"
                    )

        print(f"Tabela {table_name} carregada com {df.count()} registros")
        return df

    except Exception as e:
        print(f"Erro ao carregar {table_name}: {str(e)}")
        # Fallback: tentar carregar sem configurações especiais
        try:
            print(f"Tentando fallback para {table_name}...")
            df = glueContext.create_dynamic_frame.from_catalog(
                database=BRONZE_DATABASE,
                table_name=table_name,
                transformation_ctx=f"{transformation_ctx}_fallback",
            ).toDF()

            # Ainda tentar converter timestamps no fallback
            timestamp_columns = [
                "criado_em",
                "atualizado_em",
                "data_venda",
                "data_nascimento",
            ]
            for col_name in timestamp_columns:
                if col_name in df.columns:
                    try:
                        df = df.withColumn(col_name, to_timestamp(col(col_name)))
                    except:
                        print(f"Fallback: mantendo {col_name} como string")

            return df
        except Exception as e2:
            print(f"Fallback também falhou para {table_name}: {str(e2)}")
            raise


def bulk_load_bronze_tables(table_configs):
    """Carrega múltiplas tabelas bronze de uma vez"""
    loaded_tables = {}

    for table_name, config in table_configs.items():
        timestamp_col = config.get("timestamp_column", "criado_em")
        apply_incremental = config.get("apply_incremental", True)

        print(f"Carregando {table_name}...")
        loaded_tables[table_name] = load_bronze_table_with_timestamp(
            table_name,
            f"{table_name}_bronze",
            timestamp_column=timestamp_col,
            apply_incremental=apply_incremental,
        )

    return loaded_tables


def create_dimension_with_mapping(df, table_name, column_mappings, join_tables=None):
    """Cria dimensão aplicando mapeamento de colunas e joins"""

    # Validar se as colunas existem no DataFrame
    available_columns = df.columns
    print(f"Criando {table_name} - Colunas disponíveis: {available_columns}")

    # Verificar se todas as colunas do mapeamento existem
    missing_columns = []
    valid_mappings = {}

    for old_col, new_col in column_mappings.items():
        if old_col in available_columns:
            valid_mappings[old_col] = new_col
        else:
            missing_columns.append(old_col)
            print(f"AVISO: Coluna '{old_col}' não encontrada em {table_name}")

    if missing_columns:
        print(f"Colunas ausentes em {table_name}: {missing_columns}")
        print(f"Continuando com mapeamento parcial: {valid_mappings}")

    if not valid_mappings:
        print(f"ERRO: Nenhuma coluna válida encontrada para {table_name}")
        # Retornar DataFrame vazio com metadados
        empty_df = spark.createDataFrame([], StructType([]))
        return add_silver_metadata(empty_df)

    if join_tables:
        for join_config in join_tables:
            join_df = join_config["df"]
            join_condition = join_config["condition"]
            join_type = join_config.get("type", "left")
            alias = join_config.get("alias")

            if alias:
                join_df = join_df.alias(alias)

            df = df.join(join_df, join_condition, join_type)

    select_cols = [
        col(old_col).alias(new_col) for old_col, new_col in valid_mappings.items()
    ]

    dim_df = df.select(*select_cols).distinct()

    dim_df = add_silver_metadata(apply_basic_filters(dim_df, table_name))

    print(f"{table_name.upper()} criada: {dim_df.count()} registros")
    return dim_df


def get_max_timestamp_from_multiple_tables(table_timestamp_mapping):
    """Calcula o maior timestamp de múltiplas tabelas"""
    max_timestamps = {}

    for table_name, (df, timestamp_col) in table_timestamp_mapping.items():
        try:
            if df.count() > 0:
                # Filtrar apenas registros com timestamp não nulo
                filtered_df = df.filter(col(timestamp_col).isNotNull())
                if filtered_df.count() > 0:
                    max_val = filtered_df.agg(sql_max(timestamp_col)).collect()[0][0]
                    if max_val:
                        max_timestamps[table_name] = max_val
                        print(f"Max timestamp para {table_name}: {max_val}")
        except Exception as e:
            print(f"Erro ao calcular max timestamp para {table_name}: {str(e)}")
            continue

    if max_timestamps:
        overall_max = max(max_timestamps.values())
        print(f"Maior timestamp processado: {overall_max}")
        print(f"Detalhes por tabela: {max_timestamps}")
        return overall_max

    print("Nenhum timestamp válido encontrado, usando timestamp atual")
    return datetime.now()


def calculate_data_quality_metrics(tables_dict):
    """Calcula métricas de qualidade de dados de forma otimizada"""
    metrics = {}

    if "dim_produtos" in tables_dict:
        produtos_df = tables_dict["dim_produtos"]
        metrics["produtos_sem_categoria"] = produtos_df.filter(
            col("categoria_nome").isNull()
        ).count()
        metrics["produtos_sem_fornecedor"] = produtos_df.filter(
            col("fornecedor_nome").isNull()
        ).count()

    if "facts_vendas" in tables_dict:
        vendas_df = tables_dict["facts_vendas"]
        metrics["vendas_sem_cliente"] = vendas_df.filter(
            col("dim_cliente_id").isNull()
        ).count()
        metrics["vendas_sem_produto"] = vendas_df.filter(
            col("dim_produto_id").isNull()
        ).count()

        aggregations = vendas_df.agg(
            count("*").alias("total_records"),
            sql_sum("total_venda").alias("total_value"),
            sql_sum("quantidade").alias("total_items"),
        ).collect()[0]

        metrics.update(
            {
                "facts_vendas_count": aggregations["total_records"],
                "facts_vendas_total_value": aggregations["total_value"] or 0,
                "facts_vendas_total_items": aggregations["total_items"] or 0,
            }
        )

    return metrics


try:
    print("=== CARREGANDO TABELAS BRONZE ===")
    bronze_tables_config = {
        "vendas": {"timestamp_column": "data_venda", "apply_incremental": True},
        "itens_venda": {"timestamp_column": "criado_em", "apply_incremental": True},
        "produtos": {"timestamp_column": "criado_em", "apply_incremental": True},
        "clientes": {"timestamp_column": "criado_em", "apply_incremental": True},
        "categorias": {"timestamp_column": "criado_em", "apply_incremental": True},
        "fornecedores": {"timestamp_column": "criado_em", "apply_incremental": True},
        "enderecos": {"timestamp_column": "criado_em", "apply_incremental": True},
    }

    bronze_data = bulk_load_bronze_tables(bronze_tables_config)

    vendas_bronze = bronze_data["vendas"]
    itens_venda_bronze = bronze_data["itens_venda"]
    produtos_bronze = bronze_data["produtos"]
    clientes_bronze = bronze_data["clientes"]
    categorias_bronze = bronze_data["categorias"]
    fornecedores_bronze = bronze_data["fornecedores"]
    enderecos_bronze = bronze_data["enderecos"]

    print(
        f"Carregamento concluído: {vendas_bronze.count()} vendas, {produtos_bronze.count()} produtos"
    )
    
    # Validar se todas as tabelas foram carregadas com dados
    table_checks = {
        "vendas": vendas_bronze,
        "itens_venda": itens_venda_bronze,
        "produtos": produtos_bronze,
        "clientes": clientes_bronze,
        "categorias": categorias_bronze,
        "fornecedores": fornecedores_bronze,
        "enderecos": enderecos_bronze,
    }
    
    for table_name, df in table_checks.items():
        count = df.count()
        cols = df.columns
        print(f"Tabela {table_name}: {count} registros, colunas: {cols}")
        
        # Verificar se tem a coluna 'id' que é obrigatória em todas as tabelas
        if 'id' not in cols:
            print(f"ALERTA: Tabela {table_name} não possui coluna 'id'!")
        
        if count == 0:
            print(f"ALERTA: Tabela {table_name} está vazia!")

except Exception as e:
    print(f"Erro ao carregar dados Bronze: {str(e)}")
    raise

# DIM_FORNECEDORES
print(f"=== Criando DIM_FORNECEDORES ===")
print(f"Colunas em fornecedores_bronze: {fornecedores_bronze.columns}")
print(f"Contagem de registros: {fornecedores_bronze.count()}")

# Schema esperado baseado na documentação:
# id, nome, email, telefone, cnpj, endereco, cidade, estado, cep, ativo, criado_em

# Mostrar algumas linhas para debug se há dados
if fornecedores_bronze.count() > 0:
    try:
        print("Primeiras 5 linhas de fornecedores_bronze:")
        fornecedores_bronze.show(5, truncate=False)
    except Exception as e:
        print(f"Erro ao mostrar dados: {str(e)}")
else:
    print("AVISO: Tabela fornecedores_bronze está vazia!")

# Mapeamento baseado no schema real
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
    "criado_em": "fornecedor_criado_em",
}

dim_fornecedores = create_dimension_with_mapping(
    fornecedores_bronze, "dim_fornecedores", dim_fornecedores_mapping
)

# DIM_CATEGORIAS
print(f"=== Criando DIM_CATEGORIAS ===")
print(f"Colunas em categorias_bronze: {categorias_bronze.columns}")
print(f"Contagem de registros: {categorias_bronze.count()}")

# Schema esperado: id, nome, descricao, ativa, criado_em
dim_categorias_mapping = {
    "id": "categoria_id",
    "nome": "categoria_nome",
    "descricao": "categoria_descricao",
    "ativa": "categoria_ativa",
    "criado_em": "categoria_criado_em",
}

dim_categorias = create_dimension_with_mapping(
    categorias_bronze, "dim_categorias", dim_categorias_mapping
)

# DIM_PRODUTOS - Abordagem mais explícita para evitar ambiguidade
print(f"=== Criando DIM_PRODUTOS ===")
print(f"Colunas em produtos_bronze: {produtos_bronze.columns}")
print(f"Contagem de registros: {produtos_bronze.count()}")

# Schema esperado: id, nome, descricao, categoria_id, fornecedor_id, preco, custo, peso, quantidade_estoque, em_estoque, ativo, criado_em
try:
    produtos_base = produtos_bronze.select(
        col("id").alias("produto_id"),
        col("nome").alias("produto_nome"),
        col("descricao").alias("produto_descricao"),
        col("categoria_id"),
        col("fornecedor_id"),
        col("preco").alias("produto_preco"),
        col("custo").alias("produto_custo"),
        col("peso").alias("produto_peso"),
        col("quantidade_estoque").alias("produto_quantidade_estoque"),
        col("em_estoque").alias("produto_em_estoque"),
        col("ativo").alias("produto_ativo"),
        col("criado_em").alias("produto_criado_em"),
    )

    print(f"Produtos base criado com colunas: {produtos_base.columns}")

except Exception as e:
    print(f"Erro ao criar produtos_base: {str(e)}")
    print(f"Colunas disponíveis em produtos_bronze: {produtos_bronze.columns}")
    raise

# Join com categorias
if categorias_bronze.count() > 0:
    try:
        print(f"Colunas em categorias_bronze: {categorias_bronze.columns}")
        categorias_for_join = categorias_bronze.select(
            col("id").alias("cat_id"),
            col("nome").alias("categoria_nome"),
            col("descricao").alias("categoria_descricao"),
        )
        produtos_base = produtos_base.join(
            categorias_for_join,
            produtos_base.categoria_id == categorias_for_join.cat_id,
            "left",
        ).drop("cat_id")
        print(f"Join com categorias realizado com sucesso")
    except Exception as e:
        print(f"Erro no join com categorias: {str(e)}")
        print("Continuando sem join com categorias...")

# Join com fornecedores
if fornecedores_bronze.count() > 0:
    try:
        print(f"Colunas em fornecedores_bronze: {fornecedores_bronze.columns}")
        fornecedores_for_join = fornecedores_bronze.select(
            col("id").alias("forn_id"),
            col("nome").alias("fornecedor_nome"),
            col("email").alias("fornecedor_email"),
            col("telefone").alias("fornecedor_telefone"),
        )
        produtos_base = produtos_base.join(
            fornecedores_for_join,
            produtos_base.fornecedor_id == fornecedores_for_join.forn_id,
            "left",
        ).drop("forn_id")
        print(f"Join com fornecedores realizado com sucesso")
    except Exception as e:
        print(f"Erro no join com fornecedores: {str(e)}")
        print("Continuando sem join com fornecedores...")

dim_produtos = add_silver_metadata(
    apply_basic_filters(produtos_base.distinct(), "dim_produtos")
)
print(f"DIM_PRODUTOS criada: {dim_produtos.count()} registros")
print(f"Colunas finais em DIM_PRODUTOS: {dim_produtos.columns}")

# Verificar se há dados válidos
if dim_produtos.count() > 0:
    sample_produto = dim_produtos.first()
    print(f"Exemplo de produto: ID={getattr(sample_produto, 'produto_id', 'N/A')}")
else:
    print("Aviso: DIM_PRODUTOS está vazia - verificar joins ou dados de entrada")

# DIM_CLIENTES - Com colunas calculadas
print(f"=== Criando DIM_CLIENTES ===")
print(f"Colunas em clientes_bronze: {clientes_bronze.columns}")
print(f"Contagem de registros: {clientes_bronze.count()}")

# Schema esperado: id, nome, sobrenome, email, telefone, data_nascimento, endereco_id, ativo, criado_em
try:
    print(f"Colunas em clientes_bronze: {clientes_bronze.columns}")
    dim_clientes = clientes_bronze.select(
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
            year(current_date()) - year(to_date(col("data_nascimento"))),
        )
        .otherwise(lit(None))
        .alias("cliente_idade"),
        col("genero").alias("cliente_genero"),
        reduce_timestamp_precision(col("criado_em")).alias("cliente_criado_em"),
    ).distinct()

    print(f"Dimensão clientes criada com {dim_clientes.count()} registros")

except Exception as e:
    print(f"Erro ao criar dim_clientes: {str(e)}")
    print(f"Colunas disponíveis: {clientes_bronze.columns}")
    raise

dim_clientes = add_silver_metadata(apply_basic_filters(dim_clientes, "dim_clientes"))
print(f"DIM_CLIENTES criada: {dim_clientes.count()} registros")
print(f"Colunas finais em DIM_CLIENTES: {dim_clientes.columns}")

# Verificar dados válidos
if dim_clientes.count() > 0:
    sample_cliente = dim_clientes.first()
    print(f"Exemplo de cliente: ID={getattr(sample_cliente, 'cliente_id', 'N/A')}, Nome={getattr(sample_cliente, 'cliente_nome_completo', 'N/A')}")
else:
    print("Aviso: DIM_CLIENTES está vazia")

# DIM_ENDERECOS
print(f"=== Criando DIM_ENDERECOS ===")
print(f"Colunas em enderecos_bronze: {enderecos_bronze.columns}")
print(f"Contagem de registros: {enderecos_bronze.count()}")

# Schema esperado: id, cliente_id, cep, logradouro, numero, complemento, bairro, cidade, estado, endereco_principal, criado_em
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
    "criado_em": "endereco_criado_em",
}

dim_enderecos = create_dimension_with_mapping(
    enderecos_bronze, "dim_enderecos", dim_enderecos_mapping
)
print(f"DIM_ENDERECOS criada: {dim_enderecos.count()} registros")
print(f"Colunas finais em DIM_ENDERECOS: {dim_enderecos.columns}")

# Verificar dados válidos
if dim_enderecos.count() > 0:
    sample_endereco = dim_enderecos.first()
    print(f"Exemplo de endereço: ID={getattr(sample_endereco, 'endereco_id', 'N/A')}, Cidade={getattr(sample_endereco, 'endereco_cidade', 'N/A')}")
else:
    print("Aviso: DIM_ENDERECOS está vazia")

# DIM_TEMPO
print(f"=== Criando DIM_TEMPO ===")
print(f"Colunas em vendas_bronze: {vendas_bronze.columns}")
print(f"Contagem de registros: {vendas_bronze.count()}")

# Schema esperado para vendas: id, cliente_id, produto_id, quantidade, preco_unitario, desconto, total, data_venda, status, endereco_entrega_id, criado_em
# Primeiro converter data_venda para timestamp/date apropriado
vendas_with_date = vendas_bronze.withColumn(
    "data_venda_converted",
    coalesce(
        to_timestamp(col("data_venda")),
        to_date(col("data_venda")).cast(TimestampType()),
    ),
)

dim_tempo = vendas_with_date.select(
    to_date(col("data_venda_converted")).alias("data"),
    year(col("data_venda_converted")).alias("ano"),
    month(col("data_venda_converted")).alias("mes"),
    dayofmonth(col("data_venda_converted")).alias("dia"),
    dayofweek(col("data_venda_converted")).alias("dia_semana"),
    weekofyear(col("data_venda_converted")).alias("semana_ano"),
    quarter(col("data_venda_converted")).alias("trimestre"),
    date_format(col("data_venda_converted"), "MMMM").alias("mes_nome"),
    date_format(col("data_venda_converted"), "EEEE").alias("dia_semana_nome"),
    when(dayofweek(col("data_venda_converted")).isin([1, 7]), "Final de Semana")
    .otherwise("Dia Útil")
    .alias("tipo_dia"),
).distinct()

# Filtrar registros onde data_venda não é nula
dim_tempo = dim_tempo.filter(col("data").isNotNull())

dim_tempo = add_silver_metadata(dim_tempo)
print(f"DIM_TEMPO criada: {dim_tempo.count()} registros")
print(f"Colunas finais em DIM_TEMPO: {dim_tempo.columns}")

# Verificar dados válidos
if dim_tempo.count() > 0:
    sample_tempo = dim_tempo.first()
    print(f"Exemplo de tempo: Data={getattr(sample_tempo, 'data', 'N/A')}, Ano={getattr(sample_tempo, 'ano', 'N/A')}")
else:
    print("Aviso: DIM_TEMPO está vazia")

# Fato

# FACTS_VENDAS - Tabela de Fatos de Vendas
print(f"=== Criando FACTS_VENDAS ===")
print(f"Colunas em vendas_bronze: {vendas_bronze.columns}")
print(f"Contagem vendas: {vendas_bronze.count()}")
print(f"Colunas em itens_venda_bronze: {itens_venda_bronze.columns}")
print(f"Contagem itens_venda: {itens_venda_bronze.count()}")

# Schema esperado vendas: id, cliente_id, produto_id, quantidade, preco_unitario, desconto, total, data_venda, status, endereco_entrega_id, criado_em
# Schema esperado itens_venda: id, venda_id, produto_id, quantidade, preco_unitario, subtotal
# Primeiro preparar as tabelas com conversões de timestamp
try:

    vendas_prepared = vendas_bronze.withColumn(
        "data_venda_converted",
        coalesce(
            to_timestamp(col("data_venda")),
            to_date(col("data_venda")).cast(TimestampType()),
        ),
    )

    facts_vendas = (
        vendas_prepared.alias("v")
        .join(
            itens_venda_bronze.alias("iv"), col("v.id") == col("iv.venda_id"), "inner"
        )
        .select(
            col("v.id").alias("venda_id"),
            col("iv.id").alias("item_venda_id"),
            col("v.cliente_id").alias("dim_cliente_id"),
            col("iv.produto_id").alias("dim_produto_id"),
            col("v.endereco_entrega_id").alias("dim_endereco_id"),
            to_date(col("v.data_venda_converted")).alias("dim_tempo_data"),
            col("iv.quantidade").alias("quantidade"),
            col("iv.preco_unitario").alias("preco_unitario"),
            col("iv.subtotal").alias("subtotal"),
            col("v.subtotal").alias("subtotal_venda"),
            col("v.frete").alias("frete"),
            col("v.total").alias("total_venda"),
            col("v.status").alias("status_venda"),
            col("v.metodo_pagamento").alias("metodo_pagamento"),
            col("v.status_pagamento").alias("status_pagamento"),
            col("v.data_venda_converted").alias("data_venda_timestamp"),
            # Partições
            year(col("v.data_venda_converted")).alias("ano"),
            month(col("v.data_venda_converted")).alias("mes"),
            dayofmonth(col("v.data_venda_converted")).alias("dia"),
        )
    )

except Exception as e:
    print(f"Erro ao criar facts_vendas: {str(e)}")
    print(f"Detalhes do erro: {str(e)}")
    print(f"Colunas vendas: {vendas_bronze.columns}")
    print(f"Colunas itens_venda: {itens_venda_bronze.columns}")
    raise

facts_vendas = add_silver_metadata(apply_basic_filters(facts_vendas, "facts_vendas"))
print(f"FACTS_VENDAS criada: {facts_vendas.count()} registros")
print(f"Colunas finais em FACTS_VENDAS: {facts_vendas.columns}")

# Verificar dados válidos
if facts_vendas.count() > 0:
    sample_venda = facts_vendas.first()
    print(f"Exemplo de venda: ID={getattr(sample_venda, 'venda_id', 'N/A')}, Total={getattr(sample_venda, 'total_venda', 'N/A')}")
else:
    print("Aviso: FACTS_VENDAS está vazia")


def reduce_timestamp_precision(df):
    """Reduz a precisão de colunas timestamp para evitar problemas de serialização"""
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            print(f"Reduzindo precisão da coluna timestamp {field.name}")
            # Converter para timestamp com precisão de milissegundos
            df = df.withColumn(
                field.name,
                to_timestamp(date_format(col(field.name), "yyyy-MM-dd HH:mm:ss")),
            )

    return df


def save_to_silver(df, table_name, partition_keys=[]):
    """Salva DataFrame na camada Silver"""
    try:
        # Reduzir precisão de timestamps para evitar problemas de serialização
        df = reduce_timestamp_precision(df)

        dyf = DynamicFrame.fromDF(df, glueContext, table_name)

        write_options = {
            "path": f"{S3_SILVER_PATH}{table_name}/",
            "partitionKeys": partition_keys,
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
                    "partitionOverwriteMode": "dynamic",
                },
                transformation_ctx=f"write_{table_name}",
            )
        elif IS_INCREMENTAL and table_name.startswith("dim_"):

            print(f"Salvando {table_name} em modo incremental (overwrite completo)")
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options=write_options,
                format="parquet",
                format_options={"writeMode": "overwrite"},
                transformation_ctx=f"write_{table_name}",
            )
        else:

            print(f"Salvando {table_name} em modo completo (overwrite)")
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options=write_options,
                format="parquet",
                format_options={"writeMode": "overwrite"},
                transformation_ctx=f"write_{table_name}",
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
            saved_tables[table_name] = {"status": "success", "records": df.count()}
        except Exception as e:
            print(f"Erro ao salvar {table_name}: {str(e)}")
            saved_tables[table_name] = {"status": "failed", "error": str(e)}
            raise

    total_records = sum(
        [
            info["records"]
            for info in saved_tables.values()
            if info["status"] == "success"
        ]
    )
    print(
        f"Bulk save concluído: {len(saved_tables)} tabelas, {total_records:,} registros totais"
    )

    return saved_tables


silver_tables = {
    "dim_fornecedores": dim_fornecedores,
    "dim_categorias": dim_categorias,
    "dim_produtos": dim_produtos,
    "dim_clientes": dim_clientes,
    "dim_enderecos": dim_enderecos,
    "dim_tempo": dim_tempo,
    "facts_vendas": facts_vendas,
}

partition_configs = {"dim_tempo": ["ano", "mes"], "facts_vendas": ["ano", "mes", "dia"]}

save_results = bulk_save_to_silver(silver_tables, partition_configs)

timestamp_mapping = {
    "vendas": (vendas_bronze, "data_venda"),
    "clientes": (clientes_bronze, "criado_em"),
    "produtos": (produtos_bronze, "criado_em"),
    "categorias": (categorias_bronze, "criado_em"),
    "fornecedores": (fornecedores_bronze, "criado_em"),
    "enderecos": (enderecos_bronze, "criado_em"),
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
    **quality_metrics,
}

# Log das estatísticas
for key, value in stats.items():
    print(f"{key}: {value}")

report_schema = StructType(
    [
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
        StructField(
            "last_processed_timestamp", StringType(), True
        ),  # Mudança para StringType
        StructField("processed_at", StringType(), True),  # Mudança para StringType
    ]
)

# Converter timestamps para string antes de criar o DataFrame
last_timestamp_str = (
    max_processed_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    if max_processed_timestamp
    else None
)
processed_at_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

report_data = [
    (
        EXECUTION_DATE,
        TRIGGERED_BY,
        IS_INCREMENTAL,
        quality_metrics.get("facts_vendas_count", 0),
        float(quality_metrics.get("facts_vendas_total_value", 0.0)),
        quality_metrics.get("facts_vendas_total_items", 0),
        quality_metrics.get("produtos_sem_categoria", 0),
        quality_metrics.get("produtos_sem_fornecedor", 0),
        quality_metrics.get("vendas_sem_cliente", 0),
        quality_metrics.get("vendas_sem_produto", 0),
        last_timestamp_str,  # String em vez de timestamp
        processed_at_str,  # String em vez de timestamp
    )
]

report_df = spark.createDataFrame(report_data, report_schema)

save_to_silver(report_df, "_execution_reports", ["execution_date"])

job.commit()
