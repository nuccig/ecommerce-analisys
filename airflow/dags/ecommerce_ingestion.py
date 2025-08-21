import json
import logging
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Dict, List, Optional

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

from airflow import DAG
import pyarrow as pa
import pyarrow.parquet as pq

@dataclass
class TableConfig:
    """Configuração de uma tabela"""

    table: str
    date_column: str | None
    incremental: bool

@dataclass
class ExtractionResult:
    """Resultado de uma extração"""

    table_name: str
    extract_date: str
    records: int
    status: str
    s3_key: Optional[str] = None
    error: Optional[str] = None
    file_size_mb: float = 0.0

class S3Manager:
    """Gerencia operações com S3"""

    def __init__(self, bucket: str, aws_conn_id: str = "aws_default"):
        self.bucket = bucket
        self.aws_conn_id = aws_conn_id
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.client = self.s3_hook.get_conn()

    def upload_json(self, data: Dict, s3_key: str):
        """Upload JSON para S3"""
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    def upload_parquet(self, df: pd.DataFrame, s3_key: str, metadata: Dict, table_schemas: Dict = None) -> float:
        """Upload DataFrame como Parquet para S3 com schema otimizado"""
        table_name = metadata.get("table_name", s3_key.split('/')[1])
        table_schema = table_schemas.get(table_name, {}) if table_schemas else {}
                
        df_copy = df.copy()
        schema_fields = []
        
        type_mapping = {
            'timestamp': (lambda c: pd.to_datetime(df_copy[c], errors='coerce').dt.floor('us'), pa.timestamp('us'), "timestamp"),
            'date': (lambda c: pd.to_datetime(df_copy[c], errors='coerce').dt.date, pa.date32(), "date"),
            'int32': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('Int32'), pa.int32(), "int"),
            'uint32': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('Int32'), pa.uint32(), "int"),
            'int64': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('Int64'), pa.int64(), "bigint"),
            'bigint': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('Int64'), pa.int64(), "bigint"),
            'uint64': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('uint64'), pa.uint64(), "bigint"),
            'decimal': (lambda c: pd.to_numeric(df_copy[c], errors='coerce').astype('float64'), pa.float64(), "double"),
            'bool': (lambda c: df_copy[c].astype('boolean'), pa.bool_(), "boolean"),
            'time': (lambda c: df_copy[c].astype('string'), pa.string(), "string"),
            'string': (lambda c: df_copy[c].astype('string'), pa.string(), "string"),
        }
        
        for col in df_copy.columns:
            col_type = table_schema.get(col)
            
            # Processar coluna baseado no tipo
            if col_type and col_type in type_mapping:
                transform_func, pa_type, hive_type = type_mapping[col_type]
                df_copy[col] = transform_func(col)
            elif col.endswith('_timestamp'):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.floor('us')
                pa_type, hive_type = pa.timestamp('us'), "timestamp"
            elif col == '_extraction_date':
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.date
                pa_type, hive_type = pa.date32(), "date"
            else:
                df_copy[col] = df_copy[col].astype('string')
                pa_type, hive_type = pa.string(), "string"
            
            field = pa.field(col, pa_type).with_metadata({"HIVE_TYPE_STRING": hive_type})
            schema_fields.append(field)
        
        parquet_buffer = BytesIO()
        try:
            table = pa.Table.from_pandas(df_copy, schema=pa.schema(schema_fields), preserve_index=False)
            pq.write_table(table, parquet_buffer, compression='snappy', 
                          use_deprecated_int96_timestamps=False, coerce_timestamps='us')
        except Exception as e:
            print(f"Schema dinâmico falhou, usando padrão: {e}")
            df_copy.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", 
                              index=False, use_deprecated_int96_timestamps=False)
        
        parquet_buffer.seek(0)
        
        self.client.put_object(
            Bucket=self.bucket, Key=s3_key, Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream", Metadata=metadata
        )
        
        return round(len(parquet_buffer.getvalue()) / (1024 * 1024), 2)

class DatabaseExtractor:
    """Gerencia extração de dados do banco"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._engine = None
        self._table_schemas = {}

    @property
    def engine(self):
        """Lazy loading da engine"""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
            logging.info(f"Engine criada: {type(self._engine).__name__}")
        return self._engine

    def dispose(self):
        """Fecha conexões"""
        if self._engine:
            self._engine.dispose()

    def extract_data(
        self, table_config: TableConfig, extract_date: date
    ) -> pd.DataFrame:
        """Extrai dados de uma tabela para uma data específica"""
        extract_date_str = extract_date.strftime("%Y-%m-%d")

        if table_config.incremental and table_config.date_column:
            query = f"""
            SELECT * FROM {table_config.table} 
            WHERE DATE({table_config.date_column}) = '{extract_date_str}'
            """
        else:
            query = f"SELECT * FROM {table_config.table}"

        logging.info(f"{table_config.table} - {extract_date_str}: Executando query")

        with self.engine.connect() as conn:
            df = pd.read_sql(sql=query, con=conn.connection)

        if not df.empty:
            df["_extraction_date"] = extract_date
            df["_extraction_timestamp"] = pd.Timestamp.now()
            df["_source_table"] = table_config.table

        return df
  
    def get_table_schema(self, table_name: str) -> Dict:
        """Extrai schema da tabela diretamente do MySQL"""
        if table_name in self._table_schemas:
            return self._table_schemas[table_name]
        
        try:
            
            with self.engine.connect() as conn:
                query = f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        COLUMN_TYPE,
                        EXTRA
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """
                    
                df = pd.read_sql(sql=query, con=conn.connection)
            
            schema = {}
            for _, row in df.iterrows():
                col_name = row['COLUMN_NAME']
                data_type = row['DATA_TYPE'].lower()
                column_type = row['COLUMN_TYPE'].lower()
                
                if data_type in ['int', 'integer', 'tinyint', 'smallint', 'mediumint']:
                    if 'unsigned' in column_type:
                        schema[col_name] = 'uint32'
                    else:
                        schema[col_name] = 'int32'
                elif data_type == 'bigint':
                    if 'unsigned' in column_type:
                        schema[col_name] = 'uint64'
                    else:
                        schema[col_name] = 'bigint'
                elif data_type in ['decimal', 'numeric', 'float', 'double']:
                    schema[col_name] = 'decimal'
                elif data_type == 'boolean':
                    schema[col_name] = 'bool'
                elif data_type == 'date':
                    schema[col_name] = 'date'
                elif data_type in ['datetime', 'timestamp']:
                    schema[col_name] = 'timestamp'
                elif data_type == 'time':
                    schema[col_name] = 'time'
                else:
                    schema[col_name] = 'string'
            
            self._table_schemas[table_name] = schema
            logging.info(f"Schema extraído para {table_name}: {len(schema)} colunas")
            
            return schema
            
        except Exception as e:
            logging.error(f"Erro ao extrair schema de {table_name}: {str(e)}")
            return {}
        
    def get_all_table_schemas(self, table_names: List[str]) -> Dict:
        """Extrai schemas de todas as tabelas de uma vez"""
        schemas = {}
        for table_name in table_names:
            schemas[table_name] = self.get_table_schema(table_name)
        return schemas

class S3PathBuilder:
    """Constrói caminhos S3 padronizados"""

    @staticmethod
    def build_table_path(table_name: str, extract_date: date) -> str:
        """Constrói caminho particionado para tabela"""
        year = extract_date.year
        month = extract_date.strftime("%m")
        day = extract_date.strftime("%d")
        return f"bronze/{table_name}/year={year}/month={month}/day={day}/{table_name}_{extract_date.strftime('%Y-%m-%d')}.parquet"

    @staticmethod
    def build_report_path(execution_date: datetime) -> str:
        """Constrói caminho para relatório"""
        return f"bronze/_reports/extraction_report_{execution_date.strftime('%Y-%m-%d')}.json"

class EcommerceDataExtractor:
    """Classe principal para extração de dados do e-commerce"""

    def __init__(
        self,
        mysql_connection: str,
        s3_bucket: str,
        aws_conn_id: str = "aws_default",
    ):
        self.db_extractor = DatabaseExtractor(mysql_connection)
        self.s3_manager = S3Manager(s3_bucket, aws_conn_id)
        self.s3_bucket = s3_bucket

        self.table_configs = {
            "vendas": TableConfig("vendas", "data_venda", True),
            "produtos": TableConfig("produtos", "criado_em", True),
            "itens_venda": TableConfig("itens_venda", None, False),
            "clientes": TableConfig("clientes", "criado_em", True),
            "categorias": TableConfig("categorias", "criado_em", True),
            "fornecedores": TableConfig("fornecedores", "criado_em", True),
            "enderecos": TableConfig("enderecos", "criado_em", True),
        }

        self.table_schemas = self._load_table_schemas()

    def extract_single_table_date(
        self, table_name: str, table_config: TableConfig, extract_date: date
    ) -> ExtractionResult:
        """Extrai uma tabela para uma data específica"""
        extract_date_str = extract_date.strftime("%Y-%m-%d")

        try:
            df = self.db_extractor.extract_data(table_config, extract_date)

            if df.empty:
                logging.info(f"{table_name} - {extract_date_str}: Sem registros")
                return ExtractionResult(
                    table_name=table_name,
                    extract_date=extract_date_str,
                    records=0,
                    status="empty",
                )

            s3_key = S3PathBuilder.build_table_path(table_name, extract_date)

            metadata = {
                "execution_date": extract_date_str,
                "table_name": table_name,
                "record_count": str(len(df)),
                "extraction_type": (
                    "incremental" if table_config.incremental else "full"
                ),
                "format": "parquet",
                "compression": "snappy",
            }

            file_size_mb = self.s3_manager.upload_parquet(df, s3_key, metadata, self.table_schemas)

            logging.info(
                f"{table_name} - {extract_date_str}: {len(df)} registros → {s3_key}"
            )

            return ExtractionResult(
                table_name=table_name,
                extract_date=extract_date_str,
                records=len(df),
                status="success",
                s3_key=s3_key,
                file_size_mb=file_size_mb,
            )

        except Exception as e:
            logging.error(f"{table_name} - {extract_date_str}: {str(e)}")
            return ExtractionResult(
                table_name=table_name,
                extract_date=extract_date_str,
                records=0,
                status="error",
                error=str(e),
            )

    def run_extraction(self, execution_datetime: datetime) -> Dict:
        """Executa extração para a data de execução"""
        logging.info("Iniciando extração de dados do e-commerce")

        extraction_summary = {}
        total_extracted_records = 0
        extract_date = execution_datetime.date()

        try:
            for table_name, table_config in self.table_configs.items():
                logging.info(f"Processando tabela: {table_name}")

                result = self.extract_single_table_date(
                    table_name, table_config, extract_date
                )

                if result.status == "success":
                    table_records = result.records
                    processed_dates = [result.extract_date]
                    status = "success"
                elif result.status in ["empty", "no_data"]:
                    table_records = 0
                    processed_dates = [result.extract_date]
                    status = "no_data"
                else:
                    table_records = 0
                    processed_dates = []
                    status = "error"

                extraction_summary[table_name] = {
                    "records": table_records,
                    "status": status,
                    "dates_processed": processed_dates,
                    "format": "parquet",
                }

                total_extracted_records += table_records
                logging.info(f"{table_name}: {table_records} registros extraídos")

            report = self._generate_report(
                execution_datetime, extraction_summary, total_extracted_records
            )
            report_key = S3PathBuilder.build_report_path(execution_datetime)
            self.s3_manager.upload_json(report, report_key)

            logging.info(f"Relatório salvo: {report_key}")
            logging.info(
                f"Execução finalizada: {total_extracted_records} registros totais extraídos"
            )

            return report

        finally:
            self.db_extractor.dispose()

    def _load_table_schemas(self) -> Dict:
        """Carrega schemas de todas as tabelas do banco de dados"""
        try:
            table_names = list(self.table_configs.keys())
            schemas = self.db_extractor.get_all_table_schemas(table_names)
            
            logging.info(f"Schemas carregados para {len(schemas)} tabelas:")
            for table_name, schema in schemas.items():
                logging.info(f"  {table_name}: {len(schema)} colunas")
                # Log dos tipos para debug
                for col, tipo in schema.items():
                    if tipo in ['timestamp', 'date']:
                        logging.info(f"    {col}: {tipo}")
            
            return schemas
        except Exception as e:
            logging.error(f"Erro ao carregar schemas: {str(e)}")
            return {}

    def _generate_report(
        self, execution_datetime: datetime, extraction_summary: Dict, total_records: int
    ) -> Dict:
        """Gera relatório de execução"""
        return {
            "dag_id": "ecommerce_mysql_to_s3_bronze",
            "execution_date": execution_datetime.strftime("%Y-%m-%d"),
            "execution_timestamp": datetime.now().isoformat(),
            "status": "completed",
            "format": "parquet",
            "total_tables": len(self.table_configs),
            "successful_extractions": len(
                [t for t in extraction_summary.values() if t["status"] == "success"]
            ),
            "total_records": total_records,
            "bucket": self.s3_bucket,
            "tables": extraction_summary,
        }

# Configurações
S3_BUCKET = "nuccig-data-analysis-ecommerce"
MYSQL_CONNECTION = "mysql+pymysql://admin:minhasenha123@terraform-20250724042256770800000002.csdsw6cyc9qd.us-east-1.rds.amazonaws.com:3306/ecommerce"
AWS_CONN_ID = "aws_default"

def extract_mysql_to_s3(data_interval_start: datetime, **context):
    """Wrapper para usar no PythonOperator"""
    extractor = EcommerceDataExtractor(
        mysql_connection=MYSQL_CONNECTION,
        s3_bucket=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
    )

    return extractor.run_extraction(data_interval_start)

default_args = {
    "owner": "gustavo-nucci",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "ecommerce_mysql_to_s3_bronze",
    default_args=default_args,
    description="Ingestão E-commerce MySQL para S3 Bronze",
    schedule="0 4 */1 * *",
    catchup=False,
    tags=["bronze", "ecommerce", "mysql"],
)

extract_task = PythonOperator(
    task_id="extract_mysql_to_s3_bronze",
    python_callable=extract_mysql_to_s3,
    dag=dag,
)