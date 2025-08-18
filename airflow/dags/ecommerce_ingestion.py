import json
import logging
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Dict, List, Optional

import boto3
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import create_engine

from airflow import DAG

warnings.filterwarnings("ignore")


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

    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.region = region
        self.client = boto3.client("s3", region_name=region)

    def _extract_date_from_path(self, s3_key: str) -> Optional[date]:
        """Extrai data do caminho S3"""
        try:
            parts = s3_key.split("/")
            if (
                len(parts) >= 5
                and "year=" in parts[2]
                and "month=" in parts[3]
                and "day=" in parts[4]
            ):

                year = parts[2].split("=")[1]
                month = parts[3].split("=")[1]
                day = parts[4].split("=")[1]
                date_str = f"{year}-{month}-{day}"
                return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, IndexError):
            pass
        return None

    def get_last_extracted_date(self, table_name: str) -> Optional[date]:
        """Busca a última data extraída para uma tabela no S3"""
        try:
            prefix = f"bronze/{table_name}/"
            paginator = self.client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket, Prefix=prefix, Delimiter="/"
            )

            dates = []
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        date = self._extract_date_from_path(obj["Key"])
                        if date:
                            dates.append(date)

            return max(dates) if dates else None

        except Exception as e:
            logging.warning(f"Erro ao buscar última data para {table_name}: {str(e)}")
            return None

    def upload_parquet(self, df: pd.DataFrame, s3_key: str, metadata: Dict) -> float:
        """Upload DataFrame como Parquet para S3"""
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        parquet_buffer.seek(0)

        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
            Metadata=metadata,
        )

        return round(len(parquet_buffer.getvalue()) / (1024 * 1024), 2)

    def upload_json(self, data: Dict, s3_key: str):
        """Upload JSON para S3"""
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )


class DatabaseExtractor:
    """Gerencia extração de dados do banco"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._engine = None

    @property
    def engine(self):
        """Lazy loading da engine"""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
            logging.info(f"Engine criada: {type(self._engine).__name__}")
        return self._engine

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
            df["_extraction_date"] = extract_date_str
            df["_extraction_timestamp"] = datetime.now().isoformat()
            df["_source_table"] = table_config.table

        return df

    def dispose(self):
        """Fecha conexões"""
        if self._engine:
            self._engine.dispose()


class DateCalculator:
    """Calcula datas para extração"""

    def __init__(self, max_backfill_days: int = 30):
        self.max_backfill_days = max_backfill_days

    def get_dates_to_extract(
        self,
        s3_manager: S3Manager,
        table_name: str,
        execution_date: datetime,
        table_config: TableConfig,
    ) -> List[date]:
        """Determina quais datas precisam ser extraídas"""

        if not table_config.incremental:
            return [execution_date.date()]

        last_extracted_date = s3_manager.get_last_extracted_date(table_name)

        if last_extracted_date is None:
            logging.info(
                f"Primeira extração para {table_name}. Extraindo: {execution_date.date()}"
            )
            return [execution_date.date()]

        current_date = execution_date.date()
        start_date = last_extracted_date + timedelta(days=1)

        if start_date > current_date:
            logging.info(f"{table_name} já está atualizado até {last_extracted_date}")
            return []

        dates_to_extract = []
        date_iter = start_date
        days_count = 0

        while date_iter <= current_date and days_count < self.max_backfill_days:
            dates_to_extract.append(date_iter)
            date_iter += timedelta(days=1)
            days_count += 1

        if days_count >= self.max_backfill_days:
            logging.warning(
                f"{table_name}: Limitado a {self.max_backfill_days} dias de backfill. "
                f"Último extraído: {last_extracted_date}"
            )

        logging.info(
            f"{table_name}: Extraindo {len(dates_to_extract)} datas - "
            f"de {start_date} até {current_date}"
        )
        return dates_to_extract


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
        aws_region: str = "us-east-1",
        max_backfill_days: int = 30,
    ):
        self.db_extractor = DatabaseExtractor(mysql_connection)
        self.s3_manager = S3Manager(s3_bucket, aws_region)
        self.date_calculator = DateCalculator(max_backfill_days)
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

            file_size_mb = self.s3_manager.upload_parquet(df, s3_key, metadata)

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
        """Executa extração completa"""
        logging.info("Iniciando extração de dados do e-commerce")

        extraction_summary = {}
        total_extracted_records = 0

        try:
            for table_name, table_config in self.table_configs.items():
                logging.info(f"Processando tabela: {table_name}")

                dates_to_extract = self.date_calculator.get_dates_to_extract(
                    self.s3_manager, table_name, execution_datetime, table_config
                )

                if not dates_to_extract:
                    extraction_summary[table_name] = {
                        "records": 0,
                        "status": "up_to_date",
                        "dates_processed": [],
                    }
                    continue

                table_records = 0
                processed_dates = []

                for extract_date in dates_to_extract:
                    result = self.extract_single_table_date(
                        table_name, table_config, extract_date
                    )

                    if result.status == "success":
                        table_records += result.records
                        processed_dates.append(result.extract_date)
                    elif result.status in ["empty", "no_data"]:
                        processed_dates.append(result.extract_date)

                extraction_summary[table_name] = {
                    "records": table_records,
                    "status": "success" if table_records > 0 else "no_data",
                    "dates_processed": processed_dates,
                    "backfill_days": len(dates_to_extract),
                    "format": "parquet",
                }

                total_extracted_records += table_records
                logging.info(
                    f"{table_name}: {table_records} registros totais em {len(processed_dates)} datas"
                )

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

    def full_extract(self, execution_datetime: datetime) -> Dict:
        """
        Executa uma carga completa inicial de todas as tabelas,
        reutilizando a infraestrutura existente
        """
        logging.info("Iniciando carga completa inicial de dados do e-commerce")

        extraction_summary = {}
        total_extracted_records = 0

        try:
            for table_name, table_config in self.table_configs.items():
                logging.info(f"Processando carga completa da tabela: {table_name}")

                if table_config.date_column:
                    dates_to_extract = self._get_all_dates_in_table(table_config)
                    if not dates_to_extract:
                        logging.info(
                            f"{table_name}: Nenhuma data encontrada, extraindo como snapshot"
                        )
                        dates_to_extract = [execution_datetime.date()]
                else:
                    dates_to_extract = [execution_datetime.date()]

                table_records = 0
                processed_dates = []

                for extract_date in dates_to_extract:

                    temp_config = TableConfig(
                        table=table_config.table,
                        date_column=(
                            table_config.date_column
                            if table_config.date_column
                            else None
                        ),
                        incremental=bool(table_config.date_column),
                    )

                    result = self.extract_single_table_date(
                        table_name, temp_config, extract_date
                    )

                    if result.status == "success":
                        table_records += result.records
                        processed_dates.append(result.extract_date)
                    elif result.status in ["empty", "no_data"]:
                        processed_dates.append(result.extract_date)

                extraction_summary[table_name] = {
                    "records": table_records,
                    "status": "success" if table_records > 0 else "no_data",
                    "dates_processed": processed_dates,
                    "total_partitions": len(dates_to_extract),
                    "extraction_type": "full_load_partitioned",
                    "format": "parquet",
                }

                total_extracted_records += table_records
                logging.info(
                    f"{table_name}: {table_records} registros totais em {len(processed_dates)} partições"
                )

            report = self._generate_full_load_report(
                execution_datetime, extraction_summary, total_extracted_records
            )

            report_key = f"bronze/_reports/full_load_report_{execution_datetime.strftime('%Y-%m-%d_%H-%M-%S')}.json"
            self.s3_manager.upload_json(report, report_key)

            logging.info(f"Relatório de carga completa salvo: {report_key}")
            logging.info(
                f"Carga completa finalizada: {total_extracted_records} registros totais extraídos"
            )

            return report

        finally:
            self.db_extractor.dispose()

    def _get_all_dates_in_table(self, table_config: TableConfig) -> List[date]:
        """
        Busca todas as datas únicas presentes na tabela para carga completa
        """
        try:
            query = f"""
            SELECT DISTINCT DATE({table_config.date_column}) as date_value
            FROM {table_config.table}
            WHERE {table_config.date_column} IS NOT NULL
            ORDER BY date_value
            """

            logging.info(f"{table_config.table}: Buscando todas as datas disponíveis")

            with self.db_extractor.engine.connect() as conn:
                df = pd.read_sql(sql=query, con=conn.connection)

            if df.empty:
                logging.warning(
                    f"{table_config.table}: Nenhuma data encontrada na coluna {table_config.date_column}"
                )
                return []

            dates = [
                pd.to_datetime(row["date_value"]).date() for _, row in df.iterrows()
            ]

            logging.info(
                f"{table_config.table}: {len(dates)} datas únicas encontradas para carga completa"
            )
            return dates

        except Exception as e:
            logging.error(f"{table_config.table}: Erro ao buscar datas - {str(e)}")
            return []

    def _generate_full_load_report(
        self, execution_datetime: datetime, extraction_summary: Dict, total_records: int
    ) -> Dict:
        """
        Gera relatório específico para carga completa,
        """

        base_report = self._generate_report(
            execution_datetime, extraction_summary, total_records
        )

        successful_tables = [
            t for t in extraction_summary.values() if t["status"] == "success"
        ]
        total_partitions = sum(
            t.get("total_partitions", 1) for t in extraction_summary.values()
        )

        base_report.update(
            {
                "dag_id": "ecommerce_mysql_to_s3_bronze_full_load",
                "extraction_type": "full_load_partitioned",
                "total_partitions": total_partitions,
                "summary_stats": {
                    "largest_table_records": (
                        max(successful_tables, key=lambda x: x["records"])["records"]
                        if successful_tables
                        else 0
                    ),
                    "total_partitions_created": total_partitions,
                    "avg_records_per_partition": (
                        round(total_records / total_partitions, 2)
                        if total_partitions > 0
                        else 0
                    ),
                    "tables_with_multiple_partitions": len(
                        [
                            t
                            for t in successful_tables
                            if t.get("total_partitions", 1) > 1
                        ]
                    ),
                },
            }
        )

        return base_report

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
            "backfill_enabled": True,
            "max_backfill_days": self.date_calculator.max_backfill_days,
            "total_tables": len(self.table_configs),
            "successful_extractions": len(
                [t for t in extraction_summary.values() if t["status"] == "success"]
            ),
            "total_records": total_records,
            "table_summary": extraction_summary,
            "s3_bucket": self.s3_bucket,
        }


S3_BUCKET = "nuccig-data-analysis-ecommerce"
MYSQL_CONNECTION = "mysql+pymysql://admin:minhasenha123@terraform-20250724042256770800000002.csdsw6cyc9qd.us-east-1.rds.amazonaws.com:3306/ecommerce"
AWS_REGION = "us-east-1"
MAX_BACKFILL_DAYS = 30


def extract_mysql_to_s3(**context):
    """Wrapper para usar no PythonOperator"""
    extractor = EcommerceDataExtractor(
        mysql_connection=MYSQL_CONNECTION,
        s3_bucket=S3_BUCKET,
        aws_region=AWS_REGION,
        max_backfill_days=MAX_BACKFILL_DAYS,
    )

    return extractor.run_extraction(context["execution_date"])


def full_extract_mysql_to_s3(**context):
    """Wrapper para carga completa inicial no PythonOperator"""
    extractor = EcommerceDataExtractor(
        mysql_connection=MYSQL_CONNECTION,
        s3_bucket=S3_BUCKET,
        aws_region=AWS_REGION,
        max_backfill_days=MAX_BACKFILL_DAYS,
    )

    return extractor.full_extract(context["execution_date"])


default_args = {
    "owner": "data-team",
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

# DAG para carga completa inicial (execução manual)
dag_full_load = DAG(
    "ecommerce_mysql_to_s3_bronze_full_load",
    default_args=default_args,
    description="Carga Completa Inicial E-commerce MySQL para S3 Bronze",
    schedule=None,  # Apenas execução manual
    catchup=False,
    tags=["bronze", "ecommerce", "mysql", "full-load", "initial"],
)

extract_task = PythonOperator(
    task_id="extract_mysql_to_s3_bronze",
    python_callable=extract_mysql_to_s3,
    dag=dag,
)

full_extract_task = PythonOperator(
    task_id="full_extract_mysql_to_s3_bronze",
    python_callable=full_extract_mysql_to_s3,
    dag=dag_full_load,
)
