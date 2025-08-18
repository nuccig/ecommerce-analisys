[ RDS / API ]
       │
       ▼
[ Airflow (EC2, ECS ou MWAA) ]
       │
       ▼
─────  BRONZE  ───────────────────────────────
- Dados brutos no S3
- Formato: Parquet
- Particionado por data
- Nomeação ex.: s3://datalake/bronze/tabela/ano=2025/mes=08/dia=12
       │
       ▼
[ Athena / Glue Job ]
       │
       ▼
─────  SILVER  ───────────────────────────────
- Dados limpos e padronizados
- Deduplicação, conversão de tipos, normalização
- Salvos no S3 (Parquet, particionado)
- Catálogo no AWS Glue
       │
       ▼
[ Athena / Glue Job ]
       │
       ▼
─────  GOLD  ─────────────────────────────────
- Dados agregados e prontos para BI
- Métricas, KPIs, joins resolvidos
- Salvos no S3 (Parquet otimizado)
- Particionado + compactado para leitura rápida
       │
       ├──► [ Athena + QuickSight ] → Dashboards
       ├──► [ Athena + Power BI / Metabase ] → Relatórios
       └──► [ Lambda + Athena ] → APIs
