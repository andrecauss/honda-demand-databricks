# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 99 — Carga histórica de ordens de venda
# MAGIC
# MAGIC - **Propósito:** Inicializar `raw_sales_order` a partir do histórico Parquet do SAP.
# MAGIC - **Entrada:** Arquivos Parquet de ordens de venda históricas
# MAGIC - **Saída:** `parts_hdbk_sandbox.dt_sales_orders.raw_sales_order`
# MAGIC - **Chave:** numero_ov + item · **Carga:** Bootstrap completo, execução excepcional

# COMMAND ----------

# DBTITLE 1,Initialize Sales Orders Table and Metadata with Spark S ...
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import re
import unicodedata
import uuid

CATALOG = "parts_hdbk_sandbox"
SCHEMA = "dt_sales_orders"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.raw_sales_order"

HISTORICAL_PATH = (
    "/Volumes/parts_hdbk_sandbox/dt_sales_orders/sap_sales_order/historical"
)

BUSINESS_KEY = ["numero_ov", "item"]
LOAD_MODE = "INITIAL_LOAD"

RAW_BUSINESS_COLUMNS = [
    "numero_ov", "data", "tipo_ov", "motivo_ov", "bloqueio_rem",
    "motivo_recusa", "org_vendas", "canal_dist", "setor_ativ", "centro",
    "emissor_da_ordem", "numero_pedido", "autor", "item", "material",
    "categoria_do_item", "item_superior", "quantidade", "um",
]
SOURCE_METADATA_COLUMNS = [
    "_source_file_name", "_source_file_path", "_source_file_modification_time",
]
AUDIT_COLUMNS = [
    "_ingested_at", "_last_updated_at", "_ingested_by", "_load_type", "_load_id",
]
RAW_COLUMNS = RAW_BUSINESS_COLUMNS + SOURCE_METADATA_COLUMNS + AUDIT_COLUMNS
LOAD_ID = str(uuid.uuid4())

# Deixe desabilitado na carga produtiva: cada validação abaixo lê a tabela novamente.
VALIDATE_AFTER_WRITE = False

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
CURRENT_USER = spark.sql("SELECT current_user() AS user").first()["user"]


# COMMAND ----------

# DBTITLE 1,Normalize Columns and Standardize Sales Order Data with ...
def normalize_column_name(column_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", column_name)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]", "_", normalized.lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def normalize_dataframe_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[normalize_column_name(column) for column in df.columns])


def read_historical_sales_order() -> DataFrame:
    # recursiveFileLookup é necessário porque o histórico pode conter subdiretórios.
    return (
        spark.read
        .format("parquet")
        .option("recursiveFileLookup", "true")
        .load(HISTORICAL_PATH)
    )


def transform_sales_order(df: DataFrame) -> DataFrame:
    df = normalize_dataframe_columns(df)

    required_columns = set(RAW_BUSINESS_COLUMNS)
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {sorted(missing_columns)}")

    standardized_df = df.withColumns({
        "quantidade": F.coalesce(
            F.col("quantidade").cast("decimal(18,3)"),
            F.lit(0).cast("decimal(18,3)"),
        ),
        "_source_file_name": F.col("_metadata.file_name"),
        "_source_file_path": F.col("_metadata.file_path"),
        "_source_file_modification_time": F.col("_metadata.file_modification_time"),
        "_ingested_at": F.from_utc_timestamp(
            F.current_timestamp(), "America/Sao_Paulo"
        ),
        "_last_updated_at": F.from_utc_timestamp(
            F.current_timestamp(), "America/Sao_Paulo"
        ),
        "_ingested_by": F.lit(CURRENT_USER),
        "_load_type": F.lit(LOAD_MODE),
        "_load_id": F.lit(LOAD_ID),
    })

    return standardized_df.select(*RAW_COLUMNS)


# COMMAND ----------

# DBTITLE 1,Write and Validate Historical Sales Orders Data in Delt ...
def write_historical_sales_order(df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(RAW_TABLE)
    )


def validate_historical_load() -> None:
    # Execute somente quando for necessário auditar a carga.
    duplicate_count = (
        spark.table(RAW_TABLE)
        .groupBy(*BUSINESS_KEY)
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    total_rows = spark.table(RAW_TABLE).count()
    print(f"Linhas carregadas: {total_rows}")
    print(f"Chaves duplicadas: {duplicate_count}")


# COMMAND ----------

# DBTITLE 1,Load Transform and Commit Historical Sales Orders Data
source_df = read_historical_sales_order()
historical_df = transform_sales_order(source_df)

# Esta é a única ação sobre os arquivos de origem.
write_historical_sales_order(historical_df)

last_commit = (
    DeltaTable.forName(spark, RAW_TABLE)
    .history(1)
    .select("version", "timestamp", "operation", "operationMetrics")
    .first()
)
print(f"Carga histórica concluída. Versão Delta: {last_commit['version']}")
print(f"Métricas da escrita: {last_commit['operationMetrics']}")

if VALIDATE_AFTER_WRITE:
    validate_historical_load()


# COMMAND ----------

# DBTITLE 1,Add Description Metadata to Raw Sales Order Table
# ============================================================
# 1. DICIONÁRIO COM COMENTÁRIOS DOS CAMPOS
# ============================================================
COLUMN_COMMENTS = {
    "numero_ov": "Número da Ordem de Venda, ou pedido, que um cliente (dealer ou concessionária) faz",
    "data": "Data do Pedido / Ordem de Venda",
    "tipo_ov": "Tipo do Pedido ou Ordem de Venda",
    "motivo_ov": "Justificativa ou razão do pedido, fornecendo contexto sobre as necessidades do cliente.",
    "bloqueio_rem": "Indica se há algum bloqueio relacionado à remessa do pedido, afetando o envio.",
    "motivo_recusa": "Explicação fornecida quando um pedido é recusado, ajudando a entender as causas de não conformidade.",
    "org_vendas": "Organização de Vendas ou Segmento de Negócio: 0200 representa 2W e 0500 representa 4W",
    "canal_dist": "Categoria de canal de distribuição: 01 representa Doméstico (Nacional); 02 representa Exportação (Overseas)",
    "setor_ativ": "Segmento de atividade relacionado, como acessórios, produto de força, DSH",
    "centro": "Depósito que atende o pedido: 0203 (Sumaré 2W), 0503 (Sumaré 4W), 0209 (Jaboatão 2W), 0505 (Jaboatão 5W), 0232 (Manaus 2W)",
    "emissor_da_ordem": "Identificador do código do cliente / concessionária que fez o pedido (emitiu o pedido)",
    "numero_pedido": "pedido interno da concessionária - dado não relevant",
    "autor": "Nome ou identificação do responsável que inseriu o pedido, útil para rastreabilidade.",
    "item": "Identificador único do item dentro do pedido, que associa o pedido ao seu conteúdo específico.",
    "material": "Identificador único do material, SKU ou partnumber solicitado pelo cliente",
    "categoria_do_item": "Classificação do item dentro do pedido, ajudando a agrupar produtos similares.",
    "item_superior": "Identificador do item superior na hierarquia de produtos, se aplicável.",
    "quantidade": "Quantidade de peças solicitadas pelo cliente",
    "um": "Unidade de Medida utilizada para quantificar os itens solicitados no pedido.",
    "_source_file_name": "Nome do arquivo fonte de onde os dados foram extraídos, importante para rastreamento.",
    "_source_file_path": "Caminho do arquivo fonte no sistema, necessário para auditoria e verificação.",
    "_source_file_modification_time": "Timestamp indicando quando o arquivo fonte foi modificado, útil para controle de versão.",
    "_ingested_at": "Data e hora em que os dados foram incorporados ao sistema, importante para gestão de dados.",
    "_last_updated_at": "Data e hora da última atualização dos dados, permitindo monitorar mudanças ao longo do tempo.",
    "_ingested_by": "Identifica quem fez a ingestão dos dados, essencial para governança e rastreamento.",
    "_load_type": "Tipo de carregamento de dados, que pode indicar a natureza da operação de ingestão.",
    "_load_id": "Identificador único associado ao processo de carregamento, usado para controle e auditoria.",
}

# ============================================================
# 2. COMENTÁRIO DETALHADO DA TABELA (com contexto de negócio)
# ============================================================
TABLE_COMMENT = """
Camada Raw da Sales Order SAP. Dados normalizados e auditados sem regras de negócio.

Chave de Negócio: numero_ov + item
Atualização: Carga histórica única (bootstrap)

Relacionamentos:
  - emissor_da_ordem -> tabela de clientes/concessionárias
  - material -> tabela de produtos/SKUs
  - centro -> tabela de depósitos/centros de distribuição

Casos de uso comuns:
  - Análise de volume de vendas por período (coluna data)
  - Segmentação por organização de vendas (org_vendas: 0200=2W, 0500=4W)
  - Análise de canal de distribuição (canal_dist: 01=Doméstico, 02=Exportação)
  - Monitoramento de pedidos bloqueados (bloqueio_rem) ou recusados (motivo_recusa)
  - Análise de demanda por centro de distribuição e setor de atividade
"""

spark.sql(f"""
    COMMENT ON TABLE {RAW_TABLE} IS '{TABLE_COMMENT.replace("'", "''")}'
""")

# ============================================================
# 3. COMENTÁRIOS NAS COLUNAS
# ============================================================
for column_name, comment in COLUMN_COMMENTS.items():
    escaped_comment = comment.replace("'", "''")
    spark.sql(f"""
        COMMENT ON COLUMN {RAW_TABLE}.{column_name} IS '{escaped_comment}'
    """)

print(f"✓ Comentários adicionados para {len(COLUMN_COMMENTS)} colunas")

# ============================================================
# 4. TAGS DO UNITY CATALOG (classificação de dados)
# ============================================================
# Tags da tabela
spark.sql(f"""
    ALTER TABLE {RAW_TABLE} SET TAGS (
        'domain' = 'sales',
        'layer' = 'raw',
        'source' = 'sap',
        'data_classification' = 'internal'
    )
""")

# Tags em colunas sensíveis/importantes
spark.sql(f"ALTER TABLE {RAW_TABLE} ALTER COLUMN emissor_da_ordem SET TAGS ('pii' = 'customer_id', 'business_key' = 'true')")
spark.sql(f"ALTER TABLE {RAW_TABLE} ALTER COLUMN numero_ov SET TAGS ('business_key' = 'true')")
spark.sql(f"ALTER TABLE {RAW_TABLE} ALTER COLUMN item SET TAGS ('business_key' = 'true')")
spark.sql(f"ALTER TABLE {RAW_TABLE} ALTER COLUMN material SET TAGS ('business_key' = 'true', 'joins_to' = 'dim_products')")
spark.sql(f"ALTER TABLE {RAW_TABLE} ALTER COLUMN data SET TAGS ('temporal_key' = 'true')")

print("✓ Tags aplicadas na tabela e colunas")

# ============================================================
# 5. PROPRIEDADES CUSTOMIZADAS (TBLPROPERTIES)
# ============================================================
spark.sql(f"""
    ALTER TABLE {RAW_TABLE} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Demand Planning',
        'data_domain' = 'Sales Orders',
        'source_system' = 'SAP',
        'refresh_frequency' = 'historical_sales_order',
        'primary_key' = 'numero_ov,item',
        'related_tables' = 'dim_customers,dim_products,dim_warehouses',
        'data_retention_days' = 'indefinite',
        'quality_checks' = 'enabled'
    )
""")

print("✓ Propriedades customizadas configuradas")

print("\n" + "="*60)
print(f"✓ Metadados completos aplicados à tabela {RAW_TABLE}")
print("  - Comentários detalhados (tabela + 27 colunas)")
print("  - Tags de classificação (Unity Catalog)")
print("  - Propriedades customizadas (primary_key definida em TBLPROPERTIES)")
print("="*60)


# COMMAND ----------

display(
    spark.table(RAW_TABLE)
    .groupBy("org_vendas")
    .agg(
        F.date_format(F.min("data"), "dd/MM/yyyy").alias("Data_Inicial_OVs"),
        F.date_format(F.max("data"), "dd/MM/yyyy").alias("Data_Final_OVs"),
        F.date_format(F.min("_ingested_at"), "dd/MM/yyyy").alias("Upload"),
        F.date_format(F.min("_last_updated_at"), "dd/MM/yyyy").alias("Ultima_Atualizacao"),
    )
)

# COMMAND ----------

org_vendas_param = "0200"  # Defina o valor desejado

display(
    spark.table(RAW_TABLE)
    .where(F.col("org_vendas") == org_vendas_param)
    .groupBy("org_vendas", "_source_file_name")
    .agg(
        F.date_format(F.min("data"), "yyyy/MM/dd").alias("Data_Minima"),
        F.date_format(F.max("data"), "yyyy/MM/dd").alias("Data_Maxima"),
    )
    .orderBy(F.col("Data_Maxima").desc())
)
