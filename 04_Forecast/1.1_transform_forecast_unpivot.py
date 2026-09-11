# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Visão Geral
# MAGIC %md
# MAGIC # 1.1 — Transformação Forecast Enriched (Unpivot)
# MAGIC
# MAGIC - **Propósito:** Dinamizar as 36 colunas de horizonte (`n+0`…`n+35`) em linhas, calculando o mês-alvo do forecast.
# MAGIC - **Entrada:** `parts_hdbk_sandbox.pr_forecast.raw_forecast_enriched`
# MAGIC - **Saída:** `parts_hdbk_sandbox.pr_forecast.refined_forecast_enriched`
# MAGIC - **Chave:** `forecast_cycle` + `segment` + `material` + `lag` · **Carga:** Incremental por `_load_id`
# MAGIC - **Filtros:**
# MAGIC   - Remoção de linhas Enrich com `forecast_qty = 0`
# MAGIC   - Horizonte limitado aos **primeiros 12 meses** (`lag` 0–11)
# MAGIC - **Lookup:** `material` → `main_material` via `pr_cadastrao.material_cadeia` (chave: `segment`=`empresa`, `material`=`material` → `item_principal_cadeia`)
# MAGIC - **Auditoria:** `_ingested_at`, `_ingested_by`, `_load_type`, `_load_id`

# COMMAND ----------

# DBTITLE 1,Imports
import uuid
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Parâmetros
# ---------------------------------------------------------------------------
# Parâmetros
# ---------------------------------------------------------------------------
CATALOG     = "parts_hdbk_sandbox"
SCHEMA      = "pr_forecast"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.raw_forecast_enriched"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.refined_forecast_enriched"
LOOKUP_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_cadeia"

# ---------------------------------------------------------------------------
# Colunas
# ---------------------------------------------------------------------------
HORIZON_RANGE   = range(36)  # n+0 .. n+35
HORIZON_COLUMNS = [f"n+{i}" for i in HORIZON_RANGE]

ID_COLUMNS = [
    "forecast_cycle",
    "department",
    "forecast_type",
    "segment",
    "market",
    "main_material",
    "forecast_classification",
]

# ---------------------------------------------------------------------------
# Auditoria
# ---------------------------------------------------------------------------
LOAD_MODE    = "incremental"
LOAD_ID      = str(uuid.uuid4())
CURRENT_USER = spark.sql("SELECT current_user()").first()[0]

print(f"Origem  : {SOURCE_TABLE}")
print(f"Destino : {TARGET_TABLE}")
print(f"Load ID : {LOAD_ID}")
print(f"Usuário : {CURRENT_USER}")

# COMMAND ----------

# DBTITLE 1,Leitura incremental
# ---------------------------------------------------------------------------
# Leitura incremental: apenas _load_id que ainda não foram processados
# ---------------------------------------------------------------------------
if spark.catalog.tableExists(TARGET_TABLE):
    processed_ids = (
        spark.table(TARGET_TABLE)
        .select("_raw_load_id")
        .distinct()
    )
    df_raw = (
        spark.table(SOURCE_TABLE)
        .join(processed_ids, F.col("_load_id") == F.col("_raw_load_id"), "left_anti")
    )
else:
    df_raw = spark.table(SOURCE_TABLE)

new_rows = df_raw.count()
print(f"Registros novos na raw: {new_rows:,}")

if new_rows == 0:
    print("Nenhum registro novo. Encerrando.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# DBTITLE 1,Unpivot: horizonte em linhas
# ---------------------------------------------------------------------------
# UNPIVOT: transforma n+0..n+35 em linhas (horizon + forecast_qty)
# ---------------------------------------------------------------------------
# Usa stack() para gerar 36 linhas por registro original:
#   lag            (int)    — offset em meses (0..35)
#   forecast_qty   (double) — quantidade prevista
#   forecast_month (date)   — mês-alvo = forecast_cycle + lag meses
# ---------------------------------------------------------------------------

stack_expr = ", ".join(
    f"{i}, `n+{i}`" for i in HORIZON_RANGE
)

df_unpivot = (
    df_raw
    .select(
        *ID_COLUMNS,
        F.expr(f"stack({len(HORIZON_COLUMNS)}, {stack_expr}) AS (lag, forecast_qty)"),
        # Preserva o _load_id original da raw para controle incremental
        F.col("_load_id").alias("_raw_load_id"),
    )
    .withColumn(
        "forecast_month",
        F.expr("add_months(forecast_cycle, lag)")
    )
)

rows_before_filter = df_unpivot.count()

# ---------------------------------------------------------------------------
# Filtro de negócio: remover linhas Enrich com quantidade zero
# ---------------------------------------------------------------------------
df_unpivot = df_unpivot.filter(
    ~((F.col("forecast_type") == "Enrich") & (F.col("forecast_qty") == 0))
)

# ---------------------------------------------------------------------------
# Filtro de horizonte: manter apenas os primeiros 12 meses (lag 0..11)
# ---------------------------------------------------------------------------
df_unpivot = df_unpivot.filter(
    (F.col("lag") >= 0) & (F.col("lag") <= 11)
)

rows_after_filter = df_unpivot.count()
print(f"Registros após unpivot: {rows_before_filter:,}")
print(f"Registros após filtro (Enrich + qty=0): {rows_after_filter:,}")
print(f"Removidos: {rows_before_filter - rows_after_filter:,}")

# COMMAND ----------

# DBTITLE 1,Auditoria e seleção final
# ---------------------------------------------------------------------------
# Colunas de auditoria e ordem final
# ---------------------------------------------------------------------------

# Renomear main_material → material (partnumber original)
df_unpivot = df_unpivot.withColumnRenamed("main_material", "material")

# Lookup: trazer item_principal_cadeia como novo main_material
df_cadeia = (
    spark.table(LOOKUP_TABLE)
    .select(
        F.col("empresa"),
        F.col("material").alias("_lookup_material"),
        F.col("item_principal_cadeia"),
    )
)

df_unpivot = (
    df_unpivot
    .join(
        df_cadeia,
        (F.col("segment") == F.col("empresa"))
        & (F.col("material") == F.col("_lookup_material")),
        "left",
    )
    .withColumn(
        "main_material",
        F.coalesce(F.col("item_principal_cadeia"), F.col("material")),
    )
    .drop("empresa", "_lookup_material", "item_principal_cadeia")
)

df_final = (
    df_unpivot
    .withColumns({
        # Limpar prefixos "Domestic - " e "Export - " do department
        "department": F.regexp_replace(F.col("department"), r"^(Domestic|Export)\s*-\s*", ""),
        # Extrair tag da forecast_classification (tudo após o primeiro " - ")
        "forecast_classification_tag": F.when(
            F.col("forecast_classification").contains(" - "),
            F.regexp_extract(F.col("forecast_classification"), r"^.+? - (.+)$", 1)
        ),
        "_ingested_at": F.from_utc_timestamp(
            F.current_timestamp(), "America/Sao_Paulo"
        ),
        "_ingested_by": F.lit(CURRENT_USER),
        "_load_type": F.lit(LOAD_MODE),
        "_load_id": F.lit(LOAD_ID),
    })
    .select(
        # Chaves de negócio
        "forecast_cycle",
        "department",
        "forecast_type",
        "segment",
        "market",
        "material",
        "main_material",
        "forecast_classification",
        "forecast_classification_tag",
        # Horizonte dinamizado
        "lag",
        "forecast_month",
        "forecast_qty",
        # Auditoria
        "_raw_load_id",
        "_ingested_at",
        "_ingested_by",
        "_load_type",
        "_load_id",
    )
)

print("Schema final:")
df_final.printSchema()

# COMMAND ----------

# DBTITLE 1,Amostra
display(df_final.orderBy("forecast_cycle", "main_material", "lag").limit(10))

# COMMAND ----------

# DBTITLE 1,Persistência (append)
# Persiste em modo append na tabela Delta
df_final.write.mode("append").saveAsTable(TARGET_TABLE)
print(f"Dados inseridos com sucesso em {TARGET_TABLE} (modo append)")

# COMMAND ----------

# DBTITLE 1,Validação final
# Validação resumida da tabela após inserção
df_check = spark.table(TARGET_TABLE)
print(f"Total de linhas na tabela: {df_check.count():,}")

print(f"\nDistribuição por horizonte (amostra):")
display(
    df_check.groupBy("lag")
    .agg(
        F.count("*").alias("qtd_linhas"),
        F.sum("forecast_qty").alias("total_qty"),
    )
    .orderBy("lag")
)

print(f"\nAuditoria da última carga:")
display(
    df_check
    .groupBy("_load_id", "_raw_load_id", "_ingested_by", "_load_type")
    .agg(
        F.count("*").alias("qtd_linhas"),
        F.min("_ingested_at").alias("ingested_at"),
    )
    .orderBy(F.col("ingested_at").desc())
)

# COMMAND ----------

# DBTITLE 1,Metadados das tabelas de forecast
# ==============================================================================
# METADADOS DAS TABELAS DE FORECAST
# ==============================================================================
# Aplica comentários de tabela e colunas, tags de governança e
# propriedades de negócio nas tabelas raw_forecast_enriched e
# refined_forecast_enriched. Garante rastreabilidade e documentação
# no Unity Catalog.
# ==============================================================================

# ---------------------------------------------------------------------------
# 1) raw_forecast_enriched
# ---------------------------------------------------------------------------
RAW_TABLE = SOURCE_TABLE
RAW_COMMENT = """
Forecast enriched bruto ingerido dos arquivos Excel SAP.

Modelo: Append incremental por _load_id
Chave: forecast_cycle + segment + main_material (36 colunas de horizonte n+0..n+35)
Fonte: /Volumes/parts_hdbk_sandbox/pr_forecast/forecast_enriched/current/
"""

RAW_COLUMN_COMMENTS = {
    "forecast_cycle": "Primeiro dia do mês do ciclo de forecast (date).",
    "department": "Departamento de origem (ex: Domestic - Parts, Export - Parts).",
    "forecast_type": "Tipo de forecast (ex: Enrich, Judgment, Consensus).",
    "segment": "Código da empresa SAP (ex: 0200=2W, 0500=4W).",
    "market": "Mercado de destino (ex: Domestic, Export).",
    "main_material": "Código do material/peça (partnumber SAP).",
    "forecast_classification": "Classificação do forecast (ex: Supply - Parts).",
    "_source_file_name": "Nome do arquivo Excel fonte.",
    "_source_file_path": "Caminho completo do arquivo fonte no Volume.",
    "_ingested_at": "Data e hora da ingestão do registro.",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_type": "Tipo de carga (incremental).",
    "_load_id": "Identificador único da execução da carga (UUID).",
}
# Colunas de horizonte n+0..n+35
for i in range(36):
    RAW_COLUMN_COMMENTS[f"n+{i}"] = f"Quantidade prevista para o mês forecast_cycle + {i} meses."

RAW_METADATA_VERSION = "1"

raw_props = (
    spark.sql(f"DESCRIBE DETAIL {RAW_TABLE}")
    .select("properties").first()["properties"] or {}
)

if raw_props.get("forecast_raw_metadata_version") != RAW_METADATA_VERSION:
    spark.sql(
        f"COMMENT ON TABLE {RAW_TABLE} IS "
        f"'{RAW_COMMENT.replace(chr(39), chr(39)+chr(39))}'"
    )
    for col_name, comment in RAW_COLUMN_COMMENTS.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {RAW_TABLE}.`{col_name}` IS '{escaped}'")

    spark.sql(f"""
        ALTER TABLE {RAW_TABLE} SET TAGS (
            'domain' = 'forecast', 'layer' = 'raw',
            'source' = 'sap', 'history_model' = 'append_incremental',
            'data_classification' = 'internal'
        )
    """)
    spark.sql(f"""
        ALTER TABLE {RAW_TABLE} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Forecast',
            'source_system' = 'SAP',
            'refresh_frequency' = 'incremental_append',
            'natural_key' = 'forecast_cycle, segment, main_material',
            'forecast_raw_metadata_version' = '{RAW_METADATA_VERSION}'
        )
    """)
    print(f"Metadados versão {RAW_METADATA_VERSION} aplicados à tabela {RAW_TABLE}")
else:
    print(f"Metadados versão {RAW_METADATA_VERSION} já aplicados em {RAW_TABLE}; DDL ignorada.")

# ---------------------------------------------------------------------------
# 2) refined_forecast_enriched
# ---------------------------------------------------------------------------
REFINED_TABLE = TARGET_TABLE
REFINED_COMMENT = """
Forecast enriched dinamizado (unpivot) com horizonte limitado a 12 meses e lookup de cadeia de materiais.

Modelo: Append incremental por _load_id
Chave: forecast_cycle + segment + material + lag
Fonte: raw_forecast_enriched (transformado)
Lookup: pr_cadastrao.material_cadeia (material → main_material via item_principal_cadeia)
"""

REFINED_COLUMN_COMMENTS = {
    "forecast_cycle": "Primeiro dia do mês do ciclo de forecast (date).",
    "department": "Departamento de origem, sem prefixo Domestic/Export.",
    "forecast_type": "Tipo de forecast (ex: Enrich, Judgment, Consensus).",
    "segment": "Código da empresa SAP (ex: 0200=2W, 0500=4W).",
    "market": "Mercado de destino (ex: Domestic, Export).",
    "material": "Código do material/peça original (partnumber SAP).",
    "main_material": "Material principal na cadeia de substituição (item_principal_cadeia via pr_cadastrao.material_cadeia).",
    "forecast_classification": "Classificação do forecast (ex: Forecastable/Non-Forecastable).",
    "forecast_classification_tag": "Tag extraída da classificação (tudo após o primeiro ' - ').",
    "lag": "Offset em meses do horizonte (0 = mês corrente, até 11).",
    "forecast_month": "Mês-alvo do forecast: forecast_cycle + lag meses.",
    "forecast_qty": "Quantidade prevista para o mês-alvo.",
    "_raw_load_id": "Identificador da carga original na tabela raw.",
    "_ingested_at": "Data e hora da ingestão do registro.",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_type": "Tipo de carga (incremental).",
    "_load_id": "Identificador único da execução da carga (UUID).",
}

REFINED_METADATA_VERSION = "1"

refined_props = (
    spark.sql(f"DESCRIBE DETAIL {REFINED_TABLE}")
    .select("properties").first()["properties"] or {}
)

if refined_props.get("forecast_refined_metadata_version") != REFINED_METADATA_VERSION:
    spark.sql(
        f"COMMENT ON TABLE {REFINED_TABLE} IS "
        f"'{REFINED_COMMENT.replace(chr(39), chr(39)+chr(39))}'"
    )
    for col_name, comment in REFINED_COLUMN_COMMENTS.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {REFINED_TABLE}.`{col_name}` IS '{escaped}'")

    spark.sql(f"""
        ALTER TABLE {REFINED_TABLE} SET TAGS (
            'domain' = 'forecast', 'layer' = 'refined',
            'source' = 'sap', 'history_model' = 'append_incremental',
            'data_classification' = 'internal'
        )
    """)
    spark.sql(f"""
        ALTER TABLE {REFINED_TABLE} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Forecast',
            'source_system' = 'SAP',
            'refresh_frequency' = 'incremental_append',
            'natural_key' = 'forecast_cycle, segment, material, lag',
            'forecast_refined_metadata_version' = '{REFINED_METADATA_VERSION}'
        )
    """)
    print(f"Metadados versão {REFINED_METADATA_VERSION} aplicados à tabela {REFINED_TABLE}")
else:
    print(f"Metadados versão {REFINED_METADATA_VERSION} já aplicados em {REFINED_TABLE}; DDL ignorada.")

# COMMAND ----------

# DBTITLE 1,Carregar utilitários de movimentação
# MAGIC %run ../99_utils_volume_file_ops

# COMMAND ----------

# DBTITLE 1,Mover arquivos processados para archive
# ---------------------------------------------------------------------------
# Move arquivos processados de current/ para archive/
# ---------------------------------------------------------------------------
SOURCE_PATH  = "/Volumes/parts_hdbk_sandbox/pr_forecast/forecast_enriched/current/"
ARCHIVE_PATH = "/Volumes/parts_hdbk_sandbox/pr_forecast/forecast_enriched/archive/"

result = move_volume_files(
    source_path=SOURCE_PATH,
    destination_path=ARCHIVE_PATH,
    extensions=[".xlsx"],
    dry_run=False,
    validate_empty=True,
)