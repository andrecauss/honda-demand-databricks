# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 2.3 — Histórico de materiais (SCD2)
# MAGIC
# MAGIC - **Propósito:** Manter o histórico das alterações do cadastro mestre de materiais.
# MAGIC - **Entrada:** `pr_cadastrao/sap_cadastraorefinado/current`
# MAGIC - **Saídas:** `pr_cadastrao.material_spec_changes` e `_agents_databases.material_spec_changes`
# MAGIC - **Chave:** Empresa + Material · **Carga:** Mensal, SCD Type 2

# COMMAND ----------

# DBTITLE 1,Guarda: verificar arquivos em current/
# ==============================================================================
# GUARDA: VERIFICAR ARQUIVOS EM CURRENT/
# ==============================================================================
# Verifica a existência de arquivos Excel no diretório de entrada antes de
# prosseguir. Encerra o notebook com exit code "NO_FILES" se vazio,
# evitando execução desnecessária das células subsequentes.
# ==============================================================================

import os

SOURCE_DIR = "/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/"

files = [f for f in os.listdir(SOURCE_DIR) if f.lower().endswith((".xls", ".xlsx"))]

if not files:
    print("Nenhum arquivo Excel em current/. Encerrando sem processar.")
    dbutils.notebook.exit("NO_FILES")
else:
    print(f"{len(files)} arquivo(s) detectado(s) em current/: {files}")

# COMMAND ----------

# DBTITLE 1,Função de Sanitização de Colunas
# ------------------------------------------------------------------------------
# FUNÇÃO AUXILIAR: SANITIZAÇÃO DE NOMES DE COLUNAS
# ------------------------------------------------------------------------------
# Necessária porque os arquivos Excel SAP contêm headers com acentuação
# e espaços, incompatíveis com nomes de colunas Delta/Spark.
# ------------------------------------------------------------------------------

import re
import unicodedata


def sanitize_col_name(name):
    """
    Sanitiza nome de coluna para compatibilidade com Spark/Delta.

    Remove acentos via normalização NFKD, converte para lowercase e
    substitui caracteres inválidos por underscore.

    Args:
        name (str): Nome original da coluna (pode conter acentos,
            espaços e caracteres especiais).

    Returns:
        str: Nome sanitizado em snake_case ASCII.

    Example:
        >>> sanitize_col_name("Preço de Venda Líquida")
        'preco_de_venda_liquida'
    """
    if not name:
        return "col_unknown"
    # Remover acentos via normalização Unicode NFKD
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    # Substituir caracteres inválidos por underscore
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ascii_name)
    # Remover underscores consecutivos e nas pontas
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean.lower()

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos Excel (.xlsx)
# ==============================================================================
# LEITURA DOS ARQUIVOS EXCEL (.xlsx)
# ==============================================================================
# Carrega todos os arquivos Excel do diretório current/ com inferSchema
# desabilitado (todas as colunas como STRING) para preservar zeros à
# esquerda em códigos SAP (ex: material "001234", empresa "0200").
# Extrai metadados de arquivo para determinar data de referência.
# ==============================================================================

df_raw = (spark.read
    .format("excel")
    .option("header", "true")
    .option("inferSchema", "false")
    .load("/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/")
)

print(f"Arquivos carregados: {len(df_raw.columns)} colunas, {df_raw.count()} linhas (incluindo possíveis headers duplicados)")

# Extrair nomes de arquivo da origem para determinar data de referência do snapshot
source_file_names = [
    row.file_name
    for row in df_raw.selectExpr("_metadata.file_name AS file_name").distinct().collect()
]
print(f"Arquivos fonte detectados: {source_file_names}")

# COMMAND ----------

# DBTITLE 1,Tratamento de header e sanitização de colunas
# ==============================================================================
# TRATAMENTO DE HEADER E SANITIZAÇÃO DE COLUNAS
# ==============================================================================
# Quando múltiplos arquivos Excel são carregados, o reader pode não aplicar
# o header corretamente (colunas _c0, _c1...). Este bloco detecta essa
# situação, extrai nomes da primeira linha, sanitiza e remove linhas de
# cabeçalho duplicadas provenientes de cada arquivo .xlsx.
# ==============================================================================

first_col = df_raw.columns[0]

if first_col.startswith("_c"):
    # Header não foi aplicado - extrair nomes da primeira linha e renomear
    header_row = df_raw.first()
    original_names = [header_row[i] if header_row[i] else f"col_{i}" for i in range(len(df_raw.columns))]
    sanitized_names = [sanitize_col_name(n) for n in original_names]

    # Tratar duplicatas adicionando sufixo incremental
    seen = {}
    for i, name in enumerate(sanitized_names):
        if name in seen:
            seen[name] += 1
            sanitized_names[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 0

    df = df_raw.toDF(*sanitized_names)

    # Remover linhas de cabeçalho de TODOS os arquivos (cada .xlsx carrega seu próprio header)
    header_values = [v for v in [header_row[0], header_row[1], header_row[2]] if v]
    df = df.filter(
        ~(
            (df[sanitized_names[0]] == header_values[0]) &
            (df[sanitized_names[1]] == header_values[1]) &
            (df[sanitized_names[2]] == header_values[2])
        )
    )
else:
    # Header aplicado - apenas sanitizar os nomes existentes
    original_names = df_raw.columns
    sanitized_names = [sanitize_col_name(n) for n in original_names]

    seen = {}
    for i, name in enumerate(sanitized_names):
        if name in seen:
            seen[name] += 1
            sanitized_names[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 0

    df = df_raw.toDF(*sanitized_names)

print(f"DataFrame final: {df.count()} linhas, {len(df.columns)} colunas")
print(f"Primeiras colunas: {df.columns[:10]}")

# COMMAND ----------

# DBTITLE 1,Escopo de campos SCD2
# ==============================================================================
# ESCOPO DE CAMPOS DESTA IMPLEMENTAÇÃO SCD2
# ==============================================================================
# Se um campo não estiver listado abaixo, ele NÃO será importado para a
# tabela histórica e NÃO será avaliado para mudança.
#
# CAMPOS DE ORIGEM INCLUÍDOS:
#   Chave de negócio:
#     • empresa — chave de negócio
#     • material — chave de negócio
#   Campo usado somente na validação da origem:
#     • centro — confirma que os campos técnicos são consistentes entre centros
#   Campos rastreados por mudança (hash SHA-256):
#     • intercambiabilidade
#     • item_principal_cadeia
#     • data_cadeia
#     • cut_in_material
#     • cut_off_material
#     • cadeia
#     • modelo_comercial_principal
#     • status_compra
#
# REGRAS DE CONTROLE TEMPORAL:
#   • Carga inicial (tabela vazia): start_date = 1900-01-01 (sentinel)
#   • Chave nova: start_date = 1º dia do mês do arquivo
#   • Hash alterado: nova versão com start_date = 1º dia do mês do arquivo
#   • Chave ausente no novo snapshot: end_date + is_current = false
#   • end_date de encerrados = 1º dia do mês do novo snapshot
#
# COLUNAS TÉCNICAS GERADAS:
#   • row_hash — SHA-256 para Hash Comparison
#   • start_date, end_date, is_current — controle temporal SCD2
#   • _ingested_at, _last_updated_at — timestamps de auditoria
#   • _ingested_by, _load_type, _load_id, _source_file_path — rastreabilidade
# ==============================================================================

print("✓ Escopo de campos SCD2 documentado.")

# COMMAND ----------

# DBTITLE 1,Preparar stage SCD2 com hash
# ==============================================================================
# PREPARAR STAGE SCD2 COM HASH
# ==============================================================================
# Seleciona colunas de negócio, calcula hash SHA-256 dos campos rastreados,
# adiciona colunas técnicas (start_date, end_date, is_current, auditoria)
# e valida ausência de versões conflitantes por chave na mesma carga.
# O resultado é persistido como temp view "vw_material_spec_changes_stage".
# ==============================================================================

from pyspark.sql import functions as F
import uuid

SOURCE_PATH = "/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/"
MAIN_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_spec_changes"
AGENTS_TABLE = "parts_hdbk_sandbox._agents_databases.material_spec_changes"

BUSINESS_KEY_COLUMNS = ["empresa", "material"]
SOURCE_CONTEXT_COLUMNS = ["centro"]
MATERIAL_TYPE_COLUMN = "tipo_de_material"
TRACKED_COLUMNS = [
    "intercambiabilidade",
    "item_principal_cadeia",
    "data_cadeia",
    "cut_in_material",
    "cut_off_material",
    "cadeia",
    "modelo_comercial_principal",
    "status_compra",
]
INCLUDED_SOURCE_COLUMNS = BUSINESS_KEY_COLUMNS + TRACKED_COLUMNS
REQUIRED_SOURCE_COLUMNS = (
    BUSINESS_KEY_COLUMNS
    + SOURCE_CONTEXT_COLUMNS
    + [MATERIAL_TYPE_COLUMN]
    + TRACKED_COLUMNS
)
TECHNICAL_COLUMNS = [
    "row_hash",
    "start_date",
    "end_date",
    "is_current",
    "_ingested_at",
    "_last_updated_at",
    "_ingested_by",
    "_load_type",
    "_load_id",
    "_source_file_path",
]
TARGET_COLUMNS = INCLUDED_SOURCE_COLUMNS + TECHNICAL_COLUMNS

missing_columns = [column for column in REQUIRED_SOURCE_COLUMNS if column not in df.columns]
if missing_columns:
    raise ValueError(f"Colunas obrigatórias ausentes no DataFrame sanitizado: {missing_columns}")

from datetime import datetime

CURRENT_USER = spark.sql("SELECT current_user()").first()[0]
LOAD_MODE = "scd2_hash"
LOAD_ID = str(uuid.uuid4())
LOAD_TS = spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS load_ts").first()["load_ts"]
SENTINEL_DATE = datetime(1900, 1, 1, 0, 0, 0)

# Extrair data de referência do snapshot a partir do nome dos arquivos
# Padrão esperado: CAD{centro}_{yyyy}.{MM}.xls → primeiro dia do mês
import re
ref_dates = set()
for fname in source_file_names:
    match = re.search(r'(\d{4})\.(\d{2})', fname)
    if match:
        ref_dates.add(datetime(int(match.group(1)), int(match.group(2)), 1))

if not ref_dates:
    raise ValueError(f"Não foi possível extrair data de referência dos arquivos: {source_file_names}")
if len(ref_dates) > 1:
    raise ValueError(
        f"Arquivos com datas de referência diferentes na mesma carga: {ref_dates}. "
        "Todos os arquivos devem ser do mesmo mês."
    )

REFERENCE_DATE = ref_dates.pop()

is_initial_load = (
    not spark.catalog.tableExists(MAIN_TABLE)
    or spark.table(MAIN_TABLE).limit(1).count() == 0
)

effective_start = F.lit(SENTINEL_DATE) if is_initial_load else F.lit(REFERENCE_DATE)
load_ts = F.lit(LOAD_TS)

print(f"Data de referência do snapshot: {REFERENCE_DATE.strftime('%Y-%m-%d')}")
print(f"Carga inicial (tabela vazia): {is_initial_load}")
print(f"start_date efetivo: {'1900-01-01 (sentinel)' if is_initial_load else REFERENCE_DATE.strftime('%Y-%m-%d')}")

# --- Filtro por tipo de material (apenas ZHAW, ZFER, ZRO1) ---
MATERIAL_TYPES = ["ZHAW", "ZFER", "ZRO1"]
df_filtered = df.filter(F.col(MATERIAL_TYPE_COLUMN).isin(MATERIAL_TYPES))
print(f"Registros após filtro {MATERIAL_TYPE_COLUMN} {MATERIAL_TYPES}: {df_filtered.count()}")

# Centro não compõe a chave do histórico. Antes de consolidar uma linha por
# empresa + material, a carga deve provar que os campos técnicos são iguais
# em todos os centros presentes no snapshot.
normalized_source_df = df_filtered.select(
    *[
        F.trim(F.col(column).cast("string")).alias(column)
        for column in REQUIRED_SOURCE_COLUMNS
    ]
)

hash_expression = F.sha2(
    F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column), F.lit("<NULL>"))
            for column in TRACKED_COLUMNS
        ],
    ),
    256,
)

source_with_hash_df = normalized_source_df.withColumn("row_hash", hash_expression)

inconsistent_materials_df = (
    source_with_hash_df
    .groupBy(*BUSINESS_KEY_COLUMNS)
    .agg(
        F.countDistinct("row_hash").alias("technical_versions"),
        F.sort_array(F.collect_set("centro")).alias("centros"),
    )
    .filter(F.col("technical_versions") > 1)
)

if inconsistent_materials_df.limit(1).count() > 0:
    display(
        inconsistent_materials_df
        .orderBy(F.desc("technical_versions"), *BUSINESS_KEY_COLUMNS)
        .limit(20)
    )
    raise ValueError(
        "Há campos técnicos divergentes entre centros para a mesma empresa + material. "
        "Revise o snapshot antes de prosseguir."
    )

source_df = (
    source_with_hash_df
    .dropDuplicates(BUSINESS_KEY_COLUMNS + ["row_hash"])
    .select(*INCLUDED_SOURCE_COLUMNS, "row_hash")
)

stage_df = (
    source_df
    .withColumn("start_date", effective_start)
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("_ingested_at", load_ts)
    .withColumn("_last_updated_at", load_ts)
    .withColumn("_ingested_by", F.lit(CURRENT_USER))
    .withColumn("_load_type", F.lit(LOAD_MODE))
    .withColumn("_load_id", F.lit(LOAD_ID))
    .withColumn("_source_file_path", F.lit(SOURCE_PATH))
    .dropDuplicates(BUSINESS_KEY_COLUMNS + ["row_hash"])
    .select(*TARGET_COLUMNS)
)

stage_df.createOrReplaceTempView("vw_material_spec_changes_stage")

print(f"Colunas de negócio incluídas: {INCLUDED_SOURCE_COLUMNS}")
print(f"Colunas avaliadas por hash: {TRACKED_COLUMNS}")
print(f"Load ID: {LOAD_ID}")
print(f"Load timestamp: {LOAD_TS}")
print(f"Registros preparados no stage SCD2: {stage_df.count()}")
display(stage_df.orderBy(*BUSINESS_KEY_COLUMNS).limit(5))

# COMMAND ----------

# DBTITLE 1,Aplicar SCD2 na tabela principal
# ==============================================================================
# APLICAR SCD2 NA TABELA PRINCIPAL
# ==============================================================================
# Executa o processo completo de SCD2 via MERGE + UPDATE + INSERT:
#   1. Cria tabela se não existir (ensure_scd2_table)
#   2. MERGE: encerra registros com hash alterado (end_date + is_current=false)
#   3. UPDATE: encerra registros ausentes no novo snapshot (exclusão lógica)
#   4. INSERT: insere registros novos ou com hash diferente
# Saída: parts_hdbk_sandbox.pr_cadastrao.material_spec_changes
# ==============================================================================


def ensure_scd2_table(target_table: str) -> None:
    """
    Cria a tabela SCD2 Delta se ela não existir.

    Args:
        target_table (str): Nome completo da tabela (catalog.schema.table).

    Side Effects:
        Cria tabela Delta com schema SCD2 completo.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            empresa STRING,
            material STRING,
            intercambiabilidade STRING,
            item_principal_cadeia STRING,
            data_cadeia STRING,
            cut_in_material STRING,
            cut_off_material STRING,
            cadeia STRING,
            modelo_comercial_principal STRING,
            status_compra STRING,
            row_hash STRING,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_current BOOLEAN,
            _ingested_at TIMESTAMP,
            _last_updated_at TIMESTAMP,
            _ingested_by STRING,
            _load_type STRING,
            _load_id STRING,
            _source_file_path STRING
        )
        USING DELTA
    """)


def apply_scd2_merge(target_table: str) -> None:
    """
    Aplica o processo completo de SCD2 com Hash Comparison.

    Executa três operações em sequência:
      1. MERGE: encerra registros vigentes cujo hash mudou
      2. UPDATE: encerra registros vigentes ausentes no novo snapshot
      3. INSERT: insere novos registros ou versões alteradas

    Args:
        target_table (str): Nome completo da tabela alvo (catalog.schema.table).

    Side Effects:
        Modifica a tabela target_table com operações MERGE/UPDATE/INSERT.
        Exibe sumário de registros processados no console.
    """
    ensure_scd2_table(target_table)

    changed_candidates = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {target_table} target
        INNER JOIN vw_material_spec_changes_stage source
            ON target.empresa = source.empresa
           AND target.material = source.material
           AND target.is_current = TRUE
        WHERE target.row_hash <> source.row_hash
    """).first()["total"]

    missing_candidates = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {target_table} target
        LEFT ANTI JOIN vw_material_spec_changes_stage source
            ON target.empresa = source.empresa
           AND target.material = source.material
        WHERE target.is_current = TRUE
    """).first()["total"]

    insert_candidates = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM vw_material_spec_changes_stage source
        LEFT ANTI JOIN {target_table} target
            ON target.empresa = source.empresa
           AND target.material = source.material
           AND target.is_current = TRUE
           AND target.row_hash = source.row_hash
    """).first()["total"]

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING vw_material_spec_changes_stage AS source
            ON target.empresa = source.empresa
           AND target.material = source.material
           AND target.is_current = TRUE
        WHEN MATCHED AND target.row_hash <> source.row_hash THEN
          UPDATE SET
            target.end_date = source.start_date,
            target.is_current = FALSE,
            target._last_updated_at = source._last_updated_at,
            target._ingested_by = source._ingested_by,
            target._load_type = source._load_type,
            target._load_id = source._load_id,
            target._source_file_path = source._source_file_path
    """)

    spark.sql(f"""
        UPDATE {target_table} AS target
        SET
            end_date = TIMESTAMP '{REFERENCE_DATE.strftime("%Y-%m-%d %H:%M:%S.%f")}',
            is_current = FALSE,
            _last_updated_at = TIMESTAMP '{LOAD_TS.strftime("%Y-%m-%d %H:%M:%S.%f")}',
            _ingested_by = '{CURRENT_USER}',
            _load_type = '{LOAD_MODE}',
            _load_id = '{LOAD_ID}',
            _source_file_path = '{SOURCE_PATH}'
        WHERE target.is_current = TRUE
          AND NOT EXISTS (
              SELECT 1
              FROM vw_material_spec_changes_stage source
              WHERE target.empresa = source.empresa
                AND target.material = source.material
          )
    """)

    spark.sql(f"""
        INSERT INTO {target_table}
        SELECT source.*
        FROM vw_material_spec_changes_stage source
        LEFT ANTI JOIN {target_table} target
            ON target.empresa = source.empresa
           AND target.material = source.material
           AND target.is_current = TRUE
           AND target.row_hash = source.row_hash
    """)

    summary_df = spark.sql(f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
            SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_rows
        FROM {target_table}
    """)

    print(f"Tabela alvo: {target_table}")
    print(f"Registros alterados a encerrar: {changed_candidates}")
    print(f"Registros ausentes no novo snapshot a encerrar: {missing_candidates}")
    print(f"Registros novos ou alterados a inserir: {insert_candidates}")
    display(summary_df)


apply_scd2_merge(MAIN_TABLE)
print(f"Carga SCD2 concluída na tabela principal: {MAIN_TABLE}")

# COMMAND ----------

# DBTITLE 1,Aplicar SCD2 em _agents_databases
# ------------------------------------------------------------------------------
# RÉPLICA SCD2 - TABELA _AGENTS_DATABASES
# ------------------------------------------------------------------------------
# Aplica o mesmo processo SCD2 na réplica usada por agentes de IA,
# garantindo consistência entre as duas tabelas de destino.
# ------------------------------------------------------------------------------

# Criar schema se não existir
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS parts_hdbk_sandbox._agents_databases
    COMMENT 'Schema para réplicas de tabelas acessadas por agentes de IA'
""")

apply_scd2_merge(AGENTS_TABLE)
print(f"Carga SCD2 concluída na réplica adicional: {AGENTS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Validar estado da tabela histórica
# MAGIC %sql
# MAGIC -- ==============================================================================
# MAGIC -- VALIDAÇÃO DO ESTADO DA TABELA HISTÓRICA
# MAGIC -- ==============================================================================
# MAGIC -- Verifica consistência do SCD2: total de versões por chave, contagem de
# MAGIC -- registros correntes (is_current=true) e datas de início/fim. Espera-se
# MAGIC -- exatamente 1 versão corrente por chave de negócio.
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC WITH versions AS (
# MAGIC   SELECT
# MAGIC     empresa,
# MAGIC     material,
# MAGIC     COUNT(*) AS total_versions,
# MAGIC     SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions,
# MAGIC     MIN(start_date) AS first_start_date,
# MAGIC     MAX(COALESCE(end_date, start_date)) AS last_change_date
# MAGIC   FROM parts_hdbk_sandbox.pr_cadastrao.material_spec_changes
# MAGIC   GROUP BY empresa, material
# MAGIC )
# MAGIC SELECT
# MAGIC   empresa,
# MAGIC   material,
# MAGIC   total_versions,
# MAGIC   current_versions,
# MAGIC   first_start_date,
# MAGIC   last_change_date
# MAGIC FROM versions
# MAGIC ORDER BY total_versions DESC, empresa, material
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Comentários e metadados das tabelas
# ==============================================================================
# COMENTÁRIOS E METADADOS DAS TABELAS SCD2
# ==============================================================================
# Aplica comentários de tabela e colunas, tags de governança (business_key,
# tracked_change, scd_control) e propriedades de negócio em ambas as
# tabelas de destino. Garante rastreabilidade e documentação no Unity Catalog.
# ==============================================================================

TARGET_TABLES = [MAIN_TABLE, AGENTS_TABLE]

COLUMN_COMMENTS = {
    # --- Chave de negócio ---
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave de negócio histórica.",
    "material": "Código único do material/peça (partnumber SAP). Parte da chave de negócio histórica.",
    # --- Campos rastreados por mudança ---
    "intercambiabilidade": "Indica se o material possui intercambiabilidade com outros. Campo rastreado por SCD2.",
    "item_principal_cadeia": "Material principal na cadeia de substituição. Campo rastreado por SCD2.",
    "data_cadeia": "Data de início da cadeia de substituição. Campo rastreado por SCD2.",
    "cut_in_material": "Data de início de utilização do material (cut-in). Campo rastreado por SCD2.",
    "cut_off_material": "Data de fim de utilização do material (cut-off). Campo rastreado por SCD2.",
    "cadeia": "Código da cadeia de substituição. Campo rastreado por SCD2.",
    "modelo_comercial_principal": "Modelo comercial principal do material. Campo rastreado por SCD2.",
    "status_compra": "Status de compra do material no SAP. Campo rastreado por SCD2.",

    # --- Controle SCD2 ---
    "row_hash": "Hash SHA-256 das colunas rastreadas, usado para detectar mudanças sem comparar campo a campo.",
    "start_date": "Data/hora de início da vigência desta versão do cadastro.",
    "end_date": "Data/hora de fim da vigência desta versão do cadastro. Nulo quando a versão está vigente.",
    "is_current": "Indica se esta é a versão vigente do cadastro para a chave de negócio.",

    # --- Auditoria ---
    "_ingested_at": "Data e hora da ingestão desta versão histórica.",
    "_last_updated_at": "Data e hora da última atualização técnica do registro histórico.",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_type": "Tipo de carga executada para a tabela histórica.",
    "_load_id": "Identificador único da execução da carga (UUID).",
    "_source_file_path": "Caminho do diretório fonte dos arquivos ingeridos.",
}

# =============================================================================
# 2. COMENTÁRIO DAS TABELAS
# =============================================================================
TABLE_COMMENT = """
Camada Refined do histórico de cadastro de materiais SAP.

Modelo: Slowly Changing Dimension Type 2 (SCD2)
Chave de negócio: empresa + material
Detecção de mudança: Hash Comparison (SHA-256)
Campos rastreados: intercambiabilidade, item_principal_cadeia, data_cadeia, cut_in_material, cut_off_material, cadeia, modelo_comercial_principal, status_compra
Atualização: append histórico quando houver mudança nos campos rastreados ou quando a chave aparecer pela primeira vez; encerramento lógico do registro vigente quando a chave deixar de aparecer no novo snapshot
Fonte: /Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/
"""

for target_table in TARGET_TABLES:
    spark.sql(f"""
        COMMENT ON TABLE {target_table} IS '{TABLE_COMMENT.replace(chr(39), chr(39)+chr(39))}'
    """)

    for column_name, comment in COLUMN_COMMENTS.items():
        escaped_comment = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {target_table}.{column_name} IS '{escaped_comment}'")

    spark.sql(f"""
        ALTER TABLE {target_table} SET TAGS (
            'domain' = 'materials',
            'layer' = 'refined',
            'source' = 'sap',
            'history_model' = 'scd2',
            'data_classification' = 'internal'
        )
    """)

    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN empresa SET TAGS ('business_key' = 'true')")
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN material SET TAGS ('business_key' = 'true')")

    for tracked_column in TRACKED_COLUMNS:
        spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {tracked_column} SET TAGS ('tracked_change' = 'true')")

    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN row_hash SET TAGS ('scd_control' = 'hash')")
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN start_date SET TAGS ('scd_control' = 'start_date')")
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN end_date SET TAGS ('scd_control' = 'end_date')")
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN is_current SET TAGS ('scd_control' = 'is_current')")

    spark.sql(f"""
        ALTER TABLE {target_table} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Materials Master Data',
            'source_system' = 'SAP',
            'refresh_frequency' = 'monthly_scd2',
            'natural_key' = 'empresa, material',
            'tracked_columns' = 'intercambiabilidade, item_principal_cadeia, data_cadeia, cut_in_material, cut_off_material, cadeia, modelo_comercial_principal, status_compra'
        )
    """)

    print(f"Metadados aplicados à tabela {target_table}")

print(f"\nMetadados completos aplicados \u00e0s tabelas SCD2: {TARGET_TABLES}")