# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Célula 1
# MAGIC %md
# MAGIC # 1.3 — Histórico de materiais (SCD2)
# MAGIC
# MAGIC - **Propósito:** Manter o histórico das alterações do cadastro mestre de materiais.
# MAGIC - **Entrada:** `pr_cadastrao.material_cadastrao` (gerada pelo 1.1, com `item_principal_cadeia` original do SAP e a data de referência em `reference_date`)
# MAGIC - **Saídas:** `pr_cadastrao.material_spec_changes` (tabela) e `_agents_databases.material_spec_changes` (view, sem colunas de auditoria)
# MAGIC - **Chave:** Empresa + Material · **Carga:** Mensal, SCD Type 2
# MAGIC - **Campos rastreados:** intercambiabilidade, item_principal_cadeia, data_cadeia, cut_in_material, cut_off_material, cadeia, modelo_comercial_principal, status_compra e `lead_time` (origem: `prazo_entrega_prevista_lead_time_firme`)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela material_cadastrao
# ==============================================================================
# LEITURA DA TABELA MATERIAL_CADASTRAO
# ==============================================================================
# Única entrada do notebook: a tabela gerada pelo 1.1, que mantém
# item_principal_cadeia com o valor original do SAP (SIM/nulo), igual ao
# histórico SCD2 existente. A data de referência vem da coluna reference_date
# (gravada pelo 1.1 a partir do nome dos arquivos), sem consultar o volume.
# Encerra com "NO_DATA" se a tabela estiver vazia e falha se houver mais de
# uma data de referência.
# ==============================================================================

import uuid
from datetime import datetime

from pyspark.sql import functions as F

CADASTRAO_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_cadastrao"

df = spark.table(CADASTRAO_TABLE)

if "reference_date" not in df.columns:
    raise RuntimeError(
        f"{CADASTRAO_TABLE} não possui a coluna reference_date. "
        "Execute a versão atual do notebook 1.1 antes deste."
    )

load_info = df.agg(
    F.count(F.lit(1)).alias("rows"),
    F.count_if(F.col("reference_date").isNull()).alias("null_ref"),
    F.sort_array(F.collect_set("reference_date")).alias("ref_dates"),
    F.sort_array(F.collect_set("_source_file_path")).alias("source_paths"),
).first()

if load_info["rows"] == 0:
    print(f"{CADASTRAO_TABLE} está vazia. Encerrando sem processar.")
    dbutils.notebook.exit("NO_DATA")

if load_info["null_ref"] > 0 or len(load_info["ref_dates"]) != 1:
    raise ValueError(
        f"{CADASTRAO_TABLE} deve ter exatamente uma reference_date não nula. "
        f"Encontradas: {load_info['ref_dates']} (nulas: {load_info['null_ref']})."
    )

_ref = load_info["ref_dates"][0]
REFERENCE_DATE = datetime(_ref.year, _ref.month, 1)
SOURCE_PATH = ", ".join(load_info["source_paths"])

print(f"Entrada: {CADASTRAO_TABLE} ({len(df.columns)} colunas, {load_info['rows']} linhas)")
print(f"Data de referência: {REFERENCE_DATE.strftime('%Y-%m-%d')}")

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
#     • lead_time (origem: prazo_entrega_prevista_lead_time_firme)
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

# REFERENCE_DATE, SOURCE_PATH e imports (F, uuid, datetime) vêm da célula
# "Leitura da tabela material_cadastrao".
MAIN_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_spec_changes"
AGENTS_TABLE = "parts_hdbk_sandbox._agents_databases.material_spec_changes"

BUSINESS_KEY_COLUMNS = ["empresa", "material"]
SOURCE_CONTEXT_COLUMNS = ["centro"]
MATERIAL_TYPE_COLUMN = "tipo_de_material"
LEAD_TIME_SOURCE_COLUMN = "prazo_entrega_prevista_lead_time_firme"
TRACKED_COLUMNS = [
    "intercambiabilidade",
    "item_principal_cadeia",
    "data_cadeia",
    "cut_in_material",
    "cut_off_material",
    "cadeia",
    "modelo_comercial_principal",
    "status_compra",
    "lead_time",
]
INCLUDED_SOURCE_COLUMNS = BUSINESS_KEY_COLUMNS + TRACKED_COLUMNS
REQUIRED_SOURCE_COLUMNS = (
    BUSINESS_KEY_COLUMNS
    + SOURCE_CONTEXT_COLUMNS
    + [MATERIAL_TYPE_COLUMN]
    + [column for column in TRACKED_COLUMNS if column != "lead_time"]
    + [LEAD_TIME_SOURCE_COLUMN]
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

CURRENT_USER = spark.sql("SELECT current_user()").first()[0]
LOAD_MODE = "scd2_hash"
LOAD_ID = str(uuid.uuid4())
LOAD_TS = spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS load_ts").first()["load_ts"]
SENTINEL_DATE = datetime(1900, 1, 1, 0, 0, 0)

# --- Guarda: impedir carga retroativa (fora de ordem cronológica) ----------
if spark.catalog.tableExists(MAIN_TABLE):
    _max_start = spark.sql(f"""
        SELECT MAX(start_date) AS max_sd
        FROM {MAIN_TABLE}
        WHERE is_current = TRUE
    """).first()["max_sd"]
    if _max_start and REFERENCE_DATE <= _max_start:
        raise ValueError(
            f"Carga retroativa bloqueada: REFERENCE_DATE ({REFERENCE_DATE.strftime('%Y-%m-%d')}) "
            f"<= max start_date vigente ({_max_start}). "
            f"Os snapshots devem ser carregados em ordem cronológica crescente."
        )
    print(f"✓ Guarda cronológica: REFERENCE_DATE ({REFERENCE_DATE.strftime('%Y-%m-%d')}) > max vigente ({_max_start})")

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
        if column != LEAD_TIME_SOURCE_COLUMN
    ],
    F.trim(F.col(LEAD_TIME_SOURCE_COLUMN).cast("string")).alias("lead_time"),
)

# --- Validação e normalização de datas SAP ---------------------------------
# Detecta valores fora do padrão DD.MM.YYYY (ex: M/D/YY de exportações
# com locale en-US) e normaliza automaticamente. Previne incidentes como
# o de abril/2026 onde o arquivo fonte veio com formatação americana.
# ---------------------------------------------------------------------------
SAP_DATE_REGEX = r'^\d{2}\.\d{2}\.\d{4}$'
SAP_NULL_DATE = '00.00.0000'
DATE_COLUMNS = ['cut_in_material', 'cut_off_material', 'data_cadeia']

for _col_name in DATE_COLUMNS:
    _non_conforming = normalized_source_df.filter(
        F.col(_col_name).isNotNull()
        & (F.col(_col_name) != SAP_NULL_DATE)
        & (~F.col(_col_name).rlike(SAP_DATE_REGEX))
    ).count()

    if _non_conforming > 0:
        print(
            f"⚠ {_col_name}: {_non_conforming:,} valores com formato não-SAP "
            f"detectados. Normalizando M/D/YY → DD.MM.YYYY..."
        )
        normalized_source_df = normalized_source_df.withColumn(
            _col_name,
            F.when(
                F.col(_col_name).isNotNull()
                & (F.col(_col_name) != SAP_NULL_DATE)
                & (~F.col(_col_name).rlike(SAP_DATE_REGEX)),
                # to_date('M/d/yy') mapeia tudo para 20XX;
                # anos > 2029 pertencem ao século 19 → subtrair 1200 meses
                F.when(
                    F.year(F.to_date(F.col(_col_name), 'M/d/yy')) > 2029,
                    F.date_format(
                        F.add_months(F.to_date(F.col(_col_name), 'M/d/yy'), -1200),
                        'dd.MM.yyyy',
                    ),
                ).otherwise(
                    F.date_format(F.to_date(F.col(_col_name), 'M/d/yy'), 'dd.MM.yyyy')
                ),
            ).otherwise(F.col(_col_name)),
        )
        print(f"  ✓ {_col_name}: normalização concluída.")
    else:
        print(f"✓ {_col_name}: formato SAP (DD.MM.YYYY) OK.")

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
            lead_time STRING,
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

    # --- Guarda anti-duplicata: rejeitar reprocessamento do mesmo snapshot ---
    # Se já existem registros vigentes com o mesmo start_date, o snapshot
    # deste período já foi carregado. Bloqueia para evitar duplicatas como
    # o incidente de maio/2026 (176k registros duplicados por re-execução).
    # Para reprocessar, remova os registros do período antes de re-executar.
    # -----------------------------------------------------------------------
    _existing_for_period = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {target_table}
        WHERE start_date = TIMESTAMP '{REFERENCE_DATE.strftime("%Y-%m-%d %H:%M:%S.%f")}'
          AND is_current = TRUE
    """).first()["cnt"]

    if _existing_for_period > 0:
        raise ValueError(
            f"Guarda anti-duplicata: já existem {_existing_for_period:,} registros "
            f"vigentes com start_date = {REFERENCE_DATE.strftime('%Y-%m-%d')} "
            f"em {target_table}. O snapshot deste período já foi carregado. "
            f"Para reprocessar, remova os registros deste período primeiro."
        )
    print(
        f"✓ Guarda anti-duplicata: nenhum registro vigente com "
        f"start_date = {REFERENCE_DATE.strftime('%Y-%m-%d')}"
    )

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

    target_cols = ', '.join(TARGET_COLUMNS)
    source_cols = ', '.join([f'source.{c}' for c in TARGET_COLUMNS])

    spark.sql(f"""
        INSERT INTO {target_table} ({target_cols})
        SELECT {source_cols}
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

# DBTITLE 1,Migrar base existente para lead_time
# ------------------------------------------------------------------------------
# MIGRAR BASE EXISTENTE PARA LEAD_TIME
# ------------------------------------------------------------------------------
# Evolui o schema histórico para incluir a coluna lead_time e faz backfill
# dos registros vigentes a partir do snapshot atual, sem criar versões SCD2
# artificiais somente por causa da mudança de modelagem.
# ------------------------------------------------------------------------------

lead_time_comment = (
    "Prazo de entrega prevista (lead time firme) do material no snapshot SAP. "
    "Origem: coluna prazo_entrega_prevista_lead_time_firme."
)

# Backfill somente quando a coluna acabou de ser criada. Nas cargas mensais
# seguintes a célula é no-op, para não sobrescrever a auditoria (_load_type,
# _load_id) dos registros vigentes gravados pelo merge SCD2.
lead_time_added = "lead_time" not in spark.table(MAIN_TABLE).columns

if lead_time_added:
    spark.sql(
        f"ALTER TABLE {MAIN_TABLE} ADD COLUMNS (lead_time STRING COMMENT '{lead_time_comment}')"
    )
    print(f"✓ Coluna lead_time adicionada em {MAIN_TABLE}")
else:
    print(f"✓ Coluna lead_time já existe em {MAIN_TABLE}; backfill ignorado (no-op).")

if lead_time_added:
  recalc_summary_df = spark.sql(f"""
    WITH stage AS (
        SELECT
            empresa,
            material,
            lead_time,
            sha2(
                concat_ws(
                    '||',
                    coalesce(intercambiabilidade, '<NULL>'),
                    coalesce(item_principal_cadeia, '<NULL>'),
                    coalesce(data_cadeia, '<NULL>'),
                    coalesce(cut_in_material, '<NULL>'),
                    coalesce(cut_off_material, '<NULL>'),
                    coalesce(cadeia, '<NULL>'),
                    coalesce(modelo_comercial_principal, '<NULL>'),
                    coalesce(status_compra, '<NULL>'),
                    coalesce(lead_time, '<NULL>')
                ),
                256
            ) AS recomputed_row_hash
        FROM vw_material_spec_changes_stage
    )
    SELECT
        COUNT(*) AS current_rows_matched_to_stage,
        SUM(CASE WHEN COALESCE(stage.lead_time, '') <> '' THEN 1 ELSE 0 END) AS current_rows_with_lead_time,
        SUM(CASE WHEN COALESCE(target.lead_time, '') <> COALESCE(stage.lead_time, '') THEN 1 ELSE 0 END) AS rows_with_lead_time_delta,
        SUM(CASE WHEN target.row_hash <> stage.recomputed_row_hash THEN 1 ELSE 0 END) AS rows_with_recomputed_hash_delta
    FROM {MAIN_TABLE} target
    INNER JOIN stage
        ON target.empresa = stage.empresa
       AND target.material = stage.material
    WHERE target.is_current = TRUE
""")

  print("Resumo da reavaliação da base vigente:")
  display(recalc_summary_df)

  spark.sql(f"""
    MERGE INTO {MAIN_TABLE} AS target
    USING (
        SELECT
            empresa,
            material,
            lead_time,
            sha2(
                concat_ws(
                    '||',
                    coalesce(intercambiabilidade, '<NULL>'),
                    coalesce(item_principal_cadeia, '<NULL>'),
                    coalesce(data_cadeia, '<NULL>'),
                    coalesce(cut_in_material, '<NULL>'),
                    coalesce(cut_off_material, '<NULL>'),
                    coalesce(cadeia, '<NULL>'),
                    coalesce(modelo_comercial_principal, '<NULL>'),
                    coalesce(status_compra, '<NULL>'),
                    coalesce(lead_time, '<NULL>')
                ),
                256
            ) AS recomputed_row_hash
        FROM vw_material_spec_changes_stage
    ) AS source
        ON target.empresa = source.empresa
       AND target.material = source.material
       AND target.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        target.lead_time = source.lead_time,
        target.row_hash = source.recomputed_row_hash,
        target._last_updated_at = TIMESTAMP '{LOAD_TS.strftime("%Y-%m-%d %H:%M:%S.%f")}',
        target._ingested_by = '{CURRENT_USER}',
        target._load_type = 'schema_evolution_lead_time',
        target._load_id = '{LOAD_ID}',
        target._source_file_path = '{SOURCE_PATH}'
""")

  print(f"✓ Registros vigentes atualizados com lead_time em {MAIN_TABLE}")

  display(
    spark.sql(f"""
        SELECT
            empresa,
            material,
            lead_time,
            is_current,
            start_date,
            end_date
        FROM {MAIN_TABLE}
        WHERE is_current = TRUE
        ORDER BY empresa, material
        LIMIT 5
    """)
  )


# COMMAND ----------

# DBTITLE 1,View para _agents_databases (sem auditoria)
# ------------------------------------------------------------------------------
# VIEW PARA _AGENTS_DATABASES (SEM COLUNAS DE AUDITORIA)
# ------------------------------------------------------------------------------
# Cria uma view sobre a tabela principal, excluindo colunas de auditoria.
# Sem duplicação de dados — sempre sincronizada com a tabela fonte.
# Comentários de coluna são herdados automaticamente da tabela base.
# ------------------------------------------------------------------------------

# Criar schema se não existir
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS parts_hdbk_sandbox._agents_databases
    COMMENT 'Schema para views acessadas por agentes de IA'
""")

AGENTS_VIEW_COMMENT = (
    "View SCD2 do cadastro mestre de materiais SAP para agentes de IA. "
    "Exclui colunas de auditoria. Fonte: "
    f"{MAIN_TABLE}"
)

spark.sql(f"""
    CREATE OR REPLACE VIEW {AGENTS_TABLE}
    COMMENT '{AGENTS_VIEW_COMMENT}'
    AS
    SELECT
        empresa, material,
        intercambiabilidade, item_principal_cadeia, data_cadeia,
        cut_in_material, cut_off_material, cadeia,
        modelo_comercial_principal, status_compra, lead_time,
        row_hash, start_date, end_date, is_current
    FROM {MAIN_TABLE}
""")

print(f"View criada: {AGENTS_TABLE}")
print(f"Fonte: {MAIN_TABLE}")

# COMMAND ----------

# DBTITLE 1,Metadados da tabela histórica principal
# ==============================================================================
# METADADOS DA TABELA HISTÓRICA PRINCIPAL
# ==============================================================================
# Aplica comentários de tabela e colunas, tags de governança e
# propriedades de negócio na tabela material_spec_changes.
# Garante rastreabilidade e documentação no Unity Catalog.
# ==============================================================================

SCD2_COMMENT = """
Histórico SCD2 do cadastro mestre de materiais SAP.

Modelo: Slowly Changing Dimension Type 2 com Hash Comparison
Chave de negócio: empresa + material
Campos rastreados: intercambiabilidade, item_principal_cadeia, data_cadeia,
    cut_in_material, cut_off_material, cadeia, modelo_comercial_principal,
    status_compra, lead_time
Fonte: parts_hdbk_sandbox.pr_cadastrao.material_cadastrao (data de referência: coluna reference_date)
"""

SCD2_COLUMN_COMMENTS = {
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave de negócio histórica.",
    "material": "Código único do material/peça (partnumber SAP). Parte da chave de negócio histórica.",
    "intercambiabilidade": "Indicador de intercambiabilidade do material.",
    "item_principal_cadeia": "Item principal na cadeia de substituição.",
    "data_cadeia": "Data da cadeia de substituição.",
    "cut_in_material": "Material substituto (cut-in) na cadeia.",
    "cut_off_material": "Material descontinuado (cut-off) na cadeia.",
    "cadeia": "Identificador da cadeia de substituição.",
    "modelo_comercial_principal": "Modelo comercial principal associado ao material.",
    "status_compra": "Status de compra do material no SAP.",
    "lead_time": "Prazo de entrega prevista (lead time firme) do material no snapshot SAP.",
    "row_hash": "SHA-256 dos campos rastreados para detecção de mudanças (SCD2).",
    "start_date": "Data de início da versão (SCD2). Sentinel 1900-01-01 na carga inicial.",
    "end_date": "Data de encerramento da versão (SCD2). NULL se vigente.",
    "is_current": "Flag indicando se é a versão corrente do registro.",
    "_ingested_at": "Timestamp da primeira ingestão do registro.",
    "_last_updated_at": "Timestamp da última atualização (merge/update).",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_type": "Tipo de carga executada (ex: scd2_hash).",
    "_load_id": "Identificador único da execução da carga (UUID).",
    "_source_file_path": "Diretório dos arquivos SAP que definem a data de referência da carga (dados lidos de pr_cadastrao.material_cadastrao).",
}

METADATA_VERSION = "4"
table_properties = (
    spark.sql(f"DESCRIBE DETAIL {MAIN_TABLE}")
    .select("properties")
    .first()["properties"]
    or {}
)
current_metadata_version = table_properties.get("scd2_metadata_version")

if current_metadata_version != METADATA_VERSION:
    spark.sql(
        f"COMMENT ON TABLE {MAIN_TABLE} IS "
        f"'{SCD2_COMMENT.replace(chr(39), chr(39) + chr(39))}'"
    )

    for col_name, comment in SCD2_COLUMN_COMMENTS.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {MAIN_TABLE}.{col_name} IS '{escaped}'")

    spark.sql(f"""
        ALTER TABLE {MAIN_TABLE} SET TAGS (
            'domain' = 'materials', 'layer' = 'refined',
            'source' = 'sap', 'history_model' = 'scd2_hash',
            'data_classification' = 'internal'
        )
    """)

    spark.sql(f"""
        ALTER TABLE {MAIN_TABLE} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Materials Master',
            'source_system' = 'SAP',
            'refresh_frequency' = 'monthly_scd2',
            'natural_key' = 'empresa, material',
            'scd2_metadata_version' = '{METADATA_VERSION}'
        )
    """)

    print(f"Metadados versão {METADATA_VERSION} aplicados à tabela {MAIN_TABLE}")
else:
    print(f"Metadados versão {METADATA_VERSION} já aplicados; DDL de governança ignorada.")

# COMMAND ----------

# DBTITLE 1,Metadados da view _agents_databases
# ------------------------------------------------------------------------------
# METADADOS DA VIEW _AGENTS_DATABASES
# ------------------------------------------------------------------------------
# Aplica propriedades e tags na view usada por agentes de IA.
# Comentários de tabela e colunas já foram definidos via CREATE VIEW
# e são herdados da tabela base, respectivamente.
# Aplicado incondicionalmente (views são recriadas a cada execução).
# ------------------------------------------------------------------------------

spark.sql(f"""
    ALTER VIEW {AGENTS_TABLE} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Andre Causs',
        'data_domain' = 'Materials Master',
        'source_system' = 'SAP',
        'natural_key' = 'empresa, material',
        'view_of' = '{MAIN_TABLE}'
    )
""")

print(f"Propriedades aplicadas à view {AGENTS_TABLE}")

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

# Verifica se os metadados já foram aplicados (presença de tags na tabela)
_metadata_already_applied = False
try:
    _tags = spark.sql(f"SELECT tag_name FROM system.information_schema.table_tags WHERE catalog_name = 'parts_hdbk_sandbox' AND schema_name = 'pr_cadastrao' AND table_name = 'material_spec_changes' AND tag_name = 'domain'").count()
    _metadata_already_applied = _tags > 0
except Exception:
    _metadata_already_applied = False

if not _metadata_already_applied:
    print("Metadados ainda não aplicados ou tabela recém-criada. Aplicando...")
else:
    print("Metadados já aplicados anteriormente. Pulando célula. (Para reaplicar, remova as tags da tabela.)")

# Só executa o restante se os metadados ainda não foram aplicados
if not _metadata_already_applied:
    pass  # bloco de guarda — o código real segue abaixo

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
    "lead_time": "Prazo de entrega prevista (lead time firme) do material no snapshot SAP. Campo rastreado por SCD2.",

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
    "_source_file_path": "Diretório dos arquivos SAP que definem a data de referência da carga (dados lidos de pr_cadastrao.material_cadastrao).",
}

# =============================================================================
# 2. COMENTÁRIO DAS TABELAS
# =============================================================================
TABLE_COMMENT = """
Camada Refined do histórico de cadastro de materiais SAP.

Modelo: Slowly Changing Dimension Type 2 (SCD2)
Chave de negócio: empresa + material
Detecção de mudança: Hash Comparison (SHA-256)
Campos rastreados: intercambiabilidade, item_principal_cadeia, data_cadeia, cut_in_material, cut_off_material, cadeia, modelo_comercial_principal, status_compra, lead_time
Atualização: append histórico quando houver mudança nos campos rastreados ou quando a chave aparecer pela primeira vez; encerramento lógico do registro vigente quando a chave deixar de aparecer no novo snapshot
Fonte: parts_hdbk_sandbox.pr_cadastrao.material_cadastrao (data de referência: coluna reference_date)
"""

if not _metadata_already_applied:
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
            'tracked_columns' = 'intercambiabilidade, item_principal_cadeia, data_cadeia, cut_in_material, cut_off_material, cadeia, modelo_comercial_principal, status_compra, lead_time'
        )
    """)

    print(f"Metadados aplicados à tabela {target_table}")

  print(f"\nMetadados completos aplicados às tabelas SCD2: {TARGET_TABLES}")

if _metadata_already_applied:
    print(f"Tabelas SCD2 já possuem metadados: {TARGET_TABLES}")