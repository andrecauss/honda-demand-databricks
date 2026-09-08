# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 2.2 — Snapshot de estoque de materiais
# MAGIC
# MAGIC - **Propósito:** Registrar mensalmente estoques, quantidades e preços dos materiais.
# MAGIC - **Entrada:** `pr_cadastrao/sap_cadastraorefinado/current`
# MAGIC - **Saída:** `pr_cadastrao.material_inventory_history`
# MAGIC - **Chave:** Empresa + Material + Centro + Data de referência · **Carga:** Mensal, substituição atômica por centro

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
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ascii_name)
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean.lower()

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos Excel (.xlsx)
# ==============================================================================
# LEITURA DOS ARQUIVOS EXCEL (.xlsx)
# ==============================================================================
# Carrega todos os arquivos Excel do diretório current/ com inferSchema
# desabilitado (todas as colunas como STRING) para preservar zeros à
# esquerda em códigos SAP. Extrai metadados de arquivo para rastreabilidade.
# ==============================================================================

df_raw = (spark.read
    .format("excel")
    .option("header", "true")
    .option("inferSchema", "false")
    .load("/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/")
)

print(f"Arquivos carregados: {len(df_raw.columns)} colunas")

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
    header_row = df_raw.first()
    original_names = [header_row[i] if header_row[i] else f"col_{i}" for i in range(len(df_raw.columns))]
    sanitized_names = [sanitize_col_name(n) for n in original_names]

    seen = {}
    for i, name in enumerate(sanitized_names):
        if name in seen:
            seen[name] += 1
            sanitized_names[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 0

    df = df_raw.toDF(*sanitized_names)

    header_values = [v for v in [header_row[0], header_row[1], header_row[2]] if v]
    df = df.filter(
        ~(
            (df[sanitized_names[0]] == header_values[0]) &
            (df[sanitized_names[1]] == header_values[1]) &
            (df[sanitized_names[2]] == header_values[2])
        )
    )
else:
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

print(f"DataFrame preparado: {len(df.columns)} colunas")

# COMMAND ----------

# DBTITLE 1,Snapshot mensal de inventário
# ==============================================================================
# SNAPSHOT MENSAL DE INVENTÁRIO
# ==============================================================================
# Agregação: empresa + material + centro (duplicatas idênticas são consolidadas)
# Operação: conversão de tipos, cálculo de stock_total e substituição atômica dos centros processados
# Proteções: chaves obrigatórias, conflito por chave e validação numérica
# Saída: parts_hdbk_sandbox.pr_cadastrao.material_inventory_history
# Nota: reference_date extraída do padrão CAD{centro}_{yyyy}.{MM}.xlsx
# ==============================================================================

from pyspark.sql import functions as F
from datetime import datetime
import uuid

SOURCE_PATH = "/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/"
INVENTORY_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_inventory_history"

# --- Extrair data de referência do nome dos arquivos ---
ref_dates = set()
for fname in source_file_names:
    match = re.search(r'(\d{4})\.(\d{2})', fname)
    if match:
        ref_dates.add(datetime(int(match.group(1)), int(match.group(2)), 1))

if not ref_dates:
    raise ValueError(f"Não foi possível extrair data de referência dos arquivos: {source_file_names}")
if len(ref_dates) > 1:
    raise ValueError(f"Arquivos com datas diferentes na mesma carga: {ref_dates}")

REFERENCE_DATE = ref_dates.pop()

CURRENT_USER = spark.sql("SELECT current_user()").first()[0]
LOAD_ID = str(uuid.uuid4())
LOAD_TS = spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS ts").first()["ts"]

print(f"Data de referência do snapshot: {REFERENCE_DATE.strftime('%Y-%m-%d')}")

# --- Colunas de inventário ---
INVENTORY_COLUMNS = [
    "centro", "material", "empresa",
    "estoque_livre_no_centro", "estoque_disponivel_para_venda",
    "estoque_bloqueado", "estoque_em_transito",
    "estoque_em_poder_de_terceiros", "estoque_em_controle_qualidade",
    "estoque_devolucoes", "saldo_da_carteira_de_pedidos",
    "quantidade_em_pi", "quantidade_em_bo",
    "preco_de_rede_price_de_venda_liquida",
]

MATERIAL_TYPE_COLUMN = "tipo_de_material"
REQUIRED_SOURCE_COLUMNS = INVENTORY_COLUMNS + [MATERIAL_TYPE_COLUMN]

missing_inv = [c for c in REQUIRED_SOURCE_COLUMNS if c not in df.columns]
if missing_inv:
    raise ValueError(f"Colunas de inventário ausentes no DataFrame: {missing_inv}")

# Todas as colunas numéricas são DECIMAL(18,2) para preservar frações
DECIMAL_COLUMNS = [
    "estoque_livre_no_centro", "estoque_disponivel_para_venda",
    "estoque_bloqueado", "estoque_em_transito",
    "estoque_em_poder_de_terceiros", "estoque_em_controle_qualidade",
    "estoque_devolucoes",
    "quantidade_em_pi", "quantidade_em_bo",
    "saldo_da_carteira_de_pedidos",
    "preco_de_rede_price_de_venda_liquida",
]
BUSINESS_KEY_COLUMNS = ["empresa", "material", "centro"]
MATERIAL_TYPES = ["ZHAW", "ZFER", "ZRO1"]

inventory_source_df = (
    df
    .filter(F.col(MATERIAL_TYPE_COLUMN).isin(MATERIAL_TYPES))
    .select(*INVENTORY_COLUMNS)
)
print(f"Filtro de tipo de material aplicado: {MATERIAL_TYPES}")

# Chaves nulas ou vazias nunca devem entrar no histórico. Além de impedir uma
# identificação confiável, valores nulos não se comportam como iguais em joins.
invalid_key_condition = None
for key_column in BUSINESS_KEY_COLUMNS:
    current_condition = F.col(key_column).isNull() | (F.trim(F.col(key_column)) == "")
    invalid_key_condition = (
        current_condition
        if invalid_key_condition is None
        else invalid_key_condition | current_condition
    )

invalid_key_rows = (
    inventory_source_df
    .filter(invalid_key_condition)
    .select(*BUSINESS_KEY_COLUMNS)
    .limit(10)
    .collect()
)
if invalid_key_rows:
    raise ValueError(
        "A carga contém registros com empresa, material ou centro nulo/vazio. "
        f"Amostra: {[row.asDict() for row in invalid_key_rows]}"
    )


def normalize_decimal_string(column_name: str):
    """Normaliza números priorizando a convenção brasileira para milhares."""
    raw_value = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")),
        r"[\s\u00A0R$]",
        "",
    )

    return (
        F.when(raw_value.isNull() | (raw_value == ""), F.lit(None).cast("string"))
        # Notação científica: 1.23E-4 ou 1.23E+4
        .when(
            raw_value.rlike(r"^-?\d+(\.\d+)?[Ee][+-]?\d+$"),
            raw_value,
        )
        # Formato brasileiro com milhar e decimal: 1.234,56
        .when(
            raw_value.rlike(r"^-?\d{1,3}(\.\d{3})+,\d+$"),
            F.regexp_replace(F.regexp_replace(raw_value, r"\.", ""), ",", "."),
        )
        # Formato brasileiro somente com milhar: 1.234 representa 1234.
        # A regra vem antes do ponto decimal para eliminar essa ambiguidade.
        .when(
            raw_value.rlike(r"^-?\d{1,3}(\.\d{3})+$"),
            F.regexp_replace(raw_value, r"\.", ""),
        )
        # Formato internacional com milhar e decimal: 1,234.56
        .when(
            raw_value.rlike(r"^-?\d{1,3}(,\d{3})+\.\d+$"),
            F.regexp_replace(raw_value, ",", ""),
        )
        # Vírgula como separador decimal: 1234,56
        .when(raw_value.rlike(r"^-?\d+,\d+$"), F.regexp_replace(raw_value, ",", "."))
        # Número inteiro ou com ponto decimal: 1234 ou 1234.56
        .when(raw_value.rlike(r"^-?\d+(\.\d+)?$"), raw_value)
        .otherwise(F.lit(None).cast("string"))
    )


parsed_columns = {
    f"__parsed_{column_name}": normalize_decimal_string(column_name).cast("decimal(38,6)")
    for column_name in DECIMAL_COLUMNS
}
parsed_df = inventory_source_df.withColumns(parsed_columns)

# Falha explicitamente em vez de transformar silenciosamente conteúdo inválido
# em NULL. Valores fracionários são preservados como DECIMAL(18,2).
invalid_numeric_condition = None
for column_name in DECIMAL_COLUMNS:
    original = F.trim(F.col(column_name).cast("string"))
    parsed = F.col(f"__parsed_{column_name}")
    target_decimal = F.expr(
        f"try_cast(`__parsed_{column_name}` AS DECIMAL(18,2))"
    )
    current_condition = (
        original.isNotNull()
        & (original != "")
        & (
            parsed.isNull()
            | target_decimal.isNull()
        )
    )
    invalid_numeric_condition = invalid_numeric_condition | current_condition

invalid_numeric_rows = (
    parsed_df
    .filter(invalid_numeric_condition)
    .select(*BUSINESS_KEY_COLUMNS, *DECIMAL_COLUMNS)
    .limit(10)
    .collect()
)
if invalid_numeric_rows:
    raise ValueError(
        "A carga contém quantidades inválidas ou preços incompatíveis com DECIMAL(18,2). "
        f"Amostra: {[row.asDict() for row in invalid_numeric_rows]}"
    )

normalized_df = parsed_df.withColumns({
    column_name: F.round(F.col(f"__parsed_{column_name}"), 2).cast("decimal(18,2)")
    for column_name in DECIMAL_COLUMNS
}).drop(*parsed_columns.keys())

# Duplicatas idênticas são aceitáveis; duas versões diferentes para a mesma
# chave no mesmo snapshot são ambíguas e interrompem a carga.
payload_columns = [c for c in INVENTORY_COLUMNS if c not in BUSINESS_KEY_COLUMNS]
conflicting_keys = (
    normalized_df
    .withColumn(
        "__payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c) for c in payload_columns]),
                {"ignoreNullFields": "false"},
            ),
            256,
        ),
    )
    .groupBy(*BUSINESS_KEY_COLUMNS)
    .agg(F.countDistinct("__payload_hash").alias("payload_versions"))
    .filter(F.col("payload_versions") > 1)
    .limit(10)
    .collect()
)
if conflicting_keys:
    raise ValueError(
        "A carga contém valores divergentes para a mesma combinação "
        "empresa + material + centro. "
        f"Amostra: {[row.asDict() for row in conflicting_keys]}"
    )

inventory_df = normalized_df.dropDuplicates(BUSINESS_KEY_COLUMNS)

STOCK_TOTAL_COLUMNS = [
    "estoque_livre_no_centro", "estoque_bloqueado",
    "estoque_em_transito", "estoque_em_poder_de_terceiros",
    "estoque_em_controle_qualidade",
]

inventory_df = inventory_df.withColumns({
    "stock_total": sum(F.coalesce(F.col(c), F.lit(0).cast("decimal(18,2)")) for c in STOCK_TOTAL_COLUMNS).cast("decimal(18,2)"),
    "reference_date": F.lit(REFERENCE_DATE).cast("date"),
    "_ingested_at": F.lit(LOAD_TS),
    "_ingested_by": F.lit(CURRENT_USER),
    "_load_id": F.lit(LOAD_ID),
    "_source_file_path": F.lit(SOURCE_PATH),
})

TARGET_COLUMNS = [
    "centro", "material", "empresa",
    *DECIMAL_COLUMNS,
    "stock_total", "reference_date",
    "_ingested_at", "_ingested_by", "_load_id", "_source_file_path",
]
inventory_df = inventory_df.select(*TARGET_COLUMNS)

if not inventory_df.limit(1).collect():
    raise ValueError("O snapshot de inventário está vazio; nenhuma tabela foi alterada.")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {INVENTORY_TABLE} (
        centro STRING, material STRING, empresa STRING,
        estoque_livre_no_centro DECIMAL(18,2), estoque_disponivel_para_venda DECIMAL(18,2),
        estoque_bloqueado DECIMAL(18,2), estoque_em_transito DECIMAL(18,2),
        estoque_em_poder_de_terceiros DECIMAL(18,2), estoque_em_controle_qualidade DECIMAL(18,2),
        estoque_devolucoes DECIMAL(18,2), saldo_da_carteira_de_pedidos DECIMAL(18,2),
        quantidade_em_pi DECIMAL(18,2), quantidade_em_bo DECIMAL(18,2),
        preco_de_rede_price_de_venda_liquida DECIMAL(18,2),
        stock_total DECIMAL(18,2),
        reference_date DATE,
        _ingested_at TIMESTAMP, _ingested_by STRING,
        _load_id STRING, _source_file_path STRING
    ) USING DELTA
""")

target_price_type = next(
    field.dataType.simpleString()
    for field in spark.table(INVENTORY_TABLE).schema.fields
    if field.name == "preco_de_rede_price_de_venda_liquida"
)
if target_price_type != "decimal(18,2)":
    raise RuntimeError(
        f"A tabela {INVENTORY_TABLE} ainda utiliza {target_price_type} para o preço. "
        "Execute primeiro migrations/001_migrate_inventory_price_float_to_decimal."
    )

# A substituição com replaceWhere é uma única transação Delta. O predicado
# inclui somente empresa + centro presentes na carga, preservando os demais
# centros do mesmo mês quando o reprocessamento for parcial.
def sql_string_literal(value: str) -> str:
    """Escapa um valor de chave para uso seguro no predicado SQL."""
    return "'" + value.replace("'", "''") + "'"


reference_date_sql = REFERENCE_DATE.strftime("%Y-%m-%d")
processed_scopes = (
    inventory_df
    .select("empresa", "centro")
    .distinct()
    .orderBy("empresa", "centro")
    .collect()
)

scope_predicate = " OR ".join(
    "("
    f"empresa = {sql_string_literal(row['empresa'])} "
    f"AND centro = {sql_string_literal(row['centro'])}"
    ")"
    for row in processed_scopes
)
replace_predicate = (
    f"reference_date = '{reference_date_sql}' "
    f"AND ({scope_predicate})"
)

(
    inventory_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", replace_predicate)
    .saveAsTable(INVENTORY_TABLE)
)

written_count = (
    spark.table(INVENTORY_TABLE)
    .filter(F.expr(replace_predicate))
    .count()
)

processed_scope_labels = [
    f"{row['empresa']}:{row['centro']}"
    for row in processed_scopes
]
print(f"Tabela: {INVENTORY_TABLE}")
print(f"Escopos substituídos em {reference_date_sql}: {processed_scope_labels}")
print(f"Registros gravados nos escopos processados: {written_count}")

# COMMAND ----------

# DBTITLE 1,Metadados da tabela de inventário
# ==============================================================================
# METADADOS DA TABELA DE INVENTÁRIO
# ==============================================================================
# Aplica comentários de tabela e colunas, tags de governança e
# propriedades de negócio na tabela material_inventory_history.
# Garante rastreabilidade e documentação no Unity Catalog.
# ==============================================================================

INVENTORY_COMMENT = """
Snapshot mensal de estoques e preços de materiais SAP.

Modelo: Snapshot mensal substituível por data de referência, empresa e centro
Chave: empresa + material + centro + reference_date
Fonte: /Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/
"""

INVENTORY_COLUMN_COMMENTS = {
    "centro": "Código do centro/depósito SAP.",
    "material": "Código único do material/peça (partnumber SAP).",
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W).",
    "estoque_livre_no_centro": "Quantidade de estoque livre (disponível) no centro.",
    "estoque_disponivel_para_venda": "Quantidade de estoque disponível para venda.",
    "estoque_bloqueado": "Quantidade de estoque bloqueado (indisponível).",
    "estoque_em_transito": "Quantidade de estoque em trânsito entre centros.",
    "estoque_em_poder_de_terceiros": "Quantidade de estoque em poder de terceiros.",
    "estoque_em_controle_qualidade": "Quantidade de estoque em inspeção de qualidade.",
    "estoque_devolucoes": "Quantidade de estoque em devoluções.",
    "saldo_da_carteira_de_pedidos": "Saldo pendente na carteira de pedidos de clientes.",
    "quantidade_em_pi": "Quantidade em Pedidos de Importação (PI).",
    "quantidade_em_bo": "Quantidade em Back Order (BO) - pedidos pendentes.",
    "preco_de_rede_price_de_venda_liquida": "Preço de venda líquida (rede).",
    "stock_total": "Estoque total físico: soma de estoque livre, bloqueado, em trânsito, poder de terceiros e controle de qualidade.",
    "reference_date": "Primeiro dia do mês de referência, extraído do nome do arquivo fonte.",
    "_ingested_at": "Data e hora da ingestão do snapshot.",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_id": "Identificador único da execução da carga (UUID).",
    "_source_file_path": "Caminho do diretório fonte dos arquivos ingeridos.",
}

METADATA_VERSION = "4"
table_properties = (
    spark.sql(f"DESCRIBE DETAIL {INVENTORY_TABLE}")
    .select("properties")
    .first()["properties"]
    or {}
)
current_metadata_version = table_properties.get("inventory_metadata_version")

if current_metadata_version != METADATA_VERSION:
    spark.sql(
        f"COMMENT ON TABLE {INVENTORY_TABLE} IS "
        f"'{INVENTORY_COMMENT.replace(chr(39), chr(39) + chr(39))}'"
    )

    for col_name, comment in INVENTORY_COLUMN_COMMENTS.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {INVENTORY_TABLE}.{col_name} IS '{escaped}'")

    spark.sql(f"""
        ALTER TABLE {INVENTORY_TABLE} SET TAGS (
            'domain' = 'materials', 'layer' = 'refined',
            'source' = 'sap', 'history_model' = 'monthly_snapshot',
            'data_classification' = 'internal'
        )
    """)

    spark.sql(f"""
        ALTER TABLE {INVENTORY_TABLE} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Materials Inventory',
            'source_system' = 'SAP',
            'refresh_frequency' = 'monthly_replace',
            'natural_key' = 'empresa, material, centro, reference_date',
            'inventory_metadata_version' = '{METADATA_VERSION}'
        )
    """)

    print(f"Metadados versão {METADATA_VERSION} aplicados à tabela {INVENTORY_TABLE}")
else:
    print(f"Metadados versão {METADATA_VERSION} já aplicados; DDL de governança ignorada.")