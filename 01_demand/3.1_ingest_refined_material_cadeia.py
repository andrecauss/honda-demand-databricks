# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.1 — Cadeia de materiais
# MAGIC
# MAGIC - **Propósito:** Definir o item principal da cadeia de substituição de cada material.
# MAGIC - **Entrada:** `pr_cadastrao.material_cadastrao`
# MAGIC - **Saída:** `pr_cadastrao.material_cadeia`
# MAGIC - **Chave:** Empresa + Material · **Carga:** Completa

# COMMAND ----------

# DBTITLE 1,Imports e Função de Limpeza de Sufixo
from pyspark.sql import functions as F


def clean_material_suffix(col, empresa_col):
    """
    Remove sufixo de letra (C/G) precedido de espaços conforme regra por empresa.

    Regras:
      - Empresa 0200: remove " C" (espaço(s) + C) no final
      - Empresa 0500: remove " C" ou " G" (espaço(s) + C/G) no final

    Args:
        col: Coluna Spark contendo o valor a ser limpo
        empresa_col: Coluna Spark contendo o código da empresa

    Returns:
        Coluna Spark com sufixo removido conforme regra
    """
    return (
        F.when(
            (empresa_col == "0200") & col.rlike(r".*\s+C$"),
            F.rtrim(F.regexp_replace(col, r"\s+C$", ""))
        ).when(
            (empresa_col == "0500") & col.rlike(r".*\s+[CG]$"),
            F.rtrim(F.regexp_replace(col, r"\s+[CG]$", ""))
        ).otherwise(col)
    )

# COMMAND ----------

# DBTITLE 1,Extração e Tratamento de Dados
# Selecionar colunas da tabela fonte e tratar item_principal_cadeia
# Regra: se NULL, vazio ou "sim", assume o próprio material
df_cadeia = (
    spark.table("parts_hdbk_sandbox.pr_cadastrao.material_cadastrao")
    .select("empresa", "material", "item_principal_cadeia")
    .withColumn(
        "item_principal_cadeia",
        F.when(
            (F.col("item_principal_cadeia").isNull()) |
            (F.trim(F.col("item_principal_cadeia")) == "") |
            (F.lower(F.trim(F.col("item_principal_cadeia"))) == "sim"),
            F.col("material")
        ).otherwise(F.col("item_principal_cadeia"))
    )
)

# Dados extraídos da tabela fonte

# COMMAND ----------

# DBTITLE 1,Aplicar Limpeza de Sufixo
# Aplicar limpeza de sufixo (" C" / " G") apenas no item_principal_cadeia
df_cadeia = df_cadeia.withColumn(
    "item_principal_cadeia",
    clean_material_suffix(F.col("item_principal_cadeia"), F.col("empresa"))
)

# Limpeza de sufixo aplicada

# COMMAND ----------

# DBTITLE 1,Persistência na Tabela Delta
# Dropar tabela antiga se existir
spark.sql("DROP TABLE IF EXISTS parts_hdbk_sandbox.pr_cadastrao.material_cadeia")

# Criar tabela Delta
df_cadeia.write.saveAsTable("parts_hdbk_sandbox.pr_cadastrao.material_cadeia")

# Tabela criada com sucesso

# COMMAND ----------

# DBTITLE 1,Definir Chave Primária
# MAGIC %sql
# MAGIC -- Definir colunas NOT NULL (requisito para PK)
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ALTER COLUMN empresa SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ALTER COLUMN material SET NOT NULL;
# MAGIC
# MAGIC -- Adicionar constraint de chave primária composta
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ADD CONSTRAINT pk_material_cadeia PRIMARY KEY (empresa, material);

# COMMAND ----------

# DBTITLE 1,Comentários e Metadados
# Aplicar comentários e tags do Unity Catalog na tabela e colunas
REFINED_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_cadeia"

# Comentário da tabela
TABLE_COMMENT = """
Camada Refined da cadeia de substituicao de materiais.
Extraida da tabela raw material_cadastrao com tratamento de sufixo.

Chave Primaria: empresa + material
Atualizacao: Derivada da carga da material_cadastrao

Regras de negocio aplicadas:
  - item_principal_cadeia NULL/vazio/sim -> assume o proprio material
  - Empresa 0200: sufixo ' C' removido do item_principal_cadeia
  - Empresa 0500: sufixo ' C' ou ' G' removido do item_principal_cadeia

Relacionamentos:
  - material -> parts_hdbk_sandbox.pr_cadastrao.material_cadastrao (FK)
  - item_principal_cadeia -> material principal na cadeia de substituicao
"""

spark.sql(f"""
    COMMENT ON TABLE {REFINED_TABLE} IS '{TABLE_COMMENT.replace(chr(39), chr(39)+chr(39))}'
""")

# Comentários nas colunas
COLUMN_COMMENTS = {
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave primária.",
    "material": "Código único do material/peça (partnumber SAP). Parte da chave primária.",
    "item_principal_cadeia": "Material principal na cadeia de substituição. Quando NULL/vazio/sim na origem, assume o próprio material. Sufixos ' C'/' G' removidos conforme regra por empresa.",
}

for column_name, comment in COLUMN_COMMENTS.items():
    escaped_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {REFINED_TABLE}.{column_name} IS '{escaped_comment}'")

# Tags do Unity Catalog
spark.sql(f"""
    ALTER TABLE {REFINED_TABLE} SET TAGS (
        'domain' = 'materials',
        'layer' = 'refined',
        'source' = 'sap',
        'data_classification' = 'internal'
    )
""")

spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN empresa SET TAGS ('business_key' = 'true')")
spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN material SET TAGS ('business_key' = 'true', 'joins_to' = 'material_cadastrao')")
spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN item_principal_cadeia SET TAGS ('business_key' = 'true')")

# Propriedades customizadas
spark.sql(f"""
    ALTER TABLE {REFINED_TABLE} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Andre Causs',
        'data_domain' = 'Material',
        'source_system' = 'SAP',
        'source_table' = 'parts_hdbk_sandbox.pr_cadastrao.material_cadastrao',
        'refresh_frequency' = 'derived_from_refined',
        'primary_key' = 'empresa, material'
    )
""")

print(f"Metadados completos aplicados a tabela {REFINED_TABLE}")
