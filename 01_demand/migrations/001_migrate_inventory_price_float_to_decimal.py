# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Migração 001 — Preço de estoque para DECIMAL(18,2)
# MAGIC
# MAGIC - **Propósito:** Converter o preço histórico de `FLOAT` para `DECIMAL(18,2)` com validação, staging por `SHALLOW CLONE` e restauração automática.
# MAGIC - **Entrada/Saída:** `pr_cadastrao.material_inventory_history`
# MAGIC - **Impacto:** Reescrita única da tabela Delta · **Execução:** Uma vez, antes da nova versão do notebook 2.2

# COMMAND ----------

# DBTITLE 1,Configuração e validação do schema atual
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType
import uuid

TABLE_NAME = "parts_hdbk_sandbox.pr_cadastrao.material_inventory_history"
PRICE_COLUMN = "preco_de_rede_price_de_venda_liquida"
TARGET_TYPE = DecimalType(18, 2)
ROUNDING_TOLERANCE = 0.0001

if not spark.catalog.tableExists(TABLE_NAME):
    print(
        f"A tabela {TABLE_NAME} ainda não existe. "
        "Nenhuma migração é necessária; o notebook 2.2 criará DECIMAL(18,2)."
    )
    dbutils.notebook.exit("TABLE_NOT_FOUND_NO_MIGRATION_REQUIRED")

price_field = next(
    (field for field in spark.table(TABLE_NAME).schema.fields if field.name == PRICE_COLUMN),
    None,
)
if price_field is None:
    raise ValueError(f"A coluna obrigatória {PRICE_COLUMN} não existe em {TABLE_NAME}.")

if price_field.dataType == TARGET_TYPE:
    print(f"{PRICE_COLUMN} já utiliza {TARGET_TYPE.simpleString()}. Nenhuma alteração realizada.")
    dbutils.notebook.exit("ALREADY_DECIMAL_18_2")

if not isinstance(price_field.dataType, (FloatType, DoubleType)):
    raise TypeError(
        f"Tipo atual não suportado para migração automática: {price_field.dataType.simpleString()}. "
        "Esperado: float ou double."
    )

print(f"Tipo atual: {price_field.dataType.simpleString()}")
print(f"Tipo desejado: {TARGET_TYPE.simpleString()}")

# COMMAND ----------

# DBTITLE 1,Validar conversão antes da reescrita
source_df = spark.table(TABLE_NAME)
price_as_double = F.col(PRICE_COLUMN).cast("double")
price_as_decimal = F.col(PRICE_COLUMN).cast(TARGET_TYPE)

validation = source_df.agg(
    F.count(F.lit(1)).alias("total_rows"),
    F.count(F.col(PRICE_COLUMN)).alias("non_null_prices"),
    F.coalesce(
        F.sum(
            F.when(F.col(PRICE_COLUMN).isNotNull() & price_as_decimal.isNull(), 1).otherwise(0)
        ),
        F.lit(0),
    ).alias("invalid_casts"),
    F.coalesce(
        F.sum(
            F.when(
                F.col(PRICE_COLUMN).isNotNull()
                & (F.abs(price_as_double - F.bround(price_as_double, 2)) > F.lit(ROUNDING_TOLERANCE)),
                1,
            ).otherwise(0)
        ),
        F.lit(0),
    ).alias("prices_with_more_than_two_decimals"),
).first()

print(f"Linhas da tabela: {validation['total_rows']}")
print(f"Preços não nulos: {validation['non_null_prices']}")
print(f"Conversões inválidas: {validation['invalid_casts']}")
print(f"Preços com precisão material acima de duas casas: {validation['prices_with_more_than_two_decimals']}")

if validation["invalid_casts"] > 0:
    raise ValueError(
        "A migração foi bloqueada porque existem preços que não cabem em DECIMAL(18,2) "
        "ou não podem ser convertidos."
    )

if validation["prices_with_more_than_two_decimals"] > 0:
    raise ValueError(
        "A migração foi bloqueada porque existem preços com mais de duas casas decimais. "
        "É necessária uma decisão explícita sobre arredondamento antes de prosseguir."
    )

# COMMAND ----------

# DBTITLE 1,Reescrever a tabela com restauração automática
source_version = (
    spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
    .select("version")
    .orderBy(F.desc("version"))
    .first()["version"]
)

table_parts = TABLE_NAME.split(".")
staging_table = (
    f"{table_parts[0]}.{table_parts[1]}."
    f"__migration_001_inventory_price_{uuid.uuid4().hex}"
)
staging_created = False

try:
    # O clone raso materializa uma origem independente sem duplicar fisicamente
    # todos os arquivos Delta e evita ler e sobrescrever a mesma tabela.
    spark.sql(f"CREATE TABLE {staging_table} SHALLOW CLONE {TABLE_NAME}")
    staging_created = True

    migrated_df = spark.table(staging_table).withColumn(
        PRICE_COLUMN,
        F.bround(F.col(PRICE_COLUMN).cast("decimal(38,6)"), 2).cast(TARGET_TYPE),
    )

    (
        migrated_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE_NAME)
    )

    migrated_field = next(
        field
        for field in spark.table(TABLE_NAME).schema.fields
        if field.name == PRICE_COLUMN
    )
    migrated_count = spark.table(TABLE_NAME).count()

    if migrated_field.dataType != TARGET_TYPE:
        raise RuntimeError(
            f"Schema final inesperado: {migrated_field.dataType.simpleString()}."
        )
    if migrated_count != validation["total_rows"]:
        raise RuntimeError(
            f"Contagem divergente após migração: antes={validation['total_rows']}, "
            f"depois={migrated_count}."
        )
except Exception as migration_error:
    restore_error = None
    try:
        current_version = (
            spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
            .select("version")
            .orderBy(F.desc("version"))
            .first()["version"]
        )
        if current_version > source_version:
            print(f"Falha detectada. Restaurando {TABLE_NAME} para a versão {source_version}.")
            spark.sql(f"RESTORE TABLE {TABLE_NAME} TO VERSION AS OF {source_version}")
        else:
            print("A falha ocorreu antes de qualquer commit; nenhuma restauração foi necessária.")
    except Exception as caught_restore_error:
        restore_error = caught_restore_error

    if restore_error is not None:
        raise RuntimeError(
            f"A migração falhou e a restauração automática também falhou. "
            f"A tabela temporária foi preservada em {staging_table}."
        ) from restore_error

    if staging_created:
        spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
    raise RuntimeError("Migração falhou; a tabela original foi preservada ou restaurada.") from migration_error

if staging_created:
    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")

print(
    f"Migração concluída: {TABLE_NAME}.{PRICE_COLUMN} agora utiliza "
    f"{TARGET_TYPE.simpleString()}, preservando {migrated_count} registros."
)
