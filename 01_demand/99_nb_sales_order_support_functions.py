# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Manutenção de ordens de venda
# MAGIC
# MAGIC - **Propósito:** Remover manualmente um mês carregado incorretamente antes do reprocessamento.
# MAGIC - **Entrada e saída:** `parts_hdbk_sandbox.dt_sales_orders.raw_sales_order`
# MAGIC - **Filtro:** Ano + mês · **Carga:** Ad hoc, fora do job automático
# MAGIC - **Atenção:** Conferir a contagem da primeira célula antes de executar o `DELETE`.

# COMMAND ----------

# =====================================================================================
# André Causs - 25/07/2026 : Exclusão de um mês inteiro da tabela Delta
# Utilizar para remover dados carregados incorretamente antes de um novo processamento.
# =====================================================================================

from pyspark.sql import functions as F

# Parâmetros
TABELA = "parts_hdbk_sandbox.dt_sales_orders.raw_sales_order"
ANO = 2026
MES = 6

# Visualiza quantidade de registros que serão removidos
df_validacao = spark.sql(f"""
SELECT *
FROM {TABELA}
WHERE YEAR(data) = {ANO}
  AND MONTH(data) = {MES}
""")

print(f"Registros encontrados: {df_validacao.count():,}")

# COMMAND ----------

# =====================================================================================
# André Causs - 25/07/2026 : DELETE do mês selecionado
# =====================================================================================

spark.sql(f"""
DELETE FROM {TABELA}
WHERE YEAR(data) = {ANO}
  AND MONTH(data) = {MES}
""")

print("Exclusão concluída.")
