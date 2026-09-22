# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # 5.2 — Base analítica de demanda
# MAGIC
# MAGIC - **Propósito:** Disponibilizar ordens de venda enriquecidas em uma base analítica unificada.
# MAGIC - **Entradas:** `raw_sales_order`, `material_cadeia`, `knvv_sap` e `kna1_sap`
# MAGIC - **Saída:** `_agents_databases.demand_analytical_base` (view)
# MAGIC - **Granularidade:** Item da ordem de venda · **Carga:** Completa

# COMMAND ----------

# DBTITLE 1,⚙️ Parâmetros Configuráveis
# ==============================================================================
# PARÂMETROS CONFIGURÁVEIS
# ==============================================================================
# Ajuste estes valores conforme necessário para alterar o comportamento do pipeline
# ==============================================================================

# Janela de análise temporal (em meses)
# Define quantos meses fechados de histórico serão incluídos na análise
# Valor padrão: 60 meses (5 anos)

JANELA_MESES = 60

print(f"⚙️ Parâmetros configurados:")
print(f"   • Janela temporal: {JANELA_MESES} meses fechados")

# COMMAND ----------

# DBTITLE 1,Cálculo de Parâmetros Automáticos
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ==============================================================================
# CÁLCULO DE PARÂMETROS TEMPORAIS
# ==============================================================================
# Define janela de análise automática baseada na data mais recente disponível
# nos dados de origem. A janela retroativa é definida pelo parâmetro JANELA_MESES.
# ==============================================================================

# Identifica a data mais recente nas ordens de venda
data_max_df = spark.table("parts_hdbk_sandbox.dt_sales_orders.raw_sales_order").agg({"data": "max"})
data_max_row = data_max_df.collect()[0]
data_max = data_max_row[0]  # Data mais recente

# Define ano e mês de referência com base na data mais recente
if data_max:
    # Converte para datetime se necessário
    if isinstance(data_max, str):
        data_referencia = datetime.strptime(data_max, "%Y-%m-%d")
    else:
        data_referencia = data_max

    ano = data_referencia.year
    mes = data_referencia.month

    # Calcula data_minima: primeiro dia do mês que inicia a janela de JANELA_MESES fechados
    # Ex: se último mês é junho/2026 e JANELA_MESES=48, então julho/2022 até junho/2026
    data_minima_dt = data_referencia - relativedelta(months=JANELA_MESES - 1)
    # Pega o primeiro dia do mês resultante
    data_minima_dt = data_minima_dt.replace(day=1)
    data_minima = data_minima_dt.strftime("%Y-%m-%d")

    # Define como variável Python para uso em células SQL via substituição
    # (spark.conf.set só aceita chaves pré-definidas do Spark)

    print(f"📅 Data de referência (mais recente): {data_referencia.strftime('%Y-%m-%d')}")
    print(f"📅 Ano: {ano}, Mês: {mes}")
    print(f"📅 Data mínima ({JANELA_MESES} meses atrás): {data_minima}")
else:
    raise ValueError("Não foi possível determinar a data mais recente das ordens de venda")

# COMMAND ----------

# DBTITLE 1,Sales Orders com Cadeia e Centro Original
# ==============================================================================
# VIEW: vw_sales_orders
# ==============================================================================
# Enriquece raw sales orders com hierarquia de cadeia de produtos e dados de
# cliente. Serve como camada base para todas as agregações de demanda.
#
# JOINS:
#   1. material_cadeia: mapeia SKU → item_principal_cadeia (família)
#   2. knvv_sap: obtém centro_original (distribuição) por cliente/org/canal
#   3. kna1_sap: obtém dados cadastrais do cliente (razão social, estado, país)
#
# FILTRO TEMPORAL: data >= data_minima (calculada dinamicamente)
# ==============================================================================

# Usa f-string Python para garantir interpolação correta do parâmetro data_minima
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW vw_sales_orders AS
SELECT
  rso.numero_ov,
  rso.item,
  rso.data,
  rso.tipo_ov,
  rso.org_vendas,
  rso.canal_dist,
  rso.emissor_da_ordem,
  rso.centro,
  rso.material,
  rso.quantidade,
  COALESCE(mc.item_principal_cadeia, rso.material) AS item_principal_cadeia,
  k.cen AS centro_original,
  kna.razao_social,
  kna.estado,
  kna.pais,
  CASE WHEN rso.org_vendas = '0200' THEN '2W - Motos'
       WHEN rso.org_vendas = '0500' THEN '4W - Automóveis'
  END AS segmento,
  CASE WHEN rso.canal_dist = '01' THEN 'Doméstico'
       WHEN rso.canal_dist = '02' THEN 'Exportação'
  END AS mercado,
  CASE rso.centro
    WHEN '0203' THEN 'Sumaré 2W'
    WHEN '0503' THEN 'Sumaré 4W'
    WHEN '0209' THEN 'Jaboatão 2W'
    WHEN '0505' THEN 'Jaboatão 4W'
    WHEN '0232' THEN 'Manaus 2W'
  END AS centro_nome
FROM parts_hdbk_sandbox.dt_sales_orders.raw_sales_order rso
LEFT JOIN parts_hdbk_sandbox.pr_cadastrao.material_cadeia mc
  ON rso.material = mc.material
  AND rso.org_vendas = mc.empresa
LEFT JOIN parts_hdbk_sandbox.dm_customers.knvv_sap k
  ON rso.emissor_da_ordem = k.cliente
  AND rso.org_vendas = k.orgv
  AND rso.canal_dist = k.cdst
  AND rso.setor_ativ = k.sa
LEFT JOIN parts_hdbk_sandbox.dm_customers.kna1_sap kna
  ON rso.emissor_da_ordem = kna.cliente
WHERE rso.data >= '{data_minima}'
""")

print(f"✓ View vw_sales_orders criada com filtro: data >= {data_minima}")

# COMMAND ----------

# DBTITLE 1,View demand_analytical_base
from pyspark.sql.functions import col

# ==============================================================================
# MAPEAMENTO DE CAMPOS (nome + tipo)
# ==============================================================================
# Dicionário de-para para renomear e converter tipos das colunas.
# Estrutura: "campo_original": ("novo_nome", "tipo_destino")
#   - novo_nome: nome padronizado da coluna
#   - tipo_destino: tipo Spark para cast (string, int, long, double, date,
#                   timestamp, etc.). Use None para manter o tipo original.
# ==============================================================================

MAPEAMENTO_CAMPOS = {
    # campo_original        : (novo_nome,                      tipo_destino)
    "numero_ov":            ("numero_ordem_venda",             "string"),
    "item":                 ("item_ordem_venda",               "string"),
    "data":                 ("data_ordem",                     "date"),
    "tipo_ov":              ("tipo_ordem_venda",               "string"),
    "org_vendas":           ("organizacao_vendas",             "string"),
    "canal_dist":           ("canal_distribuicao",             "string"),
    "emissor_da_ordem":     ("codigo_cliente",                 "string"),
    "centro":               ("centro_fornecedor",              "string"),
    "material":             ("codigo_material",                "string"),
    "quantidade":           ("quantidade",                     "int"),
    "item_principal_cadeia": ("item_principal_cadeia",          "string"),
    "centro_original":      ("centro_distribuicao_original",   "string"),
    "razao_social":         ("cliente",                        "string"),
    "estado":               ("uf_cliente",                     "string"),
    "pais":                 ("pais_cliente",                   "string"),
    "segmento":             ("segmento",                       "string"),
    "mercado":              ("mercado",                        "string"),
    "centro_nome":          ("centro_nome",                    "string"),
}

# ==============================================================================
# OUTPUT: VIEW demand_analytical_base
# ==============================================================================
# Cria view persistente com joins + mapeamento de campos em SQL puro.
# Sem materialização — sempre sincronizada com as tabelas fonte.
# A janela temporal ({JANELA_MESES} meses) é aplicada dinamicamente via
# subquery sobre a data mais recente de raw_sales_order.
# ==============================================================================

TABELA_DESTINO = "parts_hdbk_sandbox._agents_databases.demand_analytical_base"

VIEW_COMMENT = (
    "Base analítica de demanda para agentes de IA. "
    "Ordens de venda enriquecidas com cadeia de materiais e dados de cliente. "
    f"Janela temporal: {JANELA_MESES} meses."
)

spark.sql(f"""
    CREATE OR REPLACE VIEW {TABELA_DESTINO}
    COMMENT '{VIEW_COMMENT}'
    AS
    SELECT
      CAST(rso.numero_ov AS STRING)              AS numero_ordem_venda,
      CAST(rso.item AS STRING)                   AS item_ordem_venda,
      CAST(rso.data AS DATE)                     AS data_ordem,
      CAST(rso.tipo_ov AS STRING)                AS tipo_ordem_venda,
      CAST(rso.org_vendas AS STRING)             AS organizacao_vendas,
      CAST(rso.canal_dist AS STRING)             AS canal_distribuicao,
      CAST(rso.emissor_da_ordem AS STRING)       AS codigo_cliente,
      CAST(rso.centro AS STRING)                 AS centro_fornecedor,
      CAST(rso.material AS STRING)               AS codigo_material,
      CAST(rso.quantidade AS INT)                AS quantidade,
      CAST(COALESCE(mc.item_principal_cadeia, rso.material) AS STRING)
                                                 AS item_principal_cadeia,
      CAST(k.cen AS STRING)                      AS centro_distribuicao_original,
      CAST(kna.razao_social AS STRING)           AS cliente,
      CAST(kna.estado AS STRING)                 AS uf_cliente,
      CAST(kna.pais AS STRING)                   AS pais_cliente,
      CASE WHEN rso.org_vendas = '0200' THEN '2W - Motos'
           WHEN rso.org_vendas = '0500' THEN '4W - Automóveis'
      END                                        AS segmento,
      CASE WHEN rso.canal_dist = '01' THEN 'Doméstico'
           WHEN rso.canal_dist = '02' THEN 'Exportação'
      END                                        AS mercado,
      CASE rso.centro
        WHEN '0203' THEN 'Sumaré 2W'
        WHEN '0503' THEN 'Sumaré 4W'
        WHEN '0209' THEN 'Jaboatão 2W'
        WHEN '0505' THEN 'Jaboatão 4W'
        WHEN '0232' THEN 'Manaus 2W'
      END                                        AS centro_nome
    FROM parts_hdbk_sandbox.dt_sales_orders.raw_sales_order rso
    LEFT JOIN parts_hdbk_sandbox.pr_cadastrao.material_cadeia mc
      ON rso.material = mc.material AND rso.org_vendas = mc.empresa
    LEFT JOIN parts_hdbk_sandbox.dm_customers.knvv_sap k
      ON rso.emissor_da_ordem = k.cliente
      AND rso.org_vendas = k.orgv
      AND rso.canal_dist = k.cdst
      AND rso.setor_ativ = k.sa
    LEFT JOIN parts_hdbk_sandbox.dm_customers.kna1_sap kna
      ON rso.emissor_da_ordem = kna.cliente
    WHERE rso.data >= date_trunc('month', add_months(
        (SELECT max(data) FROM parts_hdbk_sandbox.dt_sales_orders.raw_sales_order),
        -{JANELA_MESES - 1}
    ))
""")

print(f"✓ View {TABELA_DESTINO} criada/atualizada.")
print(f"  Janela temporal: {JANELA_MESES} meses")
print(f"  Schema:")
for field in spark.table(TABELA_DESTINO).schema.fields:
    print(f"   • {field.name:<30} {field.dataType.simpleString()}")

# COMMAND ----------

# DBTITLE 1,Metadados da view demand_analytical_base
# ==============================================================================
# METADADOS DA VIEW DEMAND_ANALYTICAL_BASE
# ==============================================================================
# Aplica comentários de colunas e propriedades na view.
# Comentário da view já definido no CREATE VIEW.
# Aplicado incondicionalmente (views são recriadas a cada execução).
# ==============================================================================

DAB_COLUMN_COMMENTS = {
    "numero_ordem_venda": "Número da ordem de venda SAP.",
    "item_ordem_venda": "Número do item dentro da ordem de venda.",
    "data_ordem": "Data da ordem de venda.",
    "tipo_ordem_venda": "Tipo da ordem de venda SAP.",
    "organizacao_vendas": "Código da organização de vendas (ex: 0200=2W, 0500=4W).",
    "canal_distribuicao": "Canal de distribuição (01=Doméstico, 02=Exportação).",
    "codigo_cliente": "Código do cliente (emissor da ordem).",
    "centro_fornecedor": "Código do centro fornecedor SAP.",
    "codigo_material": "Código do material/peça (partnumber SAP).",
    "quantidade": "Quantidade solicitada no item da ordem.",
    "item_principal_cadeia": "Material principal na cadeia de substituição.",
    "centro_distribuicao_original": "Centro de distribuição original do cliente.",
    "cliente": "Razão social do cliente.",
    "uf_cliente": "Estado/UF do cliente.",
    "pais_cliente": "País do cliente.",
    "segmento": "Segmento de negócio (2W - Motos / 4W - Automóveis).",
    "mercado": "Mercado de destino (Doméstico / Exportação).",
    "centro_nome": "Nome descritivo do centro de distribuição.",
}

for col_name, comment in DAB_COLUMN_COMMENTS.items():
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {TABELA_DESTINO}.`{col_name}` IS '{escaped}'")

spark.sql(f"""
    ALTER VIEW {TABELA_DESTINO} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Andre Causs',
        'data_domain' = 'Demand Analytics',
        'source_system' = 'SAP',
        'natural_key' = 'numero_ordem_venda, item_ordem_venda'
    )
""")

print(f"Metadados aplicados à view {TABELA_DESTINO}")