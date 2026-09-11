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
# MAGIC - **Saída:** `_agents_databases.demand_analytical_base`
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

# DBTITLE 1,Mapeamento de Campos
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
# APLICA RENOMEAÇÃO E CONVERSÃO DE TIPOS
# ==============================================================================
# Fonte: vw_sales_orders (view criada na célula anterior)
# Lógica: computa colunas disponíveis uma única vez (evita Analyze RPC
# repetido dentro do loop — lint SCPAP001).
# ==============================================================================

df = spark.table("vw_sales_orders")

# Computa schema uma vez antes do loop para evitar RPCs repetidos
colunas_disponiveis = set(df.columns)

colunas_select = [
    (col(col_original).cast(tipo) if tipo else col(col_original)).alias(col_novo)
    for col_original, (col_novo, tipo) in MAPEAMENTO_CAMPOS.items()
    if col_original in colunas_disponiveis
]

df = df.select(colunas_select)

print("✓ Mapeamento aplicado (nome + tipo). Schema resultante:")
for field in df.schema.fields:
    print(f"   • {field.name:<30} {field.dataType.simpleString()}")

display(df.limit(5))

# ==============================================================================
# OUTPUT: demand_analytical_base
# ==============================================================================
# Persiste o DataFrame já mapeado (nomes + tipos) como tabela Delta.
# Saída: parts_hdbk_sandbox._agents_databases.demand_analytical_base
# Mode: overwrite (tabela é recriada a cada execução)
# ==============================================================================

TABELA_DESTINO = "parts_hdbk_sandbox._agents_databases.demand_analytical_base"

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_DESTINO)

print(f"✓ Tabela {TABELA_DESTINO} criada/atualizada.")

# COMMAND ----------

# DBTITLE 1,Metadados da tabela demand_analytical_base
# ==============================================================================
# METADADOS DA TABELA DEMAND_ANALYTICAL_BASE
# ==============================================================================
# Aplica comentários de tabela e colunas, tags de governança e
# propriedades de negócio na tabela _agents_databases.demand_analytical_base.
# Garante rastreabilidade e documentação no Unity Catalog.
# ==============================================================================

DAB_TABLE = TABELA_DESTINO
DAB_COMMENT = """
Base analítica de demanda para agentes AI. Ordens de venda enriquecidas com cadeia de materiais e dados de cliente.

Modelo: Overwrite completo a cada execução
Chave: numero_ordem_venda + item_ordem_venda
Fonte: raw_sales_order + material_cadeia + knvv_sap + kna1_sap (joins)
"""

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

DAB_METADATA_VERSION = "1"

dab_props = (
    spark.sql(f"DESCRIBE DETAIL {DAB_TABLE}")
    .select("properties").first()["properties"] or {}
)

if dab_props.get("dab_metadata_version") != DAB_METADATA_VERSION:
    spark.sql(
        f"COMMENT ON TABLE {DAB_TABLE} IS "
        f"'{DAB_COMMENT.replace(chr(39), chr(39)+chr(39))}'"
    )
    for col_name, comment in DAB_COLUMN_COMMENTS.items():
        escaped = comment.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {DAB_TABLE}.`{col_name}` IS '{escaped}'")

    spark.sql(f"""
        ALTER TABLE {DAB_TABLE} SET TAGS (
            'domain' = 'demand', 'layer' = 'analytical',
            'source' = 'sap', 'history_model' = 'full_overwrite',
            'data_classification' = 'internal'
        )
    """)
    spark.sql(f"""
        ALTER TABLE {DAB_TABLE} SET TBLPROPERTIES (
            'business_owner' = 'Demand Planning',
            'technical_owner' = 'Andre Causs',
            'data_domain' = 'Demand Analytics',
            'source_system' = 'SAP',
            'refresh_frequency' = 'full_overwrite',
            'natural_key' = 'numero_ordem_venda, item_ordem_venda',
            'dab_metadata_version' = '{DAB_METADATA_VERSION}'
        )
    """)
    print(f"Metadados versão {DAB_METADATA_VERSION} aplicados à tabela {DAB_TABLE}")
else:
    print(f"Metadados versão {DAB_METADATA_VERSION} já aplicados em {DAB_TABLE}; DDL ignorada.")