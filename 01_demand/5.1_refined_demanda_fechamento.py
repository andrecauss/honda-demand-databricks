# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.1 — Apuração da demanda
# MAGIC
# MAGIC - **Propósito:** Consolidar a demanda histórica em visões para planejamento e exportação.
# MAGIC - **Entradas:** `dt_sales_orders.raw_sales_order`, `material_cadeia`, `knvv_sap` e `kna1_sap`
# MAGIC - **Saídas:** Tabelas `pr_demand.refined_demand_*`
# MAGIC - **Granularidade:** Varia por visão · **Carga:** Completa, mensal

# COMMAND ----------

# DBTITLE 1,⚙️ Parâmetros Configuráveis
# ==============================================================================
# PARÂMETROS CONFIGURÁVEIS
# ==============================================================================
# Ajuste estes valores conforme necessário para alterar o comportamento do pipeline
# ==============================================================================

# Janela de análise temporal (em meses)
# Define quantos meses fechados de histórico serão incluídos na análise
# Valor padrão: 24 meses (2 anos)
#JANELA_MESES = 24
JANELA_MESES = 60

# Janela específica para "Pedido ZPUG por Cliente" (refined_demand_zpug_cliente_*).
# Separada de JANELA_MESES porque essa agregação é por cliente x item, então o
# volume de linhas cresce muito mais rápido com o histórico — em 24 meses já
# estourava o teto de exportação em arquivo único do notebook 6.2.
JANELA_MESES_ZPUG_CLI = 12

print(f"⚙️ Parâmetros configurados:")
print(f"   • Janela temporal: {JANELA_MESES} meses fechados")
print(f"   • Janela ZPUG por Cliente: {JANELA_MESES_ZPUG_CLI} meses fechados")

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
    # Ex: se último mês é junho/2026 e JANELA_MESES=24, então julho/2024 até junho/2026
    data_minima_dt = data_referencia - relativedelta(months=JANELA_MESES - 1)
    # Pega o primeiro dia do mês resultante
    data_minima_dt = data_minima_dt.replace(day=1)
    data_minima = data_minima_dt.strftime("%Y-%m-%d")

    # Define como variável Python para uso em células SQL via substituição
    # (spark.conf.set só aceita chaves pré-definidas do Spark)

    # Mesma lógica acima, aplicada à janela reduzida de ZPUG por Cliente
    data_minima_zpug_dt = data_referencia - relativedelta(months=JANELA_MESES_ZPUG_CLI - 1)
    data_minima_zpug_dt = data_minima_zpug_dt.replace(day=1)
    data_minima_zpug = data_minima_zpug_dt.strftime("%Y-%m-%d")

    print(f"📅 Data de referência (mais recente): {data_referencia.strftime('%Y-%m-%d')}")
    print(f"📅 Ano: {ano}, Mês: {mes}")
    print(f"📅 Data mínima ({JANELA_MESES} meses atrás): {data_minima}")
    print(f"📅 Data mínima ZPUG por Cliente ({JANELA_MESES_ZPUG_CLI} meses atrás): {data_minima_zpug}")
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
  kna.pais
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

# DBTITLE 1,Funções Auxiliares
from pyspark.sql.functions import date_format, lit
from functools import reduce

# ==============================================================================
# BIBLIOTECA DE FUNÇÕES AUXILIARES E CONSTANTES
# ==============================================================================
# Este módulo contém todas as funções reutilizáveis e constantes para
# processamento de demanda. Garante consistência e padronização em todas as
# agregações.
# ==============================================================================

# Constantes de negócio
MESES_PT = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
            7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"}
PREFIXOS = {"0200": "HDA", "0500": "HAB"}
ABAS_2W = ["TTL", "0203", "0209", "0232"]
ABAS_4W = ["TTL", "0503", "0505"]
SCHEMA = "parts_hdbk_sandbox.pr_demand"

# Base de dados originária (view temporária enriquecida)
df = spark.table("vw_sales_orders")

# ==============================================================================
# FUNÇÕES AUXILIARES DE TRANSFORMAÇÃO
# ==============================================================================

def _nome_arquivo(org_vendas, sufixo):
    """
    Gera nome padronizado de arquivo de exportação.

    Args:
        org_vendas (str): Código da organização de vendas ('0200' ou '0500')
        sufixo (str): Tipo de demanda (ex: 'Demanda Fechada', 'Demanda Aberta')

    Returns:
        str: Nome formatado (ex: 'HDA 2026 Jul Demanda Fechada')

    Example:
        >>> _nome_arquivo('0200', 'Demanda Fechada')
        'HDA 2026 Jul Demanda Fechada'
    """
    prefixo = PREFIXOS.get(org_vendas, "OUT")
    mes_abrev = MESES_PT.get(mes, "")
    return f"{prefixo} {ano} {mes_abrev} {sufixo}"


def _pivot(df_filtrado, colunas_grupo, operacao="soma"):
    """
    Aplica pivoteamento temporal com agregação por período (yyyy/MM).

    Transforma linhas de transações em colunas mensais, permitindo análise
    de tendência ao longo do tempo. Cada coluna representa um mês/ano.

    Args:
        df_filtrado (DataFrame): DataFrame pré-filtrado com dados de vendas
        colunas_grupo (list): Colunas de agrupamento (ex: ['item_principal_cadeia'])
        operacao (str): Tipo de agregação - 'soma' (sum) ou 'contagem' (count)

    Returns:
        DataFrame: DataFrame pivoteado com colunas yyyy/MM e valores INTEGER

    Example:
        >>> df_pivot = _pivot(df, ['material'], operacao='soma')
        >>> # Resultado: | material | 2024/01 | 2024/02 | 2024/03 | ...
    """
    df_fmt = df_filtrado.withColumn("data_aaaa_mm", date_format("data", "yyyy/MM"))
    if operacao == "soma":
        df_pivot = df_fmt.groupBy(colunas_grupo).pivot("data_aaaa_mm").agg({"quantidade": "sum"})
    else:
        df_pivot = df_fmt.groupBy(colunas_grupo).pivot("data_aaaa_mm").agg({"quantidade": "count"})

    # Converte todas as colunas de data (yyyy/MM) para INT
    df_pivot = df_pivot.fillna(0)
    for col_name in df_pivot.columns:
        if col_name not in colunas_grupo:
            df_pivot = df_pivot.withColumn(col_name, df_pivot[col_name].cast("int"))

    return df_pivot


TABELAS_BASE = [
    "refined_demand_fechada",
    "refined_demand_fechada_sem_zesp",
    "refined_demand_aberta",
    "refined_demand_linha",
    "refined_demand_mi",
    "refined_demand_me",
    "refined_demand_me_sem_zesp",
    "refined_demand_zpug",
    "refined_demand_zpug_cliente",
    "refined_demand_distribuicao",
]


def _limpar_tabelas():
    """
    Remove todas as tabelas refinadas existentes para recriação limpa.

    Garante que cada execução do notebook parta de um estado limpo,
    evitando dados duplicados ou inconsistências de schema. Busca e remove
    automaticamente todas as tabelas cujos nomes começam com os prefixos
    definidos em TABELAS_BASE.

    Side Effects:
        - Dropa tabelas Delta no schema parts_hdbk_sandbox.pr_demand
        - Exibe mensagens de progresso no console
    """
    # Lista todas as tabelas no schema que começam com os prefixos das tabelas base
    tabelas_existentes = spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()
    for row in tabelas_existentes:
        nome_tabela = row.tableName
        # Verifica se a tabela começa com algum dos prefixos base
        if any(nome_tabela.startswith(base) for base in TABELAS_BASE):
            spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{nome_tabela}")
            print(f"✖ {SCHEMA}.{nome_tabela} removida")
    print("\n✓ Todas as tabelas limpas. Pronto para append.")


def _append(df_resultado, tabela):
    """
    Persiste DataFrame em tabela Delta com suporte a evolução de schema.

    Cria a tabela se não existir (com column mapping habilitado) ou
    adiciona linhas via append se já existir. Column mapping permite
    renomeação de colunas sem reescrita de dados.

    Args:
        df_resultado (DataFrame): DataFrame a ser persistido
        tabela (str): Nome da tabela (sem schema, ex: 'refined_demand_fechada_HDA')

    Side Effects:
        - Cria ou atualiza tabela Delta em parts_hdbk_sandbox.pr_demand
        - Exibe contagem de linhas no console
    """
    full_name = f"{SCHEMA}.{tabela}"
    if not spark.catalog.tableExists(full_name):
        df_resultado.createOrReplaceTempView("_tmp_write")
        spark.sql(f"""
            CREATE TABLE {full_name}
            TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
            AS SELECT * FROM _tmp_write
        """)
        print(f"✓ {full_name} criada ({df_resultado.count()} linhas)")
    else:
        df_resultado.write.mode("append").option("mergeSchema", "true").saveAsTable(full_name)
        print(f"✓ {full_name} append ({df_resultado.count()} linhas)")


def _reordenar(df_resultado, cols_id):
    """
    Reordena colunas do DataFrame para layout padronizado de exportação.

    Garante que as colunas fixas (file, sheet, identificadores de negócio)
    aparecem primeiro, seguidas pelas colunas temporais em ordem cronológica.

    Args:
        df_resultado (DataFrame): DataFrame pivoteado a ser reordenado
        cols_id (list): Colunas de identificação (ex: ['Item Principal Cadeia'])

    Returns:
        DataFrame: DataFrame com colunas ordenadas (file, sheet, IDs, datas)
    """
    fixas = ["file", "sheet"] + cols_id
    datas = sorted([c for c in df_resultado.columns if c not in fixas])
    return df_resultado.select(fixas + datas)


print("Auxiliares carregadas.")

# COMMAND ----------

# DBTITLE 1,Limpar tabelas de saída
# ==============================================================================
# LIMPEZA PRÉVIA DO SCHEMA DE SAÍDA
# ==============================================================================
# Remove todas as tabelas refined_demand_* existentes para garantir consistência
# e evitar acumulação de dados entre execuções. Esta é uma operação destrutiva
# que deve ser executada apenas em ambientes controlados.
# ==============================================================================

_limpar_tabelas()

# COMMAND ----------

# DBTITLE 1,Segmento 2W HDA
# ==============================================================================
# SEGMENTO 2W - HDA (MOTOS)
# ==============================================================================
# org_vendas: 0200
# Centros de Distribuição:
#   • 0203 - Sumaré (SP)
#   • 0209 - Jaboatão (PE)
#   • 0232 - Manaus (AM)
#   • TTL  - Total consolidado dos 3 centros
#
# Este bloco gera 10 conjuntos de tabelas refinadas:
#   1. Demanda Fechada (item_principal_cadeia)
#   2. Demanda Fechada sem ZESP (tipo_ov != 'ZESP')
#   3. Demanda Aberta (material/SKU)
#   4. Demanda Linha (contagem por item_principal_cadeia)
#   5. Demanda MI - Mercado Interno (canal_dist='01')
#   6. Demanda ME - Mercado Externo (canal_dist='02')
#   7. Demanda ME sem ZESP (canal_dist='02' AND tipo_ov != 'ZESP')
#   8. Pedido ZPUG (tipo_ov='ZPUG')
#   9. Pedido ZPUG por Cliente (tipo_ov='ZPUG' + emissor_da_ordem)
#  10. Distribuição (análise percentual por centro e mercado)
# ==============================================================================

print("🏍️ Iniciando processamento HDA (2W Motos)...")

# COMMAND ----------

# DBTITLE 1,HDA Demanda Fechada
# ------------------------------------------------------------------------------
# HDA - DEMANDA FECHADA
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200' (2W Motos)
# Saída: refined_demand_fechada_HDA
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada")

from pyspark.sql.functions import to_date
df_base = df.filter((df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_fechada_HDA"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda Fechada sem ZESP
# ------------------------------------------------------------------------------
# HDA - DEMANDA FECHADA SEM ZESP
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200' AND tipo_ov != 'ZESP'
# Saída: refined_demand_fechada_sem_zesp_HDA
# Nota: Exclui tipo_ov='ZESP' (Pedido Inicial de Exportação)
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada sem ZESP")

df_base = df.filter((df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.tipo_ov != "ZESP"))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_fechada_sem_zesp_HDA"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda Aberta
# ------------------------------------------------------------------------------
# HDA - DEMANDA ABERTA
# ------------------------------------------------------------------------------
# Agregação: material (SKU individual)
# Operação: Soma de quantidades
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200'
# Saída: refined_demand_aberta_HDA
# Nota: Demanda aberta é detalhada por SKU, não por família
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda Aberta")

df_base = df.filter((df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["material"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("material", "Material")
    df_pivot = _reordenar(df_pivot, ["Material"])

    tabela_completa = "refined_demand_aberta_HDA"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda Linha
# ------------------------------------------------------------------------------
# HDA - DEMANDA LINHA
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Contagem de linhas (pedidos)
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200'
# Saída: refined_demand_linha_HDA
# Nota: Mede volume de pedidos, não quantidade solicitada
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda Linha")

df_base = df.filter((df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"], operacao="contagem")
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_linha_HDA"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda MI
# ------------------------------------------------------------------------------
# HDA - DEMANDA MI (MERCADO INTERNO)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200' AND canal_dist='01' (Mercado Interno)
# Saída: refined_demand_mi_HDA
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda MI")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "01")
)

# Processa cada aba e salva na mesma tabela
for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_mi_HDA"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda ME
# ------------------------------------------------------------------------------
# HDA - DEMANDA ME (MERCADO EXTERNO)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (exportação não é segregada por centro)
# Filtros: org_vendas='0200' AND canal_dist='02' (Mercado Externo)
# Saída: refined_demand_me_HDA
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda ME")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "02")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_me_HDA"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Demanda ME sem ZESP
# ------------------------------------------------------------------------------
# HDA - DEMANDA ME SEM ZESP (MERCADO EXTERNO SEM ZESP)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (exportação não é segregada por centro)
# Filtros: org_vendas='0200' AND canal_dist='02' AND tipo_ov != 'ZESP'
# Saída: refined_demand_me_sem_zesp_HDA
# Nota: Exclui tipo_ov='ZESP' (Pedido Inicial de Exportação)
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda ME sem ZESP")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "02") & (df.tipo_ov != "ZESP")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_me_sem_zesp_HDA"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Pedido ZPUG
# ------------------------------------------------------------------------------
# HDA - PEDIDO ZPUG
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia
# Operação: Soma de quantidades
# Centros: TTL apenas (ZPUG não é segregado por centro)
# Filtros: org_vendas='0200' AND tipo_ov='ZPUG'
# Saída: refined_demand_zpug_HDA
# Nota: ZPUG = Pedido Urgente de Garantia (tipo especial de ordem prioritária)
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_zpug_HDA"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Pedido ZPUG por Cliente
# ------------------------------------------------------------------------------
# HDA - PEDIDO ZPUG POR CLIENTE
# ------------------------------------------------------------------------------
# Agregação: emissor_da_ordem + item_principal_cadeia (cliente x família)
# Operação: Soma de quantidades
# Centros: TTL apenas
# Filtros: org_vendas='0200' AND tipo_ov='ZPUG'
# Saída: refined_demand_zpug_cliente_HDA
# Nota: Quebra ZPUG (Pedido Urgente de Garantia) por cliente para análise
#       individualizada de demanda prioritária
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG Cliente")

df_base = df.filter(
    (df.data >= data_minima_zpug) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["emissor_da_ordem", "item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = df_pivot.withColumnRenamed("emissor_da_ordem", "Cliente")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia", "Cliente"])

tabela_completa = "refined_demand_zpug_cliente_HDA"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HDA Distribuição por Centro e Mercado
from pyspark.sql.functions import sum as _sum, col, round as _round, when
from operator import add
from datetime import date
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------------
# HDA - DISTRIBUIÇÃO POR CENTRO E MERCADO
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma + Cálculo de percentuais de distribuição
# Centros: TTL apenas (visão consolidada)
# Períodos:
#   - Centros: últimos 6 meses (janela móvel)
#   - Mercado (MI/ME): últimos 12 meses (janela móvel)
# Saída: refined_demand_distribuicao_HDA
# Colunas finais: DMD_{centro}, %_{centro}, DMD_{MI/ME}, %_{MI/ME}
# Nota: Permite análise de concentração/distribuição geográfica e de mercado
# ------------------------------------------------------------------------------
ref_date = date(ano, mes, 1)
data_fim = ref_date + relativedelta(months=1) - relativedelta(days=1)
data_6m = (ref_date - relativedelta(months=5)).strftime("%Y-%m-%d")
data_12m = (ref_date - relativedelta(months=11)).strftime("%Y-%m-%d")
data_fim_str = data_fim.strftime("%Y-%m-%d")

print(f"Centro: {data_6m} a {data_fim_str} (6 meses)")
print(f"Mercado: {data_12m} a {data_fim_str} (12 meses)")


def _calc_centro(df_seg, periodo_inicio, periodo_fim):
    """Pivot de centro com % para um segmento."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("centro_original")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])

    # Converte colunas de quantidade para INT antes de calcular percentuais
    for c in cols:
        df_pivot = df_pivot.withColumn(c, col(c).cast("int"))

    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


def _calc_mercado(df_seg, periodo_inicio, periodo_fim):
    """Pivot de mercado (MI/ME) com %."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("canal_dist")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    mercado_map = {"01": "MI", "02": "ME"}
    for old_name, new_name in mercado_map.items():
        if old_name in df_pivot.columns:
            df_pivot = df_pivot.withColumnRenamed(old_name, new_name)
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])

    # Converte colunas de quantidade para INT antes de calcular percentuais
    for c in cols:
        df_pivot = df_pivot.withColumn(c, col(c).cast("int"))

    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


# --- HDA (2W) ---
df_hda = df.filter(df.org_vendas == "0200")
df_centro_hda, centro_hda_cols = _calc_centro(df_hda, data_6m, data_fim_str)
df_mercado_hda, mercado_hda_cols = _calc_mercado(df_hda, data_12m, data_fim_str)

# --- HDA: JOIN Centros HDA + Mercado HDA ---
resultado_hda = (
    df_centro_hda
    .join(df_mercado_hda, "item_principal_cadeia", "full")
    .fillna(0)
)
resultado_hda = resultado_hda.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")

# --- Ordem HDA: Centros HDA | % HDA | Mercado HDA | % Mercado HDA ---
dmd_hda = [f"DMD_{c}" for c in centro_hda_cols]
pct_hda = [f"%_{c}" for c in centro_hda_cols]
dmd_mercado_hda = [f"DMD_{c}" for c in mercado_hda_cols]
pct_mercado_hda = [f"%_{c}" for c in mercado_hda_cols]

arquivo_hda = f"HDA {ano} {MESES_PT.get(mes, '')} Distribuição"
resultado_hda = resultado_hda.withColumn("file", lit(arquivo_hda)).withColumn("sheet", lit("TTL"))

cols_final_hda = ["file", "sheet", "Item Principal Cadeia"] + dmd_hda + pct_hda + dmd_mercado_hda + pct_mercado_hda
resultado_hda = resultado_hda.select(cols_final_hda)

# Salva tabela HDA
tabela_completa_hda = "refined_demand_distribuicao_HDA"
_append(resultado_hda, tabela_completa_hda)

# COMMAND ----------

# DBTITLE 1,Segmento 4W HAB
# ==============================================================================
# SEGMENTO 4W - HAB (AUTOMÓVEIS)
# ==============================================================================
# org_vendas: 0500
# Centros de Distribuição:
#   • 0503 - Sumaré (SP)
#   • 0505 - Jaboatão (PE)
#   • TTL  - Total consolidado dos 2 centros
#
# Este bloco gera 10 conjuntos de tabelas refinadas:
#   1. Demanda Fechada (item_principal_cadeia)
#   2. Demanda Fechada sem ZESP (tipo_ov != 'ZESP')
#   3. Demanda Aberta (material/SKU)
#   4. Demanda Linha (contagem por item_principal_cadeia)
#   5. Demanda MI - Mercado Interno (canal_dist='01')
#   6. Demanda ME - Mercado Externo (canal_dist='02')
#   7. Demanda ME sem ZESP (canal_dist='02' AND tipo_ov != 'ZESP')
#   8. Pedido ZPUG (tipo_ov='ZPUG')
#   9. Pedido ZPUG por Cliente (tipo_ov='ZPUG' + emissor_da_ordem)
#  10. Distribuição (análise percentual por centro e mercado)
# ==============================================================================

print("🚗 Iniciando processamento HAB (4W Automóveis)...")

# COMMAND ----------

# DBTITLE 1,HAB Demanda Fechada
# ------------------------------------------------------------------------------
# HAB - DEMANDA FECHADA
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0503, 0505
# Filtros: org_vendas='0500' (4W Autos)
# Saída: refined_demand_fechada_HAB
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_4W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_fechada_HAB"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda Fechada sem ZESP
# ------------------------------------------------------------------------------
# HAB - DEMANDA FECHADA SEM ZESP
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0503, 0505
# Filtros: org_vendas='0500' AND tipo_ov != 'ZESP'
# Saída: refined_demand_fechada_sem_zesp_HAB
# Nota: Exclui tipo_ov='ZESP' (Pedido Inicial de Exportação)
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada sem ZESP")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov != "ZESP"))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_4W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_fechada_sem_zesp_HAB"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda Aberta
# ------------------------------------------------------------------------------
# HAB - DEMANDA ABERTA
# ------------------------------------------------------------------------------
# Agregação: material (SKU individual)
# Operação: Soma de quantidades
# Centros: TTL, 0503, 0505
# Filtros: org_vendas='0500'
# Saída: refined_demand_aberta_HAB
# Nota: Demanda aberta é detalhada por SKU, não por família
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda Aberta")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_4W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["material"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("material", "Material")
    df_pivot = _reordenar(df_pivot, ["Material"])

    tabela_completa = "refined_demand_aberta_HAB"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda Linha
# ------------------------------------------------------------------------------
# HAB - DEMANDA LINHA
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Contagem de linhas (pedidos)
# Centros: TTL, 0503, 0505
# Filtros: org_vendas='0500'
# Saída: refined_demand_linha_HAB
# Nota: Mede volume de pedidos, não quantidade solicitada
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda Linha")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

# Processa cada aba e salva na mesma tabela
for aba in ABAS_4W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"], operacao="contagem")
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
    df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

    tabela_completa = "refined_demand_linha_HAB"
    _append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda MI
# ------------------------------------------------------------------------------
# HAB - DEMANDA MI (MERCADO INTERNO)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (MI não é segregado por centro)
# Filtros: org_vendas='0500' AND canal_dist='01' (Mercado Interno)
# Saída: refined_demand_mi_HAB
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda MI")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "01")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_mi_HAB"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda ME
# ------------------------------------------------------------------------------
# HAB - DEMANDA ME (MERCADO EXTERNO)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (exportação não é segregada por centro)
# Filtros: org_vendas='0500' AND canal_dist='02' (Mercado Externo)
# Saída: refined_demand_me_HAB
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda ME")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "02")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_me_HAB"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Demanda ME sem ZESP
# ------------------------------------------------------------------------------
# HAB - DEMANDA ME SEM ZESP (MERCADO EXTERNO SEM ZESP)
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (exportação não é segregada por centro)
# Filtros: org_vendas='0500' AND canal_dist='02' AND tipo_ov != 'ZESP'
# Saída: refined_demand_me_sem_zesp_HAB
# Nota: Exclui tipo_ov='ZESP' (Pedido Inicial de Exportação)
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Demanda ME sem ZESP")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.canal_dist == "02") & (df.tipo_ov != "ZESP")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_me_sem_zesp_HAB"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Pedido ZPUG
# ------------------------------------------------------------------------------
# HAB - PEDIDO ZPUG
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL apenas (ZPUG não é segregado por centro)
# Filtros: org_vendas='0500' AND tipo_ov='ZPUG'
# Saída: refined_demand_zpug_HAB
# Nota: ZPUG = Pedido Urgente de Garantia (tipo especial de ordem prioritária)
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima))) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia"])

tabela_completa = "refined_demand_zpug_HAB"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Pedido ZPUG por Cliente
# ------------------------------------------------------------------------------
# HAB - PEDIDO ZPUG POR CLIENTE
# ------------------------------------------------------------------------------
# Agregação: emissor_da_ordem + item_principal_cadeia (cliente x família)
# Operação: Soma de quantidades
# Centros: TTL apenas
# Filtros: org_vendas='0500' AND tipo_ov='ZPUG'
# Saída: refined_demand_zpug_cliente_HAB
# Nota: Quebra ZPUG (Pedido Urgente de Garantia) por cliente para análise
#       individualizada de demanda prioritária
# ------------------------------------------------------------------------------
org_vendas = "0500"
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG Cliente")

df_base = df.filter(
    (df.data >= to_date(lit(data_minima_zpug))) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

# Processa aba TTL e salva na tabela
aba = "TTL"
df_pivot = _pivot(df_base, ["emissor_da_ordem", "item_principal_cadeia"])
df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
df_pivot = df_pivot.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
df_pivot = df_pivot.withColumnRenamed("emissor_da_ordem", "Cliente")
df_pivot = _reordenar(df_pivot, ["Item Principal Cadeia", "Cliente"])

tabela_completa = "refined_demand_zpug_cliente_HAB"
_append(df_pivot, tabela_completa)

# COMMAND ----------

# DBTITLE 1,HAB Distribuição por Centro e Mercado
from pyspark.sql.functions import sum as _sum, col, round as _round, when
from operator import add
from datetime import date
from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------------
# HAB - DISTRIBUIÇÃO POR CENTRO E MERCADO
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma + Cálculo de percentuais de distribuição
# Centros: TTL apenas (visão consolidada)
# Períodos:
#   - Centros: últimos 6 meses (janela móvel)
#   - Mercado (MI/ME): últimos 12 meses (janela móvel)
# Saída: refined_demand_distribuicao_HAB
# Colunas finais: DMD_{centro}, %_{centro}, DMD_{MI/ME}, %_{MI/ME}
# Nota: Permite análise de concentração/distribuição geográfica e de mercado
# ------------------------------------------------------------------------------
ref_date = date(ano, mes, 1)
data_fim = ref_date + relativedelta(months=1) - relativedelta(days=1)
data_6m = (ref_date - relativedelta(months=5)).strftime("%Y-%m-%d")
data_12m = (ref_date - relativedelta(months=11)).strftime("%Y-%m-%d")
data_fim_str = data_fim.strftime("%Y-%m-%d")

print(f"Centro: {data_6m} a {data_fim_str} (6 meses)")
print(f"Mercado: {data_12m} a {data_fim_str} (12 meses)")


def _calc_centro(df_seg, periodo_inicio, periodo_fim):
    """Pivot de centro com % para um segmento."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("centro_original")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])

    # Converte colunas de quantidade para INT antes de calcular percentuais
    for c in cols:
        df_pivot = df_pivot.withColumn(c, col(c).cast("int"))

    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


def _calc_mercado(df_seg, periodo_inicio, periodo_fim):
    """Pivot de mercado (MI/ME) com %."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("canal_dist")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    mercado_map = {"01": "MI", "02": "ME"}
    for old_name, new_name in mercado_map.items():
        if old_name in df_pivot.columns:
            df_pivot = df_pivot.withColumnRenamed(old_name, new_name)
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])

    # Converte colunas de quantidade para INT antes de calcular percentuais
    for c in cols:
        df_pivot = df_pivot.withColumn(c, col(c).cast("int"))

    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


# --- HAB (4W) ---
df_hab = df.filter(df.org_vendas == "0500")
df_centro_hab, centro_hab_cols = _calc_centro(df_hab, data_6m, data_fim_str)
df_mercado_hab, mercado_hab_cols = _calc_mercado(df_hab, data_12m, data_fim_str)

# --- HAB: JOIN Centros HAB + Mercado HAB ---
resultado_hab = (
    df_centro_hab
    .join(df_mercado_hab, "item_principal_cadeia", "full")
    .fillna(0)
)
resultado_hab = resultado_hab.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")

# --- Ordem HAB: Centros HAB | % HAB | Mercado HAB | % Mercado HAB ---
dmd_hab = [f"DMD_{c}" for c in centro_hab_cols]
pct_hab = [f"%_{c}" for c in centro_hab_cols]
dmd_mercado_hab = [f"DMD_{c}" for c in mercado_hab_cols]
pct_mercado_hab = [f"%_{c}" for c in mercado_hab_cols]

arquivo_hab = f"HAB {ano} {MESES_PT.get(mes, '')} Distribuição"
resultado_hab = resultado_hab.withColumn("file", lit(arquivo_hab)).withColumn("sheet", lit("TTL"))

cols_final_hab = ["file", "sheet", "Item Principal Cadeia"] + dmd_hab + pct_hab + dmd_mercado_hab + pct_mercado_hab
resultado_hab = resultado_hab.select(cols_final_hab)

# Salva tabela HAB
tabela_completa_hab = "refined_demand_distribuicao_HAB"
_append(resultado_hab, tabela_completa_hab)

# COMMAND ----------

# DBTITLE 1,📊 Diagnóstico Final - Tabelas Exportadas
# ==============================================================================
# DIAGNÓSTICO FINAL - VALIDAÇÃO DAS TABELAS EXPORTADAS
# ==============================================================================
# Lista todas as tabelas refined_demand_* criadas no schema pr_demand,
# exibindo contagens e organizando por segmento e tipo de demanda.
# ==============================================================================

from pyspark.sql.functions import col, count, lit
import pandas as pd

print("="*80)
print("📊 DIAGNÓSTICO FINAL - TABELAS REFINADAS DE DEMANDA")
print("="*80)
print(f"Schema: {SCHEMA}")
print(f"Período: {data_minima} até {data_referencia.strftime('%Y-%m-%d')} ({JANELA_MESES} meses)\n")

# Lista todas as tabelas no schema
tabelas = spark.sql(f"SHOW TABLES IN {SCHEMA}").filter(
    col("tableName").startswith("refined_demand_")
).collect()

if not tabelas:
    print("⚠️  Nenhuma tabela encontrada. Execute o notebook completamente.")
else:
    # Coleta informações de cada tabela
    dados_tabelas = []
    for row in tabelas:
        nome_tabela = row.tableName
        full_name = f"{SCHEMA}.{nome_tabela}"

        # Conta linhas
        try:
            num_linhas = spark.table(full_name).count()

            # Extrai segmento (HDA ou HAB)
            if "_HDA_" in nome_tabela:
                segmento = "HDA (2W)"
            elif "_HAB_" in nome_tabela:
                segmento = "HAB (4W)"
            else:
                segmento = "Outro"

            # Extrai tipo de demanda
            tipo = nome_tabela.replace("refined_demand_", "").split("_HDA_")[0].split("_HAB_")[0]
            tipo = tipo.replace("_", " ").title()

            # Extrai centro
            if "_TTL" in nome_tabela:
                centro = "TTL"
            elif "_0203" in nome_tabela:
                centro = "0203"
            elif "_0209" in nome_tabela:
                centro = "0209"
            elif "_0232" in nome_tabela:
                centro = "0232"
            elif "_0503" in nome_tabela:
                centro = "0503"
            elif "_0505" in nome_tabela:
                centro = "0505"
            else:
                centro = "-"

            dados_tabelas.append({
                "Segmento": segmento,
                "Tipo Demanda": tipo,
                "Centro": centro,
                "Nome Tabela": nome_tabela,
                "Linhas": num_linhas
            })
        except Exception as e:
            print(f"⚠️  Erro ao processar {nome_tabela}: {e}")

    # Converte para DataFrame Pandas para exibição formatada
    df_diagnostico = pd.DataFrame(dados_tabelas)
    df_diagnostico = df_diagnostico.sort_values(["Segmento", "Tipo Demanda", "Centro"])

    # Exibe por segmento
    for segmento in df_diagnostico["Segmento"].unique():
        print(f"\n{'='*80}")
        print(f"🏍️  {segmento}" if "2W" in segmento else f"🚗  {segmento}")
        print(f"{'='*80}")

        df_seg = df_diagnostico[df_diagnostico["Segmento"] == segmento]

        for tipo in df_seg["Tipo Demanda"].unique():
            df_tipo = df_seg[df_seg["Tipo Demanda"] == tipo]
            total_linhas = df_tipo["Linhas"].sum()
            num_tabelas = len(df_tipo)

            print(f"\n  📋 {tipo}:")
            print(f"     Tabelas: {num_tabelas}")

            for _, row in df_tipo.iterrows():
                print(f"     • {row['Centro']:4s} → {row['Nome Tabela']:60s} ({row['Linhas']:,} linhas)")

            print(f"     ─────────────────────────────────────────────────────────────────────")
            print(f"     TOTAL {tipo}: {total_linhas:,} linhas")

    # Resumo geral
    print(f"\n{'='*80}")
    print("📊 RESUMO GERAL")
    print(f"{'='*80}")
    print(f"  Total de tabelas criadas: {len(dados_tabelas)}")
    print(f"  Total de linhas (todas as tabelas): {df_diagnostico['Linhas'].sum():,}")
    print(f"  Tabelas HDA (2W): {len(df_diagnostico[df_diagnostico['Segmento'] == 'HDA (2W)'])}")
    print(f"  Tabelas HAB (4W): {len(df_diagnostico[df_diagnostico['Segmento'] == 'HAB (4W)'])}")
    print(f"\n✅ Pipeline de refinamento executado com sucesso!")
    print(f"={'='*80}\n")
