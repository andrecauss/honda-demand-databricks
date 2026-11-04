# Databricks notebook source
# DBTITLE 1,Título
# MAGIC %md
# MAGIC # Administração — Tabela Calendário
# MAGIC
# MAGIC Notebook para criação e manutenção da dimensão de calendário (`dim_calendario`) com feriados nacionais do Brasil.

# COMMAND ----------

# DBTITLE 1,Configuração
# =============================================================================
# CONFIGURAÇÃO
# Parâmetros para geração da tabela calendário
# =============================================================================

# Intervalo de datas a gerar
DATA_INICIO = "2010-01-01"
DATA_FIM = "2030-12-31"

# Tabela destino no Unity Catalog
TABELA_DESTINO = "parts_hdbk_sandbox._utils.dim_calendario"

print(f"📅 Intervalo: {DATA_INICIO} a {DATA_FIM}")
print(f"📦 Tabela destino: {TABELA_DESTINO}")

# COMMAND ----------

# DBTITLE 1,Feriados Brasileiros
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType

# =============================================================================
# FERIADOS NACIONAIS DO BRASIL
# Feriados fixos (Lei nº 662/1949 e Lei nº 6.802/1980)
# Feriados móveis calculados a partir da Páscoa
# =============================================================================

def calcular_pascoa(ano):
    """Calcula a data da Páscoa pelo algoritmo de Meeus/Jones/Butcher."""
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1
    return date(ano, mes, dia)


def obter_feriados_brasileiros(ano_inicio, ano_fim):
    """
    Gera lista de feriados nacionais brasileiros (fixos e móveis)
    para cada ano no intervalo informado.
    """
    feriados = []
    for ano in range(ano_inicio, ano_fim + 1):
        # Feriados fixos
        fixos = [
            (date(ano, 1, 1),   "Confraternização Universal"),
            (date(ano, 4, 21),  "Tiradentes"),
            (date(ano, 5, 1),   "Dia do Trabalho"),
            (date(ano, 9, 7),   "Independência do Brasil"),
            (date(ano, 10, 12), "Nossa Senhora Aparecida"),
            (date(ano, 11, 2),  "Finados"),
            (date(ano, 11, 15), "Proclamação da República"),
            (date(ano, 12, 25), "Natal"),
        ]
        feriados.extend(fixos)

        # Feriados móveis (baseados na Páscoa)
        pascoa = calcular_pascoa(ano)
        moveis = [
            (pascoa - timedelta(days=48), "Carnaval (Segunda)"),
            (pascoa - timedelta(days=47), "Carnaval (Terça)"),
            (pascoa - timedelta(days=2),  "Sexta-feira Santa"),
            (pascoa + timedelta(days=60), "Corpus Christi"),
        ]
        feriados.extend(moveis)
    return feriados


# Gerar feriados para o período configurado
ano_inicio = int(DATA_INICIO[:4])
ano_fim = int(DATA_FIM[:4])
lista_feriados = obter_feriados_brasileiros(ano_inicio, ano_fim)

# Converter para DataFrame Spark
schema_feriados = StructType([
    StructField("data_feriado", DateType(), False),
    StructField("nome_feriado", StringType(), True),
])
df_feriados = spark.createDataFrame(lista_feriados, schema=schema_feriados)

# Desduplicar datas que coincidem (ex: Sexta-feira Santa + Tiradentes em 21/04)
df_feriados = (
    df_feriados
    .groupBy("data_feriado")
    .agg(F.concat_ws(" / ", F.collect_list("nome_feriado")).alias("nome_feriado"))
)

print(f"✅ {df_feriados.count()} datas de feriado geradas para {ano_inicio}–{ano_fim}")
display(df_feriados.orderBy("data_feriado"))

# COMMAND ----------

# DBTITLE 1,Criação da Tabela Calendário
# =============================================================================
# CRIAÇÃO / SUBSTITUIÇÃO DA TABELA CALENDÁRIO
# Gera todas as datas no intervalo com colunas analíticas e feriados
# =============================================================================

# Gerar sequência de datas
df_datas = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{DATA_INICIO}'), 
        to_date('{DATA_FIM}'), 
        interval 1 day
    )) AS data
""")

# Mapeamento: dia da semana (PT / EN)
map_dia_semana_pt = F.create_map(
    F.lit(1), F.lit("Segunda"), F.lit(2), F.lit("Terça"),
    F.lit(3), F.lit("Quarta"),  F.lit(4), F.lit("Quinta"),
    F.lit(5), F.lit("Sexta"),   F.lit(6), F.lit("Sábado"),
    F.lit(7), F.lit("Domingo"),
)
map_dia_semana_en = F.create_map(
    F.lit(1), F.lit("Monday"),    F.lit(2), F.lit("Tuesday"),
    F.lit(3), F.lit("Wednesday"), F.lit(4), F.lit("Thursday"),
    F.lit(5), F.lit("Friday"),    F.lit(6), F.lit("Saturday"),
    F.lit(7), F.lit("Sunday"),
)

# Mapeamento: nome do mês (PT / EN)
map_nome_mes_pt = F.create_map(
    F.lit(1),  F.lit("Janeiro"),   F.lit(2),  F.lit("Fevereiro"),
    F.lit(3),  F.lit("Março"),     F.lit(4),  F.lit("Abril"),
    F.lit(5),  F.lit("Maio"),      F.lit(6),  F.lit("Junho"),
    F.lit(7),  F.lit("Julho"),     F.lit(8),  F.lit("Agosto"),
    F.lit(9),  F.lit("Setembro"),  F.lit(10), F.lit("Outubro"),
    F.lit(11), F.lit("Novembro"),  F.lit(12), F.lit("Dezembro"),
)
map_nome_mes_en = F.create_map(
    F.lit(1),  F.lit("January"),   F.lit(2),  F.lit("February"),
    F.lit(3),  F.lit("March"),     F.lit(4),  F.lit("April"),
    F.lit(5),  F.lit("May"),       F.lit(6),  F.lit("June"),
    F.lit(7),  F.lit("July"),      F.lit(8),  F.lit("August"),
    F.lit(9),  F.lit("September"), F.lit(10), F.lit("October"),
    F.lit(11), F.lit("November"),  F.lit(12), F.lit("December"),
)
map_nome_mes_abrev_pt = F.create_map(
    F.lit(1),  F.lit("Jan"),   F.lit(2),  F.lit("Fev"),
    F.lit(3),  F.lit("Mar"),     F.lit(4),  F.lit("Abr"),
    F.lit(5),  F.lit("Mai"),      F.lit(6),  F.lit("Jun"),
    F.lit(7),  F.lit("Jul"),     F.lit(8),  F.lit("Ago"),
    F.lit(9),  F.lit("Set"),  F.lit(10), F.lit("Out"),
    F.lit(11), F.lit("Nov"),  F.lit(12), F.lit("Dez"),
)
map_nome_mes_abrv_en = F.create_map(
    F.lit(1),  F.lit("Jan"),   F.lit(2),  F.lit("Feb"),
    F.lit(3),  F.lit("Mar"),     F.lit(4),  F.lit("Apr"),
    F.lit(5),  F.lit("May"),       F.lit(6),  F.lit("Jun"),
    F.lit(7),  F.lit("Jul"),      F.lit(8),  F.lit("Aug"),
    F.lit(9),  F.lit("Sep"), F.lit(10), F.lit("Oct"),
    F.lit(11), F.lit("Nov"),  F.lit(12), F.lit("Dec"),
)

# Dia da semana ISO: 1=Segunda ... 7=Domingo
# Spark dayofweek retorna 1=Domingo..7=Sábado → converter para ISO
dia_semana_iso = ((F.dayofweek("data") + 5) % 7) + 1

# KI (Ano Fiscal Honda): abril a março
# 103ki = abr/2026–mar/2027, 104ki = abr/2027–mar/2028, etc.
ki_numero = F.when(F.month("data") >= 4, F.year("data") - 1923).otherwise(F.year("data") - 1924)

# Montar DataFrame do calendário
df_calendario = (
    df_datas
    .withColumn("ano",                F.year("data"))
    .withColumn("mes",                F.month("data"))
    .withColumn("dia",                F.dayofmonth("data"))
    .withColumn("dia_semana",         dia_semana_iso)
    .withColumn("nome_dia_semana_pt", map_dia_semana_pt[dia_semana_iso])
    .withColumn("nome_dia_semana_en", map_dia_semana_en[dia_semana_iso])
    .withColumn("nome_mes_pt",        map_nome_mes_pt[F.month("data")])
    .withColumn("nome_mes_en",        map_nome_mes_en[F.month("data")])
    .withColumn("nome_mes_abrev_pt",  map_nome_mes_abrev_pt[F.month("data")])
    .withColumn("nome_mes_abrev_en",  map_nome_mes_abrv_en[F.month("data")])
    .withColumn("ano_mes",            F.date_format("data", "yyyy-MM"))
    .withColumn("semana_ano",         F.weekofyear("data"))
    .withColumn("fim_semana",      dia_semana_iso >= 6)
    .withColumn("ki_numero",          ki_numero)
    .withColumn("ki",                 F.concat(ki_numero.cast("string"), F.lit("ki")))
)

# Juntar com feriados brasileiros
df_calendario = (
    df_calendario
    .join(df_feriados, df_calendario["data"] == df_feriados["data_feriado"], "left")
    .withColumn("feriado", F.col("data_feriado").isNotNull())
    .withColumn("dia_util",   ~(F.col("fim_semana") | F.col("feriado")))
    .drop("data_feriado")
)

# Garantir ordem das colunas
df_calendario = df_calendario.select(
    "data", "ano", "mes", "dia", "dia_semana",
    "nome_dia_semana_pt", "nome_dia_semana_en",
    "nome_mes_pt", "nome_mes_en",
    "nome_mes_abrev_pt", "nome_mes_abrev_en",
    "ano_mes", "semana_ano",
    "ki_numero", "ki",
    "fim_semana", "feriado", "nome_feriado", "dia_util"
)

# Gravar tabela no Unity Catalog
(
    df_calendario
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

# Adicionar comentário na tabela
spark.sql(f"""
    COMMENT ON TABLE {TABELA_DESTINO} IS 
    'Dimensão Calendário com feriados nacionais do Brasil e ano fiscal KI (Honda). Gerado automaticamente.'
""")

total = spark.table(TABELA_DESTINO).count()
print(f"✅ Tabela {TABELA_DESTINO} criada com sucesso!")
print(f"📊 Total de registros: {total:,}")

# COMMAND ----------

# DBTITLE 1,Validação
# =============================================================================
# VALIDAÇÃO
# Resumo geral e distribuição de feriados/dias úteis por ano
# =============================================================================

df_val = spark.table(TABELA_DESTINO)

# Resumo geral
resumo = df_val.agg(
    F.count("*").alias("total_registros"),
    F.min("data").alias("data_inicio"),
    F.max("data").alias("data_fim"),
    F.countDistinct("ano").alias("qtd_anos"),
    F.sum(F.when(F.col("feriado"), 1).otherwise(0)).alias("total_feriados"),
    F.sum(F.when(F.col("fim_semana"), 1).otherwise(0)).alias("total_fins_semana"),
    F.sum(F.when(F.col("dia_util"), 1).otherwise(0)).alias("total_dias_uteis"),
).collect()[0]

print("=" * 60)
print("VALIDAÇÃO — RESUMO GERAL")
print("=" * 60)
print(f"📊 Total de registros:  {resumo['total_registros']:,}")
print(f"📅 Período:             {resumo['data_inicio']} a {resumo['data_fim']}")
print(f"📆 Anos cobertos:       {resumo['qtd_anos']}")
print(f"🎉 Total de feriados:   {resumo['total_feriados']}")
print(f"🏖️  Fins de semana:      {resumo['total_fins_semana']}")
print(f"💼 Dias úteis:           {resumo['total_dias_uteis']}")
print()
print("DISTRIBUIÇÃO POR ANO:")
print("=" * 60)

# Tabela de distribuição por ano
display(
    df_val.groupBy("ano").agg(
        F.count("*").alias("total_dias"),
        F.sum(F.when(F.col("feriado"), 1).otherwise(0)).alias("feriados"),
        F.sum(F.when(F.col("fim_semana"), 1).otherwise(0)).alias("fins_semana"),
        F.sum(F.when(F.col("dia_util"), 1).otherwise(0)).alias("dias_uteis"),
        F.round(
            F.sum(F.when(F.col("dia_util"), 1).otherwise(0)) * 100.0 / F.count("*"), 1
        ).alias("pct_dias_uteis"),
    ).orderBy("ano")
)

# COMMAND ----------

# DBTITLE 1,Manutenção de Feriados
# MAGIC %md
# MAGIC ---
# MAGIC ## Manutenção de Feriados
# MAGIC
# MAGIC Execute a célula abaixo **isoladamente** para incluir ou excluir datas de feriado da tabela existente, sem regenerar o calendário inteiro.

# COMMAND ----------

# DBTITLE 1,Override de Feriados
# =============================================================================
# MANUTENÇÃO DE FERIADOS
# Execute esta célula isoladamente para incluir/excluir feriados
# na tabela existente, sem regenerar o calendário.
# =============================================================================

# Tabela alvo
_TABELA = "parts_hdbk_sandbox._utils.dim_calendario"

# Datas para INCLUIR como feriado
# Formato: [("YYYY-MM-DD", "Nome do Feriado"), ...]
INCLUIR_FERIADOS = [
    # ("2025-01-25", "Aniversário de São Paulo"),
    # ("2026-07-09", "Revolução Constitucionalista"),
]

# Datas para EXCLUIR dos feriados (remover marcação)
# Formato: ["YYYY-MM-DD", ...]
EXCLUIR_FERIADOS = [
    # "2025-02-03",
    # "2026-06-04",
]

# --- Aplicar inclusões ---
for dt_str, nome in INCLUIR_FERIADOS:
    nome_esc = nome.replace("'", "''")
    spark.sql(f"""
        UPDATE {_TABELA}
        SET feriado      = true,
            nome_feriado = CASE
                WHEN nome_feriado IS NULL THEN '{nome_esc}'
                ELSE concat(nome_feriado, ' / ', '{nome_esc}')
            END,
            dia_util     = false
        WHERE data = DATE '{dt_str}'
    """)
    print(f"✅ {dt_str} → incluído como feriado: {nome}")

# --- Aplicar exclusões ---
for dt_str in EXCLUIR_FERIADOS:
    spark.sql(f"""
        UPDATE {_TABELA}
        SET feriado      = false,
            nome_feriado = NULL,
            dia_util     = NOT fim_semana
        WHERE data = DATE '{dt_str}'
    """)
    print(f"🚫 {dt_str} → removido dos feriados")

if not INCLUIR_FERIADOS and not EXCLUIR_FERIADOS:
    print("ℹ️  Nenhuma alteração configurada.")
    print("   Preencha INCLUIR_FERIADOS e/ou EXCLUIR_FERIADOS acima e execute novamente.")
else:
    print(f"\n✅ Manutenção concluída: {len(INCLUIR_FERIADOS)} inclusão(ões), {len(EXCLUIR_FERIADOS)} exclusão(ões)")