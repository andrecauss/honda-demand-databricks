# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
#   "xlsxwriter",
# ]
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 6.2 — Exportação da demanda para Excel (legado)
# MAGIC
# MAGIC - **Propósito:** Exportar tabelas refinadas para arquivos Excel organizados por praça.
# MAGIC - **Entrada:** Tabelas `pr_demand.refined_demand_*`
# MAGIC - **Saída:** Arquivos XLSX no volume de exportação
# MAGIC - **Carga:** Sob demanda · **Status:** Legado; preferir a versão v2

# COMMAND ----------

# DBTITLE 1,List tables in schema
# ------------------------------------------------------------------------------
# Descoberta de Tabelas do Schema
# ------------------------------------------------------------------------------
# Lista todas as tabelas disponíveis no schema pr_demand para exportação.
# Filtra apenas tabelas que começam com 'refined_demand_'.
# ------------------------------------------------------------------------------
tables = spark.sql("SHOW TABLES IN parts_hdbk_sandbox.pr_demand").collect()
table_names = [
    row.tableName for row in tables
    if row.tableName.startswith("refined_demand_")
]

print(f"Total de tabelas refinadas encontradas: {len(table_names)}")
print(f"Tabelas: {sorted(table_names)}")

# COMMAND ----------

# DBTITLE 1,Install openpyxl library
# ------------------------------------------------------------------------------
# Instalação de Dependências
# ------------------------------------------------------------------------------
# Instala bibliotecas necessárias para manipulação de arquivos Excel:
# - openpyxl: Leitura e escrita de arquivos .xlsx (formato Office Open XML)
# - xlsxwriter: Engine otimizado para pandas.to_excel() com melhor performance
#   e suporte a formatação avançada
# ------------------------------------------------------------------------------
%pip install openpyxl xlsxwriter

# COMMAND ----------

# DBTITLE 1,Funções Auxiliares de Exportação
# ==============================================================================
# FUNÇÕES AUXILIARES DE EXPORTAÇÃO
# ==============================================================================
# Define funções reutilizáveis para exportação de tabelas Delta para
# formato Excel, com suporte a múltiplas abas e organização por arquivo.
# ==============================================================================
import pandas as pd
from io import BytesIO

# Caminho do volume de destino para armazenamento dos arquivos exportados
VOLUME_PATH = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"

print(f"Volume de destino: {VOLUME_PATH}")


def export_table_to_excel(table_name, schema="parts_hdbk_sandbox.pr_demand"):
    """
    Exporta uma tabela Delta para formato Excel (.xlsx).

    A função detecta automaticamente a estrutura da tabela e aplica a
    lógica de exportação apropriada:

    - Tabelas com colunas 'file' e 'sheet': organizadas em múltiplos workbooks,
      onde cada valor único de 'file' gera um arquivo .xlsx, e cada valor
      único de 'sheet' dentro desse arquivo se torna uma aba separada.

    - Tabelas simples (sem file/sheet): exportadas como um único arquivo
      Excel com uma única aba.

    Args:
        table_name (str): Nome da tabela a ser exportada (sem o prefixo de
            schema/catalog).
        schema (str, optional): Schema completo no formato catalog.schema.
            Defaults to "parts_hdbk_sandbox.pr_demand".

    Returns:
        None

    Raises:
        Exception: Propaga qualquer erro ocorrido durante a exportação,
            após registrá-lo no console.

    Side Effects:
        - Cria arquivos .xlsx temporários no sistema de arquivos local
        - Copia arquivos para o volume Unity Catalog especificado
        - Imprime mensagens de progresso e status no console

    Example:
        >>> export_table_to_excel("refined_demand_fechada_novos_modelos")
        [OK] refined_demand_fechada_novos_modelos -> HDA 2026 Jul Demanda Fechada.xlsx (1250 linhas, 5 abas)

    Note:
        - Nomes de abas Excel são truncados em 31 caracteres (limitação do formato)
        - Colunas 'file' e 'sheet' são removidas dos dados exportados
        - Utiliza xlsxwriter como engine para melhor performance
    """
    full_table_name = f"{schema}.{table_name}"

    # Carrega DataFrame e cacheia schema ANTES do try/except
    # (otimização Spark Connect - evita múltiplas chamadas Analyze RPC)
    df = spark.table(full_table_name)
    columns_list = df.columns
    has_file_sheet = "file" in columns_list and "sheet" in columns_list

    try:
        # Ação Spark: converte DataFrame para pandas DENTRO do try/except
        # para capturar erros de execução distribuída (Spark Connect lazy evaluation)
        pdf = df.toPandas()

        if has_file_sheet:
            # Tabelas organizadas: um workbook por 'file', uma aba por 'sheet'
            # dentro de cada workbook
            for file_name, file_group in pdf.groupby("file"):
                # Cria Excel em memória (BytesIO) - sem acesso ao filesystem local
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    for sheet_name, sheet_group in file_group.groupby("sheet"):
                        # Remove colunas de controle (metadados de organização)
                        # antes de exportar os dados de negócio
                        data = sheet_group.drop(columns=["file", "sheet"])

                        # Excel impõe limite de 31 caracteres para nomes de abas
                        data.to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)

                # Escreve buffer em memória diretamente no volume UC
                output_path = f"{VOLUME_PATH}/{file_name}.xlsx"
                buffer.seek(0)
                dbutils.fs.put(output_path, buffer.read(), overwrite=True)

                n_sheets = file_group["sheet"].nunique()
                print(f"[OK] {table_name} -> {file_name}.xlsx ({len(file_group)} linhas, {n_sheets} abas)")
        else:
            # Tabelas simples: um workbook com uma única aba
            buffer = BytesIO()
            pdf.to_excel(buffer, index=False, engine='xlsxwriter')

            output_path = f"{VOLUME_PATH}/{table_name}.xlsx"
            buffer.seek(0)
            dbutils.fs.put(output_path, buffer.read(), overwrite=True)
            print(f"[OK] {table_name}.xlsx: {len(pdf)} linhas")

    except Exception as e:
        print(f"[ERRO] Erro ao exportar {table_name}: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Limpar Volume de Exportação
# ==============================================================================
# LIMPEZA DO VOLUME DE EXPORTAÇÃO
# ==============================================================================
# Remove todos os arquivos .xlsx existentes no volume de destino antes da
# nova exportação.
# ==============================================================================
print("\n" + "="*70)
print("LIMPANDO VOLUME DE EXPORTAÇÃO")
print("="*70)

try:
    # Lista todos os arquivos atualmente presentes no volume
    files = dbutils.fs.ls(VOLUME_PATH)

    if len(files) == 0:
        print("Volume já está vazio.")
    else:
        print(f"\nEncontrados {len(files)} arquivos no volume.")
        print("Removendo todos os arquivos...\n")

        # Remove cada arquivo individualmente
        for file_info in files:
            file_path = file_info.path
            dbutils.fs.rm(file_path)
            print(f"[REMOVIDO] {file_info.name}")

        print(f"\n[OK] Volume limpo com sucesso! {len(files)} arquivos removidos.")

except Exception as e:
    # Falha na limpeza não deve bloquear a exportação
    print(f"[AVISO] Erro ao limpar volume: {str(e)}")
    print("Continuando com a exportação...")

# COMMAND ----------

# DBTITLE 1,Exportação Dinâmica de Todas as Tabelas Refinadas
# ==============================================================================
# EXPORTAÇÃO DINÂMICA DE TODAS AS TABELAS REFINADAS
# ==============================================================================
# Itera sobre todas as tabelas descobertas no schema e exporta cada uma para
# formato Excel usando a função export_table_to_excel.
# ==============================================================================

print("\n" + "="*70)
print("TESTE: EXPORTANDO 2 TABELAS")
print("="*70)

tabelas_exportadas = 0
tabelas_com_erro = 0
erros = []

# TESTE: processa apenas as 2 primeiras tabelas
for table_name in sorted(table_names)[:2]:
    try:
        print(f"\n[EXPORTANDO] {table_name}...")
        export_table_to_excel(table_name)
        tabelas_exportadas += 1
    except Exception as e:
        tabelas_com_erro += 1
        erro_msg = f"{table_name}: {str(e)}"
        erros.append(erro_msg)
        print(f"[ERRO] {erro_msg}")

print("\n" + "="*70)
print("RESUMO DA EXPORTAÇÃO")
print("="*70)
print(f"✓ Tabelas exportadas com sucesso: {tabelas_exportadas}")
if tabelas_com_erro > 0:
    print(f"✗ Tabelas com erro: {tabelas_com_erro}")
    print("\nDetalhes dos erros:")
    for erro in erros:
        print(f"  - {erro}")
else:
    print("✓ Nenhum erro detectado")
print(f"\n📁 Local: {VOLUME_PATH}")
print("\nℹ️  Os arquivos Excel estão prontos para compartilhamento.")
