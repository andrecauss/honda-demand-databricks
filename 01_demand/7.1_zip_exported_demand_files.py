# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 7.1 — Compactação dos arquivos de demanda
# MAGIC
# MAGIC - **Propósito:** Reunir os arquivos exportados em um pacote ZIP para distribuição.
# MAGIC - **Entrada:** Arquivos da pasta de saída do orquestrador
# MAGIC - **Saída:** `historical_demand_YYYY-MM-DD.zip`
# MAGIC - **Carga:** Sob demanda

# COMMAND ----------

# DBTITLE 1,Zip arquivos da pasta out/
import os
import zipfile
import shutil
from datetime import datetime

data_atual = datetime.now().strftime("%Y-%m-%d")

source_dir = "/Volumes/parts_hdbk_sandbox/_file_orchestrator/demand/01_apuracao_demanda/out/"
zip_name = f"historical_demand_{data_atual}.zip"
zip_path = f"/Volumes/parts_hdbk_sandbox/_file_orchestrator/demand/01_apuracao_demanda/out/{zip_name}"
tmp_zip = f"/tmp/{zip_name}"

files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
print(f"Arquivos encontrados: {len(files)}\n")

with zipfile.ZipFile(tmp_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
    for f in files:
        full_path = os.path.join(source_dir, f)
        size = os.path.getsize(full_path)
        zf.write(full_path, arcname=f)
        print(f"  Adicionado: {f} ({size:,} bytes)")

shutil.copy2(tmp_zip, zip_path)
os.remove(tmp_zip)

zip_size = os.path.getsize(zip_path)
print(f"\nZip criado em: {zip_path}")
print(f"Tamanho do zip: {zip_size:,} bytes")
