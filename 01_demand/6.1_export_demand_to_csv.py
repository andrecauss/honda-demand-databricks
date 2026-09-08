# Databricks notebook source
# MAGIC %md
# MAGIC # 6.1 — Exportação da demanda para CSV
# MAGIC
# MAGIC - **Propósito:** Exportar as tabelas do schema de demanda para arquivos CSV.
# MAGIC - **Entrada:** `parts_hdbk_sandbox.pr_demand`
# MAGIC - **Saída:** Arquivos CSV no volume de exportação
# MAGIC - **Carga:** Sob demanda

# COMMAND ----------

# DBTITLE 1,List tables in schema
# List all tables in the schema
tables = spark.sql("SHOW TABLES IN parts_hdbk_sandbox.pr_demand").collect()
table_names = [row.tableName for row in tables]

print(f"Found {len(table_names)} tables to export:")
for table in table_names:
    print(f"  - {table}")

# COMMAND ----------

# DBTITLE 1,Export tables to CSV
# Export each table to CSV
volume_path = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"

for table_name in table_names:
    try:
        # Read table
        full_table_name = f"parts_hdbk_sandbox.pr_demand.{table_name}"
        df = spark.table(full_table_name)

        # Export to CSV
        output_path = f"{volume_path}/{table_name}.csv"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

        row_count = df.count()
        print(f"✓ Exported {table_name}: {row_count} rows -> {output_path}")

    except Exception as e:
        print(f"✗ Error exporting {table_name}: {str(e)}")

print("\nExport completed!")

# COMMAND ----------

# DBTITLE 1,Reorganize CSV files
# Reorganize CSV files - move from folders and rename
import os

volume_path = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"

# List all items in the volume (folders created by Spark write)
all_items = dbutils.fs.ls(volume_path)

# Filter only directories that end with .csv (these are the folders created by Spark)
csv_folders = [item for item in all_items if item.isDir() and item.name.endswith('.csv/')]

print(f"Found {len(csv_folders)} CSV folders to reorganize...\n")

for folder in csv_folders:
    # Get the table name from the folder name (remove .csv/ suffix)
    table_name = folder.name.replace('.csv/', '')
    folder_path = folder.path

    try:
        # List files in the folder
        files = dbutils.fs.ls(folder_path)

        # Find the part-*.csv file (ignore _SUCCESS and other metadata files)
        csv_file = None
        for file in files:
            if file.name.startswith('part-') and file.name.endswith('.csv'):
                csv_file = file.path
                break

        if csv_file:
            # New file name at the root level
            new_file_path = f"{volume_path}/{table_name}.csv.tmp"

            # Copy the file to the root level
            dbutils.fs.cp(csv_file, new_file_path)

            # Remove the folder
            dbutils.fs.rm(folder_path, recurse=True)

            # Rename to final name
            final_file_path = f"{volume_path}/{table_name}.csv"
            dbutils.fs.mv(new_file_path, final_file_path)

            print(f"✓ Reorganized {table_name}.csv")
        else:
            print(f"✗ No CSV file found in {table_name} folder")

    except Exception as e:
        print(f"✗ Error reorganizing {table_name}: {str(e)}")

print("\nReorganization completed!")
print(f"\nFinal files in {volume_path}:")
final_files = [file.name for file in dbutils.fs.ls(volume_path) if not file.isDir()]
for file_name in sorted(final_files):
    print(f"  - {file_name}")

# COMMAND ----------

# DBTITLE 1,Create tar.gz archive
import subprocess
from datetime import datetime

volume_path = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
archive_name = f"demand_refined_export_{timestamp}.tar.gz"
archive_path = f"{volume_path}/{archive_name}"

print("Creating tar.gz archive...\n")

result = subprocess.run(
    f"cd {volume_path} && tar -czf {archive_name} *.csv",
    shell=True,
    capture_output=True,
    text=True
)

if result.returncode == 0:
    size_result = subprocess.run(
        f"ls -lh {archive_path}",
        shell=True,
        capture_output=True,
        text=True
    )

    contents_result = subprocess.run(
        f"tar -tzf {archive_path} | wc -l",
        shell=True,
        capture_output=True,
        text=True
    )

    print(f"✓ Archive created successfully!")
    print(f"\nFile: {archive_name}")
    print(f"Location: {archive_path}")
    print(size_result.stdout)
    print(f"\nTotal files: {contents_result.stdout.strip()}")
else:
    print(f"✗ Error: {result.stderr}")
