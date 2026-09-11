# Databricks notebook source
# DBTITLE 1,Visão Geral
# MAGIC %md
# MAGIC # 99 — Utilitários de movimentação de arquivos em Volumes
# MAGIC
# MAGIC - **Propósito:** Funções reutilizáveis para mover, listar e validar arquivos entre UC Volumes.
# MAGIC - **Uso:** `%run ../99_utils_volume_file_ops` a partir de qualquer notebook do projeto.
# MAGIC - **Funções disponíveis:**
# MAGIC   - `move_volume_files(source, destination, ...)` — Move arquivos entre diretórios de volumes
# MAGIC   - `list_volume_files(path, extensions)` — Lista arquivos com filtro opcional por extensão
# MAGIC   - `validate_directory_empty(path)` — Valida que um diretório está vazio

# COMMAND ----------

# DBTITLE 1,Imports
import os
from typing import List, Optional, Dict, Any

# COMMAND ----------

# DBTITLE 1,list_volume_files
def list_volume_files(
    path: str,
    extensions: Optional[List[str]] = None,
) -> List[str]:
    """
    Lista arquivos em um diretório de UC Volume, com filtro opcional por extensão.

    Args:
        path: Caminho do diretório no volume (ex: /Volumes/catalog/schema/volume/subdir/).
        extensions: Lista de extensões para filtrar (ex: [".xlsx", ".csv"]).
                    Se None, retorna todos os arquivos.

    Returns:
        Lista de nomes de arquivo encontrados.

    Example:
        >>> list_volume_files("/Volumes/cat/sch/vol/current/", [".xlsx"])
        ['file1.xlsx', 'file2.xlsx']
    """
    try:
        entries = dbutils.fs.ls(path)
    except Exception as e:
        print(f"Erro ao listar {path}: {e}")
        return []

    files = [f.name for f in entries if not f.isDir()]

    if extensions:
        ext_lower = {ext.lower() for ext in extensions}
        files = [f for f in files if os.path.splitext(f)[1].lower() in ext_lower]

    return sorted(files)

# COMMAND ----------

# DBTITLE 1,move_volume_files
def move_volume_files(
    source_path: str,
    destination_path: str,
    extensions: Optional[List[str]] = None,
    dry_run: bool = False,
    validate_empty: bool = True,
) -> Dict[str, Any]:
    """
    Move arquivos entre diretórios de UC Volumes.

    Realiza a cópia para o destino e remove o arquivo de origem apenas após
    confirmar o sucesso da cópia (estratégia copy-then-delete).

    Args:
        source_path: Caminho de origem (ex: /Volumes/cat/sch/vol/current/).
        destination_path: Caminho de destino (ex: /Volumes/cat/sch/vol/history/).
        extensions: Filtro opcional de extensões (ex: [".xlsx"]).
                    Se None, move todos os arquivos.
        dry_run: Se True, apenas simula sem mover.
        validate_empty: Se True, valida que a origem ficou sem os arquivos
                        movidos após a operação.

    Returns:
        Dict com chaves:
            - total_found (int): Arquivos encontrados na origem
            - moved (list[str]): Nomes dos arquivos movidos com sucesso
            - failed (list[tuple[str, str]]): (nome, erro) para cada falha
            - dry_run (bool): Se foi execução simulada

    Raises:
        RuntimeError: Se validate_empty=True e restaram arquivos que deveriam
                      ter sido movidos, ou se houve falhas na movimentação.

    Example:
        >>> result = move_volume_files(
        ...     "/Volumes/cat/sch/vol/current/",
        ...     "/Volumes/cat/sch/vol/history/",
        ...     extensions=[".xlsx"],
        ... )
        >>> print(f"{len(result['moved'])} arquivo(s) movido(s)")
    """
    # Garante trailing slash nos caminhos
    source_path = source_path.rstrip("/") + "/"
    destination_path = destination_path.rstrip("/") + "/"

    files = list_volume_files(source_path, extensions)

    result: Dict[str, Any] = {
        "total_found": len(files),
        "moved": [],
        "failed": [],
        "dry_run": dry_run,
    }

    modo = "SIMULAÇÃO" if dry_run else "EXECUÇÃO"
    print(f"{'=' * 60}")
    print(f"MOVIMENTAÇÃO DE ARQUIVOS — {modo}")
    print(f"{'=' * 60}")
    print(f"Origem  : {source_path}")
    print(f"Destino : {destination_path}")
    print(f"Filtro  : {extensions or 'todos'}")
    print(f"Arquivos: {len(files)}")
    print()

    if not files:
        print("Nenhum arquivo encontrado. Nada a fazer.")
        return result

    for file_name in files:
        src = f"{source_path}{file_name}"
        dst = f"{destination_path}{file_name}"

        try:
            if dry_run:
                print(f"  [TESTE]   {file_name} \u2192 destino/")
            else:
                dbutils.fs.cp(src, dst)
                dbutils.fs.rm(src)
                print(f"  [MOVIDO]  {file_name} \u2192 destino/")

            result["moved"].append(file_name)

        except Exception as e:
            error_msg = str(e)
            result["failed"].append((file_name, error_msg))
            print(f"  [ERRO]    {file_name}: {error_msg}")

    # Relatório
    print(f"\n{'=' * 60}")
    print(f"RESULTADO: {len(result['moved'])} movido(s), {len(result['failed'])} erro(s)")
    print(f"{'=' * 60}")

    # Validações pós-movimentação
    if result["failed"] and not dry_run:
        raise RuntimeError(
            f"Falha ao mover {len(result['failed'])} arquivo(s): "
            + "; ".join(f"{name}: {err}" for name, err in result["failed"])
        )

    if validate_empty and not dry_run and result["moved"]:
        remaining = list_volume_files(source_path, extensions)
        if remaining:
            raise RuntimeError(
                f"Restaram {len(remaining)} arquivo(s) na origem que deveriam "
                f"ter sido movidos: {remaining}"
            )
        print("\u2713 Origem validada: nenhum arquivo restante.")

    return result

# COMMAND ----------

# DBTITLE 1,validate_directory_empty
def validate_directory_empty(
    path: str,
    extensions: Optional[List[str]] = None,
    raise_on_failure: bool = True,
) -> bool:
    """
    Valida que um diretório está vazio (ou sem arquivos das extensões indicadas).

    Args:
        path: Caminho do diretório no volume.
        extensions: Se fornecido, valida apenas para essas extensões.
        raise_on_failure: Se True, levanta RuntimeError ao encontrar arquivos.

    Returns:
        True se o diretório está vazio (ou sem arquivos das extensões dadas).

    Raises:
        RuntimeError: Se raise_on_failure=True e existirem arquivos.
    """
    files = list_volume_files(path, extensions)

    if files:
        msg = (
            f"Diretório não está vazio: {path} "
            f"({len(files)} arquivo(s): {files[:5]}{'...' if len(files) > 5 else ''})"
        )
        if raise_on_failure:
            raise RuntimeError(msg)
        print(f"\u2717 {msg}")
        return False

    print(f"\u2713 Diretório vazio: {path}")
    return True

# COMMAND ----------

# DBTITLE 1,Confirmação de carregamento
print("\u2713 Funções de movimentação de arquivos carregadas:")
print("  \u2022 list_volume_files(path, extensions)")
print("  \u2022 move_volume_files(source, destination, extensions, dry_run, validate_empty)")
print("  \u2022 validate_directory_empty(path, extensions, raise_on_failure)")