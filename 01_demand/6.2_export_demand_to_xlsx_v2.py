# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # 6.2 — Exportação da demanda para Excel
# MAGIC
# MAGIC - **Propósito:** Gerar workbooks Excel com múltiplas abas a partir das tabelas refinadas.
# MAGIC - **Entrada:** Tabelas `pr_demand.refined_demand_*`
# MAGIC - **Saída:** Arquivos XLSX no volume de saída do orquestrador
# MAGIC - **Carga:** Sob demanda

# COMMAND ----------

# DBTITLE 1,Instalação de Dependências
# ------------------------------------------------------------------------------
# Instalação de Dependências
# ------------------------------------------------------------------------------
# openpyxl: escrita de .xlsx. É o único engine necessário — a montagem do
# workbook é feita diretamente pelo openpyxl, sem pandas.to_excel, para
# permitir múltiplas abas em um único arquivo antes do upload.
#
# ATENÇÃO: %pip install REINICIA o interpretador Python e apaga todas as
# variáveis definidas antes dele. Por isso esta célula precisa vir antes de
# qualquer célula que defina estado (tabelas, constantes, funções).
# ------------------------------------------------------------------------------
%pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Configuração e Funções Auxiliares de Exportação
# ==============================================================================
# CONFIGURAÇÃO E FUNÇÕES AUXILIARES DE EXPORTAÇÃO
# ==============================================================================
# Define o mecanismo de escrita de arquivos Excel no volume Unity Catalog.
#
# DECISÃO TÉCNICA — por que não dbutils.fs.put:
#   dbutils.fs.put() grava TEXTO (contents: str). Passar os bytes de um .xlsx
#   corrompe o arquivo ou levanta erro de tipo. O caminho correto para binário
#   é montar o arquivo em um temporário local e enviá-lo pela Files API do
#   Workspace, que é o mesmo mecanismo usado pelo Lakeflow Designer.
#
# DECISÃO TÉCNICA — por que um upload por arquivo:
#   Todas as abas de um workbook são conhecidas de uma vez (groupby). Montar
#   o workbook inteiro em memória e subir uma única vez evita reler e
#   regravar o arquivo no volume a cada aba.
# ==============================================================================
import os
import re
import tempfile

import openpyxl
from databricks.sdk import WorkspaceClient

# Caminho do volume de destino para armazenamento dos arquivos exportados
SCHEMA = "parts_hdbk_sandbox.pr_demand"
VOLUME_PATH = "/Volumes/parts_hdbk_sandbox/_file_orchestrator/demand/01_apuracao_demanda/out"

# Tetos de segurança: toPandas() traz tudo para o driver, então a coleta é
# limitada. O teto de células é o limite prático do Excel para um único arquivo.
MAX_LINHAS_ARQUIVO = 1_000_000
MAX_CELULAS_EXCEL = 5_000_000

# Caracteres que o Excel proíbe em nomes de aba
_CARACTERES_INVALIDOS_ABA = re.compile(r"[\[\]:*?/\\]")

# Prefixos que o Excel interpreta como fórmula ao abrir o arquivo
_GATILHOS_FORMULA = "=+-@\t\r"

# Caracteres de controle que o openpyxl se recusa a gravar
try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE as _CARACTERES_ILEGAIS
except ImportError:
    _CARACTERES_ILEGAIS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

print(f"Volume de destino: {VOLUME_PATH}")


def _limite_linhas(n_colunas):
    """
    Calcula o número máximo de linhas gravável em uma aba.

    Args:
        n_colunas (int): Quantidade de colunas da aba

    Returns:
        int: Teto de linhas respeitando MAX_CELULAS_EXCEL e MAX_LINHAS_ARQUIVO
    """
    return min(MAX_LINHAS_ARQUIVO, max(1, MAX_CELULAS_EXCEL // max(1, n_colunas)))


def _neutralizar_formula(valor):
    """
    Impede injeção de fórmula em células de texto.

    Valores vindos do SAP que começam com '=', '+', '-' ou '@' seriam
    interpretados como fórmula ao abrir a planilha. O prefixo com apóstrofo
    força o Excel a tratar o conteúdo como texto literal.

    Args:
        valor: Valor da célula

    Returns:
        Valor original, ou prefixado com apóstrofo se começar com gatilho
    """
    if isinstance(valor, str) and valor and valor[0] in _GATILHOS_FORMULA:
        return "'" + valor
    return valor


def _valor_seguro(valor):
    """
    Remove caracteres de controle que o openpyxl não consegue gravar.

    Args:
        valor: Valor da célula

    Returns:
        Valor limpo — tipos numéricos e None passam intactos, o resto vira str
    """
    if isinstance(valor, str):
        return _CARACTERES_ILEGAIS.sub("", valor)
    if isinstance(valor, (int, float, bool, type(None))):
        return valor
    return _CARACTERES_ILEGAIS.sub("", str(valor))


def _nome_aba(valor):
    """
    Converte um valor da coluna 'sheet' em nome de aba válido no Excel.

    O Excel proíbe os caracteres [ ] : * ? / \\ e limita o nome a 31
    caracteres. Nomes inválidos fazem o openpyxl levantar exceção.

    Args:
        valor: Valor bruto da coluna 'sheet'

    Returns:
        str: Nome de aba válido (nunca vazio)
    """
    nome = _CARACTERES_INVALIDOS_ABA.sub("-", str(valor)).strip("'")[:31]
    return nome or "Planilha"


def _nome_arquivo_seguro(valor):
    """
    Valida o nome de arquivo vindo da coluna 'file'.

    O valor vem dos dados, então precisa ser barrado antes de virar caminho
    dentro do volume — '/' ou '..' escapariam do diretório de destino.

    Args:
        valor: Valor bruto da coluna 'file'

    Returns:
        str: Nome de arquivo validado

    Raises:
        ValueError: Se o nome contiver '/' ou '..'
    """
    nome = str(valor)
    if "/" in nome or ".." in nome:
        raise ValueError(
            f"nome de arquivo inválido {nome!r}: não pode conter '/' ou '..'"
        )
    return nome


def _subir_para_volume(caminho_destino, escrever_conteudo):
    """
    Grava um arquivo binário no volume Unity Catalog.

    Monta o arquivo em um temporário no disco local do driver e o envia pela
    Files API. Substitui dbutils.fs.put(), que só grava texto e corrompe
    arquivos .xlsx.

    Args:
        caminho_destino (str): Caminho completo no volume (/Volumes/...)
        escrever_conteudo (callable): Função que recebe o caminho do
            temporário e grava o conteúdo nele (ex: workbook.save)

    Returns:
        None

    Side Effects:
        - Cria e remove um arquivo temporário no driver
        - Sobrescreve o arquivo de destino no volume
    """
    descritor, temporario = tempfile.mkstemp(suffix=".xlsx")
    os.close(descritor)
    try:
        escrever_conteudo(temporario)
        with open(temporario, "rb") as arquivo:
            WorkspaceClient().files.upload(caminho_destino, arquivo, overwrite=True)
    finally:
        if os.path.isfile(temporario):
            os.remove(temporario)


def _exportar_abas(abas, caminho_destino):
    """
    Monta um workbook com uma ou mais abas e o envia ao volume.

    Todas as abas são escritas em memória antes do envio, de forma que cada
    arquivo exige um único upload independentemente do número de abas.

    Args:
        abas (dict): Mapeamento {nome_da_aba: pandas.DataFrame}. A ordem do
            dicionário define a ordem das abas no arquivo.
        caminho_destino (str): Caminho completo no volume (/Volumes/...)

    Returns:
        None

    Raises:
        ValueError: Se alguma aba exceder o teto de linhas/células do Excel

    Side Effects:
        - Grava o arquivo .xlsx no volume, sobrescrevendo o existente
    """
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)

    nomes_usados = set()
    for nome_bruto, pdf in abas.items():
        # Sanitização e truncamento podem colidir dois nomes distintos; o
        # Excel não aceita abas com nome repetido no mesmo arquivo.
        nome = _nome_aba(nome_bruto)
        if nome in nomes_usados:
            sufixo = 2
            while f"{nome[:28]}_{sufixo}" in nomes_usados:
                sufixo += 1
            nome = f"{nome[:28]}_{sufixo}"
        nomes_usados.add(nome)

        limite = _limite_linhas(len(pdf.columns))
        if len(pdf) > limite:
            raise ValueError(
                f"aba {nome!r} tem {len(pdf):,} linhas e excede o limite de "
                f"{limite:,} para exportação em arquivo único "
                f"({MAX_CELULAS_EXCEL:,} células em {len(pdf.columns):,} colunas)"
            )

        planilha = workbook.create_sheet(nome)
        planilha.append([_valor_seguro(coluna) for coluna in pdf.columns])
        for registro in pdf.itertuples(index=False, name=None):
            planilha.append([_neutralizar_formula(_valor_seguro(v)) for v in registro])

    _subir_para_volume(caminho_destino, workbook.save)


_IDENTIFICADOR_VALIDO = re.compile(r"^[A-Za-z0-9_]+$")


def mapear_arquivos(nomes_tabelas, schema=SCHEMA):
    """
    Descobre qual arquivo .xlsx e qual aba cada tabela alimenta.

    Um workbook é definido pelo valor da coluna 'file', que atravessa
    VÁRIAS tabelas: o gerador (notebook 5.1) grava uma tabela por centro,
    todas com o mesmo 'file' e um 'sheet' distinto. Por isso a unidade de
    exportação é o arquivo, não a tabela — exportar tabela a tabela faria
    cada uma sobrescrever o arquivo da anterior.

    A descoberta é feita em duas consultas apenas, independentemente do
    número de tabelas:
      1. information_schema: quais tabelas têm as colunas de controle
      2. um único UNION ALL de GROUP BY: os pares (file, sheet) de todas

    Args:
        nomes_tabelas (list[str]): Tabelas candidatas à exportação
        schema (str, optional): Schema completo no formato catalog.schema.
            Defaults to SCHEMA.

    Returns:
        dict: {nome_do_arquivo: [(tabela, nome_da_aba), ...]}. Tabelas sem
            as colunas de controle viram um arquivo próprio de aba única,
            representadas com nome_da_aba = None.

    Note:
        Tabelas cujo nome não seja um identificador simples são ignoradas,
        já que o nome é interpolado na consulta de descoberta.
    """
    catalogo, nome_schema = schema.split(".")
    seguras = [t for t in nomes_tabelas if _IDENTIFICADOR_VALIDO.match(t)]

    colunas = spark.sql(
        f"""
        SELECT table_name, lower(column_name) AS coluna
        FROM {catalogo}.information_schema.columns
        WHERE table_schema = '{nome_schema}'
          AND lower(column_name) IN ('file', 'sheet')
        """
    ).collect()

    controle = {}
    for linha in colunas:
        controle.setdefault(linha.table_name, set()).add(linha.coluna)

    com_controle = [t for t in seguras if controle.get(t, set()) >= {"file", "sheet"}]
    sem_controle = [t for t in seguras if t not in set(com_controle)]

    plano = {}

    if com_controle:
        # Uma única query resolve o mapeamento inteiro. Cada GROUP BY lê
        # apenas duas colunas de uma tabela Delta (armazenamento colunar),
        # então o custo é desprezível mesmo com dezenas de tabelas.
        consulta = "\nUNION ALL\n".join(
            f"SELECT '{t}' AS tabela, file, sheet FROM {schema}.{t} GROUP BY file, sheet"
            for t in com_controle
        )
        for linha in spark.sql(consulta).collect():
            plano.setdefault(linha.file, []).append((linha.tabela, linha.sheet))

    # Tabelas sem colunas de controle viram arquivos independentes de aba única
    for tabela in sem_controle:
        plano.setdefault(tabela, []).append((tabela, None))

    return plano


def _coletar_aba(nome_tabela, nome_arquivo, nome_aba, schema=SCHEMA):
    """
    Carrega no driver os dados de uma aba, já sem as colunas de controle.

    Args:
        nome_tabela (str): Tabela de origem
        nome_arquivo (str): Valor de 'file' que identifica o workbook
        nome_aba (str | None): Valor de 'sheet'. None para tabelas sem as
            colunas de controle (a tabela inteira vira uma aba).
        schema (str, optional): Schema completo. Defaults to SCHEMA.

    Returns:
        pandas.DataFrame: Dados da aba

    Raises:
        ValueError: Se a aba exceder o teto de linhas para arquivo único
    """
    df = spark.table(f"{schema}.{nome_tabela}")

    if nome_aba is not None:
        # O filtro é redundante enquanto cada tabela carregar um único par
        # (file, sheet), mas mantém o resultado correto se isso mudar.
        df = df.filter((df.file == nome_arquivo) & (df.sheet == nome_aba)).drop(
            "file", "sheet"
        )

    # Coleta limitada: toPandas() traz tudo ao driver, então busca-se uma
    # linha a mais que o teto apenas para detectar o estouro
    pdf = df.limit(MAX_LINHAS_ARQUIVO + 1).toPandas()
    if len(pdf) > MAX_LINHAS_ARQUIVO:
        raise ValueError(
            f"{nome_tabela} excede {MAX_LINHAS_ARQUIVO:,} linhas — "
            f"exportação em arquivo único não é viável"
        )
    return pdf


def _ordenar_abas(contribuicoes):
    """
    Ordena as abas de um workbook: TTL primeiro, depois os centros.

    O total consolidado (TTL) abre o arquivo por ser a visão de leitura
    principal; os centros seguem em ordem alfabética. A ordenação
    alfabética pura jogaria TTL para o fim.

    Args:
        contribuicoes (list): Pares (tabela, nome_da_aba)

    Returns:
        list: Mesmos pares, reordenados
    """
    return sorted(
        contribuicoes,
        key=lambda par: (str(par[1]).upper() != "TTL", str(par[1])),
    )


def exportar_arquivo(nome_arquivo, contribuicoes):
    """
    Monta e envia ao volume um arquivo .xlsx com todas as suas abas.

    Args:
        nome_arquivo (str): Nome do arquivo, sem extensão (valor de 'file')
        contribuicoes (list): Pares (tabela, nome_da_aba) que compõem o
            arquivo, conforme devolvido por mapear_arquivos()

    Returns:
        int: Total de linhas gravadas no arquivo

    Raises:
        ValueError: Se o nome formar um caminho inválido ou alguma aba
            exceder o teto de linhas/células do Excel

    Side Effects:
        - Grava o arquivo .xlsx no volume, sobrescrevendo o existente
        - Imprime uma linha de progresso por arquivo

    Example:
        >>> exportar_arquivo("HAB 2026 Jun Demanda Aberta",
        ...                  [("refined_demand_aberta_HAB_TTL", "TTL")])
        [OK] HAB 2026 Jun Demanda Aberta.xlsx: 3 abas (TTL, 0503, 0505), 46227 linhas
        46227
    """
    caminho = f"{VOLUME_PATH}/{_nome_arquivo_seguro(nome_arquivo)}.xlsx"

    abas = {}
    for nome_tabela, nome_aba in _ordenar_abas(contribuicoes):
        pdf = _coletar_aba(nome_tabela, nome_arquivo, nome_aba)
        abas[nome_aba if nome_aba is not None else nome_tabela] = pdf

    _exportar_abas(abas, caminho)

    total_linhas = sum(len(pdf) for pdf in abas.values())
    print(
        f"[OK] {nome_arquivo}.xlsx: {len(abas)} abas "
        f"({', '.join(str(a) for a in abas)}), {total_linhas} linhas"
    )
    return total_linhas

# COMMAND ----------

# DBTITLE 1,Descoberta de Tabelas e Mapeamento de Arquivos
# ------------------------------------------------------------------------------
# Descoberta de Tabelas e Mapeamento de Arquivos
# ------------------------------------------------------------------------------
# Lista as tabelas refinadas do schema e descobre quais delas compõem cada
# arquivo .xlsx. Várias tabelas alimentam o mesmo arquivo (uma por aba), então
# esse mapeamento é o que permite montar cada workbook completo.
# ------------------------------------------------------------------------------
tabelas = spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()
nomes_tabelas = sorted(
    linha.tableName
    for linha in tabelas
    if linha.tableName.startswith("refined_demand_")
)

plano_exportacao = mapear_arquivos(nomes_tabelas)

print(f"Tabelas refinadas encontradas: {len(nomes_tabelas)}")
print(f"Arquivos .xlsx a gerar: {len(plano_exportacao)}\n")
for nome_arquivo in sorted(plano_exportacao):
    abas = [str(aba) for _, aba in _ordenar_abas(plano_exportacao[nome_arquivo])]
    print(f"  {nome_arquivo}.xlsx  <-  {len(abas)} abas: {', '.join(abas)}")

# COMMAND ----------

# DBTITLE 1,Limpeza do Volume de Exportação
# ==============================================================================
# LIMPEZA DO VOLUME DE EXPORTAÇÃO
# ==============================================================================
# Remove os .xlsx da raiz do volume antes da exportação, sem backup. Cada
# arquivo do plano atual já seria sobrescrito de qualquer forma — isto existe
# para tirar do volume os arquivos ÓRFÃOS: sobras de uma tabela que saiu do
# schema e não faz mais parte de plano_exportacao.
# ==============================================================================
print("\n" + "=" * 70)
print("LIMPANDO VOLUME DE EXPORTAÇÃO")
print("=" * 70)

arquivos_existentes = [
    item for item in dbutils.fs.ls(VOLUME_PATH) if item.name.lower().endswith(".xlsx")
]

if not arquivos_existentes:
    print("Nenhum .xlsx na raiz do volume — nada a fazer.")
else:
    for item in arquivos_existentes:
        dbutils.fs.rm(item.path)
        print(f"[REMOVIDO] {item.name}")
    print(f"\n[OK] {len(arquivos_existentes)} arquivos removidos.")

# COMMAND ----------

# DBTITLE 1,Exportação dos Arquivos Excel
# ==============================================================================
# EXPORTAÇÃO DOS ARQUIVOS EXCEL
# ==============================================================================
# Itera sobre os ARQUIVOS do plano — não sobre as tabelas. Cada arquivo é
# montado com todas as suas abas e enviado ao volume em um único upload.
#
# MODO TESTE: LIMITE_ARQUIVOS restringe a execução aos primeiros arquivos.
# O limite é por arquivo (e não por tabela) justamente para que cada arquivo
# gerado saia completo, com todas as abas. Defina como None para exportar tudo.
# ==============================================================================
LIMITE_ARQUIVOS = None

arquivos_ordenados = sorted(plano_exportacao)
arquivos_alvo = (
    arquivos_ordenados[:LIMITE_ARQUIVOS] if LIMITE_ARQUIVOS else arquivos_ordenados
)

print("\n" + "=" * 70)
print(f"EXPORTANDO {len(arquivos_alvo)} DE {len(arquivos_ordenados)} ARQUIVOS")
print("=" * 70)

arquivos_gerados = 0
linhas_gravadas = 0
erros = []

for nome_arquivo in arquivos_alvo:
    try:
        print(f"\n[EXPORTANDO] {nome_arquivo}...")
        linhas_gravadas += exportar_arquivo(nome_arquivo, plano_exportacao[nome_arquivo])
        arquivos_gerados += 1
    except Exception as erro:
        erros.append(f"{nome_arquivo}: {erro}")
        print(f"[ERRO] {nome_arquivo}: {erro}")

print("\n" + "=" * 70)
print("RESUMO DA EXPORTAÇÃO")
print("=" * 70)
print(f"✓ Arquivos .xlsx gerados: {arquivos_gerados}")
print(f"✓ Linhas gravadas: {linhas_gravadas:,}")
if erros:
    print(f"✗ Arquivos com erro: {len(erros)}")
    print("\nDetalhes dos erros:")
    for erro in erros:
        print(f"  - {erro}")
else:
    print("✓ Nenhum erro detectado")
print(f"\n📁 Local: {VOLUME_PATH}")
print("\nℹ️  Os arquivos Excel estão prontos para compartilhamento.")
