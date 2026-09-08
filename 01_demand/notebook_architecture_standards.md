# Padrões para notebooks Databricks

- **Responsável:** Demand Planning — Honda Parts Division
- **Versão:** 3.0
- **Atualização:** 2026-09-05

## Objetivo

Manter os notebooks fáceis de entender, executar e versionar, com o mínimo de
dependências e manutenção documental.

O padrão privilegia documentação estática. O cabeçalho não deve importar
módulos, imprimir conteúdo ou interferir na execução do pipeline.

## 1. Primeira célula: visão geral em Markdown

Todo notebook começa com uma célula Markdown curta contendo somente as
informações necessárias para entender seu contrato.

Campos obrigatórios:

- título e número do notebook;
- propósito;
- entradas;
- saídas;
- granularidade ou chave, quando aplicável;
- modo ou frequência da carga.

Exemplo:

```markdown
# 2.3 — Histórico de materiais (SCD2)

- **Propósito:** Manter o histórico das alterações do cadastro de materiais.
- **Entrada:** `pr_cadastrao/sap_cadastraorefinado/current`
- **Saída:** `pr_cadastrao.material_spec_changes`
- **Chave:** Empresa + Material · **Carga:** Mensal, SCD Type 2
```

No formato Databricks Source `.py`, usar:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 2.3 — Histórico de materiais (SCD2)
# MAGIC
# MAGIC - **Propósito:** Manter o histórico das alterações do cadastro de materiais.
# MAGIC - **Entrada:** `pr_cadastrao/sap_cadastraorefinado/current`
# MAGIC - **Saída:** `pr_cadastrao.material_spec_changes`
# MAGIC - **Chave:** Empresa + Material · **Carga:** Mensal, SCD Type 2

# COMMAND ----------
```

### O que não colocar no cabeçalho

- histórico de alterações, pois essa responsabilidade é do Git;
- dependências que já estejam evidentes nos imports;
- lista completa das transformações;
- explicações extensas de regras de negócio;
- autor e data atualizados manualmente;
- código Python ou imports usados apenas para exibir documentação.

## 2. Estrutura recomendada

Após a visão geral, organizar o notebook nesta ordem sempre que aplicável:

1. instalação de dependências;
2. imports;
3. parâmetros e constantes;
4. validações de entrada;
5. leitura;
6. transformação;
7. persistência;
8. validação final resumida.

Dependências instaladas com `%pip` devem vir antes de células que criem estado,
pois a instalação pode reiniciar o interpretador Python.

## 3. Comentários

Comentários devem explicar decisões e regras de negócio, não repetir o código.

Bom exemplo:

```python
# ZESP representa pedido inicial de exportação e não deve compor a demanda
# recorrente usada no planejamento.
df_sem_zesp = df.filter(F.col("tipo_ov") != "ZESP")
```

Evitar grandes blocos decorativos. Separadores são permitidos apenas para
delimitar etapas importantes do notebook.

## 4. Funções

Funções reutilizáveis devem:

- usar `snake_case`;
- ter responsabilidade única;
- declarar tipos quando isso melhorar a leitura;
- possuir docstring curta com argumentos, retorno e efeitos colaterais
  relevantes;
- evitar dependência implícita de variáveis globais quando possível.

## 5. Configuração

Catálogo, schemas, volumes, janelas temporais e outros parâmetros operacionais
devem ficar agrupados no início do notebook ou em uma configuração central.

Não usar caminhos pessoais, como `/Workspace/Users/<usuario>/...`, para carregar
documentação ou utilitários.

Arquivos YAML só devem ser adicionados quando forem consumidos por automação,
implantação ou validação. Não duplicar neles a documentação do cabeçalho.

## 6. Logs e consumo de recursos

Em execução automática:

- imprimir somente início, fim, parâmetros relevantes e resultado resumido;
- evitar `display()` fora de diagnóstico interativo;
- evitar múltiplos `count()` sobre o mesmo DataFrame;
- não executar coleta completa no driver sem limite explícito;
- não imprimir documentação estática a cada execução.

## 7. Fonte oficial de cada informação

| Informação | Fonte oficial |
| --- | --- |
| Contrato do notebook | Primeira célula Markdown |
| Regra de negócio | Próxima ao código correspondente |
| Ordem geral do pipeline | `README.md` |
| Tabelas e colunas | Unity Catalog |
| Histórico de mudanças | Git |
| Configuração de jobs e ambientes | Automação declarativa, quando adotada |

## Checklist

Antes de publicar um notebook, verificar:

- [ ] primeira célula Markdown conforme o padrão;
- [ ] entradas e saídas correspondem ao código;
- [ ] chave ou granularidade está explícita;
- [ ] nenhuma documentação interfere na execução;
- [ ] parâmetros operacionais estão agrupados;
- [ ] regras relevantes estão explicadas perto da transformação;
- [ ] logs não provocam ações Spark desnecessárias;
- [ ] outputs de execução não foram versionados.
