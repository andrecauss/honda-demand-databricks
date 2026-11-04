# Processo de criação da tabela DE-PARA

## Material original → material acabado em item principal de cadeia

## 1. Objetivo

Estabelecer o relacionamento de cada material do cadastrão aberto com seu respectivo código acabado, no estado de item principal de cadeia, para facilitar o planejamento manual de peças, sem utilização de sistema com MRP.

## 2. Escopo dos dados

O processo utiliza os dados constantes nas seguintes fontes:

- Cadastrão aberto do centro principal: `CADXX03`
  `/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/`
- Lista técnica do centro produtivo principal: `0x17`
  `/Volumes/parts_hdbk_sandbox/dm_materials/sap/bom_production_center/`

## 3. Premissas e convenções gerais

1. Devem ser respeitados os conceitos já adotados pela área de negócio na atribuição dos grupos de compradores `040` ou `054` para itens resultantes de processos especiais, mesmo quando consumirem apenas um componente.
2. Materiais com sufixo ` N` representam um estado sem embalagem. Para obter a representação exata do código, o sufixo ` N` deve ser removido.
3. Para códigos especiais com sufixos ` C`, ` G` ou ` NP`, devem ser aplicadas as convenções abaixo.

### 3.1. Tratamento de códigos especiais

| Sufixo do item | Grupo de compradores | 2W: demanda histórica, forecast, estoque e carteira de pedidos | 4W: demanda histórica, forecast, estoque e carteira de pedidos |
|---|---:|---|---|
| ` C` | Não aplicável | Unificar no item sem sufixo | Unificar no item sem sufixo |
| ` G` | `040` | Manter no item original | Unificar no item sem sufixo |
| ` G` | Diferente de `040` | Unificar no item sem sufixo | Unificar no item sem sufixo |
| ` NP` | Não aplicável | Unificar no item sem sufixo | Unificar no item sem sufixo |

## 4. Resumo do algoritmo

O algoritmo é dividido em duas etapas:

1. **Etapa A:** identificação dos itens componentes, ou materiais sem embalagem, da lista técnica que possuem uma relação única e direta com seu próprio código acabado embalado.
2. **Etapa B:** tratamento dos códigos especiais no cadastrão, atribuição dos códigos principais de cadeia e inclusão das informações da lista técnica para definição do código acabado final.

## 5. Descrição do algoritmo

## 5.1. Etapa A — Criação da tabela de relacionamento direto entre componente sem embalagem e item produzido

Esta etapa tem como base a lista técnica do centro produtivo e agrega informações do cadastrão aberto.

### 5.1.1. Preparação da lista técnica

1. Baixar a lista técnica do centro produtivo `0217` ou `0517` por meio da transação `ZSPPRPP002`.
2. Criar o campo `Empresa` a partir do campo `Centro`, utilizando os dois primeiros dígitos do campo `Centro` concatenados com `00`.
3. Remover as linhas cujo campo `Material` termine com ` C`, ` G` ou ` NP`.

### 5.1.2. Tratamento do componente e seleção de relacionamentos 1:1

4. Fazer um *join* entre o resultado da etapa anterior, utilizando os campos `Empresa` e `Componente`, e o cadastrão aberto `CAD0203` ou `CAD0503`, utilizando os campos `Empresa` e `Material`.
5. Trazer o campo `Tipo de Material` de cada `Componente` e manter somente as linhas cujo tipo seja `ZHAW`, `ZFER` ou `ZRO1`. Excluir os componentes com outros tipos de material.
6. Criar um agrupamento por `Empresa` e `Material` e contar o número de linhas para verificar a quantidade de componentes por código acabado.
7. Selecionar somente as linhas cuja contagem seja igual a `1`, removendo os materiais produzidos a partir de mais de um componente.
8. Trazer o campo `Item Principal da Cadeia` do cadastrão aberto para converter o componente em item principal de cadeia.
9. Renomear o campo `Item Principal da Cadeia` para `Item Principal da Cadeia (Componente)`.
10. No campo `Item Principal da Cadeia (Componente)`, substituir os valores nulos ou iguais a `SIM` pelo valor do campo `Componente`.
11. Remover as linhas cujo valor do campo `Item Principal da Cadeia (Componente)` termine em ` N`. Esses materiais serão tratados diretamente na Etapa B.
12. Manter somente os campos:
    - `Empresa`;
    - `Item Principal da Cadeia (Componente)`;
    - `Material`.

### 5.1.3. Tratamento do material produzido e geração da tabela de relacionamento

13. Fazer um *join* entre o resultado anterior, utilizando os campos `Empresa` e `Material`, e o cadastrão aberto `CAD0203` ou `CAD0503`, também pelos campos `Empresa` e `Material`.
14. Trazer o campo `Grupo de Compradores` de cada `Material` e remover as linhas cujo grupo seja `040` ou `054`, eliminando os materiais produzidos em processo de montagem *in-house*.
15. Trazer o campo `Item Principal da Cadeia` do cadastrão aberto para converter o material produzido em item principal de cadeia.
16. Renomear o campo `Item Principal da Cadeia` para `Item Principal da Cadeia (Material Produzido)`.
17. No campo `Item Principal da Cadeia (Material Produzido)`, substituir os valores nulos ou iguais a `SIM` pelo valor do campo `Material`.
18. Manter somente os campos:
    - `Empresa`;
    - `Item Principal da Cadeia (Componente)`;
    - `Item Principal da Cadeia (Material Produzido)`.
19. Remover as linhas duplicadas.

### 5.1.4. Saída da Etapa A

A saída desta etapa é uma tabela de relacionamentos diretos 1:1 entre:

- `Item Principal da Cadeia (Componente)`; e
- `Item Principal da Cadeia (Material Produzido)`.

## 5.2. Etapa B — Criação da tabela DE-PARA de materiais

Esta etapa tem como base o cadastrão aberto e agrega as informações geradas na Etapa A, referentes às listas técnicas com relacionamento 1:1.

### 5.2.1. Preparação e tratamento dos materiais

1. Baixar o cadastrão aberto `CAD0203` ou `CAD0503`.
2. Manter somente as linhas cujo campo `Tipo de Material` seja `ZHAW`, `ZFER` ou `ZRO1`.
3. Criar o campo `Material sem CGN` a partir do campo `Material`, aplicando a seguinte regra:

```text
Se Material termina com " N"
ou Material termina com " C"
ou (Material termina com " G" e Empresa = "0500")
ou (Material termina com " G" e Empresa = "0200" e Grupo de Compradores <> "040")
ou Material termina com " NP"
então remover o sufixo do campo Material;
caso contrário, copiar o próprio valor do campo Material.
```

### 5.2.2. Definição do item principal de cadeia

4. Fazer um *join* entre o resultado da etapa anterior, pelos campos `Empresa` e `Material sem CGN`, e o cadastrão aberto `CAD0203` ou `CAD0503`, pelos campos `Empresa` e `Material`.
5. Trazer o campo `Item Principal da Cadeia` associado ao campo `Material sem CGN`, convertendo o material sem CGN em seu item principal.
6. Renomear o campo `Item Principal da Cadeia` para `Item Principal da Cadeia (Material sem CGN)`.
7. No campo `Item Principal da Cadeia (Material sem CGN)`, substituir os valores nulos ou iguais a `SIM` pelo próprio valor do campo `Material sem CGN`.

### 5.2.3. Associação com a tabela da Etapa A

8. Fazer um *left join* entre o resultado anterior, pelos campos `Empresa` e `Item Principal da Cadeia (Material sem CGN)`, e o resultado da Etapa A, pelos campos `Empresa` e `Item Principal da Cadeia (Componente)`.
9. Trazer o campo `Item Principal da Cadeia (Material Produzido)` correspondente ao campo `Item Principal da Cadeia (Material sem CGN)`.
10. No campo `Item Principal da Cadeia (Material Produzido)`, substituir os valores nulos ou iguais a `SIM` pelo próprio valor do campo `Item Principal da Cadeia (Material sem CGN)`.
11. Renomear o campo `Item Principal da Cadeia (Material Produzido)` para `Item Principal da Cadeia (Material Final)`.

### 5.2.4. Estrutura da tabela final

12. Manter somente os campos:

| Campo | Descrição |
|---|---|
| `Empresa` | Empresa associada ao material |
| `Material` | Código original do material |
| `Material sem CGN` | Código após o tratamento dos sufixos especiais |
| `Item Principal da Cadeia (Material sem CGN)` | Item principal associado ao material tratado |
| `Item Principal da Cadeia (Material Final)` | Código acabado final atribuído ao material |

## 6. Resultado esperado

Ao final do processo, a tabela DE-PARA relacionará cada material original ao respectivo item principal de cadeia e ao código de material final, considerando:

- os tipos de material elegíveis;
- os sufixos especiais ` C`, ` G`, ` N` e ` NP`;
- os grupos de compradores `040` e `054`;
- os relacionamentos diretos 1:1 identificados na lista técnica;
- as regras específicas aplicáveis às empresas `0200` e `0500`.

---

**Fim do processo.**
