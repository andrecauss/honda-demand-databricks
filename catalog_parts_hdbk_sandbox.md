# Catálogo `parts_hdbk_sandbox`

Estrutura do catálogo Unity Catalog `parts_hdbk_sandbox`: schemas, tabelas, views e volumes.

> Gerado a partir de `parts_hdbk_sandbox.information_schema` em 2026-11-04. Não há funções (UDFs) registradas no catálogo.

## Visão geral

```
parts_hdbk_sandbox
├── _agents_databases        (5 views)
├── _file_orchestrator       (1 volume)
├── _utils                   (1 tabela)
├── dm_customers             (2 tabelas, 2 volumes)
├── dm_materials             (1 volume)
├── dm_suppliers             (vazio)
├── dt_deliveries            (vazio)
├── dt_production_orders     (vazio)
├── dt_purchase_orders       (vazio)
├── dt_sales_orders          (1 tabela, 1 volume)
├── dt_stock_position        (vazio)
├── pr_cadastrao             (5 tabelas, 2 volumes)
├── pr_demand                (21 tabelas, 1 view, 1 volume)
├── pr_deployment            (vazio)
├── pr_forecast              (2 tabelas, 1 volume)
├── pr_purchase_orders_mgmt  (vazio)
└── pr_stock_mgmt            (vazio)
```

Convenção de prefixos:

* `dm_` — dados mestres (clientes, materiais, fornecedores)
* `dt_` — dados transacionais (ordens de venda, entregas, compras, estoque)
* `pr_` — produtos/processos analíticos (cadastrão, demanda, forecast, etc.)
* `_` — schemas de suporte (agentes de IA, orquestração de arquivos, utilitários)

---

## `_agents_databases`

Views de consumo para agentes de IA (excluem colunas de auditoria).

| Objeto | Tipo | Colunas | Fonte / Descrição |
| --- | --- | --- | --- |
| `demand_analytical_base` | VIEW | 18 | Ordens de venda enriquecidas com cadeia de materiais e dados de cliente. Janela de 60 meses. |
| `forecast_enriched` | VIEW | 12 | Forecast dinamizado (unpivot), horizonte de 12 meses, com lookup de cadeia. Fonte: `pr_forecast.refined_forecast_enriched` |
| `material_cadastrao` | VIEW | 139 | Cadastro de materiais SAP. Fonte: `pr_cadastrao.material_cadastrao` |
| `material_inventory_history` | VIEW | 16 | Snapshot mensal de estoques e preços. Fonte: `pr_cadastrao.material_inventory_history` |
| `material_spec_changes` | VIEW | 15 | Histórico SCD2 do cadastro mestre. Fonte: `pr_cadastrao.material_spec_changes` |

## `_file_orchestrator`

| Objeto | Tipo | Descrição |
| --- | --- | --- |
| `demand` | VOLUME (MANAGED) | Área de recepção/orquestração dos arquivos de demanda |

## `_utils`

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `dim_calendario` | TABLE (Delta) | 19 | Dimensão calendário com feriados nacionais do Brasil e ano fiscal KI (Honda) |

## `dm_customers`

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `kna1_sap` | TABLE (Delta) | 5 | Dados gerais do cliente (KNA1 SAP): país, cidade, estado, razão social. PK: `cliente`. Carga manual via volume |
| `knvv_sap` | TABLE (Delta) | 5 | Área de vendas do cliente (KNVV SAP) com o centro que atende o cliente. PK: `Cliente + OrgV + CDst + SA` |
| `kna1_sap` | VOLUME (MANAGED) | — | Arquivos XLSX de origem da `kna1_sap` |
| `knvv_sap` | VOLUME (MANAGED) | — | Arquivos XLS de origem da `knvv_sap` |

## `dm_materials`

| Objeto | Tipo | Descrição |
| --- | --- | --- |
| `sap` | VOLUME (MANAGED) | Arquivos SAP de materiais |

## `dm_suppliers`

Sem objetos.

## `dt_deliveries`

Sem objetos.

## `dt_production_orders`

Sem objetos.

## `dt_purchase_orders`

Sem objetos.

## `dt_sales_orders`

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `raw_sales_order` | TABLE (Delta) | 27 | Camada raw da Sales Order SAP. Chave: `numero_ov + item`. `org_vendas`: 0200=2W, 0500=4W; `canal_dist`: 01=Doméstico, 02=Exportação |
| `sap_sales_order` | VOLUME (MANAGED) | — | Arquivos de origem das ordens de venda SAP |

## `dt_stock_position`

Sem objetos.

## `pr_cadastrao`

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `material_cadastrao` | TABLE (Delta) | 145 | Cadastro de materiais SAP (raw, colunas STRING). PK: `empresa + material`. Full overwrite |
| `material_cadeia` | TABLE (Delta) | 3 | Cadeia de substituição de materiais (refined). PK: `empresa + material`. Remove sufixos ' C' (0200) e ' C'/' G' (0500) do `item_principal_cadeia` |
| `material_de_para` | TABLE (Delta) | 10 | DE-PARA material original → item principal de cadeia acabado (BOM 1:1, sufixos C, G, N, NP). Chave: `empresa + material` |
| `material_inventory_history` | TABLE (Delta) | 20 | Snapshot mensal de estoques e preços. Chave: `empresa + material + centro + reference_date` |
| `material_spec_changes` | TABLE (Delta) | 21 | SCD2 do cadastro mestre (hash comparison). Chave de negócio: `empresa + material` |
| `sap_cadastraorefinado` | VOLUME (MANAGED) | — | Arquivos do cadastrão refinado SAP (`current/`) |
| `out_material_de_para` | VOLUME (MANAGED) | — | Saída/exportação do DE-PARA de materiais |

## `pr_demand`

Tabelas refined de demanda, separadas por empresa (sufixos `_hda` e `_hab`). As tabelas `refined_demand_*` não têm comentário no UC; as descrições abaixo foram derivadas do nome da tabela.

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `refined_demand_aberta_hab` / `_hda` | TABLE (Delta) | 63 | Demanda em aberto |
| `refined_demand_fechada_hab` / `_hda` | TABLE (Delta) | 63 | Demanda fechada |
| `refined_demand_fechada_sem_zesp_hab` / `_hda` | TABLE (Delta) | 63 | Demanda fechada sem ZESP |
| `refined_demand_mi_hab` / `_hda` | TABLE (Delta) | 63 | Demanda mercado interno |
| `refined_demand_me_hab` / `_hda` | TABLE (Delta) | 63 | Demanda mercado externo |
| `refined_demand_me_sem_zesp_hab` / `_hda` | TABLE (Delta) | 63 | Demanda mercado externo sem ZESP |
| `refined_demand_linha_hab` / `_hda` | TABLE (Delta) | 63 | Demanda de linha |
| `refined_demand_linha_abc_hda` | TABLE (Delta) | 14 | Curva ABC da demanda de linha HDA |
| `refined_demand_zpug_hab` / `_hda` | TABLE (Delta) | 63 | Demanda ZPUG |
| `refined_demand_zpug_cliente_hab` / `_hda` | TABLE (Delta) | 16 | Demanda ZPUG por cliente |
| `refined_demand_distribuicao_hab` | TABLE (Delta) | 11 | Distribuição da demanda HAB |
| `refined_demand_distribuicao_hda` | TABLE (Delta) | 13 | Distribuição da demanda HDA |
| `demand_analytical_base` | VIEW | 18 | Espelho de `_agents_databases.demand_analytical_base` |
| `demand_refined_exportfiles` | VOLUME (MANAGED) | — | Arquivos exportados de demanda (CSV/XLSX/ZIP) |

## `pr_deployment`

Sem objetos.

## `pr_forecast`

| Objeto | Tipo | Colunas | Descrição |
| --- | --- | --- | --- |
| `raw_forecast_enriched` | TABLE (Delta) | 49 | Forecast enriched bruto dos Excel SAP. Append por `_load_id`. Chave: `forecast_cycle + segment + main_material` (horizontes n+0..n+35) |
| `refined_forecast_enriched` | TABLE (Delta) | 17 | Forecast dinamizado (unpivot), horizonte de 12 meses, lookup em `pr_cadastrao.material_cadeia`. Chave: `forecast_cycle + segment + material + lag` |
| `forecast_enriched` | VOLUME (MANAGED) | — | Arquivos Excel de forecast enriched (`current/`) |

## `pr_purchase_orders_mgmt`

Sem objetos.

## `pr_stock_mgmt`

Sem objetos.

---

## Linhagem principal

Fluxo indicado pelos comentários das tabelas e pelos notebooks do repositório (não extraído da lineage do UC).

```
Volumes (arquivos SAP)
  ├── dt_sales_orders/sap_sales_order        → dt_sales_orders.raw_sales_order ─┐
  ├── dm_customers/kna1_sap, knvv_sap        → dm_customers.kna1_sap, knvv_sap ─┤
  ├── pr_cadastrao/sap_cadastraorefinado     → pr_cadastrao.material_cadastrao  │
  │                                              ├── material_cadeia ───────────┤
  │                                              ├── material_de_para → volume out_material_de_para
  │                                              ├── material_inventory_history
  │                                              └── material_spec_changes      │
  └── pr_forecast/forecast_enriched          → pr_forecast.raw_forecast_enriched
                                                 └── refined_forecast_enriched (lookup material_cadeia)
                                                                                │
                         pr_demand.refined_demand_* ◄───────────────────────────┘
                                └── volume demand_refined_exportfiles

_agents_databases.* → views de consumo sobre pr_cadastrao, pr_demand e pr_forecast
```

---

## Dicionário de colunas

Colunas extraídas de `information_schema.columns` em 2026-11-04. Tabelas com o mesmo schema de colunas estão agrupadas. Colunas mensais (`AAAA/MM`) e de horizonte (`n+N`) consecutivas aparecem em uma única linha; os meses refletem a janela no momento da extração e mudam a cada carga.

### `_utils`

#### `dim_calendario`

| # | Coluna | Tipo |
| --- | --- | --- |
| 1 | `data` | date |
| 2 | `ano` | int |
| 3 | `mes` | int |
| 4 | `dia` | int |
| 5 | `dia_semana` | int |
| 6 | `nome_dia_semana_pt` | string |
| 7 | `nome_dia_semana_en` | string |
| 8 | `nome_mes_pt` | string |
| 9 | `nome_mes_en` | string |
| 10 | `nome_mes_abrev_pt` | string |
| 11 | `nome_mes_abrev_en` | string |
| 12 | `ano_mes` | string |
| 13 | `semana_ano` | int |
| 14 | `ki_numero` | int |
| 15 | `ki` | string |
| 16 | `fim_semana` | boolean |
| 17 | `feriado` | boolean |
| 18 | `nome_feriado` | string |
| 19 | `dia_util` | boolean |

### `dm_customers`

#### `kna1_sap`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `cliente` | string | Codigo do cliente SAP (sem zeros a esquerda). Chave primaria. Campo original: CLIENTE. |
| 2 | `pais` | string | Pais do cliente (ex: BR=Brasil). Campo original SAP: PS. |
| 3 | `cidade` | string | Cidade/localidade do cliente. Campo original SAP: LOCAL. |
| 4 | `estado` | string | Regiao/estado do cliente (ex: SP, RJ, CE). Campo original SAP: RG. |
| 5 | `razao_social` | string | Nome ou razao social do cliente. Campo original SAP: NOME_3. |

#### `knvv_sap`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `cliente` | string | Codigo do cliente SAP (sem zeros a esquerda). Parte da chave primaria. |
| 2 | `orgv` | string | Organizacao de vendas SAP (ex: 0200=2W, 0500=4W). Parte da chave primaria. |
| 3 | `cdst` | string | Canal de distribuicao (ex: 01=Revenda). Parte da chave primaria. |
| 4 | `sa` | string | Setor de atividade (ex: 04=Pecas, 07=Pos-Venda). Parte da chave primaria. |
| 5 | `cen` | string | Centro que atende o cliente. Quando nulo na origem, preenchido com padrao (0203 para orgv 0200, 0503 para orgv 0500). |

### `dt_sales_orders`

#### `raw_sales_order`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `numero_ov` | bigint | Número da Ordem de Venda, ou pedido, que um cliente (dealer ou concessionária) faz |
| 2 | `data` | timestamp_ntz | Data do Pedido / Ordem de Venda |
| 3 | `tipo_ov` | string | Tipo do Pedido ou Ordem de Venda |
| 4 | `motivo_ov` | string | Justificativa ou razão do pedido, fornecendo contexto sobre as necessidades do cliente. |
| 5 | `bloqueio_rem` | string | Indica se há algum bloqueio relacionado à remessa do pedido, afetando o envio. |
| 6 | `motivo_recusa` | string | Explicação fornecida quando um pedido é recusado, ajudando a entender as causas de não conformidade. |
| 7 | `org_vendas` | string | Organização de Vendas ou Segmento de Negócio: 0200 representa 2W e 0500 representa 4W |
| 8 | `canal_dist` | string | Categoria de canal de distribuição: 01 representa Doméstico (Nacional); 02 representa Exportação (Overseas) |
| 9 | `setor_ativ` | string | Segmento de atividade relacionado, como acessórios, produto de força, DSH |
| 10 | `centro` | string | Depósito que atende o pedido: 0203 (Sumaré 2W), 0503 (Sumaré 4W), 0209 (Jaboatão 2W), 0505 (Jaboatão 5W), 0232 (Manaus 2W) |
| 11 | `emissor_da_ordem` | bigint | Identificador do código do cliente / concessionária que fez o pedido (emitiu o pedido) |
| 12 | `numero_pedido` | string | pedido interno da concessionária - dado não relevant |
| 13 | `autor` | string | Nome ou identificação do responsável que inseriu o pedido, útil para rastreabilidade. |
| 14 | `item` | bigint | Identificador único do item dentro do pedido, que associa o pedido ao seu conteúdo específico. |
| 15 | `material` | string | Identificador único do material, SKU ou partnumber solicitado pelo cliente |
| 16 | `categoria_do_item` | string | Classificação do item dentro do pedido, ajudando a agrupar produtos similares. |
| 17 | `item_superior` | bigint | Identificador do item superior na hierarquia de produtos, se aplicável. |
| 18 | `quantidade` | decimal(18,3) | Quantidade de peças solicitadas pelo cliente |
| 19 | `um` | string | Unidade de Medida utilizada para quantificar os itens solicitados no pedido. |
| 20 | `_source_file_name` | string | Nome do arquivo fonte de onde os dados foram extraídos, importante para rastreamento. |
| 21 | `_source_file_path` | string | Caminho do arquivo fonte no sistema, necessário para auditoria e verificação. |
| 22 | `_source_file_modification_time` | timestamp | Timestamp indicando quando o arquivo fonte foi modificado, útil para controle de versão. |
| 23 | `_ingested_at` | timestamp | Data e hora em que os dados foram incorporados ao sistema, importante para gestão de dados. |
| 24 | `_last_updated_at` | timestamp | Data e hora da última atualização dos dados, permitindo monitorar mudanças ao longo do tempo. |
| 25 | `_ingested_by` | string | Identifica quem fez a ingestão dos dados, essencial para governança e rastreamento. |
| 26 | `_load_type` | string | Tipo de carregamento de dados, que pode indicar a natureza da operação de ingestão. |
| 27 | `_load_id` | string | Identificador único associado ao processo de carregamento, usado para controle e auditoria. |

### `pr_cadastrao`

#### `material_cadastrao`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `centro` | string | Código do centro/depósito SAP que atende o material (ex: 0203=Sumaré 2W, 0503=Sumaré 4W) |
| 2 | `material` | string | Código único do material/peça (partnumber SAP). Parte da chave primária. |
| 3 | `empresa` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave primária. |
| 4 | `descricao_curta_portugues` | string | Descrição curta do material em português |
| 5 | `descricao_curta_ingles` | string | Descrição curta do material em inglês |
| 6 | `descricao_curta_espanhol` | string | Descrição curta do material em espanhol |
| 7 | `unidade_de_medida_basica` | string | Unidade de medida básica do material (ex: PC, KG, M) |
| 8 | `numero_material_antigo` | string | Número do material no sistema legado/antigo |
| 9 | `setor_de_atividade` | string | Código do setor de atividade (ex: 04=Automotivo) |
| 10 | `status_material` | string | Status do material no SAP (ex: N=Normal, R=Restrito, H=Histórico) |
| 11 | `peso_bruto` | string | Peso bruto do material (com embalagem) |
| 12 | `unidade_peso` | string | Unidade de medida do peso (ex: KG) |
| 13 | `peso_liquido` | string | Peso líquido do material (sem embalagem) |
| 14 | `tipo_de_material` | string | Tipo de material SAP (ex: ZRO1, ZHAW, ZFER) |
| 15 | `codigo_cor` | string | Código da cor do material |
| 16 | `descricao_cor` | string | Descrição da cor do material |
| 17 | `intercambiabilidade` | string | Indica se o material possui intercambiabilidade com outros |
| 18 | `item_principal_cadeia` | string | Material principal na cadeia de substituição |
| 19 | `data_cadeia` | string | Data de início da cadeia de substituição |
| 20 | `modelo_comercial_principal` | string | Modelo comercial principal ao qual o material está associado |
| 21 | `segmento_principal` | string | Segmento de negócio principal (ex: 4W, 2W) |
| 22 | `sub_segmento_principal` | string | Sub-segmento de negócio (ex: PAS=Passenger, COM=Commercial) |
| 23 | `cut_in_material` | string | Data de início de utilização do material (cut-in) |
| 24 | `cut_off_material` | string | Data de fim de utilização do material (cut-off) |
| 25 | `cadeia` | string | Código da cadeia de substituição |
| 26 | `ciclo_de_vida` | string | Código do ciclo de vida do material (ex: Z01, Z05) |
| 27 | `status_venda_mi` | string | Status de venda no mercado interno |
| 28 | `codigo_de_margem` | string | Código de classificação de margem do material |
| 29 | `origem` | string | Código de origem do material (1=Nacional, 3=Importado) |
| 30 | `atributo_produto_1` | string | Atributo adicional do produto (campo 1) |
| 31 | `atributo_produto_2` | string | Atributo adicional do produto (campo 2) |
| 32 | `embalagem_comercial` | string | Indicador de embalagem comercial |
| 33 | `codigo_de_ncm` | string | Código NCM (Nomenclatura Comum do Mercosul) para classificação fiscal |
| 34 | `pais_de_origem` | string | País de origem do material (ex: BR, JP) |
| 35 | `grupo_de_compradores` | string | Código do grupo de compradores responsável |
| 36 | `material_codigo_de_fabrica` | string | Código do material na fábrica (com sufixo de cor/variante) |
| 37 | `tipo_de_mrp` | string | Tipo de planejamento MRP (ex: PD=Planejamento determinístico, V1=Manual) |
| 38 | `lote_de_compra` | string | Tamanho do lote mínimo de compra |
| 39 | `estoque_reserva` | string | Quantidade de estoque de reserva/segurança |
| 40 | `tipo_de_suprimento` | string | Tipo de suprimento (ex: X=Compra externa, F=Fabricação própria) |
| 41 | `suprimento_especial` | string | Código de suprimento especial (ex: 20=Consignação) |
| 42 | `prazo_entrega_prevista_lead_time_firme` | string | Lead time firme de entrega em dias |
| 43 | `fornecedor` | string | Código do fornecedor principal no SAP |
| 44 | `descricao_do_fornecedor` | string | Razão social do fornecedor principal |
| 45 | `estoque_livre_no_centro` | string | Quantidade de estoque livre (disponível) no centro |
| 46 | `estoque_disponivel_para_venda` | string | Quantidade de estoque disponível para venda |
| 47 | `estoque_bloqueado` | string | Quantidade de estoque bloqueado (indisponível) |
| 48 | `estoque_em_transito` | string | Quantidade de estoque em trânsito entre centros |
| 49 | `estoque_em_poder_de_terceiros` | string | Quantidade de estoque em poder de terceiros |
| 50 | `estoque_em_controle_qualidade` | string | Quantidade de estoque em inspeção de qualidade |
| 51 | `estoque_devolucoes` | string | Quantidade de estoque em devoluções |
| 52 | `demanda_mensal_mes_atual_1` | string |  |
| 53 | `demanda_mensal_mes_atual_2` | string |  |
| 54 | `demanda_mensal_mes_atual_3` | string |  |
| 55 | `demanda_mensal_mes_atual_4` | string |  |
| 56 | `demanda_mensal_mes_atual_5` | string |  |
| 57 | `demanda_mensal_mes_atual_6` | string |  |
| 58 | `demanda_mensal_mes_atual_7` | string |  |
| 59 | `demanda_mensal_mes_atual_8` | string |  |
| 60 | `demanda_mensal_mes_atual_9` | string |  |
| 61 | `demanda_mensal_mes_atual_10` | string |  |
| 62 | `demanda_mensal_mes_atual_11` | string |  |
| 63 | `demanda_mensal_mes_atual_12` | string |  |
| 64 | `demanda_quinzenal_mes_atual` | string |  |
| 65 | `previsao_mes_atual` | string |  |
| 66 | `previsao_mes_atual_1` | string |  |
| 67 | `previsao_mes_atual_2` | string |  |
| 68 | `previsao_mes_atual_3` | string |  |
| 69 | `previsao_mes_atual_4` | string |  |
| 70 | `previsao_mes_atual_5` | string |  |
| 71 | `previsao_mes_atual_6` | string |  |
| 72 | `previsao_mes_atual_7` | string |  |
| 73 | `previsao_mes_atual_8` | string |  |
| 74 | `previsao_mes_atual_9` | string |  |
| 75 | `previsao_mes_atual_10` | string |  |
| 76 | `previsao_mes_atual_11` | string |  |
| 77 | `saldo_da_carteira_de_pedidos` | string | Saldo pendente na carteira de pedidos de clientes |
| 78 | `ordem_de_producao` | string | Quantidade em ordens de produção abertas |
| 79 | `quantidade_em_pi` | string | Quantidade em Pedidos de Importação (PI) |
| 80 | `quantidade_em_bo` | string | Quantidade em Back Order (BO) - pedidos pendentes |
| 81 | `pis` | string | Alíquota de PIS aplicada ao material |
| 82 | `cofins` | string | Alíquota de COFINS aplicada ao material |
| 83 | `preco_de_rede_price_de_venda_liquida` | string | Preço de venda líquida (rede) |
| 84 | `preco_de_rede_price_de_venda_bruta_base_sp` | string | Preço de venda bruta base São Paulo |
| 85 | `preco_de_exportacao_price_de_venda` | string | Preço de venda para exportação |
| 86 | `ipi_na_venda_price_por_ncm` | string | Alíquota de IPI na venda, determinada pelo NCM |
| 87 | `split_1` | string |  |
| 88 | `split_2` | string |  |
| 89 | `split_3` | string |  |
| 90 | `split_4` | string |  |
| 91 | `split_5` | string |  |
| 92 | `split_6` | string |  |
| 93 | `split_7` | string |  |
| 94 | `split_8` | string |  |
| 95 | `split_9` | string |  |
| 96 | `preco_medio_1` | string |  |
| 97 | `preco_medio_2` | string |  |
| 98 | `preco_medio_3` | string |  |
| 99 | `preco_medio_4` | string |  |
| 100 | `preco_medio_5` | string |  |
| 101 | `preco_medio_6` | string |  |
| 102 | `preco_medio_7` | string |  |
| 103 | `preco_medio_8` | string |  |
| 104 | `preco_medio_9` | string |  |
| 105 | `moeda_do_preco_liquido_info_record` | string | Moeda do preço líquido no Info Record SAP |
| 106 | `preco_liquido_info_record` | string | Preço líquido registrado no Info Record de compras |
| 107 | `centro_de_lucro` | string | Código do centro de lucro associado ao material |
| 108 | `valor_do_estoque` | string | Valor monetário total do estoque no centro |
| 109 | `estoque_de_seguranca` | string | Nível de estoque de segurança definido no MRP |
| 110 | `function_code` | string | Código de função do material |
| 111 | `sub_grupo` | string | Sub-grupo de classificação do material |
| 112 | `demanda_linha_mes_atual_1` | string |  |
| 113 | `demanda_linha_mes_atual_2` | string |  |
| 114 | `demanda_linha_mes_atual_3` | string |  |
| 115 | `codigo_de_reabastecimento` | string | Código que define a regra de reabastecimento |
| 116 | `periodo_qip` | string | Período QIP (Quality Inspection Period) |
| 117 | `lead_time_no_scm` | string | Lead time utilizado no planejamento SCM |
| 118 | `status_compra` | string | Status de compra do material |
| 119 | `split_11` | string |  |
| 120 | `split_12` | string |  |
| 121 | `preco_medio_11` | string |  |
| 122 | `preco_medio_12` | string |  |
| 123 | `status_venda_me` | string | Status de venda no mercado externo (exportação) |
| 124 | `grp_mat_2` | string | Grupo de material secundário |
| 125 | `split_13` | string |  |
| 126 | `preco_medio_13` | string |  |
| 127 | `modelo_comercial_principal_hub` | string | Modelo comercial principal no contexto HUB |
| 128 | `segmento_principal_hub` | string | Segmento principal no contexto HUB |
| 129 | `sub_segmento_principal_hub` | string | Sub-segmento principal no contexto HUB |
| 130 | `cut_in_material_hub` | string | Data de cut-in no contexto HUB |
| 131 | `cut_off_material_hub` | string | Data de cut-off no contexto HUB |
| 132 | `familia_nivel_1` | string | Classificação de família do produto - Nível 1 (mais agregado) |
| 133 | `familia_nivel_2` | string | Classificação de família do produto - Nível 2 |
| 134 | `familia_nivel_3` | string | Classificação de família do produto - Nível 3 (mais detalhado) |
| 135 | `split_10` | string |  |
| 136 | `preco_medio_10` | string |  |
| 137 | `split_16` | string |  |
| 138 | `preco_medio_16` | string |  |
| 139 | `reference_date` | date | Primeiro dia do mês de referência da carga, extraído do nome dos arquivos SAP (CAD{centro}_{yyyy}.{MM}). Usado por 1.2 e 1.3. |
| 140 | `_ingested_at` | timestamp | Data e hora da ingestão dos dados (timezone America/Sao_Paulo) |
| 141 | `_last_updated_at` | timestamp | Data e hora da última atualização dos dados |
| 142 | `_ingested_by` | string | Usuário responsável pela ingestão dos dados |
| 143 | `_load_type` | string | Tipo de carga (full=carga completa, incremental=incremental) |
| 144 | `_load_id` | string | Identificador único da execução de carga (UUID) |
| 145 | `_source_file_path` | string | Caminho do diretório fonte dos arquivos ingeridos |

#### `material_cadeia`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `empresa` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave primária. |
| 2 | `material` | string | Código único do material/peça (partnumber SAP). Parte da chave primária. |
| 3 | `item_principal_cadeia` | string | Material principal na cadeia de substituição. Quando NULL/vazio/sim na origem, assume o próprio material. Sufixos ' C'/' G' removidos conforme regra por empresa. |

#### `material_de_para`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `empresa` | string | Codigo da empresa SAP (ex: 0200=2W, 0500=4W) |
| 2 | `material` | string | Codigo original do material no cadastrao |
| 3 | `material_sem_cgn` | string | Codigo do material apos remocao dos sufixos especiais (C, G, N, NP) |
| 4 | `item_principal_cadeia_material_sem_cgn` | string | Item principal da cadeia associado ao material sem sufixos |
| 5 | `item_principal_cadeia_material_final` | string | Codigo acabado final atribuido ao material, considerando relacoes 1:1 da lista tecnica |
| 6 | `_ingested_at` | timestamp | Timestamp de ingestao (fuso America/Sao_Paulo) |
| 7 | `_ingested_by` | string | Usuario responsavel pela ingestao |
| 8 | `_load_type` | string | Tipo de carga (full/incremental) |
| 9 | `_load_id` | string | Identificador unico da execucao de carga |
| 10 | `_source_file_path` | string | Caminho do volume fonte |

#### `material_inventory_history`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `centro` | string | Código do centro/depósito SAP. |
| 2 | `material` | string | Código único do material/peça (partnumber SAP). |
| 3 | `empresa` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). |
| 4 | `estoque_livre_no_centro` | decimal(18,2) | Quantidade de estoque livre (disponível) no centro. |
| 5 | `estoque_disponivel_para_venda` | decimal(18,2) | Quantidade de estoque disponível para venda. |
| 6 | `estoque_bloqueado` | decimal(18,2) | Quantidade de estoque bloqueado (indisponível). |
| 7 | `estoque_em_transito` | decimal(18,2) | Quantidade de estoque em trânsito entre centros. |
| 8 | `estoque_em_poder_de_terceiros` | decimal(18,2) | Quantidade de estoque em poder de terceiros. |
| 9 | `estoque_em_controle_qualidade` | decimal(18,2) | Quantidade de estoque em inspeção de qualidade. |
| 10 | `estoque_devolucoes` | decimal(18,2) | Quantidade de estoque em devoluções. |
| 11 | `saldo_da_carteira_de_pedidos` | decimal(18,2) | Saldo pendente na carteira de pedidos de clientes. |
| 12 | `quantidade_em_pi` | decimal(18,2) | Quantidade em Pedidos de Importação (PI). |
| 13 | `quantidade_em_bo` | decimal(18,2) | Quantidade em Back Order (BO) - pedidos pendentes. |
| 14 | `preco_de_rede_price_de_venda_liquida` | decimal(18,2) | Preço de venda líquida (rede). |
| 15 | `estoque_total` | decimal(18,2) | Estoque total físico: soma de estoque livre, bloqueado, em trânsito, poder de terceiros e controle de qualidade. |
| 16 | `reference_date` | date | Primeiro dia do mês de referência, extraído do nome do arquivo fonte. |
| 17 | `_ingested_at` | timestamp | Data e hora da ingestão do snapshot. |
| 18 | `_ingested_by` | string | Usuário responsável pela execução da carga. |
| 19 | `_load_id` | string | Identificador único da execução da carga (UUID). |
| 20 | `_source_file_path` | string | Caminho do diretório fonte dos arquivos ingeridos. |

#### `material_spec_changes`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `empresa` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave de negócio histórica. |
| 2 | `material` | string | Código único do material/peça (partnumber SAP). Parte da chave de negócio histórica. |
| 3 | `intercambiabilidade` | string | Indicador de intercambiabilidade do material. |
| 4 | `item_principal_cadeia` | string | Item principal na cadeia de substituição. |
| 5 | `data_cadeia` | string | Data da cadeia de substituição. |
| 6 | `cut_in_material` | string | Material substituto (cut-in) na cadeia. |
| 7 | `cut_off_material` | string | Material descontinuado (cut-off) na cadeia. |
| 8 | `cadeia` | string | Identificador da cadeia de substituição. |
| 9 | `modelo_comercial_principal` | string | Modelo comercial principal associado ao material. |
| 10 | `status_compra` | string | Status de compra do material no SAP. |
| 11 | `row_hash` | string | SHA-256 dos campos rastreados para detecção de mudanças (SCD2). |
| 12 | `start_date` | timestamp | Data de início da versão (SCD2). Sentinel 1900-01-01 na carga inicial. |
| 13 | `end_date` | timestamp | Data de encerramento da versão (SCD2). NULL se vigente. |
| 14 | `is_current` | boolean | Flag indicando se é a versão corrente do registro. |
| 15 | `_ingested_at` | timestamp | Timestamp da primeira ingestão do registro. |
| 16 | `_last_updated_at` | timestamp | Timestamp da última atualização (merge/update). |
| 17 | `_ingested_by` | string | Usuário responsável pela execução da carga. |
| 18 | `_load_type` | string | Tipo de carga executada (ex: scd2_hash). |
| 19 | `_load_id` | string | Identificador único da execução da carga (UUID). |
| 20 | `_source_file_path` | string | Diretório dos arquivos SAP que definem a data de referência da carga (dados lidos de pr_cadastrao.material_cadastrao). |
| 21 | `lead_time` | string | Prazo de entrega prevista (lead time firme) do material no snapshot SAP. |

### `pr_demand`

#### `demand_analytical_base`

Mesmas colunas de `_agents_databases.demand_analytical_base`.

#### `refined_demand_aberta_hab`, `refined_demand_aberta_hda`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `file` | string |  |
| 2 | `sheet` | string |  |
| 3 | `Material` | string | Identificador único do material, SKU ou partnumber solicitado pelo cliente |
| 4 | `2021/11` … `2026/10` | int | 60 colunas mensais (janela móvel de meses fechados) |

#### `refined_demand_distribuicao_hab`

| # | Coluna | Tipo |
| --- | --- | --- |
| 1 | `file` | string |
| 2 | `sheet` | string |
| 3 | `Item Principal Cadeia` | string |
| 4 | `DMD_0503` | int |
| 5 | `DMD_0505` | int |
| 6 | `%_0503` | double |
| 7 | `%_0505` | double |
| 8 | `DMD_ME` | int |
| 9 | `DMD_MI` | int |
| 10 | `%_ME` | double |
| 11 | `%_MI` | double |

#### `refined_demand_distribuicao_hda`

| # | Coluna | Tipo |
| --- | --- | --- |
| 1 | `file` | string |
| 2 | `sheet` | string |
| 3 | `Item Principal Cadeia` | string |
| 4 | `DMD_0203` | int |
| 5 | `DMD_0209` | int |
| 6 | `DMD_0232` | int |
| 7 | `%_0203` | double |
| 8 | `%_0209` | double |
| 9 | `%_0232` | double |
| 10 | `DMD_ME` | int |
| 11 | `DMD_MI` | int |
| 12 | `%_ME` | double |
| 13 | `%_MI` | double |

#### `refined_demand_fechada_hab`, `refined_demand_fechada_hda`, `refined_demand_fechada_sem_zesp_hab`, `refined_demand_fechada_sem_zesp_hda`, `refined_demand_linha_hab`, `refined_demand_linha_hda`, `refined_demand_me_hab`, `refined_demand_me_hda`, `refined_demand_me_sem_zesp_hab`, `refined_demand_me_sem_zesp_hda`, `refined_demand_mi_hab`, `refined_demand_mi_hda`, `refined_demand_zpug_hab`, `refined_demand_zpug_hda`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `file` | string |  |
| 2 | `sheet` | string |  |
| 3 | `Item Principal Cadeia` | string |  |
| 4 | `2021/11` … `2026/10` | int | 60 colunas mensais (janela móvel de meses fechados) |

#### `refined_demand_linha_abc_hda`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `file` | string |  |
| 2 | `sheet` | string |  |
| 3 | `Item Principal Cadeia` | string |  |
| 4 | `2026/05` … `2026/10` | int | 6 colunas mensais (janela móvel de meses fechados) |
| 5 | `sum_6m` | int |  |
| 6 | `part_perc` | double |  |
| 7 | `perc_acc` | double |  |
| 8 | `rank` | int |  |
| 9 | `classe_linha` | string |  |

#### `refined_demand_zpug_cliente_hab`, `refined_demand_zpug_cliente_hda`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `file` | string |  |
| 2 | `sheet` | string |  |
| 3 | `Item Principal Cadeia` | string |  |
| 4 | `Cliente` | bigint |  |
| 5 | `2025/11` … `2026/10` | int | 12 colunas mensais (janela móvel de meses fechados) |

### `pr_forecast`

#### `raw_forecast_enriched`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `forecast_cycle` | date | Primeiro dia do mês do ciclo de forecast (date). |
| 2 | `department` | string | Departamento de origem (ex: Domestic - Parts, Export - Parts). |
| 3 | `forecast_type` | string | Tipo de forecast (ex: Enrich, Judgment, Consensus). |
| 4 | `segment` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). |
| 5 | `market` | string | Mercado de destino (ex: Domestic, Export). |
| 6 | `main_material` | string | Código do material/peça (partnumber SAP). |
| 7 | `forecast_classification` | string | Classificação do forecast (ex: Supply - Parts). |
| 8 | `n+0` … `n+35` | double | 36 colunas de horizonte: quantidade prevista para o mês forecast_cycle + N meses |
| 9 | `_source_file_name` | string | Nome do arquivo Excel fonte. |
| 10 | `_source_file_path` | string | Caminho completo do arquivo fonte no Volume. |
| 11 | `_ingested_at` | timestamp | Data e hora da ingestão do registro. |
| 12 | `_ingested_by` | string | Usuário responsável pela execução da carga. |
| 13 | `_load_type` | string | Tipo de carga (incremental). |
| 14 | `_load_id` | string | Identificador único da execução da carga (UUID). |

#### `refined_forecast_enriched`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `forecast_cycle` | date | Primeiro dia do mês do ciclo de forecast (date). |
| 2 | `department` | string | Departamento de origem, sem prefixo Domestic/Export. |
| 3 | `forecast_type` | string | Tipo de forecast (ex: Enrich, Judgment, Consensus). |
| 4 | `segment` | string | Código da empresa SAP (ex: 0200=2W, 0500=4W). |
| 5 | `market` | string | Mercado de destino (ex: Domestic, Export). |
| 6 | `material` | string | Código do material/peça original (partnumber SAP). |
| 7 | `main_material` | string | Material principal na cadeia de substituição (item_principal_cadeia via pr_cadastrao.material_cadeia). |
| 8 | `forecast_classification` | string | Classificação do forecast (ex: Forecastable/Non-Forecastable). |
| 9 | `forecast_classification_tag` | string | Tag extraída da classificação (tudo após o primeiro ' - '). |
| 10 | `lag` | int | Offset em meses do horizonte (0 = mês corrente, até 11). |
| 11 | `forecast_month` | date | Mês-alvo do forecast: forecast_cycle + lag meses. |
| 12 | `forecast_qty` | double | Quantidade prevista para o mês-alvo. |
| 13 | `_raw_load_id` | string | Identificador da carga original na tabela raw. |
| 14 | `_ingested_at` | timestamp | Data e hora da ingestão do registro. |
| 15 | `_ingested_by` | string | Usuário responsável pela execução da carga. |
| 16 | `_load_type` | string | Tipo de carga (incremental). |
| 17 | `_load_id` | string | Identificador único da execução da carga (UUID). |

### `_agents_databases`

#### `demand_analytical_base`

| # | Coluna | Tipo | Comentário |
| --- | --- | --- | --- |
| 1 | `numero_ordem_venda` | string | Número da ordem de venda SAP. |
| 2 | `item_ordem_venda` | string | Número do item dentro da ordem de venda. |
| 3 | `data_ordem` | date | Data da ordem de venda. |
| 4 | `tipo_ordem_venda` | string | Tipo da ordem de venda SAP. |
| 5 | `organizacao_vendas` | string | Código da organização de vendas (ex: 0200=2W, 0500=4W). |
| 6 | `canal_distribuicao` | string | Canal de distribuição (01=Doméstico, 02=Exportação). |
| 7 | `codigo_cliente` | string | Código do cliente (emissor da ordem). |
| 8 | `centro_fornecedor` | string | Código do centro fornecedor SAP. |
| 9 | `codigo_material` | string | Código do material/peça (partnumber SAP). |
| 10 | `quantidade` | int | Quantidade solicitada no item da ordem. |
| 11 | `item_principal_cadeia` | string | Material principal na cadeia de substituição. |
| 12 | `centro_distribuicao_original` | string | Centro de distribuição original do cliente. |
| 13 | `cliente` | string | Razão social do cliente. |
| 14 | `uf_cliente` | string | Estado/UF do cliente. |
| 15 | `pais_cliente` | string | País do cliente. |
| 16 | `segmento` | string | Segmento de negócio (2W - Motos / 4W - Automóveis). |
| 17 | `mercado` | string | Mercado de destino (Doméstico / Exportação). |
| 18 | `centro_nome` | string | Nome descritivo do centro de distribuição. |

#### `forecast_enriched`

Mesmas colunas de `pr_forecast.refined_forecast_enriched` (12 colunas), exceto as de auditoria: `_ingested_at`, `_ingested_by`, `_load_id`, `_load_type`, `_raw_load_id`.

#### `material_cadastrao`

Mesmas colunas de `pr_cadastrao.material_cadastrao` (139 colunas), exceto as de auditoria: `_ingested_at`, `_ingested_by`, `_last_updated_at`, `_load_id`, `_load_type`, `_source_file_path`.

#### `material_inventory_history`

Mesmas colunas de `pr_cadastrao.material_inventory_history` (16 colunas), exceto as de auditoria: `_ingested_at`, `_ingested_by`, `_load_id`, `_source_file_path`.

#### `material_spec_changes`

Mesmas colunas de `pr_cadastrao.material_spec_changes` (15 colunas), exceto as de auditoria: `_ingested_at`, `_ingested_by`, `_last_updated_at`, `_load_id`, `_load_type`, `_source_file_path`.
