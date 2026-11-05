# Proposta de estrutura do sandbox compartilhado
Versão 0.2 — 2026-11-05 — Rascunho para discussão com André.

## Escopo e estado
Esta branch contém somente documentação. Nenhum notebook, arquivo operacional, schema, volume, tabela, view, permissão ou job foi modificado no Databricks. Os nomes e caminhos de destino abaixo são propostas, não objetos já criados.

Objetivo: organizar o sandbox para quatro departamentos que compartilham fontes, mas possuem processos, calendários e produtos distintos. Os quatro departamentos confirmados são Demand, Inventory Long Term, Inventory Short Term e Spec. Os nomes dos departamentos serão sempre em inglês, conforme definido por André. Pessoas responsáveis, processos, consumidores e acessos ainda serão levantados.

| Departamento confirmado | Identificador técnico proposto | Responsabilidades e acessos |
| --- | --- | --- |
| Demand | `demand` | A detalhar na entrevista; repositório atual concentra processos de demanda |
| Inventory Long Term | `inventory_long_term` | A detalhar; não inferir horizonte, processos ou permissões pelo nome |
| Inventory Short Term | `inventory_short_term` | A detalhar; não inferir horizonte, processos ou permissões pelo nome |
| Spec | `spec` | Technical Specification; processos e acessos a detalhar |

Esses identificadores são sugestões para metadados e grupos futuros, não novos schemas por departamento. Department e process são dimensões distintas: por exemplo, forecast é um processo, sem implicar um quinto departamento.

Fonte do levantamento: árvore da main em `0e7e2a33f836db5c38617e935d60a037dcf942a7`, inventário `catalog_parts_hdbk_sandbox.md` de 2026-11-04 e leitura de notebooks. O inventário é uma fotografia documental; permissões, jobs, disponibilidade e dependências externas não foram inspecionados no ambiente.

## 1. Direção proposta
- Manter o catálogo `parts_hdbk_sandbox` nesta fase.
- Organizar bases compartilhadas por entidade; produtos por processo. Departamento identifica responsabilidade e acesso, sem obrigar uma cópia de cada fonte por área.
- Preservar a distinção existente: `dm_` para dados mestres, `dt_` para transacionais, `pr_` para processos e `_` para suporte.
- Separar classificação de domínio (schema) de maturidade (raw/trusted/refined). Um dado em `dm_` não é necessariamente bruto; o contrato registra sua camada.
- Definir um responsável e uma rotina oficial de escrita para cada base compartilhada. Consumidores não escrevem na base de outro processo.
- Preservar lotes e versões para que uma área trabalhe com seu fechamento aprovado enquanto outra atualiza dados mais recentes.
- Tratar File Orchestrator como interface de arquivos; tabelas e views continuam sendo os contratos de dados.

## 2. Hierarquia e nomenclatura
No Unity Catalog, `catalog.schema.volume` identifica um volume. Em `/Volumes/catalog/schema/volume/pasta`, os elementos seguintes ao volume são diretórios, não novos níveis de permissão.

| Objeto | Convenção proposta | Exemplo |
| --- | --- | --- |
| Catálogo | Ambiente existente | `parts_hdbk_sandbox` |
| Schema mestre | `dm_<entidade_plural>` | `dm_customers`, `dm_materials` |
| Schema transacional | `dt_<evento_plural>` | `dt_sales_orders` |
| Schema de processo | `pr_<processo>` | `pr_demand`, `pr_forecast` |
| Schema de suporte | `_<funcao>` | `_file_orchestrator`, `_agents_databases`, `_utils` |
| Tabela de fonte bruta | `raw_<fonte>_<entidade>` | `raw_sap_sales_order` |
| Base compartilhada tratada | `trusted_<entidade>` | `trusted_customers` |
| Produto com regra de negócio | `refined_<produto>` | `refined_demand_monthly` |
| View de consumo | `vw_<produto>` | `vw_demand_history` |
| Volume de intercâmbio | `<escopo>_<input_ou_output>` | `demand_input`, `demand_output` |
| Volume técnico de fonte | `files_<fonte>_<dataset>` | `files_sap_kna1` |
| Volume técnico de processo | `files_<processo>` | `files_demand` |
| Notebook | `<etapa>_<acao>_<objeto>` | `10_ingest_sales_order` |

Identificadores novos: inglês, minúsculas, snake_case, sem acentos ou espaços. Nomes dos departamentos permanecem em inglês: Demand, Inventory Long Term, Inventory Short Term e Spec. Descrições explicativas podem ficar em português; rótulos de exportação dependem do contrato. Etapas 10/20/30 indicam ordenação visual; as dependências reais pertencem ao contrato do pipeline e ao job.

Os exemplos não implicam renomeação de objetos existentes. Mapear antes de migrar. A proposta de schemas preserva os prefixos do catálogo e do usuário, em lugar da alternativa genérica de ambiente/camada/domínio presente nas instruções de arquitetura do repositório. Não atualizar essas instruções antes de decidir o padrão.

## 3. Bases compartilhadas e produtos
| Escopo | Responsabilidade proposta | Regra |
| --- | --- | --- |
| `dm_customers`, `dm_materials`, `dm_suppliers` | Fontes e bases comuns de entidades mestres | Uma ingestão oficial por contrato de fonte |
| `dt_sales_orders` e demais `dt_*` | Fontes e bases comuns transacionais | Preservar granularidade e histórico |
| `pr_cadastrao` | Transformações específicas de cadastro consolidado, cadeia e histórico | Confirmar quais saídas são produtos comuns e quais são específicas |
| `pr_demand` | Apuração histórica de demanda | Usa versões identificadas das bases compartilhadas |
| `pr_forecast` | Recebimento, tratamento e avaliação de forecast | Versão/ciclo explícito |
| Demais `pr_*` | Processos futuros | Responsáveis e contratos ainda pendentes |
| `_agents_databases` | Views aprovadas para consulta pelo agente | Expor apenas objetos necessários e permissões explícitas |
| `_utils` | Calendário e dimensões utilitárias compartilhadas | Proprietário e calendário de atualização definidos |

Dois arquivos de origem só podem ser tratados como o mesmo dataset após comparar sistema, extração, filtros, granularidade, chaves, referência temporal e cobertura. Diferenças reais justificam contratos distintos. Não deduplicar apenas pelo nome.

Para cada fonte comum, escolher dono funcional, mantenedor técnico, periodicidade, consumidores e política de publicação. A execução de um processo não pode apagar arquivos, checkpoints ou partições de outro.

## 4. File Orchestrator: interface e áreas internas
Separar volumes quando os públicos ou permissões diferirem. Pastas organizam; não constituem isolamento de permissões do Unity Catalog.

| Caminho proposto sob `/Volumes/parts_hdbk_sandbox/` | Uso | Público |
| --- | --- | --- |
| `_file_orchestrator/shared_input/<dataset>/` | Entrega de fontes comuns | Grupos autorizados a enviar essas fontes |
| `_file_orchestrator/demand_input/<dataset>/` | Entradas específicas de demanda | Upload de demanda |
| `_file_orchestrator/demand_output/<produto>/<referencia>/<run_id>/` | Resultados publicados | Download de demanda |
| `_file_orchestrator/forecast_input/<dataset>/` | Entradas específicas de forecast | Upload desse processo |
| `_file_orchestrator/forecast_output/<produto>/<referencia>/<run_id>/` | Forecast publicado | Download desse processo |
| `dm_customers/files_sap_kna1/<estado>/<batch_id>/` | Recepção técnica, arquivo original e rejeições | Rotina mantenedora e suporte autorizado |
| `pr_demand/files_demand/<estado>/<run_id>/` | Trabalho, exportação temporária e pacotes | Rotinas de demanda e suporte autorizado |

`shared_input` só é adequado quando todos os remetentes têm o mesmo escopo de acesso. Se houver fontes com públicos diferentes, criar volumes específicos, por exemplo `customers_input`; não tentar proteger subpastas com grants de volume.

Estados internos sugeridos: `received`, `archive`, `quarantine` para arquivos de fonte; `working`, `ready` para arquivos de processo. Checkpoints e schema de ingestão ficam em caminhos técnicos exclusivos da rotina e não em volumes de usuário.

Upload direto no volume exige leitura e escrita: isso também permite modificar ou excluir arquivos desse volume. Não equivale a uma caixa de envio sem leitura/exclusão. Se essa restrição for necessária, a interface de upload deverá mediar a operação; essa escolha depende da entrevista.

Não registrar arquivos de dados de tabelas Delta em volumes de intercâmbio. As tabelas permanecem objetos próprios no Unity Catalog.

## 5. Ciclo de vida de um arquivo
1. Receber: identificar dataset, remetente, nome original, referência, instante e `batch_id`; só liberar para processamento após entrega completa.
2. Validar: formato, versão de layout, colunas, referência e checksum. Fonte não cadastrada ou arquivo inválido fica em quarentena com motivo.
3. Registrar e arquivar: preservar uma cópia original imutável para os usuários operacionais; confirmar a cópia antes de remover a entrega transitória.
4. Ingerir: usar chave de idempotência `dataset_id + referencia + checksum`; tentativa repetida do mesmo conteúdo não deve gerar duplicação.
5. Processar: executar com `run_id`, lotes de entrada e versões de referência explícitos.
6. Validar saída: conferir totais, chaves e completude antes de disponibilizar.
7. Publicar: construir arquivos e ZIP na área técnica; copiar e validar o conjunto final antes de marcá-lo como disponível no controle de publicação.
8. Reter: aplicar política acordada de guarda para originais, rejeitados, resultados e logs; prazo pendente.

Não presumir que mover arquivos entre volumes ou copiar um conjunto de arquivos é uma operação atômica. O estado de publicação só muda para disponível após verificação do conjunto. Em falha, a versão publicada anterior continua identificada e utilizável.

O ZIP deve conter apenas os arquivos da execução e referência selecionadas. Pacotes anteriores não entram no novo pacote. Reprocessamento recebe novo `run_id`, vínculo com a tentativa anterior e resultado distinguível.

## 6. Convenção de arquivos
Padrão técnico proposto:
`<fonte>__<dataset>__<escopo>__<referencia>__<extracao_utc>__<batch_id>.<ext>`

Exemplo de entrada:
`sap__kna1__all__2026-10-31__20261105T100000Z__b001.xlsx`

Exemplo de saída:
`databricks__historical_demand__hda__2026-10__20261105T110000Z__r001.zip`

`b001` e `r001` são ilustrativos; adotar identificadores únicos no controle. Escopo vem de uma lista controlada: empresa, mercado ou `all`, conforme o contrato. Referência é data de snapshot, mês ou intervalo definido para cada dataset. Extração é instante UTC; o contexto operacional e o horário de apresentação são America/Sao_Paulo.

Preservar nome original no registro. Para arquivos SAP com nomes fixos, permitir alias de recepção cadastrado e normalizar internamente; não exigir renomeação manual sem avaliar o fluxo. O roteamento futuro deve identificar o dataset completo, em vez de depender somente do texto anterior ao primeiro sublinhado.

## 7. Contrato mínimo de pipeline e controles
| Campo | Conteúdo obrigatório |
| --- | --- |
| Identificação | `pipeline_id`, descrição, departamento responsável e mantenedor |
| Entradas | Dataset, caminho/objeto, camada, layout, chaves, filtros e cobertura |
| Tempo | Frequência, prazo, mês fechado versus dado corrente, dependências |
| Transformação | Regras de negócio, granularidade, chaves e política de histórico |
| Execução | Lotes de entrada, versões Delta ou snapshot publicado, `run_id` e parâmetros |
| Saídas | Tabela/view/arquivo, destino, consumidores e condição de publicação |
| Acesso | Identidade executora, grupos leitores/escritores e proprietário |
| Qualidade | Critérios que bloqueiam carga/publicação e ação de falha |
| Recuperação | Idempotência, reprocessamento, guarda e reversão |
| Mudança | Versão do contrato e consumidores afetados |

Controles propostos no suporte, sujeitos a desenho detalhado:
- `file_registry`: dataset, caminho/nome original, checksum, referência, lote, recebimento, validação, arquivamento e erro.
- `pipeline_runs`: tentativa, entradas/versões, parâmetros, dependências, início/fim e status.
- `publication_registry`: produto, referência, execução, arquivos, checksum, quantidade e status disponível/superado.

Os registros devem distinguir arquivo recebido, arquivo ingerido, transformação concluída e resultado publicado. Versão Delta permite reprodução apenas enquanto seus dados permanecerem retidos; para fechamentos de longa duração, definir snapshot persistido ou retenção compatível.

## 8. Perfis de acesso propostos
Grupos são papéis, não os quatro departamentos. Uma pessoa pode ter vários papéis aprovados.

| Papel | Arquivos de entrada | Saída publicada | Bases/tabelas | Administração |
| --- | --- | --- | --- | --- |
| Remetente de dataset | Leitura/escrita apenas no volume autorizado | Somente se também for leitor | Sem acesso por esse papel | Não |
| Leitor de exportação | Nenhum | Leitura no volume autorizado | Sem acesso por esse papel | Não |
| Usuário do agente | Nenhum por esse papel | Nenhum por esse papel | Consultas pelas views aprovadas e identidade configurada | Não |
| Analista consumidor | Nenhum por padrão | Conforme produto | SELECT em bases/views aprovadas | Não |
| Desenvolvedor de processo | Conforme necessidade | Conforme produto | Escrita apenas nos objetos do próprio processo | Sem administrar outras áreas |
| Identidade da ingestão | Leitura/escrita nos caminhos atribuídos | Nenhum por padrão | Escrita nas bases que mantém | Sem grants globais |
| Identidade do processo/publicador | Entradas técnicas necessárias | Escrita no output correspondente | Leitura contratada e escrita nas saídas próprias | Sem grants globais |
| Administradores do sandbox | Conforme responsabilidade | Conforme responsabilidade | Governança e suporte | Gerem owners e grants |

Todos os acessos UC exigem os privilégios de uso nos pais aplicáveis. Evitar grants amplos de leitura/escrita no catálogo ou schema que tornem ineficaz a seleção por objeto. Propriedade de objetos deve ser de grupo responsável; identidades de execução são distintas dos papéis humanos.

Views do agente não devem ser apenas cópias sem auditoria: revisar campos de negócio autorizados, dados de cliente, cobertura, joins e granularidade. A identidade efetivamente usada pelo agente (usuário ou identidade de serviço) precisa ser confirmada. Testar acesso permitido e negado com as identidades reais, incluindo caminhos alternativos.

Acesso somente às views depende do compute suportado, dos privilégios do owner e da configuração de execução. Preferir SQL warehouse ou compute compatível; não conceder leitura indiscriminada às bases para contornar falha. O cenário de compute dedicado antigo exige análise específica.

## 9. Mapeamento inicial: atual → proposta
| Atual, conforme inventário/código | Destino ou tratamento proposto | Decisão pendente |
| --- | --- | --- |
| `_file_orchestrator.demand`, diretórios `01_apuracao_demanda/in` e `out` | Separar volumes `demand_input` e `demand_output`; fontes comuns em entrada própria | Públicos de cada entrada/saída |
| `dm_customers.kna1_sap` e `knvv_sap` (volumes) | `files_sap_kna1` e `files_sap_knvv` | Compatibilidade de jobs e checkpoints |
| `dm_materials.sap` | Nome por dataset efetivamente contido | Listar arquivos e consumidores |
| `dt_sales_orders.sap_sales_order` | `files_sap_sales_order` | Caminhos e histórico da ingestão |
| `pr_cadastrao.sap_cadastraorefinado` | Fonte comum em `dm_materials.files_sap_material_cadastrao` | Confirmar cobertura e dono da fonte |
| `pr_cadastrao.out_material_de_para` | Trabalho em volume técnico; resultado em output do processo | Público e responsável pelo DE-PARA |
| `pr_demand.demand_refined_exportfiles` | `pr_demand.files_demand` como estágio; publicação no orquestrador | Confirmar qual caminho é realmente usado |
| `pr_forecast.forecast_enriched` (volume) | `pr_forecast.files_forecast` técnico; upload no orquestrador | Fonte e grupo remetente |
| `_agents_databases.*` | Manter fachada e especificar contrato de cada view | Revisão de dados e identidade |
| `00_cadastrao`, `01_demand`, `04_forecast`, `05_supersession` no Git | Diretórios por domínio/processo, com convenção única | Separar fonte comum de produto de negócio |

O catálogo descreve `refined_demand_aberta` como “em aberto”, enquanto o README define “Aberta” como demanda preservada no material original. Resolver essa diferença no glossário com o responsável, sem inferir alteração da regra.

## 10. Organização proposta do Git
| Diretório proposto | Conteúdo |
| --- | --- |
| `docs/structure/` | Proposta, decisões, mapa atual/destino e entrevista |
| `shared/ingestion/<dataset>/` | Notebooks mantenedores das fontes comuns |
| `shared/reference/` | Calendário e referências compartilhadas |
| `processes/<processo>/` | Transformações, exportações e contrato do processo |
| `orchestration/` | Recepção, roteamento, publicação e seus contratos |
| `consumption/agents/` | Definições e documentação das views do agente |
| `dashboards/<processo>/` | Consultas e dashboards de consumo |
| `migrations/` | Mudanças excepcionais executadas sob procedimento |

Departamentos ficam como owners nos contratos. Ainda não mover arquivos: extensões .py e .ipynb, referências entre notebooks, jobs e integração com workspace precisam ser conferidas antes da decisão.

## 11. Achados a considerar em uma etapa futura
1. README não acompanha a árvore atual: caminhos e numeração antigos, forecast descrito como não implementado apesar dos arquivos existentes.
2. Recebimento usa caminho padrão de `_file_orchestrator`, mas o widget persistido registra `dm_landing_files`. Isso é evidência no arquivo, não confirmação da configuração em execução.
3. No notebook de recebimento, `dry_run = (modo_execucao == "executar")` inverte o sentido esperado: “executar” simula e “testar” chega ao caminho de movimentação. Apenas registrado; nenhuma correção feita nesta fase.
4. O roteamento ativo de recebimento contém KNA1; VBAK/MARA aparecem apenas como exemplos comentados. Não presumir cobertura de todas as fontes.
5. O ZIP lista todos os arquivos diretamente em `out`, sem seleção por execução. Pode incluir pacotes anteriores presentes.
6. O inventário menciona volume de exportação e o código de ZIP aponta para output do orquestrador. Confirmar o contrato dos dois caminhos.
7. O inventário documenta `material_cadastrao` como raw com colunas STRING; não classificá-lo como trusted só pelo uso atual ou pelo schema.

## 12. Entrevista e implantação futura
Departamentos definidos: Demand, Inventory Long Term, Inventory Short Term e Spec. Próxima pergunta: quais são os principais processos de cada departamento, começando pela distinção entre Inventory Long Term e Inventory Short Term?

Depois, definir em conversa:
- Quem envia arquivos, quem só baixa, quem desenvolve e quem usa o agente?
- Qual grupo mantém cada fonte compartilhada e qual rotina pode atualizá-la?
- Quais processos, calendários, fechamentos e dependências existem por área?
- Quais dados são compartilháveis e quais exigem restrição?
- Como os usuários acessam o File Orchestrator hoje: Catalog Explorer, aplicação ou outro canal?
- É necessário envio sem leitura/exclusão de arquivos de outros remetentes?
- Qual identidade e qual compute são usados pelo agente e pelos jobs?
- Quais prazos de retenção, tamanho/volume de arquivos e necessidades de reprocessamento?
- Há consumidores externos, caminhos fixos ou jobs fora deste repositório?

Sequência futura, após fechar as decisões: validar inventário no workspace; completar mapa de dependências e acessos; planejar transição com compatibilidade; pilotar um dataset e um processo; conferir resultados e acessos; migrar os demais por etapas. A autorização desta branch documental não autoriza executar essa migração.

Critérios de aceitação futuros: nenhuma dependência sem dono; nenhum processo escreve em base alheia; arquivo repetido não duplica carga; falha não publica conjunto incompleto; fechamento reproduzível com versões retidas; usuário de download não modifica output; usuário do agente não acessa dados fora do escopo aprovado.

## Referências
- [Inventário atual](../../catalog_parts_hdbk_sandbox.md), extração declarada de 2026-11-04.
- [Recebimento](../../01_demand/1.0_receive_and_move_files.ipynb).
- [Compactação](../../01_demand/7.1_zip_exported_demand_files.py).
- [Privilégios de volumes — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/volumes/privileges).
- [Views e requisitos de acesso — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/views/).

As referências técnicas foram consultadas em 2026-11-05. O padrão de nomes, a separação dos volumes e os controles de publicação são recomendações desta proposta.
