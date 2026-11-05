# Proposta de estrutura do sandbox compartilhado
Versão 0.16 — 2026-11-05 — Rascunho para discussão com André.

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
| Volume — base | `<source>_<dataset>` | `sap_kna1`, `sap_material_master` |
| Volume — interface com acesso separado | `<source>_<dataset>_<input_ou_output>` | `sap_material_master_input`, `databricks_historical_demand_output` |
| Volume técnico de processo | Mesma base `<source>_<dataset>`; estados em diretórios | `databricks_historical_demand/working/<run_id>/` |
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

## 3.1. Publicação dentro dos schemas existentes — recomendação para discussão
Preservar os schemas por entidade e processo. Identificar a camada nas tabelas e controlar a leitura por objeto publicado é a recomendação inicial para o porte e a estrutura atuais. Esta seção é uma proposta detalhada; a distribuição exata dos objetos ainda precisa ser validada com André.

| Tipo de schema | Conteúdo proposto | Consulta padrão |
| --- | --- | --- |
| `dm_*` | Arquivos técnicos e tabelas Raw/Trusted de dados mestres | Apenas bases Trusted publicadas; Raw para uso técnico autorizado |
| `dt_*` | Arquivos técnicos e tabelas Raw/Trusted de dados transacionais | Apenas bases Trusted publicadas; Raw para uso técnico autorizado |
| `pr_*` | Transformações e produtos Refined do processo; entradas Raw/Trusted específicas quando justificadas pelo contrato | Apenas produtos publicados, sem SELECT amplo no schema misto |
| `_agents_databases` | Views aprovadas para o agente | Identidade configurada do agente e consumidores explicitamente autorizados |
| `_file_orchestrator` | Volumes de intercâmbio | Perfis de arquivo nos volumes autorizados |
| `_utils` | Objetos de suporte e referências publicadas | Seleção por objeto e finalidade |

A distinção entre fonte compartilhada e entrada de processo depende da semântica do dado. O forecast recebido de outra ferramenta pode continuar como entrada Raw do processo `pr_forecast`; não mover todo objeto Raw automaticamente para `dm_` ou `dt_`.

### Exemplo ilustrativo: clientes
| Objeto proposto | Função | Leitores |
| --- | --- | --- |
| `dm_customers.raw_sap_kna1` | Preservar os registros recebidos do SAP | Rotinas técnicas e Key Users autorizados |
| `dm_customers.trusted_sap_kna1` | Dados validados no grão da KNA1 | Standard Users e Key Users dos quatro departamentos, após publicação |
| `dm_customers.vw_customer_general_data` | Interface opcional com colunas e nomes estáveis sobre a base publicada | Consumidores autorizados; acesso exclusivo pela view quando houver projeção/restrição necessária |

Os nomes acima são exemplos e não classificam automaticamente a atual `dm_customers.kna1_sap` como Raw ou Trusted. Verificar as transformações e critérios de qualidade antes de atribuir camada e elaborar o mapa de migração.

### Exemplo ilustrativo: demanda
O processo consome as bases comuns aprovadas, aplica suas regras e publica seus produtos em `pr_demand`. Tabelas de trabalho continuam técnicas. O usuário recebe leitura nas tabelas Refined ou nas views publicadas adequadas à sua análise; não recebe todas as tabelas do schema por consequência.

### Tabela publicada versus view
- Conceder SELECT diretamente na tabela Trusted/Refined quando seu contrato e suas colunas forem apropriados ao público. Evitar criar uma view espelho para cada tabela sem necessidade.
- Usar view quando houver necessidade de interface estável, seleção de colunas, regra de acesso ou apresentação específica. Quando a view restringir acesso, não conceder ao mesmo público leitura da tabela-base que permitiria contornar essa restrição.
- Manter as views do agente com escopo próprio, sem obrigar todos os analistas a usar a interface do agente.
- O prefixo `vw_` identifica tipo de objeto, não prova que foi aprovado. O registro de publicação e as permissões identificam o que está liberado.

### Fluxo de publicação proposto
O mantenedor prepara e valida o objeto; registra contrato, camada, chaves, atualização, consumidores e estado publicado; o Owner concede a leitura dentro da delegação existente, ou encaminha ao TI o que depender dele. Novas tabelas técnicas não ficam automaticamente acessíveis. Uma publicação parcial ou inválida não substitui o resultado anterior aprovado.

O controle por objeto exige manter uma lista de bases publicadas e revisar grants quando novos produtos entrarem. Para a estrutura atual, é uma opção de menor mudança; se o número de objetos e públicos tornar essa gestão difícil, reavaliar schemas exclusivos de consumo. A decisão futura deve considerar o esforço operacional de manter as permissões.

## 4. File Orchestrator: interface e áreas internas
Separar volumes quando os públicos ou permissões diferirem. Pastas organizam; não constituem isolamento de permissões do Unity Catalog.

### Padrão de nomes de volumes — validado por André
Usar `<source>_<dataset>` como base. Acrescentar `_input` ou `_output` quando a finalidade precisar identificar uma interface e separar permissões. Usar inglês, minúsculas e snake_case.

`source` identifica o sistema de origem ou produtor, não o departamento remetente. `dataset` identifica o conteúdo. Exemplos: `sap_material_master`, `sap_kna1` e `databricks_historical_demand`. Fonte externa ainda desconhecida deve ser confirmada antes do nome definitivo; não substituir a origem pelo nome de quem fez o upload.

Pastas organizam estado, referência e lote; não representam fronteiras de acesso. O sufixo de finalidade não muda por si a segurança: entrada e saída com permissões diferentes precisam ser objetos de volume distintos.

| Caminho proposto sob `/Volumes/parts_hdbk_sandbox/` | Uso | Público |
| --- | --- | --- |
| `_file_orchestrator/sap_kna1_input/<referencia>/<batch_id>/` | Entrega de uma fonte comum de clientes | Grupos autorizados a enviar essa fonte |
| `_file_orchestrator/sap_sales_order_input/<referencia>/<batch_id>/` | Entrega de ordens de venda SAP | Grupos autorizados a enviar essa fonte |
| `_file_orchestrator/sap_material_master_input/<referencia>/<batch_id>/` | Exemplo de entrega do cadastro mestre SAP | Confirmar dataset e grupos remetentes |
| `_file_orchestrator/databricks_historical_demand_output/<referencia>/<run_id>/` | Resultados publicados de demanda | Download autorizado |
| `_file_orchestrator/databricks_forecast_output/<referencia>/<run_id>/` | Resultados publicados de forecast | Download autorizado |
| `dm_customers/sap_kna1/<estado>/<batch_id>/` | Recepção técnica, arquivo original e rejeições | Rotina mantenedora e suporte autorizado |
| `pr_demand/databricks_historical_demand/<estado>/<run_id>/` | Trabalho, exportação temporária e pacotes | Rotinas de demanda e suporte autorizado |

O caminho de entrada de forecast segue `_file_orchestrator/<source>_forecast_enriched_input/`, com origem a confirmar. Os exemplos de nomes são destinos propostos, sujeitos ao mapeamento de dependências.

Dimensionar volumes pela necessidade de isolamento e pelo contrato do dataset. Não criar um novo volume por mês, arquivo ou execução; para essas divisões, usar pastas. Fontes ou produtos diferentes só compartilham um volume se contrato, público e finalidade forem compatíveis; nesse caso, o nome deve representar o conjunto de dados real e o acesso é compartilhado ao volume inteiro. A proposta anterior de entradas genéricas por departamento é substituída pelos exemplos por origem e dado desta seção.

Estados internos sugeridos: `received`, `archive`, `quarantine` para arquivos de fonte; `working`, `ready` para arquivos de processo. Checkpoints e schema de ingestão ficam em caminhos técnicos exclusivos da rotina e não em volumes de usuário.

Upload direto no volume exige leitura e escrita: isso também permite modificar ou excluir arquivos desse volume. Não equivale a uma caixa de envio sem leitura/exclusão. Se essa restrição for necessária, a interface de upload deverá mediar a operação; essa escolha depende da entrevista.

Não registrar arquivos de dados de tabelas Delta em volumes de intercâmbio. As tabelas permanecem objetos próprios no Unity Catalog.

## 4.1. Exemplo real aplicado: cadastro de clientes SAP KNA1
Exemplo para avaliar o conceito com André. O fluxo proposto abaixo ainda não foi implementado.

### Situação atual verificada no código
- `01_demand/1.0_receive_and_move_files.ipynb` declara entrada padrão no volume `_file_orchestrator.demand`, pasta `01_apuracao_demanda/in/`, e mapeia prefixo KNA1 para o volume `dm_customers.kna1_sap`.
- `01_demand/2.2_ingest_customer_kna1_sap.ipynb` lê Excel diretamente na raiz desse volume, concatena os arquivos, normaliza/renomeia colunas e remove zeros à esquerda de `cliente`.
- A saída atual é a tabela `dm_customers.kna1_sap`, recriada a cada carga; o próprio notebook registra tag de camada `refined`.
- Portanto, a saída atual não é uma cópia Raw sem tratamento. Enquadrá-la como Trusted futura exige validar o contrato e os critérios de qualidade, não apenas trocar seu nome. As duas tabelas Raw/Trusted abaixo são objetos propostos.
- O cadastro participa do enriquecimento da demanda. A frequência e o caráter completo ou parcial dos arquivos recebidos ainda precisam ser definidos com o responsável.

### Caminho proposto
Todos os caminhos de arquivo desta tabela são relativos a `/Volumes/parts_hdbk_sandbox/`. As referências e IDs de lote/execução são exemplos.

| Etapa | Objeto ou caminho proposto | Regra |
| --- | --- | --- |
| Upload | `_file_orchestrator/sap_kna1_input/2026-10-31/b001/` | File User envia a extração SAP completa conforme contrato |
| Recepção técnica | `dm_customers/sap_kna1/received/b001/` | Rotina registra e verifica a cópia original antes de consumir a entrega |
| Preservação tabular | `parts_hdbk_sandbox.dm_customers.raw_sap_kna1` | Preservar valores recebidos e metadados do lote antes de normalizar a chave do cliente |
| Tratamento | `parts_hdbk_sandbox.dm_customers.trusted_sap_kna1` | Normalizar e validar conforme contrato; publicar somente após aprovação dos critérios |
| Arquivo histórico | `dm_customers/sap_kna1/archive/b001/` | Guardar original da carga aceita; falhas em `quarantine/b001/`, com motivo e estado rastreável |
| Consumo | Trusted publicada → processo `pr_demand` | Standard Users dos quatro departamentos podem consultar a base publicada; Key Users técnicos têm Raw conforme necessidade |
| Exportação de demanda, quando executada | `_file_orchestrator/databricks_historical_demand_output/2026-10/r001/` | Publicar os arquivos finais de demanda e ZIP daquela execução após validação |

`sap_kna1_input`, `sap_kna1` e `databricks_historical_demand_output` são três volumes distintos, em schemas identificados na tabela. Os elementos seguintes são pastas. O nome do volume segue origem + dado; a finalidade aparece no nome quando distingue a interface de acesso.

O File User escreve no input autorizado e lê o output autorizado; não recebe acesso técnico à Raw ou ao arquivo histórico por esse perfil. As identidades do processo têm os acessos técnicos necessários. A exportação final pertence ao processo de demanda; o cadastro KNA1 não precisa gerar um arquivo de saída próprio se não houver consumidor para ele.

### Pontos que este exemplo permite validar
- Preservação do dado original antes de transformações, como retirar zeros do código do cliente.
- Definição de uma extração completa por lote; não concatenar períodos diferentes por acidente.
- Critérios de publicação, como chaves não nulas, unicidade no grão definido e totais reconciliados.
- Folders de recebimento/histórico são estados; input/output separados atendem a públicos com permissões diferentes.
- Fonte compartilhada tem uma carga oficial; quatro departamentos consomem a mesma versão publicada.
- Mês de referência do cadastro e referência da demanda podem diferir; registrar a versão do cadastro usada na execução.

A implantação futura exigiria adaptar leitura de pastas, roteamento, ingestão e controles. Renomear volumes isoladamente não implementa o fluxo. Não houve alteração de código nesta branch.

## 4.2. Caso Cadastrão: evolução de carga mensal para diária
Aplicação detalhada do fluxo: [Cadastrão — ponta a ponta](cadastrao-flow.md).
### Contexto informado por André
- O Cadastrão chega pronto de processamento externo ao SAP/Databricks. Neste momento não existe uma etapa DM a reproduzir artificialmente.
- Manter o processo em `pr_cadastrao` enquanto as transformações externas não forem incorporadas ao Databricks.
- Atualmente são recebidos dois arquivos Excel por mês; a quantidade de arquivos aumentará. Não fixar a regra em cinco arquivos.
- Objetivo: receber arquivos a cada dia útil, unificar o conjunto e produzir várias tabelas a partir da entrada Raw.
- Algumas áreas precisam da primeira posição do mês; outras precisam da posição atual.
- Confirmado por André: cada envio diário traz novamente a base completa, e não apenas registros novos/alterados. Ainda confirmar a composição e os escopos dos arquivos do lote e a regra da primeira posição mensal.
- André sugeriu identificar o estado atual com `is_current` e sinalizar a primeira posição do mês.

Este é o caso prioritário trazido pelo usuário para testar a proposta. O exemplo KNA1 anterior permanece somente como ilustração, não substitui o processo explicado por André.

### Evidência no código atual
O notebook `00_cadastrao/1.1_ingest_raw_material_cadastrao.ipynb` lê `pr_cadastrao/sap_cadastraorefinado/current/`, transforma/consolida os arquivos, extrai ano/mês do nome, grava `reference_date` no primeiro dia do mês e recria `material_cadastrao`.

O notebook 1.2 reduz a referência a ano/mês e substitui o snapshot daquele mês nos escopos empresa/centro presentes. O notebook 1.3 também reduz a referência ao início do mês e contém guardas cronológicas e de repetição de período para SCD2 mensal. Rodar os notebooks diariamente sem redesenhar esses contratos pode substituir a referência mensal ou bloquear cargas no mesmo mês.

Os nomes e metadados atuais atribuem a fonte a SAP. O relato do usuário esclarece que o arquivo imediato já vem de um processo externo; distinguir sistema produtor imediato, sistemas de origem anteriores e tratamento já realizado.

### Nomes e localização propostos
- Entrada de intercâmbio: `_file_orchestrator.<source>_material_register_input`.
- Volume técnico: `pr_cadastrao.<source>_material_register`, com pastas `received/<batch_id>/`, `archive/<batch_id>/` e `quarantine/<batch_id>/`.
- `source` depende da identificação do processo/ferramenta que gera o arquivo. Não fixar `sap` somente por serem dados originalmente extraídos dele.
- Tabelas candidatas no próprio `pr_cadastrao`: `raw_material_register`, `trusted_material_register_history`, e interfaces `vw_material_register_current` / `vw_material_register_month_start`.
- A escolha de nomes técnicos em inglês é proposta. O schema existente `pr_cadastrao` permanece nesta etapa.
- Criar output de arquivos somente para produtos com necessidade de download. Tabelas e views já podem ser a interface de consumo dos demais.

O mapeamento anterior que sugeria transferir o arquivo para `dm_materials` não se aplica automaticamente a este caso. A localização atual por processo é preservada, conforme a explicação de André.

### Fluxo recomendado para fotografias completas por lote
1. Receber os arquivos esperados do lote diário, cuja composição será configurável por origem/empresa/centro e contrato, não por uma quantidade fixa.
2. Registrar os arquivos originais, checksum e escopo; checar completude do conjunto. Arquivos que chegam em momentos diferentes não devem publicar uma fotografia parcial como se fosse completa.
3. Preservar o conteúdo recebido na Raw com identificação de lote e origem, antes das transformações do Databricks. Raw é o dado tal como recebido nesta fronteira, mesmo que a origem externa já o tenha processado.
4. Unificar os arquivos compatíveis do mesmo lote, tratar e validar chaves, conflitos, escopos e totais. Regras de combinação dependem dos layouts: confirmar se os arquivos são partições de linhas ou fontes a relacionar.
5. Publicar uma fotografia validada com referência diária e identificação de lote. Conservar as fotografias anteriores; publicação atual aponta para o último lote válido e completo da referência apropriada.
6. Fixar um lote de referência para a primeira posição mensal, segundo regra de negócio a confirmar. As atualizações seguintes não mudam essa escolha automaticamente.
7. Derivar os produtos do Cadastrão da versão identificada, mantendo consumo atual e mensal separados e rastreáveis.

A primeira posição mensal deve referenciar um lote coerente, não selecionar a primeira ocorrência de cada material em datas diferentes. Se o contrato admitir publicações independentes por centro/empresa, definir explicitamente a referência por escopo e os critérios de completude para a visão consolidada.

### Tempo e publicação
Registrar separadamente:
- `position_date`: data da posição de negócio declarada pela fonte;
- `received_at`: instante de chegada;
- `processed_at`: instante de processamento;
- `batch_id` e revisão: conjunto de arquivos e tentativa/correção;
- referência mensal escolhida e seu lote publicado.

Receber um arquivo antigo hoje não o torna automaticamente a posição mais atual. Reenvio idêntico não cria duplicação; correção gera revisão rastreável. Alterar uma primeira posição mensal já publicada exige decisão explícita e histórico da revisão.

Até confirmação, “primeira do mês” significa apenas requisito de uma referência mensal estável; ainda não está decidido se é primeiro dia calendário, primeiro dia útil ou primeiro lote completo válido recebido. A política de falta de carga esperada e dados atrasados também fica pendente.

### Fotografias versus somente mudanças
A recomendação inicial para testar o requisito é Raw preservada por lote e fotografias diárias publicadas, pois torna diretas as consultas de posição atual e primeira posição mensal. Avaliar volume e retenção antes de definir o armazenamento final.

Para atributos técnicos que mudam pouco, SCD2 é uma alternativa/complemento: nova versão apenas quando campos rastreados mudam. Para quantidades/preços voláteis, snapshots podem ser mais simples. Não substituir todo o histórico por um MERGE que mantém apenas o estado atual, nem depender apenas da retenção de versões Delta para guardar fechamentos permanentes.

Se os arquivos forem deltas, será necessário reconstruir o estado completo antes de publicar as duas referências; ausência de um material no arquivo não significa exclusão. Mesmo com fotografia completa, interpretar ausências somente dentro do escopo completo validado.

O formato de entrada completa está confirmado. Para a proposta inicial, preservar os lotes completos recebidos e publicar fotografias validadas permite atender às duas posições diretamente. Grãos dos produtos, volume e retenção ainda orientam a materialização final; não adotar SCD2 indiscriminadamente para todas as colunas.

### Auditoria existente: preservar e complementar
André confirmou que a auditoria já faz parte do padrão do projeto. A proposta reaproveita as colunas implementadas no Cadastrão; não cria um segundo conjunto equivalente com nomes diferentes.

| Coluna existente no notebook 1.1 | Significado observado | Tratamento na proposta |
| --- | --- | --- |
| `_ingested_at` | Instante da ingestão no Databricks | Manter; representa processamento, não comprova hora de upload |
| `_last_updated_at` | Instante de atualização; na recriação atual é definido junto à ingestão | Manter sem redefinir como data de posição |
| `_ingested_by` | `current_user()` da execução | Manter; não comprova identidade de quem enviou o arquivo |
| `_load_type` | Tipo de carga; atualmente `full` | Manter conforme formato completo confirmado |
| `_load_id` | UUID da execução | Manter; associar ao lote recebido e a seus arquivos |
| `_source_file_path` | Atualmente contém `SOURCE_DIR`, caminho da pasta | Preservar rastreabilidade; complementar no desenho futuro com relação dos arquivos efetivamente consumidos |
| `reference_date` | Atualmente o primeiro dia do mês extraído do nome | Evoluir o contrato para registrar data real da posição diária e referência mensal separadas |

A proposta anteriormente chamou o instante de processamento de `processed_at`; neste caso, esse é um conceito, não uma nova coluna paralela a `_ingested_at`. Da mesma forma, `batch_id` é o conjunto recebido, enquanto `_load_id` identifica execução; podem ter relação 1:N por reprocessamento. Não criar duas chaves para o mesmo evento sem necessidade.

Se for necessário auditar upload, registrar instante e remetente reais na recepção/orquestrador e relacioná-los ao lote, usando os eventos que o canal efetivamente disponibilizar. Não inferir que instante de ingestão e usuário executor são instante e autor do upload.

`is_current` e `is_month_start` são seleções de posição de negócio; complementam auditoria e não a substituem. Originais da Raw e seus metadados de ingestão permanecem preservados.

A orientação `skills/arquiteto-de-dados/SKILL.md` traz exemplos genéricos com `created_ts`, `updated_ts` e `created_by`, enquanto o Cadastrão implementa as colunas com sublinhado acima. Documentar essa diferença e adotar o padrão implementado neste caso; a harmonização das instruções fica para revisão documental, sem alteração de código nesta etapa.

### Upload e ingestão: eventos distintos — definido por André
Mapear separadamente os dois eventos; os mesmos valores só podem aparecer quando os eventos reais coincidirem, nunca por preenchimento automático de um com os dados do outro.

| Evento | Data/hora | Usuário ou identidade | Grão e origem do registro |
| --- | --- | --- | --- |
| Upload | `_uploaded_at` (nome proposto) | `_uploaded_by` (nome proposto) | Por arquivo/entrega, capturado no canal de upload/recepção |
| Ingestão | `_ingested_at` (existente) | `_ingested_by` (existente) | Por execução, capturado no processamento do Databricks |

`_last_updated_at` continua registrando a atualização do registro conforme seu contrato; não substitui o horário do upload nem a data da posição de negócio.

O registro de arquivos relaciona o identificador de entrega, nome original, caminho completo do arquivo, remetente e instante de upload ao lote de negócio. O registro de execução relaciona esse lote ao `_load_id`, instante da ingestão e identidade executora. Um lote pode reunir arquivos enviados por pessoas diferentes; não atribuir ao lote um único remetente fictício. Um arquivo pode participar de mais de uma execução em reprocessamentos.

A futura ingestão preservará a relação de cada linha com seu arquivo de origem quando aplicável, para permitir consultar a auditoria dos dois eventos sem replicar metadados contraditórios. Após agregações ou joins, manter a linhagem aos arquivos/lotes envolvidos, em vez de escolher arbitrariamente um remetente.

No desenho de implementação, confirmar como o canal atual disponibiliza o horário real e o autor do upload. Instante de descoberta do arquivo não é automaticamente instante de upload; usuário do job não é automaticamente remetente. Se o canal não fornecer um desses dados, registrar a indisponibilidade e definir como capturá-lo, sem inventar valores.

### Flags de posição propostas
Usar `is_current` e `is_month_start` como indicadores do lote publicado, expostos nas consultas dos dados se necessário. Conservar as datas reais, a referência mensal e o `batch_id`; flags não substituem esse histórico.

Manter a seleção em um controle de lotes/referências e derivar as flags nas views, evitando reescrever todos os registros Raw anteriores a cada atualização. Se houver materialização posterior, sincronizar com a mesma seleção oficial.

| Momento ilustrativo | Lote | is_current | is_month_start |
| --- | --- | --- | --- |
| Após publicação da primeira posição de novembro | Lote A, escolhido como referência mensal | true | true |
| Após publicação da posição seguinte | Lote A | false | true |
| Após publicação da posição seguinte | Lote B, mais recente válido | true | false |

`is_current`: apenas um lote válido/completo selecionado como posição atual por escopo de publicação. Usar a data de negócio e a revisão aprovada, não simplesmente o arquivo que chegou por último.

`is_month_start`: apenas um lote de referência por mês e escopo; referências dos meses anteriores continuam preservadas. A primeira posição de dezembro não apaga a referência de novembro. A regra de escolha ainda precisa ser confirmada; pode ser automática segundo calendário/validação ou seleção explícita autorizada.

As flags se aplicam à fotografia coerente, não à primeira aparição de cada material. Todas as linhas daquele lote herdam a seleção. Isso evita montar uma posição com datas diferentes por item. Uma falha de carga mantém o último lote publicado como atual.

Fotografia completa facilita a reconstrução do estado, mas exige armazenamento e processamento dos lotes. Não calcular custo ou retenção sem volume real. Manter arquivos originais e Raw/Trusted conforme política a definir; otimizar posteriormente sem perder as referências prometidas.

### Pergunta seguinte
A primeira posição mensal deve ser escolhida automaticamente como o primeiro lote completo e válido do mês ou marcada explicitamente pelo responsável?

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

## 8. Perfis de acesso
### Definições confirmadas na entrevista
- Demand, Inventory Long Term, Inventory Short Term e Spec compartilham a consulta às bases publicadas de Trusted e Refined, sem distinção por departamento. Isso não libera automaticamente todos os schemas ou a camada Raw. Esta regra refina a definição inicial de leitura compartilhada, conforme validado por André.
- A distinção de acesso humano é por perfil: Owner, Key User e Standard User. André também solicitou um perfil para upload e download no File Orchestrator; o nome proposto é File User, ainda sujeito à confirmação. Os nomes permanecem em inglês.
- Consulta compartilhada não define automaticamente permissões de escrita, alteração de notebooks, execução de jobs ou administração.
- As responsabilidades dos processos de Inventory Long Term e Inventory Short Term foram adiadas por André e poderão ser preenchidas no futuro; não bloqueiam a proposta inicial.

| Perfil humano confirmado | Definição de permissões |
| --- | --- |
| Owner | Os quatro supervisores, um por departamento, são Owners do sandbox conforme relatado por André. Podem gerenciar acessos dentro da delegação existente; não podem criar grupos de usuários atualmente |
| Key User | Pode criar e editar notebooks e consultar as bases Trusted e Refined publicadas. Acesso à Raw conforme as fontes necessárias para desenvolvimento, investigação ou validação de cargas. Escopo de execução, escrita em dados e manutenção de objetos ainda a detalhar |
| Standard User | Pode consultar por SQL as bases Trusted e Refined publicadas, inclusive dados detalhados aprovados; não fica restrito ao agente ou dashboards. Sem leitura da Raw por padrão. Permissões de alteração não foram concedidas nesta definição |
| File User (nome proposto) | Upload nos volumes de entrada e download nos volumes de saída autorizados do File Orchestrator |

Os quatro supervisores são os Owners funcionais do sandbox. O TI mantém uma função administrativa superior e atualmente não permite que os Owners criem grupos de usuários. A criação desses grupos exige solicitação ao TI. O alcance exato da delegação técnica deve ser validado no ambiente, sem presumir privilégios administrativos globais.

“Quatro Owners” descreve a responsabilidade relatada, não confirma quatro proprietários técnicos individuais no mesmo objeto do Unity Catalog. A forma atual de atribuição (grupo existente, propriedade de objetos ou privilégios delegados) ainda não foi inspecionada.

### Regra de leitura por camada — validada por André
| Camada ou área | Regra de acesso proposta e validada |
| --- | --- |
| Raw e arquivos técnicos de origem | Área técnica. Leitura das rotinas de ingestão/processamento e dos Key Users conforme fontes necessárias para desenvolvimento, investigação e validação. Sem leitura geral do Standard User |
| Trusted publicada | Consulta compartilhada entre os quatro departamentos, por Standard Users e Key Users, inclusive via SQL e com detalhe quando previsto no contrato |
| Refined publicada | Consulta compartilhada aos produtos aprovados para consumo, por Standard Users e Key Users |
| Intermediárias e resultados ainda não publicados | Acesso das identidades e mantenedores necessários ao processo; sem liberação geral apenas por estarem em um schema de processo |
| Views do agente | Escopo publicado e aprovado para o agente; sem leitura automática da Raw |
| File Orchestrator | Perfil File User restrito aos volumes de entrada/saída autorizados; continua independente dos privilégios em tabelas |

O critério é camada + estado de publicação + necessidade do perfil, sem distinção de consulta entre departamentos. Um nome de schema ou prefixo não prova a qualidade ou a publicação da tabela. O contrato e o inventário devem indicar a classificação real.

Evitar grants amplos de SELECT no catálogo ou em schemas que misturem Raw, intermediárias e produtos publicados. Um SELECT no schema é herdado por tabelas e views atuais e futuras. Usar permissões por objetos publicados ou, após avaliar dependências, schemas de consumo que contenham somente objetos aprovados. Isso é uma proposta de controle; nenhum grant ou movimento de objeto foi executado.

Acesso à Raw para Key User não implica escrita na fonte. A leitura não altera o pipeline, mas análises feitas diretamente na Raw podem ignorar tratamentos e regras; por isso a base publicada é o ponto padrão para consumo.

### Implementação dos acessos e dependência do TI
| Responsável | Atribuição confirmada ou proposta |
| --- | --- |
| Supervisores / Owners | Gerir acessos dentro dos privilégios já delegados; confirmar quem deve ter cada perfil |
| TI / Admin | Administrar o ambiente e receber solicitação para criação dos grupos de usuários |
| TI + Owners | Validar escopo, membros e responsáveis pelos grupos antes de associar permissões aos objetos |

Owner, Key User, Standard User e File User são perfis funcionais da proposta; não significam grupos já existentes. Preparar um mapeamento de perfil → pessoas → operações → objetos e uma solicitação de grupos ao TI, quando o desenho estiver definido. Nenhuma solicitação foi enviada nesta etapa.

A proposta de grupos não bloqueia o desenho documental. Uma aplicação transitória por usuários individuais só deve ser considerada se já for permitida pela política do TI e pela delegação dos Owners; não presumir essa autorização. Não usar caminhos alternativos para contornar a restrição de criação de grupos. O perfil File User, isoladamente, propõe acesso aos arquivos de intercâmbio: não inclui grants em tabelas, edição de notebooks, execução de jobs ou administração. A pessoa pode acumular outro perfil se necessário; essa combinação ainda deve ser definida. Aplicar leitura/escrita nos volumes de entrada e somente leitura nos volumes de saída autorizados, observando a limitação de upload direto descrita na seção 4. Não tratar 'upload' como garantia de impossibilidade de leitura ou exclusão na entrada.

Os quatro departamentos não determinam, por si, perfis ou grants diferentes. Perfis podem ser implementados por grupos após detalhar suas permissões.

### Capacidades técnicas propostas, ainda não atribuídas aos perfis humanos
A matriz abaixo é um inventário de capacidades para discutir. Não cria perfis humanos adicionais nem afirma que o usuário aprovou as atribuições. As identidades técnicas dos jobs são tratadas separadamente.

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

Todos os acessos UC exigem os privilégios de uso nos pais aplicáveis. Evitar grants amplos de leitura/escrita no catálogo ou schema que tornem ineficaz a seleção por objeto. Atribuição de propriedade a grupos responsáveis é recomendação futura, dependente da criação ou disponibilidade de grupos autorizados pelo TI; não descreve o estado atual. Identidades de execução são distintas dos papéis humanos.

Views do agente não devem ser apenas cópias sem auditoria: revisar campos de negócio autorizados, dados de cliente, cobertura, joins e granularidade. A identidade efetivamente usada pelo agente (usuário ou identidade de serviço) precisa ser confirmada. Testar acesso permitido e negado com as identidades reais, incluindo caminhos alternativos.

Acesso somente às views depende do compute suportado, dos privilégios do owner e da configuração de execução. Preferir SQL warehouse ou compute compatível; não conceder leitura indiscriminada às bases para contornar falha. O cenário de compute dedicado antigo exige análise específica.

## 9. Mapeamento inicial: atual → proposta
| Atual, conforme inventário/código | Destino ou tratamento proposto | Decisão pendente |
| --- | --- | --- |
| `_file_orchestrator.demand`, diretórios `01_apuracao_demanda/in` e `out` | Separar entradas `<source>_<dataset>_input` e saída `databricks_historical_demand_output`; uma entrada comum por contrato de fonte | Públicos de cada entrada/saída |
| `dm_customers.kna1_sap` e `knvv_sap` (volumes) | `sap_kna1` e `sap_knvv` | Compatibilidade de jobs e checkpoints |
| `dm_materials.sap` | Nome por dataset efetivamente contido | Listar arquivos e consumidores |
| `dt_sales_orders.sap_sales_order` | `sap_sales_order` | Caminhos e histórico da ingestão |
| `pr_cadastrao.sap_cadastraorefinado` | Manter em `pr_cadastrao.<source>_material_register` como entrada do processo externo, conforme caso 4.2 | Identificar produtor imediato, composição e granularidade do lote |
| `pr_cadastrao.out_material_de_para` | Trabalho em volume técnico; resultado proposto em `databricks_material_mapping_output` no orquestrador | Público e responsável pelo DE-PARA |
| `pr_demand.demand_refined_exportfiles` | `pr_demand.databricks_historical_demand` como estágio; publicação no orquestrador | Confirmar qual caminho é realmente usado |
| `pr_forecast.forecast_enriched` (volume) | `pr_forecast.<source>_forecast_enriched` técnico; upload em `<source>_forecast_enriched_input`, com origem a confirmar | Fonte e grupo remetente |
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
Departamentos definidos: Demand, Inventory Long Term, Inventory Short Term e Spec. Consulta às bases Trusted e Refined publicadas permitida aos quatro, sem distinção por departamento; Raw reservada ao uso técnico e à validação, conforme necessidade. Perfis humanos: Owner, Key User e Standard User; acrescentado perfil de upload/download no File Orchestrator, com nome proposto File User. Responsabilidades específicas de Inventory Long Term e Inventory Short Term ficam pendentes para uma etapa futura, conforme orientação de André. Key User pode criar e editar notebooks, conforme confirmado por André. Standard User pode consultar por SQL as bases Trusted e Refined publicadas; Raw não é liberada por padrão. Owner: os quatro supervisores podem gerenciar acessos dentro da delegação existente, mas criação de grupos depende de solicitação ao TI/Admin. Pendentes: inspecionar limites efetivos da delegação e preparar o mapeamento de perfis/grupos; detalhar separadamente execução e escrita em bases.

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
- [Arquitetura medalhão — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion).
- [Privilégios e herança no Unity Catalog — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/access-control/privileges-reference).

As referências técnicas foram consultadas em 2026-11-05. O padrão de nomes, a separação dos volumes e os controles de publicação são recomendações desta proposta.
