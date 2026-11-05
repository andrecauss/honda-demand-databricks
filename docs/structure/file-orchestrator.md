# File Orchestrator — proposta de movimentação automatizada
Versão 0.1 · 2026-11-05 · Especificação para discussão.

## Decisão recomendada
Usar um Lakeflow Job do Databricks com uma tarefa de notebook Python como executor. O Job controla agendamento, concorrência, tentativas, timeout e alertas. O notebook implementa descoberta, roteamento, cópia verificada, registros de estado e recuperação.

Começar com uma varredura periódica das entradas cadastradas. Intervalo inicial proposto: 5 minutos, ajustável após medir latência necessária, volume, duração e custo do compute. Não é um prazo de execução garantido nem uma configuração já aplicada.

Esta entrega é uma proposta do notebook e de sua operação. Não cria ou altera notebooks executáveis, Jobs, permissões, notificações ou arquivos de dados. Nenhuma mensagem foi enviada a usuários.

## 1. Requisitos do usuário
- Automatizar movimentações associadas a `_file_orchestrator`.
- Detectar arquivos recebidos por evento ou verificação a cada X minutos.
- Aplicar rotas configuradas sem sobrepor movimentações.
- Evitar iniciar uma execução concorrente enquanto a anterior ainda estiver ativa.
- Registrar falhas e avisar os responsáveis.
- Preservar a distinção entre upload e ingestão.
- Entregar lotes completos aos processos, incluindo Cadastrão.
- Usar os padrões de nomes e acessos da [proposta geral](sandbox-structure-proposal.md).

## 2. Comparação de opções
| Opção | Benefício | Limitação para este cenário | Avaliação |
| --- | --- | --- | --- |
| Notebook com loop permanente de watcher | Rotina única sempre ativa | Mantém compute e exige gerir disponibilidade, espera, reconexão e supervisão | Não é a recomendação inicial |
| Job periódico com notebook executor | Centraliza múltiplas rotas e usa controles nativos de execução | Há latência de varredura e custo de compute a medir | Recomendação inicial |
| Job disparado por chegada de arquivo | Reduz verificações quando chegadas são irregulares | Monitoramento por caminho, restrições do gatilho e configuração do storage | Alternativa a validar no workspace |
| Orquestração externa, como Azure Data Factory | Pode aproveitar governança e integrações corporativas existentes | Introduz serviço e dependências adicionais se não houver solução já adotada | Reavaliar se o TI já padronizar essa plataforma |

A recomendação é contextual: existem vários volumes, arquivos Excel e cargas manuais completas; não há requisito confirmado de latência em segundos. A existência de uma plataforma corporativa de orquestração não foi investigada.

Gatilho de chegada é um recurso real do Databricks, mas não equivale a uma garantia de execução instantânea ou completude de lote. O comportamento documentado é checagem best effort. Renomeações/sobrescritas não devem ser a base da detecção de novas entregas; preferir nomes imutáveis e IDs de entrega. Configuração e suporte efetivo precisam ser testados no workspace.

## 3. Estrutura proposta
| Elemento | Nome proposto | Função |
| --- | --- | --- |
| Lakeflow Job | `file_orchestrator_dispatcher` | Agendar e serializar as execuções |
| Notebook executor | `orchestration/10_dispatch_files` | Descobrir e executar movimentações configuradas |
| Configuração de rotas | `_file_orchestrator.file_routes` | Fontes, destinos e políticas aprovadas |
| Registro de arquivos | `_file_orchestrator.file_registry` | Entregas e proveniência por arquivo |
| Registro de movimentações | `_file_orchestrator.file_movements` | Estado e tentativas de cada operação |
| Registro de execução | `_file_orchestrator.orchestration_runs` | Auditoria da execução do dispatcher |
| Controle de lote | `_file_orchestrator.batch_manifests` | Composição e condição de conclusão de cada lote |

Tabelas são propostas Delta no catálogo `parts_hdbk_sandbox`. Os nomes ainda podem ser ajustados para evitar duplicação com controles corporativos existentes.

Uma identidade técnica executa o Job. Os usuários de upload/download acessam os volumes autorizados; não editam o controle de rotas ou os registros operacionais por esse perfil.

## 4. Agendamento e exclusão de concorrência
### Configuração recomendada para varredura periódica
- Um único Job autorizado a realizar as movimentações gerenciadas.
- `max_concurrent_runs = 1` configurado explicitamente.
- Uma tarefa executora; operações de arquivos sequenciais, sem branches paralelos de movimentação.
- Intervalo de 5 minutos como parâmetro inicial proposto.
- Para este modo de varredura, desabilitar fila de execuções periódicas: um horário que coincida com execução ativa pode ser ignorado; a próxima varredura reconcilia tudo que continuar pendente.
- O horário ignorado não é a unidade de trabalho. Os arquivos e seus estados persistidos são a fila de trabalho; continuam disponíveis até conclusão.
- Configurar explicitamente tentativas e timeout; não depender de defaults do workspace.
- Proposta inicial: até duas novas tentativas para falhas transitórias, com intervalo configurável. Limites de duração dependem de medição e precisam ser definidos antes da ativação.

O limite de concorrência é por Job. Não impede sozinho que outro Job ou uma execução interativa do mesmo notebook altere os mesmos caminhos. Por isso, centralizar os escritores e restringir a execução direta do worker. Operações manuais usam o mesmo Job com parâmetros, sem criar um segundo escritor.

Se futuramente houver múltiplos executores, será necessário um mecanismo de claim/lock compartilhado com garantias transacionais verificadas, recuperação e expiração segura. Não tratar um arquivo marcador ou um simples INSERT sem exclusividade como trava distribuída.

### Sequência e falhas
Uma operação seguinte começa apenas após a operação anterior ter encerrado em estado seguro. Falha operacional mantém o arquivo pendente/erro e interrompe a sequência dependente; tentativas retomam pelo estado registrado.

Arquivo de negócio inválido pode ir para quarentena com motivo, após isolamento verificado. O lote afetado não é liberado. Continuar rotas independentes após quarentena é política a validar; a proposta inicial é conservadora e encerra o ciclo em falha para gerar alerta, deixando as demais pendências para varredura após resolução.

Manter estado bloqueado e alerta deduplicado para falha persistente; não repetir movimentação destrutiva ou emitir o mesmo alerta indefinidamente a cada varredura.

## 5. Rotas configuradas
Cada rota contém:
- identificador, dataset, origem produtora e responsável;
- volume/pasta de entrada autorizado e destino técnico;
- padrão de nomes/aliases e extensões aceitas;
- regra de lote, referência de negócio e escopos esperados;
- ordem/dependências e condições de execução;
- ação: copiar, consumir entrega após cópia verificada, arquivar ou publicar;
- política de colisão, reenvio e revisão;
- tentativas, tempo máximo de pendência e destino de alerta;
- versão da configuração e estado ativo/inativo.

Não descobrir todos os volumes do catálogo e mover arquivos arbitrariamente. Varredura restrita à lista aprovada de rotas. A inclusão de um novo processo cadastra uma rota, em vez de duplicar um notebook por departamento.

O nome completo do dataset e a rota devem identificar o destino. Evitar depender exclusivamente do primeiro texto antes de um sublinhado.

## 6. Contrato do notebook executor
### Cabeçalho
Documentar propósito, entradas, saídas, granularidade, modo de execução, responsáveis e pré-requisitos. Reutilizar os padrões de auditoria do projeto.

### Parâmetros propostos
| Parâmetro | Uso |
| --- | --- |
| `mode` | `simulate` ou `execute`; simulação não altera arquivos nem estados operacionais |
| `route_id` | Todas as rotas aprovadas ou uma rota específica |
| `batch_id` | Restringir a um lote para execução controlada |
| `max_files_per_run` | Limitar trabalho por ciclo; pendências continuam registradas |
| `reprocess_request_id` | Identificar pedido autorizado de reprocessamento |
| `config_version` | Registrar qual versão das rotas foi utilizada |

O intervalo pertence ao agendamento do Job; não é implementado com sleep dentro do notebook.

### Etapas internas propostas
1. Carregar e validar configuração ativa, identidade e parâmetros.
2. Registrar a execução e reconciliar operações interrompidas.
3. Descobrir arquivos nas entradas permitidas e registrar entregas inéditas.
4. Identificar rota, lote e disponibilidade para movimentação.
5. Copiar/verificar os arquivos sequencialmente, registrando cada transição.
6. Atualizar a completude do lote e liberar somente conjuntos prontos.
7. Registrar resultado, métricas e pendências; lançar falha real ao Job quando necessário.

Notebook de movimentação não executa regras de negócio do Cadastrão. Ele publica um lote pronto para a tarefa de ingestão usar. Dependências do pipeline impedem ingestão/publicação quando as etapas anteriores falham.

## 7. Movimentação recuperável e idempotente
Transferir entre volumes é tratado como cópia + verificação + remoção controlada da origem, sem presumir rename atômico.

Procedimento por arquivo:
1. Identificar a entrega e registrar checksum, tamanho e caminhos.
2. Confirmar que a entrega está completa conforme o canal de upload.
3. Copiar para caminho de destino técnico determinístico do lote, ainda não liberado ao consumidor.
4. Verificar existência, tamanho e checksum do destino e consistência da origem.
5. Registrar que o destino está confirmado e que a entrega pode ser consumida.
6. Remover a origem transitória somente se a rota permitir e houver cópia preservada verificada.
7. Registrar a conclusão e disponibilizar o arquivo ao lote correspondente.

Se o Job cair após a cópia e antes da remoção, a execução seguinte reconhece o destino pelo registro/checksum e conclui a operação; não cria cópia extra nem sobrescreve conteúdo diferente.

Mesmo nome e mesmo conteúdo é reenvio/reconciliação. Mesmo nome com conteúdo diferente é conflito ou revisão explícita. Não sobrescrever silenciosamente.

Tamanho estável em duas observações ajuda a evitar cópias prematuras, mas não comprova sozinho entrega completa. Preferir confirmação explícita do canal, manifesto ou marcador de entrega finalizada. A implementação dessa confirmação depende da interface de upload, ainda não identificada.

## 8. Estados e auditoria
| Estado | Significado |
| --- | --- |
| `discovered` | Entrega identificada |
| `awaiting_completion` | Aguardando conclusão do arquivo/conjunto |
| `ready_to_copy` | Movimentação elegível |
| `copying` | Cópia iniciada |
| `destination_verified` | Cópia validada |
| `source_cleanup_pending` | Origem ainda deve ser consumida conforme política |
| `completed` | Movimentação concluída |
| `quarantined` | Arquivo isolado após erro de negócio |
| `failed` / `blocked` | Erro operacional ou intervenção necessária |

Preservar timestamps, ator e transição; `run_id`, `batch_id`, IDs de arquivo/operação, origem/destino, checksum, tentativas e mensagem de erro.

Upload mantém `_uploaded_at` e `_uploaded_by` como evento distinto. Movimentação registra seu próprio ator/instante. Ingestão de dados mantém `_ingested_at`, `_ingested_by`, `_load_id`, `_load_type`, `_last_updated_at` e origem conforme o contrato do processo.

Não atribuir ao remetente a identidade do executor do Job quando o canal de upload não fornecer essa informação.

## 9. Alertas e operação
Configurar no Job:
- falha definitiva, após as tentativas configuradas;
- duração acima do limite esperado;
- timeout.

Configurar no controle operacional alertas de lote incompleto além do prazo, arquivos bloqueados e conflitos. Esses eventos precisam gerar evento de falha/condição monitorada; apenas imprimir erro e concluir o notebook com sucesso não aciona automaticamente um alerta de falha.

Inicialmente propor e-mail a uma lista de distribuição mantida pela organização. E-mail de distribuição é um destino de comunicação; não é o grupo de segurança do Unity Catalog. Endereço e destinatários ainda precisam ser informados/confirmados antes de ativar.

Teams ou outro destino pode ser alternativa, mas o destino de sistema exige configuração administrativa. Essa dependência deve entrar no pedido ao TI.

O alerta nativo fornece contexto/link da execução conforme o produto; o detalhe por arquivo e rota fica no log/registro operacional. Mensagem enriquecida por arquivo depende de integração específica a projetar, sem presumir que a notificação nativa inclua todos os campos desejados.

Definir responsável por rota, critérios de desbloqueio e modo de retomar pelo mesmo Job. Deduplicar alertas repetidos da mesma ocorrência e registrar recuperação.

## 10. Aplicação ao Cadastrão
1. Usuário entrega os Excel em `_file_orchestrator.<source>_material_register_input`.
2. O dispatcher periódico identifica a entrega, espera sua confirmação, registra e copia os arquivos para `pr_cadastrao.<source>_material_register/received/<batch_id>/`.
3. O controle verifica se todos os arquivos/escopos esperados estão prontos.
4. O lote só então fica elegível à ingestão Raw do Cadastrão.
5. O pipeline de negócio unifica, valida e publica posição atual/mensal, conforme [fluxo do Cadastrão](cadastrao-flow.md).
6. Arquivamento ocorre após o estado de consumo confirmado.
7. Produtos para download são preparados fora do volume público. Uma rota de publicação transfere apenas saídas prontas para `databricks_material_register_output`, verifica o conjunto e marca a publicação.

O mesmo executor pode atender outras rotas aprovadas. Dentro desta proposta, o dispatcher serializa movimentações; processos de negócio podem ter sua própria concorrência e precisam consumir apenas lotes prontos. Não confundir exclusão de movimentação com serializar todo processamento do sandbox.

## 11. Alternativa por chegada de arquivo
Pode substituir o agendamento quando latência, frequência irregular e recursos do workspace justificarem. Manter o mesmo executor idempotente e controles de lote.

Validar no ambiente: caminhos monitoráveis, permissões, tipo de volume, configuração de eventos, quantidade de arquivos e limites aplicáveis. O inventário declara volumes managed; não presumir que eventos de arquivos já estejam habilitados ou que exijam necessariamente volumes externos.

Debounce/espera após última alteração reduz disparos, mas não substitui manifesto/completude. Para múltiplos gatilhos/Jobs, preservar a execução central única ou implementar uma fila/claim transacional; `max_concurrent_runs=1` em Jobs separados não impede concorrência entre eles.

Fila de execuções pode ser apropriada no modo por eventos; sua política deve ser revista nessa mudança de gatilho.

## 12. Validação futura da implementação
- Simular rotas e conferir todos os destinos antes de permitir execução.
- Executar duas solicitações concorrentes e confirmar uma única execução mutante.
- Interromper após cópia e antes de remoção e demonstrar retomada sem perda/duplicação.
- Testar arquivo incompleto, lote parcial, reenvio idêntico e colisão de conteúdo.
- Testar falha de acesso, timeout e tentativas.
- Demonstrar que erro operacional falha o Job e notifica o destino autorizado.
- Demonstrar que não há ingestão nem publicação de lote incompleto.
- Medir duração/custo e ajustar intervalo, limites e compute.
- Conferir permissões de input/output e ausência de caminhos fora das rotas aprovadas.

## 13. Decisões pendentes
Intervalo e janela de operação; canal de upload e confirmação; arquivos esperados por lote; destinatários dos alertas; identidade/compute do Job; limites delegados aos Owners; política de bloqueio/continuidade; retenção e parâmetros de tentativas/timeout.

A proposta pode ser revisada antes de implementação. Não há automação ativa criada por este documento.

## Fontes técnicas
Consultadas em 2026-11-05:
- [Gatilhos de Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers).
- [Configuração: concorrência, fila e timeout](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-job).
- [Gatilhos por chegada de arquivos](https://learn.microsoft.com/en-us/azure/databricks/jobs/file-arrival-triggers).
- [Notificações e destinos administrativos](https://learn.microsoft.com/en-us/azure/databricks/jobs/notifications).
- [Política de tentativas das tarefas](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-task).

Agendamento, concorrência e notificações são recursos documentados. O intervalo de cinco minutos, o executor central, as tabelas de controle e as políticas de recuperação são recomendações desta proposta, ainda não verificadas no workspace.
