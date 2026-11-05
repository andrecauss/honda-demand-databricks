# Envio de arquivos por processo

O Databricks App recebe lotes de diferentes processos. O usuário escolhe o processo e envia os arquivos juntos. O app determina o volume e a pasta de entrada pela rota selecionada.

1. Confira o contrato em [file_routes.sql](file_routes.sql) e crie ou alinhe a tabela `parts_hdbk_sandbox._file_orchestrator.file_routes`.
2. Cadastre uma linha por rota, com ID único, nome, caminho de entrada exclusivo, caminho de destino e extensões sem ponto. Marque `is_active` e `upload_enabled` para listar a rota no app. Para retirar uma rota da tela enquanto o dispatcher conclui lotes pendentes, desligue apenas `upload_enabled`.
3. No Databricks Apps, adicione o SQL Warehouse com chave `sql-warehouse` e permissão **Can use**. Conceda à identidade do app `USE CATALOG`, `USE SCHEMA` e `SELECT` na tabela. Adicione também os volumes de entrada como recursos de leitura e escrita.
4. Crie as pastas de entrada, implante esta pasta como origem do app e teste duas rotas com caminhos distintos. O app consulta a tabela ao carregar a tela. Alterar uma rota na tabela dispensa um novo deploy, desde que os recursos e permissões já estejam configurados.

Cada envio cria um ID único. O app grava os arquivos e o `upload_metadata.json`, com rota, usuário, horário em UTC e tamanhos. Por último, grava `lote_pronto.txt` em UTF-8, com um nome de arquivo por linha. Se uma gravação anterior falhar, a pasta fica sem o manifesto e o dispatcher deve ignorá-la. Se houver erro na gravação do próprio manifesto, verifique o volume antes de reenviar.

Todos os usuários autorizados a acessar esta instância do app veem as rotas habilitadas. Quando processos exigirem públicos diferentes, use instâncias separadas com suas próprias rotas e permissões. Os limites iniciais do Streamlit são 50 MiB por arquivo e 200 MiB por lote. Valide os tamanhos e a concorrência reais no sandbox.

O notebook dispatcher e o Job são a etapa seguinte. Veja a [proposta do File Orchestrator](../../docs/structure/file-orchestrator.md).
