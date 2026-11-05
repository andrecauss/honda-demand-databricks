# Envio de arquivos por processo

O Databricks App recebe lotes de diferentes processos. O usuário escolhe o processo e envia os arquivos juntos. O app determina o volume e a pasta de entrada pela rota selecionada.

1. Cadastre cada rota em `routes.json` com ID, nome exibido, caminho de entrada e extensões aceitas, sem ponto. Configure um caminho exclusivo para cada rota, sem sobreposição. O exemplo do Cadastrão vem desabilitado até receber um caminho real.
2. No Databricks Apps, adicione os volumes de entrada como recursos com acesso de leitura e escrita. Crie as pastas de entrada e implante esta pasta como origem do app.
3. Habilite as rotas configuradas. Mantenha o ID e o caminho de entrada alinhados com `_file_orchestrator.file_routes`. Essa tabela define o destino final usado pelo notebook. Nesta versão, o app lê a lista de entradas do arquivo de configuração.
4. Teste duas rotas com caminhos distintos e confira o conteúdo de cada pasta de lote.

Cada envio cria um ID único. O app grava os arquivos e o `upload_metadata.json`, com rota, usuário, horário em UTC e tamanhos. Por último, grava `lote_pronto.txt` em UTF-8, com um nome de arquivo por linha. Se uma gravação anterior falhar, a pasta fica sem o manifesto e o dispatcher deve ignorá-la. Se houver erro na gravação do próprio manifesto, verifique o volume antes de reenviar.

Todos os usuários autorizados a acessar esta instância do app veem as rotas habilitadas. Quando processos exigirem públicos diferentes, use instâncias separadas com suas próprias rotas e permissões. Os limites iniciais do Streamlit são 50 MiB por arquivo e 200 MiB por lote. Valide os tamanhos e a concorrência reais no sandbox.

O notebook dispatcher e o Job são a etapa seguinte. Veja a [proposta do File Orchestrator](../../docs/structure/file-orchestrator.md).
