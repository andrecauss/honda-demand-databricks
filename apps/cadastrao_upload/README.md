# Envio do Cadastrão

Databricks App em Streamlit para enviar as planilhas de uma posição do Cadastrão em um único lote.

1. Crie um Databricks App no workspace e adicione o volume de entrada do Unity Catalog como recurso com **Can read and write**. A identidade do app precisa de acesso ao catálogo e ao schema.
2. Configure `CADASTRAO_INPUT_PATH` no `app.yaml` com o caminho real, por exemplo `/Volumes/parts_hdbk_sandbox/_file_orchestrator/<volume_de_entrada>/cadastrao`. Crie a pasta de entrada antes do primeiro teste.
3. Sincronize esta pasta com o workspace, selecione-a como origem do app e faça o deploy. Dê acesso ao app apenas aos usuários autorizados a enviar lotes. Teste primeiro no sandbox.
4. Envie duas planilhas e confira a pasta do lote. Ela deve conter as planilhas, `upload_metadata.json` e `lote_pronto.txt`. Teste também um lote do tamanho esperado na operação.

O manifesto é um TXT em UTF-8 com um nome de planilha por linha. O app grava o manifesto por último. O JSON registra identidade do usuário fornecida pelo Databricks, horário de conclusão do upload em UTC e nomes e tamanhos dos arquivos. A auditoria de ingestão pertence à etapa seguinte.

Cada envio cria um ID novo. Um envio interrompido antes da publicação do manifesto deixa uma pasta incompleta, que o dispatcher deve ignorar. O tratamento de pastas abandonadas deve seguir a política de retenção do orquestrador. Se houver erro ao gravar o manifesto, verifique o volume antes de reenviar, pois uma falha de resposta pode ocorrer depois de o arquivo ter sido gravado.

O Streamlit mantém os arquivos selecionados em memória. Os limites iniciais são 50 MiB por arquivo e 200 MiB por lote. Valide tamanho e concorrência reais no workspace antes de aumentar os limites.

O app entrega o lote no volume de entrada. O notebook dispatcher e seu Job são a próxima etapa da integração. Veja a [proposta do File Orchestrator](../../docs/structure/file-orchestrator.md).

[Configuração de volumes em Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/uc-volumes)
