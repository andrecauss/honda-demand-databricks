# Cadastrão — fluxo proposto de ponta a ponta
Versão 0.1 · 2026-11-05 · Documento para discussão.

[Proposta geral](sandbox-structure-proposal.md). Esta aplicação concreta detalha o caso Cadastrão discutido na seção 4.2 da proposta geral. Em eventual divergência, reconciliar os documentos antes da implementação.

## Escopo confirmado
- Manter o processo no schema `parts_hdbk_sandbox.pr_cadastrao`.
- A entrada já passou por processamento externo. O produtor imediato ainda precisa ser identificado; não presumir que seja o SAP.
- Receber a base completa a cada dia útil, por um conjunto de arquivos Excel que será ampliado.
- Unificar os arquivos e gerar vários produtos.
- Atender simultaneamente posição atual e primeira posição mensal.
- Auditar upload e ingestão como eventos diferentes.
- Usar `is_current` e uma flag mensal para identificar as posições nas consultas.

Somente documentação: nenhum código, objeto, arquivo de dados, job ou privilégio no Databricks foi alterado.

## 1. Objetos e nomes
`<source>` é um marcador a substituir após identificar o produtor imediato do arquivo. Os nomes seguintes são propostos.

| Tipo | Nome proposto | Finalidade |
| --- | --- | --- |
| Volume de entrada | `_file_orchestrator.<source>_material_register_input` | Interface para envio |
| Volume técnico | `pr_cadastrao.<source>_material_register` | Originais e estados internos |
| Tabela Raw | `pr_cadastrao.raw_material_register` | Dados como recebidos, por arquivo e lote |
| Histórico Trusted | `pr_cadastrao.trusted_material_register_history` | Fotografias completas unificadas e validadas |
| View atual | `pr_cadastrao.vw_material_register_current` | Última posição válida publicada |
| View mensal | `pr_cadastrao.vw_material_register_month_start` | Referência mensal selecionada e preservada |
| Produtos | Objetos `refined_*` no processo | Cadeia, estoque, atributos técnicos e demais saídas |
| Volume de saída, se houver download | `_file_orchestrator.databricks_material_register_output` | Arquivos finais aprovados |

O catálogo dos objetos da tabela é `parts_hdbk_sandbox`. Caminho de arquivo: `/Volumes/<catalog>/<schema>/<volume>/<pastas>`. As views atual/mensal consultam o mesmo histórico; não exigem duplicar cada posição em outra tabela completa.

## 2. Recebimento: um conjunto identificado
Cada entrega tem `batch_id`, data de posição e escopos esperados. O contrato lista os arquivos/escopos necessários; não fixa em dois nem em cinco arquivos.

O usuário envia os arquivos no volume de entrada. O canal registra, por arquivo:
- nome original e caminho completo;
- identificador de entrega e lote;
- `_uploaded_at` e `_uploaded_by`, quando capturados pelo canal;
- referência declarada, tamanho e checksum.

Um registro de entrega completa ou manifesto indica que o conjunto está pronto. A presença do primeiro arquivo não inicia a publicação da posição inteira. Se as áreas publicarem por escopo independente, explicitar quais escopos podem ser concluídos separadamente.

O mecanismo real de obtenção de autor e hora de upload depende do canal usado. Não substituí-los pelo usuário do job ou pelo instante em que o job encontrou o arquivo.

## 3. Originais e Raw
A rotina copia e verifica os originais no volume técnico, em `received/<batch_id>/`. Somente após confirmar a preservação pode consumir/remover a entrega transitória.

A Raw preserva os valores tal como recebidos na fronteira do Databricks, antes de normalizações de chaves ou regras internas. Se os layouts forem compatíveis, podem ser lidos no mesmo objeto Raw preservando arquivo e lote; se forem incompatíveis, o contrato define representações Raw por layout antes da consolidação. Não forçar uma união que descarte conteúdo original.

Auditoria de ingestão:
- `_load_id`: execução;
- `_ingested_at` e `_ingested_by`: momento e identidade executora;
- `_load_type = full`;
- `_source_file_path`: arquivo efetivamente consumido ou relação rastreável aos arquivos;
- `_last_updated_at`: atualização conforme o contrato do objeto.

A Raw registra novas entregas sem apagar as anteriores. Reenvio idêntico é reconhecido pelo controle; reprocessamento recebe nova execução sem exigir duplicar o mesmo conteúdo Raw.

## 4. Unificação e validação
O processamento usa apenas os arquivos do lote declarado. Unifica linhas quando os arquivos forem partições compatíveis; relações entre layouts diferentes exigem joins e chaves definidos.

Antes de publicar:
- confirmar completude dos arquivos e escopos;
- verificar compatibilidade de layouts e referências;
- validar chaves, duplicidades e conflitos no grão de cada produto;
- aplicar normalizações, tipos e regras;
- reconciliar totais necessários.

Não presumir que todos os produtos têm a mesma chave: cadastro, estoque por centro e cadeia podem ter grãos diferentes. Guardar o lote e as regras usados nas derivações.

Falha mantém o último resultado publicado disponível. O lote fica rejeitado ou pendente, com erro rastreável; os originais correspondentes podem ficar em `quarantine/<batch_id>/`.

## 5. Publicação das fotografias
Para testar o requisito, a recomendação inicial é preservar fotografias completas validadas no histórico Trusted, identificadas pela data de posição, escopo e lote/revisão. Volume e retenção orientarão a materialização final.

Separar:
- data da posição do negócio;
- instante/autor do upload;
- instante/identidade da ingestão;
- instante da publicação;
- mês de referência.

O último arquivo recebido pode representar uma posição antiga. Selecionar a posição atual pela referência de negócio e revisão aprovada, não pelo horário de chegada isoladamente.

Após sucesso, arquivar os originais em `archive/<batch_id>/` e registrar o lote publicado. Se a publicação envolver vários objetos, usar controle de publicação que só exponha o lote após todas as saídas obrigatórias concluírem; não presumir atomicidade entre tabelas distintas.

## 6. Seleção atual e mensal
Manter em controle de publicação/referências a seleção dos lotes; expor as flags nas views quando úteis. A Raw e seus metadados originais permanecem preservados.

- `is_current`: um lote completo válido escolhido como posição atual por escopo de publicação.
- `is_month_start`: um lote escolhido por mês e escopo como referência mensal estável.

A flag mensal não marca a primeira aparição de cada material. Todas as linhas da fotografia selecionada usam a mesma referência coerente. As referências dos meses anteriores permanecem identificadas.

Exemplo ilustrativo, não corresponde a cargas reais:

| Posição | Lote | Após publicar apenas A | Após publicar B |
| --- | --- | --- | --- |
| 2026-11-03, selecionada para novembro | A | current=true; month_start=true | current=false; month_start=true |
| 2026-11-04 | B | Ainda não publicada | current=true; month_start=false |

A referência mensal do exemplo pressupõe que A tenha sido selecionado segundo uma regra aprovada. Ainda falta definir se a escolha será automática ou explícita e o significado de “primeira do mês” diante de calendário, atraso ou falta de carga.

Correção de uma referência mensal já publicada precisa ser identificada e rastreável; não alterar silenciosamente o lote mensal. Fotografias completas permitem observar ausências, mas só interpretar exclusão depois de validar a completude do escopo.

## 7. Derivações, consumo e download
Os notebooks de produtos escolhem explicitamente o lote atual, a referência mensal ou outro lote aprovado. Registram essa dependência em suas execuções.

- Standard User consulta Trusted e Refined publicadas.
- Key User consulta também a Raw necessária ao desenvolvimento/validação e pode criar/editar notebooks; escrita em dados continua a detalhar.
- File User envia no input e baixa no output autorizado. Entrada tem leitura/escrita; saída tem somente leitura por esse perfil.
- Owner gerencia acessos dentro da delegação existente; criação de grupos depende do TI.

Quando houver exportação, preparar arquivos e ZIP na área técnica, por referência e `run_id`; validar e publicar apenas o conjunto final no volume de saída. Nem todo produto tabular precisa de um arquivo para download.

## 8. Pendências para implementar
1. Identificar o produtor imediato e confirmar o canal de upload e seus eventos de auditoria.
2. Definir composição, layouts e grãos dos arquivos do lote.
3. Definir a escolha da primeira posição mensal, revisão e cargas atrasadas.
4. Medir volume, prazo de retenção e janela diária de publicação.
5. Mapear quais produtos usam atual, mensal ou ambas as referências.
6. Planejar nomes definitivos, compatibilidade dos consumidores e migração dos notebooks.

O SCD2 dos atributos técnicos pode ser mantido/evoluído como produto específico. Não é obrigatório usar o mesmo modelo para estoques, preços e todos os campos do cadastro.

## 9. Relação com o código atual
Fontes consultadas:
- `00_cadastrao/1.1_ingest_raw_material_cadastrao.ipynb`;
- `00_cadastrao/1.2_ingest_refined_material_stock_snapshot.py`;
- `00_cadastrao/1.3_ingest_refined_material_historical.py`.

Hoje o 1.1 transforma/recria `material_cadastrao` e referencia o início do mês; o 1.2 substitui o snapshot mensal por escopo; o 1.3 trabalha com SCD2 mensal e guardas cronológicas. A mudança para carga diária exige rever esses contratos, não apenas agendar execução diária.

A auditoria atual usa `_ingested_*`, `_last_updated_at`, `_load_*` e `_source_file_path`; manter a convenção, complementando o evento de upload e a origem por arquivo.

A proposta detalhada está registrada, mas ainda não foi implementada ou testada no Databricks.
