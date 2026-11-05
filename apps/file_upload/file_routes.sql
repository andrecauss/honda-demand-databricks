-- Execute no sandbox após confirmar que o schema existe.
-- Se a tabela já existir, confira o contrato antes de habilitar o app.
CREATE TABLE IF NOT EXISTS parts_hdbk_sandbox._file_orchestrator.file_routes (
    route_id STRING NOT NULL COMMENT 'Identificador único da rota, validado na operação',
    display_name STRING NOT NULL COMMENT 'Nome exibido no app',
    input_path STRING NOT NULL COMMENT 'Caminho de entrada exclusivo no Unity Catalog',
    destination_path STRING NOT NULL COMMENT 'Destino técnico usado pelo dispatcher',
    extensions ARRAY<STRING> NOT NULL COMMENT 'Extensões sem ponto, por exemplo xlsx ou csv',
    is_active BOOLEAN NOT NULL COMMENT 'Rota disponível para processamento',
    upload_enabled BOOLEAN NOT NULL COMMENT 'Rota oferecida pelo app de upload'
)
USING DELTA;
