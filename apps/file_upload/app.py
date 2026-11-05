import logging
import os
from urllib.parse import urlsplit

import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient

from batch import prepare_files, upload_batch

st.set_page_config(page_title="Envio de arquivos", page_icon="📁")
st.title("Envio de arquivos")
st.write("Escolha o processo e selecione os arquivos que fazem parte do mesmo lote.")

user = st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-Preferred-Username")
if not user:
    st.error("A identidade do usuário não está disponível. Acesse pelo Databricks Apps.")
    st.stop()

warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
if not warehouse_id:
    st.error("O SQL Warehouse do app precisa ser configurado.")
    st.stop()

try:
    workspace = WorkspaceClient()
    config = workspace.config
    with sql.connect(
        server_hostname=urlsplit(config.host).hostname,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: config.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT route_id, display_name, input_path, concat_ws(',', extensions)
                FROM parts_hdbk_sandbox._file_orchestrator.file_routes
                WHERE is_active AND upload_enabled
                ORDER BY display_name, route_id
            """)
            active_routes = {}
            for route_id, label, path, extensions in cursor.fetchall():
                if route_id in active_routes or not extensions:
                    raise ValueError(f"Cadastro inválido para a rota {route_id}")
                active_routes[route_id] = {
                    "label": label,
                    "input_path": path,
                    "extensions": extensions.split(","),
                }
except Exception:
    logging.exception("Falha ao consultar as rotas de envio")
    st.error("Não foi possível carregar os processos disponíveis. Consulte os logs do app.")
    st.stop()
if not active_routes:
    st.info("Nenhum processo está disponível para envio.")
    st.stop()

route_id = st.selectbox("Processo", list(active_routes), format_func=lambda key: active_routes[key]["label"])
route = active_routes[route_id]

with st.form(f"batch_upload_{route_id}", clear_on_submit=True):
    selected = st.file_uploader("Arquivos", type=route["extensions"], accept_multiple_files=True)
    st.caption("Limites iniciais: 50 MiB por arquivo e 200 MiB por lote.")
    submitted = st.form_submit_button("Enviar lote", type="primary")

if submitted:
    try:
        extensions = {"." + extension.lower() for extension in route["extensions"]}
        files = prepare_files(selected, extensions)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    try:
        with st.spinner("Gravando os arquivos no volume…"):
            batch_id = upload_batch(workspace.files, route_id, route["input_path"], user, files)
    except Exception:
        logging.exception("Falha ao concluir o envio na rota %s", route_id)
        st.error("Não foi possível confirmar o envio. Consulte os logs do app antes de reenviar.")
    else:
        st.success(f"Lote {batch_id} recebido para {route['label']} com {len(files)} arquivo(s).")
