import json
import logging
from pathlib import Path

import streamlit as st
from databricks.sdk import WorkspaceClient

from batch import prepare_files, upload_batch

st.set_page_config(page_title="Envio de arquivos", page_icon="📁")
st.title("Envio de arquivos")
st.write("Escolha o processo e selecione os arquivos que fazem parte do mesmo lote.")

user = st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-Preferred-Username")
if not user:
    st.error("A identidade do usuário não está disponível. Acesse pelo Databricks Apps.")
    st.stop()

routes = json.loads(Path(__file__).with_name("routes.json").read_text(encoding="utf-8"))
active_routes = {route_id: route for route_id, route in routes.items() if route["enabled"]}
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
            batch_id = upload_batch(WorkspaceClient().files, route_id, route["input_path"], user, files)
    except Exception:
        logging.exception("Falha ao concluir o envio na rota %s", route_id)
        st.error("Não foi possível confirmar o envio. Consulte os logs do app antes de reenviar.")
    else:
        st.success(f"Lote {batch_id} recebido para {route['label']} com {len(files)} arquivo(s).")
