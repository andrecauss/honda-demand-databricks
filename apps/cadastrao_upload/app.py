import logging
import os

import streamlit as st
from databricks.sdk import WorkspaceClient

from batch import prepare_files, upload_batch

st.set_page_config(page_title="Envio do Cadastrão", page_icon="📁")
st.title("Envio do Cadastrão")
st.write("Selecione todas as planilhas da mesma posição e envie juntas.")

root = os.getenv("CADASTRAO_INPUT_PATH", "")
user = st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-Preferred-Username")
if not root:
    st.error("Configure o caminho do volume de entrada do Cadastrão.")
    st.stop()
if not user:
    st.error("A identidade do usuário não está disponível. Acesse pelo Databricks Apps.")
    st.stop()

with st.form("batch_upload", clear_on_submit=True):
    selected = st.file_uploader("Planilhas Excel", type=["xlsx", "xls", "xlsm"], accept_multiple_files=True)
    st.caption("Limites iniciais: 50 MB por arquivo e 200 MB por lote.")
    submitted = st.form_submit_button("Enviar lote", type="primary")

if submitted:
    try:
        files = prepare_files(selected)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    try:
        with st.spinner("Gravando as planilhas no volume…"):
            batch_id = upload_batch(WorkspaceClient().files, root, user, files)
    except Exception:
        logging.exception("Falha ao concluir o envio do Cadastrão")
        st.error("Não foi possível confirmar o envio. Consulte os logs do app antes de reenviar.")
    else:
        st.success(f"Lote {batch_id} recebido com {len(files)} arquivo(s).")
