"""Authentication and workspace-boundary helpers."""

import contextvars
import os
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

DEV_HOST = "https://dbc-e3205ad0-a772.cloud.databricks.com"
header_store: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar(
    "header_store", default={}
)


def _normalized_host(value: str) -> str:
    parsed = urlparse(value)
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}".rstrip("/")


def assert_dev_workspace() -> None:
    """Refuse to start or call APIs outside the approved DEV workspace."""
    configured = os.getenv("DATABRICKS_HOST", DEV_HOST)
    if _normalized_host(configured) != _normalized_host(DEV_HOST):
        raise RuntimeError("This MCP server is restricted to the a772 DEV workspace")


def get_user_client() -> WorkspaceClient:
    """Build a WorkspaceClient with the OAuth identity forwarded by Databricks Apps."""
    assert_dev_workspace()
    token = header_store.get().get("x-forwarded-access-token")
    if not token:
        if "DATABRICKS_APP_NAME" in os.environ:
            raise PermissionError("OAuth user token was not forwarded to the Databricks App")
        return WorkspaceClient(host=DEV_HOST)
    return WorkspaceClient(host=DEV_HOST, token=token, auth_type="pat")
