"""FastAPI and Streamable HTTP MCP application."""

from fastapi import FastAPI, Request
from fastmcp import FastMCP

from .auth import assert_dev_workspace, header_store
from .tools import load_tools

assert_dev_workspace()

mcp_server = FastMCP(name="honda-databricks-dev-admin")
load_tools(mcp_server)
mcp_app = mcp_server.http_app(stateless_http=True)

api_app = FastAPI(
    title="Honda Databricks DEV MCP",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@api_app.get("/", include_in_schema=False)
async def root() -> dict:
    return {
        "status": "healthy",
        "workspace": "dbc-e3205ad0-a772",
        "mcp_endpoint": "/mcp",
    }


combined_app = FastAPI(
    title="Honda Databricks DEV MCP",
    routes=[*mcp_app.routes, *api_app.routes],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    """Keep request-scoped OAuth headers out of global state and logs."""
    token = header_store.set({key.lower(): value for key, value in request.headers.items()})
    try:
        return await call_next(request)
    finally:
        header_store.reset(token)
