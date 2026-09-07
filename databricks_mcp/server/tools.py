"""Administrative tools for notebooks, workspace objects, Repos, and Jobs."""

import base64
import re
from typing import Any

from .auth import DEV_HOST, get_user_client

_JWT = re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")


def _error(exc: Exception) -> dict[str, str]:
    message = _JWT.sub("[REDACTED]", str(exc))
    return {"error": type(exc).__name__, "message": message[:1000]}


def _api(method: str, path: str, *, body: dict | None = None, query: dict | None = None) -> Any:
    client = get_user_client()
    return client.api_client.do(method, path, body=body, query=query)


def _require_confirmation(confirmed: bool, action: str) -> None:
    if not confirmed:
        raise ValueError(f"Set confirmed=true to authorize {action}")


def load_tools(mcp_server) -> None:
    @mcp_server.tool
    def health() -> dict:
        """Confirm that the MCP server is bound to the approved a772 DEV workspace."""
        return {"status": "healthy", "workspace": DEV_HOST, "access": "full-control"}

    @mcp_server.tool
    def current_user() -> dict:
        """Return the Databricks user whose OAuth identity is executing the request."""
        try:
            user = get_user_client().current_user.me()
            return {
                "user_name": user.user_name,
                "display_name": user.display_name,
                "active": user.active,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_list(path: str = "/") -> dict:
        """List notebooks, directories, files, and Repos under a workspace path."""
        try:
            return _api("GET", "/api/2.0/workspace/list", query={"path": path})
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_status(path: str) -> dict:
        """Get the type, language, and object ID of a workspace object."""
        try:
            return _api("GET", "/api/2.0/workspace/get-status", query={"path": path})
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_export(path: str, format: str = "SOURCE") -> dict:
        """Export a notebook or workspace object as base64 content."""
        try:
            return _api(
                "GET", "/api/2.0/workspace/export", query={"path": path, "format": format}
            )
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_mkdirs(path: str) -> dict:
        """Create a workspace directory and missing parent directories."""
        try:
            return _api("POST", "/api/2.0/workspace/mkdirs", body={"path": path}) or {
                "status": "created",
                "path": path,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_import(
        path: str,
        content: str,
        format: str = "SOURCE",
        language: str | None = None,
        content_is_base64: bool = False,
        overwrite: bool = False,
        confirmed: bool = False,
    ) -> dict:
        """Create or replace a notebook/workspace object; content may be plain text or base64."""
        try:
            if overwrite:
                _require_confirmation(confirmed, f"overwriting workspace object {path}")
            if content_is_base64:
                base64.b64decode(content, validate=True)
                encoded = content
            else:
                encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
            payload: dict[str, Any] = {
                "path": path,
                "content": encoded,
                "format": format,
                "overwrite": overwrite,
            }
            if language:
                payload["language"] = language
            return _api("POST", "/api/2.0/workspace/import", body=payload) or {
                "status": "imported",
                "path": path,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def workspace_delete(path: str, recursive: bool = False, confirmed: bool = False) -> dict:
        """Delete a workspace object; requires confirmed=true and supports recursive deletion."""
        try:
            _require_confirmation(confirmed, f"deleting workspace object {path}")
            return _api(
                "POST",
                "/api/2.0/workspace/delete",
                body={"path": path, "recursive": recursive},
            ) or {"status": "deleted", "path": path}
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def repos_list(path_prefix: str | None = None) -> dict:
        """List Databricks Repos, optionally filtering by workspace path prefix."""
        try:
            query = {"path_prefix": path_prefix} if path_prefix else None
            return _api("GET", "/api/2.0/repos", query=query)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def repo_get(repo_id: int) -> dict:
        """Get a Databricks Repo by ID."""
        try:
            return _api("GET", f"/api/2.0/repos/{repo_id}")
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def repo_create(url: str, provider: str, path: str | None = None) -> dict:
        """Clone a remote Git repository into Databricks Repos."""
        try:
            body = {"url": url, "provider": provider}
            if path:
                body["path"] = path
            return _api("POST", "/api/2.0/repos", body=body)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def repo_update(repo_id: int, branch: str | None = None, tag: str | None = None) -> dict:
        """Switch or pull a Databricks Repo to a branch or tag."""
        try:
            if bool(branch) == bool(tag):
                raise ValueError("Provide exactly one of branch or tag")
            return _api(
                "PATCH",
                f"/api/2.0/repos/{repo_id}",
                body={"branch": branch} if branch else {"tag": tag},
            ) or {"status": "updated", "repo_id": repo_id}
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def repo_delete(repo_id: int, confirmed: bool = False) -> dict:
        """Delete a Databricks Repo; requires confirmed=true."""
        try:
            _require_confirmation(confirmed, f"deleting repo {repo_id}")
            return _api("DELETE", f"/api/2.0/repos/{repo_id}") or {
                "status": "deleted",
                "repo_id": repo_id,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def jobs_list(limit: int = 25, page_token: str | None = None) -> dict:
        """List jobs in the DEV workspace."""
        try:
            query: dict[str, Any] = {"limit": min(max(limit, 1), 100)}
            if page_token:
                query["page_token"] = page_token
            return _api("GET", "/api/2.1/jobs/list", query=query)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_get(job_id: int) -> dict:
        """Get full job settings by job ID."""
        try:
            return _api("GET", "/api/2.1/jobs/get", query={"job_id": job_id})
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_create(settings: dict) -> dict:
        """Create a job from a complete Databricks Jobs API 2.1 settings object."""
        try:
            return _api("POST", "/api/2.1/jobs/create", body=settings)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_reset(job_id: int, new_settings: dict, confirmed: bool = False) -> dict:
        """Replace all settings of an existing job; requires confirmed=true."""
        try:
            _require_confirmation(confirmed, f"replacing all settings for job {job_id}")
            return _api(
                "POST",
                "/api/2.1/jobs/reset",
                body={"job_id": job_id, "new_settings": new_settings},
            ) or {"status": "updated", "job_id": job_id}
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_update(
        job_id: int, new_settings: dict, fields_to_remove: list[str] | None = None
    ) -> dict:
        """Partially update job settings and optionally remove top-level fields."""
        try:
            body: dict[str, Any] = {"job_id": job_id, "new_settings": new_settings}
            if fields_to_remove:
                body["fields_to_remove"] = fields_to_remove
            return _api("POST", "/api/2.1/jobs/update", body=body) or {
                "status": "updated",
                "job_id": job_id,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_delete(job_id: int, confirmed: bool = False) -> dict:
        """Delete a job; requires confirmed=true."""
        try:
            _require_confirmation(confirmed, f"deleting job {job_id}")
            return _api("POST", "/api/2.1/jobs/delete", body={"job_id": job_id}) or {
                "status": "deleted",
                "job_id": job_id,
            }
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def job_run_now(
        job_id: int,
        job_parameters: dict[str, str] | None = None,
        confirmed: bool = False,
    ) -> dict:
        """Start a job immediately; requires confirmed=true."""
        try:
            _require_confirmation(confirmed, f"starting job {job_id}")
            body: dict[str, Any] = {"job_id": job_id}
            if job_parameters:
                body["job_parameters"] = job_parameters
            return _api("POST", "/api/2.1/jobs/run-now", body=body)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def runs_list(
        job_id: int | None = None,
        active_only: bool = False,
        completed_only: bool = False,
        limit: int = 25,
    ) -> dict:
        """List recent or active job runs."""
        try:
            query: dict[str, Any] = {
                "active_only": active_only,
                "completed_only": completed_only,
                "limit": min(max(limit, 1), 100),
            }
            if job_id is not None:
                query["job_id"] = job_id
            return _api("GET", "/api/2.1/jobs/runs/list", query=query)
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def run_get(run_id: int, include_history: bool = True) -> dict:
        """Get a job run, including task states and optional repair history."""
        try:
            return _api(
                "GET",
                "/api/2.1/jobs/runs/get",
                query={"run_id": run_id, "include_history": include_history},
            )
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def run_output(run_id: int) -> dict:
        """Get notebook output, logs, error details, and metadata for a job run."""
        try:
            return _api("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": run_id})
        except Exception as exc:
            return _error(exc)

    @mcp_server.tool
    def run_cancel(run_id: int, confirmed: bool = False) -> dict:
        """Cancel an active run; requires confirmed=true."""
        try:
            _require_confirmation(confirmed, f"cancelling run {run_id}")
            return _api(
                "POST", "/api/2.1/jobs/runs/cancel", body={"run_id": run_id}
            ) or {"status": "cancellation_requested", "run_id": run_id}
        except Exception as exc:
            return _error(exc)
