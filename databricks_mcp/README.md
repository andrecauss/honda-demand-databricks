# Honda Databricks DEV MCP

Administrative MCP server for the complete Databricks development workspace:

`https://dbc-e3205ad0-a772.cloud.databricks.com`

The server is intentionally host-bound to `a772` and refuses to run against any other
workspace. It supports listing, exporting, importing, overwriting, and deleting workspace
objects; managing Databricks Repos; and creating, updating, running, cancelling, and deleting
Jobs.

## Security model

- OAuth only; no PATs or secrets are stored in this repository.
- Configure the Databricks App with user authorization scope `all-apis`.
- Give `CAN USE` only to the intended administrator and keep `CAN MANAGE` restricted.
- Destructive and execution tools require `confirmed=true`.
- Do not deploy this App to production. The code rejects every workspace host except `a772`.

## Deploy

Create a Databricks App named `mcp-honda-dev`, configure user authorization with `all-apis`,
then deploy this directory:

```bash
databricks auth login --host https://dbc-e3205ad0-a772.cloud.databricks.com
databricks sync . /Workspace/Users/<user>/mcp-honda-dev
databricks apps deploy mcp-honda-dev \
  --source-code-path /Workspace/Users/<user>/mcp-honda-dev
```

The MCP endpoint is `https://<app-url>/mcp`.

## Connect ChatGPT

Create an account-level OAuth App Connection with:

- Redirect URL: `https://chatgpt.com/connector_platform_oauth_redirect`
- Scope: `all-apis`
- Public client for an interactive single-user DEV connection

Then create a custom ChatGPT App using the MCP endpoint and OAuth client ID. Client IDs are
not secrets; never paste a client secret, access token, or refresh token into chat or source.
