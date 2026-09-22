"""Command-line entry point for the Databricks App."""

import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the Honda Databricks MCP server")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("DATABRICKS_APP_PORT", "8000")),
    )
    args = parser.parse_args()
    uvicorn.run("server.app:combined_app", host="0.0.0.0", port=args.port)
