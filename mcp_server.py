"""Prefect Horizon entrypoint for Flarith."""
import os

from flarith.mcp import mcp

if __name__ == "__main__":
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        stateless_http=True,
    )
