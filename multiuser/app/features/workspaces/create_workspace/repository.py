"""
Repository for create workspace operation.
"""
from app.common.db import get_pool
from app.features.workspaces.schemas import WorkspaceConfig


async def add_workspace_to_db(config: WorkspaceConfig):
    """Add a new workspace to the database."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO lightrag_workspaces (workspace, api_key, port) VALUES ($1, $2, $3)",
            config.workspace, config.api_key, config.port
        )
