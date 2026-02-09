"""
Repository for delete workspace operation.
"""
from app.common.db import get_pool


async def remove_workspace_from_db(workspace: str):
    """Remove a workspace from the database."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM lightrag_workspaces WHERE workspace = $1", workspace)
