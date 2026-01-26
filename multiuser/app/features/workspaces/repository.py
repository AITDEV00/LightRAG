"""
Shared repository functions for workspace feature.
Used by gateway and other features to look up workspaces.
"""
from typing import List, Optional

from app.common.db import get_pool
from app.features.workspaces.schemas import WorkspaceConfig


async def get_all_workspaces() -> List[WorkspaceConfig]:
    """Get all workspaces from the database."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT workspace, api_key, port FROM lightrag_workspaces")
        return [WorkspaceConfig(workspace=row['workspace'], api_key=row['api_key'], port=row['port']) for row in rows]


async def get_workspace_by_key(api_key: str) -> Optional[WorkspaceConfig]:
    """Get a workspace by API key."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT workspace, api_key, port FROM lightrag_workspaces WHERE api_key = $1", api_key)
        return WorkspaceConfig(workspace=row['workspace'], api_key=row['api_key'], port=row['port']) if row else None


async def get_workspace_by_name(name: str) -> Optional[WorkspaceConfig]:
    """Get a workspace by name."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT workspace, api_key, port FROM lightrag_workspaces WHERE workspace = $1", name)
        return WorkspaceConfig(workspace=row['workspace'], api_key=row['api_key'], port=row['port']) if row else None
