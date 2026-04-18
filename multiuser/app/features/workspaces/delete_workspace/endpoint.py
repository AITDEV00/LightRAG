"""
Endpoint for delete workspace operation.
DELETE /admin/workspaces/{workspace}
"""
from fastapi import APIRouter, HTTPException, Header, Request

from app.config.settings import ADMIN_SECRET
from app.features.workspaces.repository import get_workspace_by_name
from app.features.workspaces.delete_workspace.repository import remove_workspace_from_db
from app.features.workspaces.delete_workspace.service import (
    wipe_workspace_data,
)

router = APIRouter(tags=["admin"])


@router.delete("/admin/workspaces/{workspace}")
async def delete_workspace_endpoint(
    workspace: str,
    request: Request,
    wipe_data: bool = False,
    x_admin_key: str = Header(None, alias="X-Admin-Key")
):
    """Delete a workspace."""
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Invalid Key")
    
    # Get the ASGI dispatcher from app state
    dispatcher = request.app.state.dispatcher

    # Evict the workspace app from memory (gracefully closes DB connections)
    await dispatcher.evict_app(workspace)

    # Remove from Postgres
    await remove_workspace_from_db(workspace)
    
    # Optionally wipe the filesystem data
    if wipe_data:
        wipe_workspace_data(workspace)
        
    return {"status": "deleted"}
