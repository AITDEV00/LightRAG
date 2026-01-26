"""
Endpoint for delete workspace operation.
DELETE /admin/workspaces/{workspace}
"""
from fastapi import APIRouter, HTTPException, Header

from app.config.settings import ADMIN_SECRET
from app.common.process_manager import manager
from app.features.workspaces.repository import get_workspace_by_name
from app.features.workspaces.delete_workspace.repository import remove_workspace_from_db
from app.features.workspaces.delete_workspace.service import (
    cleanup_workspace_documents,
    wipe_workspace_data,
)

router = APIRouter(tags=["admin"])


@router.delete("/admin/workspaces/{workspace}")
async def delete_workspace_endpoint(
    workspace: str,
    wipe_data: bool = False,
    x_admin_key: str = Header(None, alias="X-Admin-Key")
):
    """Delete a workspace."""
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Invalid Key")
    
    # Attempt to clear data via API if wipe_data is requested
    if wipe_data:
        config = await get_workspace_by_name(workspace)
        if config:
            await cleanup_workspace_documents(config)

    manager.stop_process(workspace)
    await remove_workspace_from_db(workspace)
    
    if wipe_data:
        wipe_workspace_data(workspace)
        
    return {"status": "deleted"}
