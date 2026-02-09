"""
Endpoint for list workspaces operation.
GET /admin/workspaces
"""
from fastapi import APIRouter, HTTPException, Header

from app.config.settings import ADMIN_SECRET
from app.features.workspaces.list_workspaces.service import list_workspaces

router = APIRouter(tags=["admin"])


@router.get("/admin/workspaces")
async def list_workspaces_endpoint(
    x_admin_key: str = Header(None, alias="X-Admin-Key")
):
    """List all workspaces."""
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Invalid Key")
    
    workspaces = await list_workspaces()
    return [{"workspace": w.workspace, "port": w.port, "api_key": w.api_key} for w in workspaces]
