"""
Endpoint for list workspaces operation.
GET /admin/workspaces
"""
from fastapi import APIRouter, HTTPException, Header, Request

from app.config.settings import ADMIN_SECRET
from app.features.workspaces.list_workspaces.service import list_workspaces

router = APIRouter(tags=["admin"])


@router.get("/admin/workspaces")
async def list_workspaces_endpoint(
    request: Request,
    x_admin_key: str = Header(None, alias="X-Admin-Key")
):
    """List all workspaces with active/sleeping status."""
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Invalid Key")
    
    # Get the ASGI dispatcher from app state to check active apps
    dispatcher = request.app.state.dispatcher
    active_workspaces = set(dispatcher.apps.keys())

    workspaces = await list_workspaces()
    return [
        {
            "workspace": w.workspace,
            "port": w.port,
            "api_key": w.api_key,
            "is_active": w.workspace in active_workspaces,
        }
        for w in workspaces
    ]
