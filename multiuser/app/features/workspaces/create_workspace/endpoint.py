"""
Endpoint for create workspace operation.
POST /admin/workspaces
"""
import asyncpg
from fastapi import APIRouter, HTTPException, Header

from app.config.settings import ADMIN_SECRET
from app.common.process_manager import manager
from app.features.workspaces.schemas import WorkspaceCreate
from app.features.workspaces.create_workspace.service import create_workspace

router = APIRouter(tags=["admin"])


@router.post("/admin/workspaces")
async def create_workspace_endpoint(
    data: WorkspaceCreate,
    x_admin_key: str = Header(None, alias="X-Admin-Key")
):
    """Create a new workspace."""
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Invalid Key")
    
    try:
        config = await create_workspace(data.workspace)
        # Start the process via manager
        await manager.start_process(config)
        return {"status": "created", "workspace": config.workspace, "api_key": config.api_key}
    except asyncpg.UniqueViolationError:
        raise HTTPException(400, "Workspace already exists")
    except Exception as e:
        raise HTTPException(500, str(e))
