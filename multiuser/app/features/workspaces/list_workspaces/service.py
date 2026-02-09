"""
Service for list workspaces operation.
"""
from typing import List

from app.features.workspaces.schemas import WorkspaceResponse
from app.features.workspaces.list_workspaces.repository import get_all_workspaces


async def list_workspaces() -> List[WorkspaceResponse]:
    """
    Get all workspaces.
    
    Returns:
        List of workspace responses
    """
    workspaces = await get_all_workspaces()
    return [
        WorkspaceResponse(workspace=w.workspace, port=w.port, api_key=w.api_key)
        for w in workspaces
    ]
