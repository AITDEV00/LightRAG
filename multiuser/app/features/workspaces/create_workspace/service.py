"""
Service for create workspace operation.
"""
import secrets

from app.common.process_manager import find_free_port
from app.config.settings import START_PORT_RANGE
from app.features.workspaces.schemas import WorkspaceConfig
from app.features.workspaces.create_workspace.repository import add_workspace_to_db


async def create_workspace(workspace_name: str) -> WorkspaceConfig:
    """
    Create a new workspace with auto-generated API key and port.
    
    Args:
        workspace_name: Name of the workspace to create
        
    Returns:
        WorkspaceConfig with the new workspace details
    """
    api_key = secrets.token_urlsafe(32)
    new_port = await find_free_port(START_PORT_RANGE)
    config = WorkspaceConfig(workspace=workspace_name, api_key=api_key, port=new_port)
    await add_workspace_to_db(config)
    return config
