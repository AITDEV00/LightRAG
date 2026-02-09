"""
Service for create workspace operation.
"""
import secrets

from pydantic import ValidationError
from app.common.process_manager import find_free_port
from app.config.settings import START_PORT_RANGE
from app.features.workspaces.schemas import WorkspaceConfig, WorkspaceCreate
from app.features.workspaces.create_workspace.repository import add_workspace_to_db


async def create_workspace(workspace_name: str) -> WorkspaceConfig:
    """
    Create a new workspace with auto-generated API key and port.
    
    Args:
        workspace_name: Name of the workspace to create
        
    Returns:
        WorkspaceConfig with the new workspace details
        
    Raises:
        ValueError: If workspace name is invalid
    """
    # Validate workspace name using shared schema rules
    try:
        WorkspaceCreate(workspace=workspace_name)
    except ValidationError as e:
        # Extract the first error message to keep it clean
        error_msg = e.errors()[0]['msg']
        # Remove "Value error, " prefix if present (Pydantic adds it)
        if error_msg.startswith('Value error, '):
            error_msg = error_msg[13:]
        raise ValueError(error_msg)

    api_key = secrets.token_urlsafe(32)
    new_port = await find_free_port(START_PORT_RANGE)
    config = WorkspaceConfig(workspace=workspace_name, api_key=api_key, port=new_port)
    await add_workspace_to_db(config)
    return config
