"""
Service for create workspace operation.
"""
import secrets
import socket

from pydantic import ValidationError
from app.config.settings import START_PORT_RANGE
from app.features.workspaces.schemas import WorkspaceConfig, WorkspaceCreate
from app.features.workspaces.create_workspace.repository import add_workspace_to_db
from app.features.workspaces.repository import get_all_workspaces


async def _find_next_port(start_port: int = START_PORT_RANGE) -> int:
    """
    Assign a distinct port number for backward compatibility.

    The port is no longer used for subprocess binding, but we keep it
    as a unique identifier in the DB so that the schema stays intact
    and a rollback to the subprocess model is possible.
    """
    workspaces = await get_all_workspaces()
    used_ports = {w.port for w in workspaces}
    port = start_port
    while port < 65535:
        if port not in used_ports:
            return port
        port += 1
    raise RuntimeError("No free port numbers available for assignment.")


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
    new_port = await _find_next_port(START_PORT_RANGE)
    config = WorkspaceConfig(workspace=workspace_name, api_key=api_key, port=new_port)
    await add_workspace_to_db(config)
    return config
