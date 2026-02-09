"""
Service for gateway proxy operations.
Handles routing logic and health checks.
"""
import asyncio
import httpx

from app.config.settings import args
from app.common.process_manager import manager
from app.features.workspaces.schemas import WorkspaceConfig
from app.features.workspaces.repository import get_workspace_by_name, get_workspace_by_key
from app.features.workspaces.create_workspace.service import create_workspace
from app.features.gateway.schemas import RoutingResult


async def wait_for_health(port: int, timeout: float = 15.0):
    """
    Polls the service with an actual HTTP request until it responds.
    This guarantees the app is fully loaded, not just that the port is bound.
    """
    url = f"http://127.0.0.1:{port}/"
    start_time = asyncio.get_event_loop().time()
    
    async with httpx.AsyncClient() as health_client:
        while True:
            try:
                await health_client.get(url, timeout=0.5)
                return True
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
                if asyncio.get_event_loop().time() - start_time > timeout:
                    raise Exception(f"Timeout waiting for health check on port {port}")
                await asyncio.sleep(0.2)
            except Exception:
                # Any other error (like a 500 from the app) means it's reachable
                return True


async def resolve_workspace_no_auth(workspace_name: str) -> RoutingResult:
    """
    Resolve workspace in no-auth mode using X-Workspace header.
    
    Args:
        workspace_name: Name of the workspace from X-Workspace header
        
    Returns:
        RoutingResult with workspace details
        
    Raises:
        ValueError: If workspace name is invalid
        LookupError: If workspace not found and auto-create is disabled
    """
    was_started_just_now = False
    
    # Check in-memory cache first
    if workspace_name in manager.configs:
        config = manager.configs[workspace_name]
        return RoutingResult(
            workspace=config.workspace,
            port=config.port,
            api_key=config.api_key
        )
    
    # Check database
    config = await get_workspace_by_name(workspace_name)
    if config:
        # Start process if not running
        if config.workspace not in manager.processes:
            await manager.start_process(config)
            was_started_just_now = True
        # Get potentially updated config (port may have changed)
        actual_config = manager.configs.get(workspace_name, config)
        return RoutingResult(
            workspace=actual_config.workspace,
            port=actual_config.port,
            api_key=actual_config.api_key,
            was_started_just_now=was_started_just_now
        )
    
    # Auto-create if enabled
    if args.auto_create:
        print(f"✨ Auto-creating workspace: {workspace_name}")
        config = await create_workspace(workspace_name)
        await manager.start_process(config)
        # Get potentially updated config (port may have changed)
        actual_config = manager.configs.get(workspace_name, config)
        return RoutingResult(
            workspace=actual_config.workspace,
            port=actual_config.port,
            api_key=actual_config.api_key,
            was_started_just_now=True
        )
    
    raise LookupError(f"Workspace '{workspace_name}' not found")


async def resolve_workspace_with_auth(api_key: str) -> RoutingResult:
    """
    Resolve workspace in auth mode using X-API-Key header.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        RoutingResult with workspace details
        
    Raises:
        LookupError: If API key is invalid
    """
    was_started_just_now = False
    
    # Check in-memory cache first
    for ws in manager.configs.values():
        if ws.api_key == api_key:
            return RoutingResult(
                workspace=ws.workspace,
                port=ws.port,
                api_key=ws.api_key
            )
    
    # Check database
    config = await get_workspace_by_key(api_key)
    if config:
        # Start process if not running
        if config.workspace not in manager.processes:
            await manager.start_process(config)
            was_started_just_now = True
        # Get potentially updated config (port may have changed)
        actual_config = manager.configs.get(config.workspace, config)
        return RoutingResult(
            workspace=actual_config.workspace,
            port=actual_config.port,
            api_key=actual_config.api_key,
            was_started_just_now=was_started_just_now
        )
    
    raise LookupError("Invalid credentials")
