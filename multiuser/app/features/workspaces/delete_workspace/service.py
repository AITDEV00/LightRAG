"""
Service for delete workspace operation.
"""
import shutil
import httpx

from app.config.settings import DATA_ROOT, args
from app.features.workspaces.schemas import WorkspaceConfig


async def cleanup_workspace_documents(config: WorkspaceConfig) -> bool:
    """
    Attempt to clear documents via the workspace API before deletion.
    
    Args:
        config: Workspace configuration
        
    Returns:
        True if cleanup succeeded, False otherwise
    """
    try:
        print(f"🧹 [Delete] Triggering API cleanup for {config.workspace}...")
        async with httpx.AsyncClient() as cleanup_client:
            headers = {}
            if not args.disable_auth and config.api_key:
                headers["X-API-Key"] = config.api_key
            
            url = f"http://127.0.0.1:{config.port}/documents"
            resp = await cleanup_client.delete(url, headers=headers, timeout=10.0)
            if resp.status_code == 200:
                print(f"✅ [Delete] Documents cleared for {config.workspace}")
                return True
            else:
                print(f"⚠️ [Delete] Failed to clear documents for {config.workspace}: {resp.text}")
                return False
    except Exception as e:
        print(f"⚠️ [Delete] Error calling clear_documents for {config.workspace}: {e}")
        return False


def wipe_workspace_data(workspace: str):
    """Delete workspace data directory."""
    import os
    workspace_path = os.path.join(DATA_ROOT, workspace)
    shutil.rmtree(workspace_path, ignore_errors=True)
