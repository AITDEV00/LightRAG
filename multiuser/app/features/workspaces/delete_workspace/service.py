"""
Service for delete workspace operation.
"""
import shutil

from app.config.settings import DATA_ROOT


def wipe_workspace_data(workspace: str):
    """Delete workspace data directory."""
    import os
    workspace_path = os.path.join(DATA_ROOT, workspace)
    shutil.rmtree(workspace_path, ignore_errors=True)
