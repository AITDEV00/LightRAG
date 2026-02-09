"""
Repository for list workspaces operation.
Re-exports get_all_workspaces from shared repository.
"""
from app.features.workspaces.repository import get_all_workspaces

__all__ = ["get_all_workspaces"]
