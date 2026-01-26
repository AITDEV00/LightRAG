"""
Shared schemas for workspace feature.
"""
import re
from pydantic import BaseModel, field_validator


class WorkspaceConfig(BaseModel):
    """Configuration for a workspace."""
    workspace: str
    api_key: str
    port: int


class WorkspaceCreate(BaseModel):
    """Request schema for creating a workspace."""
    workspace: str

    @field_validator('workspace')
    def validate_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
            raise ValueError('Workspace name must contain only a-z, A-Z, 0-9, and _')
        return v


class WorkspaceResponse(BaseModel):
    """Response schema for workspace operations."""
    workspace: str
    port: int
    api_key: str
