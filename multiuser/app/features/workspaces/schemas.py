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
            raise ValueError(
                "Invalid workspace name. "
                "Rule: Name must contain only alphanumeric characters (A-Z, a-z, 0-9) and underscores (_). "
                "Accepted examples: 'my_project', 'research_2024', 'dev_env'."
            )
        return v


class WorkspaceResponse(BaseModel):
    """Response schema for workspace operations."""
    workspace: str
    port: int
    api_key: str
