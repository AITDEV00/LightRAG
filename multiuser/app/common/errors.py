"""
Shared exceptions for the application.
"""


class WorkspaceNotFoundError(Exception):
    """Raised when a workspace cannot be found."""
    pass


class WorkspaceExistsError(Exception):
    """Raised when trying to create a workspace that already exists."""
    pass


class InvalidWorkspaceNameError(Exception):
    """Raised when workspace name is invalid."""
    pass


class DatabaseNotInitializedError(Exception):
    """Raised when database pool is not initialized."""
    pass
