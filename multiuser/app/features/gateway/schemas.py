"""
Schemas for gateway feature.
"""
from dataclasses import dataclass


@dataclass
class RoutingResult:
    """Result of workspace routing."""
    workspace: str
    port: int
    api_key: str
    was_started_just_now: bool = False
