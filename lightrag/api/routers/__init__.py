"""
This module contains all the routers for the LightRAG API.

NOTE: Routers are no longer exported as module-level singletons.
In multi-tenant ASGI mode, each tenant needs its own router instance.
Use the create_*_routes() factory functions instead.
"""

from .document_routes import create_document_routes
from .query_routes import create_query_routes
from .graph_routes import create_graph_routes
from .er_routes import create_er_routes
from .ollama_api import OllamaAPI

__all__ = [
    "create_document_routes",
    "create_query_routes",
    "create_graph_routes",
    "create_er_routes",
    "OllamaAPI",
]
