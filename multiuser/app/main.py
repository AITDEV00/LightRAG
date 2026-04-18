"""
Main application entry point.
Wires up admin routers on a FastAPI app, then wraps everything
with the ASGI multi-tenant dispatcher for workspace routing.
"""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.config.settings import args
from app.common.db import init_db, close_db
from app.common.logger import setup_workspace_logging
from app.common.asgi_dispatcher import MultiTenantASGIRouter
from lightrag.kg.shared_storage import finalize_share_data

# Import feature routers (admin only — gateway is replaced by ASGI dispatcher)
from app.features.workspaces.create_workspace.endpoint import router as create_workspace_router
from app.features.workspaces.delete_workspace.endpoint import router as delete_workspace_router
from app.features.workspaces.list_workspaces.endpoint import router as list_workspaces_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    await init_db()
    setup_workspace_logging()

    # Start dispatcher background tasks (eviction loop, log rotation)
    dispatcher.start_background_tasks()

    print("✅ [Startup] ASGI Multi-Tenant Dispatcher is ready.")
    yield

    # Graceful shutdown
    print("⏳ [Shutdown] Cleaning up workspace apps and shared resources...")
    await dispatcher.shutdown_all()
    await close_db()
    finalize_share_data()
    print("✅ [Shutdown] Orchestrator cleanup complete.")


# Create the admin FastAPI app (handles /admin/* routes)
admin_app = FastAPI(
    lifespan=lifespan,
    title="LightRAG Orchestrator",
    root_path=args.root_path,
    docs_url="/admin/docs",
    redoc_url="/admin/redoc",
    openapi_url="/admin/openapi.json"
)

# Include admin routers
admin_app.include_router(create_workspace_router)
admin_app.include_router(delete_workspace_router)
admin_app.include_router(list_workspaces_router)

# Create the ASGI dispatcher that wraps admin_app and handles workspace routing
dispatcher = MultiTenantASGIRouter(admin_app=admin_app)

# Store dispatcher on the admin app state so admin endpoints can access it
admin_app.state.dispatcher = dispatcher

# The top-level ASGI app that uvicorn will run
app = dispatcher


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
