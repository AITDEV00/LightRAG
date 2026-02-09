"""
Main application entry point.
Wires up all feature routers and lifecycle management.
"""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.config.settings import args
from app.common.db import init_db, close_db
from app.common.process_manager import manager, watchdog_loop, log_rotation_loop

# Import feature routers
from app.features.workspaces.create_workspace.endpoint import router as create_workspace_router
from app.features.workspaces.delete_workspace.endpoint import router as delete_workspace_router
from app.features.workspaces.list_workspaces.endpoint import router as list_workspaces_router
from app.features.gateway.endpoint import router as gateway_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager."""
    await init_db()
    # Start loading in background
    startup_task = asyncio.create_task(manager.launch_startup_sequence())
    watchdog_task = asyncio.create_task(watchdog_loop())
    log_rotation_task = asyncio.create_task(log_rotation_loop())
    yield
    manager.running = False
    startup_task.cancel()
    watchdog_task.cancel()
    log_rotation_task.cancel()
    manager.stop_all()
    await close_db()



# Create FastAPI app
app = FastAPI(
    lifespan=lifespan,
    title="LightRAG Orchestrator",
    root_path=args.root_path,
    docs_url="/admin/docs",
    redoc_url="/admin/redoc",
    openapi_url="/admin/openapi.json"
)

# Include admin routers first (more specific paths)
app.include_router(create_workspace_router)
app.include_router(delete_workspace_router)
app.include_router(list_workspaces_router)

# Include gateway router last (catch-all)
app.include_router(gateway_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
