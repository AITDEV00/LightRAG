"""
ASGI Multi-Tenant Dispatcher.

Replaces the subprocess-based ``process_manager.py`` with a single-process
architecture.  Each workspace gets its own FastAPI application instance
(created via the unmodified ``lightrag.api.lightrag_server.create_app``),
and requests are routed to the correct app at the ASGI level — no HTTP
proxying, no port management, no subprocesses.

Key features:
    * Lazy loading — workspace apps are created on first request
    * Idle eviction — apps unused for ``idle_timeout`` seconds are destroyed
    * Auth / auto-create — mirrors the old gateway logic exactly
    * Per-workspace logging — sets ``current_workspace`` ContextVar so
      the ``WorkspaceLogHandler`` routes logs to the correct file
"""
import os
import copy
import time
import asyncio
import secrets
from typing import Dict, Optional

from app.config.settings import DATA_ROOT, IDLE_TIMEOUT, args
from app.common.logger import current_workspace, get_workspace_log_handler
from app.features.workspaces.repository import (
    get_workspace_by_name,
    get_workspace_by_key,
)
from app.features.workspaces.schemas import WorkspaceConfig
from app.features.workspaces.create_workspace.service import create_workspace

from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.responses import JSONResponse


async def _send_error(send: Send, status: int, detail: str):
    """Send a JSON error response directly on the ASGI transport."""
    response = JSONResponse({"detail": detail}, status_code=status)
    await response({"type": "http"}, lambda: None, send)


class MultiTenantASGIRouter:
    """
    ASGI application that multiplexes requests across isolated FastAPI
    workspace apps inside a single Python process.

    Lifecycle
    ---------
    1. A request arrives with either ``X-Workspace`` (no-auth) or
       ``X-API-Key`` (auth mode) header.
    2. The dispatcher resolves the workspace via Postgres (same logic
       as the old ``gateway/service.py``).
    3. If the workspace's FastAPI app is not yet loaded, it is created
       lazily via ``create_app()``.
    4. The ASGI scope is forwarded directly to the child app — zero
       network overhead.
    5. A background loop evicts apps that haven't been accessed for
       ``idle_timeout`` seconds.
    """

    def __init__(self, admin_app: ASGIApp):
        self.admin_app = admin_app

        self.apps: Dict[str, ASGIApp] = {}
        self.last_accessed: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.idle_timeout = IDLE_TIMEOUT

        # These are filled once the event loop starts (see start_background_tasks)
        self._eviction_task: Optional[asyncio.Task] = None
        self._log_rotation_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Background tasks (started from lifespan)
    # ------------------------------------------------------------------

    def start_background_tasks(self):
        """Kick off the eviction and log-rotation loops."""
        self._eviction_task = asyncio.create_task(self._eviction_loop())
        self._log_rotation_task = asyncio.create_task(self._log_rotation_loop())

    async def _eviction_loop(self):
        """Periodically destroy workspace apps that have been idle."""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            async with self._lock:
                for ws_name in list(self.apps.keys()):
                    idle = now - self.last_accessed.get(ws_name, 0)
                    if idle > self.idle_timeout:
                        print(
                            f"💤 [Eviction] Workspace '{ws_name}' idle for "
                            f"{idle:.0f}s (>{self.idle_timeout}s). Evicting..."
                        )
                        await self._shutdown_app(ws_name)

    async def _log_rotation_loop(self):
        """Rotate per-workspace log files every 60 seconds."""
        await asyncio.sleep(30)  # Initial delay (matches old behavior)
        print(f"📝 [LogRotation] Active. Checking every 60s.")
        while True:
            handler = get_workspace_log_handler()
            if handler is not None:
                handler.rotate_logs()
            await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # App lifecycle
    # ------------------------------------------------------------------

    async def get_app(self, workspace_name: str) -> ASGIApp:
        """Return the FastAPI app for *workspace_name*, creating it if needed."""
        self.last_accessed[workspace_name] = time.time()

        if workspace_name in self.apps:
            return self.apps[workspace_name]

        async with self._lock:
            # Double-check after acquiring lock
            if workspace_name in self.apps:
                return self.apps[workspace_name]

            print(f"🚀 [Dispatcher] Loading workspace '{workspace_name}'...")

            # Pre-initialize LightRAG config to prevent it from parsing sys.argv.
            # The core library's config uses a _GlobalArgsProxy that calls
            # parse_args() on first attribute access if not already initialized.
            # parse_args() uses parser.parse_args() which dies on our custom
            # --disable-auth / --auto-create flags.
            #
            # Solution: call initialize_config() with a pre-built args namespace
            # on the very first workspace load, so the proxy never falls through
            # to parse_args().
            from lightrag.api.config import initialize_config, _initialized

            if not _initialized:
                # Build a base config by calling parse_known_args (tolerant of
                # unknown flags like --disable-auth) instead of parse_args.
                from lightrag.api.config import parse_args as _original_parse
                import argparse
                import sys

                # Temporarily swap sys.argv to only include lightrag-recognized args
                original_argv = sys.argv
                sys.argv = [sys.argv[0]]  # Strip our custom flags
                try:
                    base_args = _original_parse()
                finally:
                    sys.argv = original_argv

                initialize_config(base_args, force=True)

            # Now we can safely copy global_args
            from lightrag.api.config import global_args as lightrag_global_args
            import argparse
            
            # The global_args is a proxy, so copy.copy() just returns the proxy. 
            # We must extract the underlying dictionary to create a truly independent Namespace
            ws_args = argparse.Namespace(**vars(lightrag_global_args))
            ws_args.workspace = workspace_name

            # Per-workspace working directory (same as old process_manager)
            work_dir = os.path.join(DATA_ROOT, workspace_name)
            os.makedirs(work_dir, exist_ok=True)
            ws_args.working_dir = work_dir

            # Per-workspace input directory to prevent file upload cross-pollution
            ws_args.input_dir = os.path.join(work_dir, "inputs")
            os.makedirs(ws_args.input_dir, exist_ok=True)

            # Inject the workspace's specific API key into the child app's config
            # so it enforces auth internally and generates the correct OpenAPI spec
            if not args.disable_auth:
                config = await get_workspace_by_name(workspace_name)
                if config and config.api_key:
                    ws_args.key = config.api_key

            # Temporarily replace sys.argv to prevent child apps from parsing parent arguments
            # during any late-imports or proxy auto-initializations.
            import sys
            original_argv = sys.argv
            sys.argv = [sys.executable, "--workspace", workspace_name]

            # Temporarily override OS environment variables so that external storage providers
            # (which check os.environ directly) route to the correct workspace and ignore global .env
            env_vars_to_mock = {
                "WORKSPACE": workspace_name,
                "MILVUS_WORKSPACE": workspace_name,
                "MONGODB_WORKSPACE": workspace_name,
                "REDIS_WORKSPACE": workspace_name,
                "QDRANT_WORKSPACE": workspace_name,
                "POSTGRES_WORKSPACE": workspace_name,
                "NEO4J_WORKSPACE": workspace_name,
                "MEMGRAPH_WORKSPACE": workspace_name,
            }
            from unittest.mock import patch

            try:
                with patch.dict(os.environ, env_vars_to_mock):
                    # Create the FastAPI app using the unmodified core function
                    from lightrag.api.lightrag_server import create_app
                    app = create_app(ws_args)

                    # Trigger the FastAPI lifespan startup (initialises DB connections)
                    await self._startup_app(app)

                self.apps[workspace_name] = app
                self.last_accessed[workspace_name] = time.time()

                print(f"✅ [Dispatcher] Workspace '{workspace_name}' is ready.")
                return app
            finally:
                sys.argv = original_argv

    async def evict_app(self, workspace_name: str):
        """Explicitly evict a workspace (used by the delete-workspace admin endpoint)."""
        async with self._lock:
            await self._shutdown_app(workspace_name)

    async def _startup_app(self, app: ASGIApp):
        """Trigger the ASGI lifespan startup for a child app."""
        startup_complete = asyncio.Event()
        shutdown_event = asyncio.Event()
        startup_failed = False
        exception = None

        # Prevent child apps from destroying process-global _shared_dicts on shutdown
        os.environ["LIGHTRAG_GUNICORN_MODE"] = "true"

        startup_sent = False

        async def receive():
            nonlocal startup_sent
            if not startup_sent:
                startup_sent = True
                return {"type": "lifespan.startup"}
            # Block until shutdown is requested
            await shutdown_event.wait()
            return {"type": "lifespan.shutdown"}

        async def send(message):
            nonlocal startup_failed, exception
            if message["type"] == "lifespan.startup.complete":
                startup_complete.set()
            elif message["type"] == "lifespan.startup.failed":
                startup_failed = True
                exception = message.get("message", "Unknown startup error")
                startup_complete.set()
            # We don't strictly need to handle shutdown.complete here unless we want to wait for it

        # Start the app lifespan in a background task
        scope = {"type": "lifespan", "asgi": {"version": "3.0"}}
        lifespan_task = asyncio.create_task(app(scope, receive, send))

        # Store the task and shutdown event so we can trigger shutdown later
        if not hasattr(app, "_lifespan_task"):
            app._lifespan_task = lifespan_task  # type: ignore[attr-defined]
            app._shutdown_event = shutdown_event  # type: ignore[attr-defined]

        # Wait for startup to complete
        await startup_complete.wait()

        if startup_failed:
            raise RuntimeError(f"Workspace app startup failed: {exception}")

    async def _shutdown_app(self, workspace_name: str):
        """Trigger lifespan shutdown and remove from cache."""
        app = self.apps.pop(workspace_name, None)
        self.last_accessed.pop(workspace_name, None)

        if app is None:
            return

        # Close the per-workspace log file
        handler = get_workspace_log_handler()
        if handler is not None:
            handler.close_workspace(workspace_name)

        # Signal lifespan shutdown
        lifespan_task = getattr(app, "_lifespan_task", None)
        shutdown_event = getattr(app, "_shutdown_event", None)

        if lifespan_task is not None and not lifespan_task.done():
            if shutdown_event:
                # Trigger the receive() function to yield lifespan.shutdown
                shutdown_event.set()
                
                # Wait for the task to finish its shutdown sequence gracefully
                try:
                    await asyncio.wait_for(lifespan_task, timeout=5.0)
                except asyncio.TimeoutError:
                    print(f"⚠️ [Dispatcher] Workspace '{workspace_name}' lifespan shutdown timed out. Cancelling...")
                    lifespan_task.cancel()
                    try:
                        await lifespan_task
                    except asyncio.CancelledError:
                        pass
            else:
                # Fallback if no event was found
                lifespan_task.cancel()
                try:
                    await lifespan_task
                except asyncio.CancelledError:
                    pass

        print(f"🛑 [Dispatcher] Workspace '{workspace_name}' evicted.")

    async def shutdown_all(self):
        """Gracefully shut down all loaded workspace apps."""
        async with self._lock:
            for ws_name in list(self.apps.keys()):
                await self._shutdown_app(ws_name)

        # Cancel background tasks
        if self._eviction_task and not self._eviction_task.done():
            self._eviction_task.cancel()
        if self._log_rotation_task and not self._log_rotation_task.done():
            self._log_rotation_task.cancel()

    # ------------------------------------------------------------------
    # Request routing (auth + workspace resolution)
    # ------------------------------------------------------------------

    async def _resolve_workspace(self, scope: Scope) -> Optional[str]:
        """
        Resolve the target workspace name from request headers.
        Mirrors the old ``gateway/service.py`` logic exactly.

        Returns the workspace name, or ``None`` if resolution fails
        (the caller will send an appropriate error).
        """
        headers = dict(scope.get("headers", []))

        if args.disable_auth:
            # --- No-auth mode: use X-Workspace header ---
            workspace_name = (
                headers.get(b"x-workspace", b"").decode("utf-8").strip()
            )
            if not workspace_name:
                return None  # Missing header

            # Check in-memory cache first
            if workspace_name in self.apps:
                return workspace_name

            # Check Postgres
            config = await get_workspace_by_name(workspace_name)
            if config:
                return config.workspace

            # Auto-create if enabled
            if args.auto_create:
                try:
                    print(f"✨ [Dispatcher] Auto-creating workspace: {workspace_name}")
                    await create_workspace(workspace_name)
                    return workspace_name
                except ValueError as e:
                    print(f"⚠️ [Dispatcher] Failed to auto-create workspace: {e}")
                    return None

            return None  # Not found and auto-create is off
        else:
            # --- Auth mode: use X-API-Key header ---
            api_key = headers.get(b"x-api-key", b"").decode("utf-8").strip()
            if not api_key:
                return None

            # Check in-memory key cache
            if not hasattr(self, "_key_cache"):
                self._key_cache = {}
            
            if api_key in self._key_cache:
                workspace_name = self._key_cache[api_key]
                if workspace_name in self.apps:
                    return workspace_name

            # Check Postgres
            config = await get_workspace_by_key(api_key)
            if config:
                if not hasattr(self, "_key_cache"):
                    self._key_cache = {}
                self._key_cache[api_key] = config.workspace
                return config.workspace

            return None  # Invalid key

    # ------------------------------------------------------------------
    # ASGI entry point
    # ------------------------------------------------------------------

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        """Main ASGI dispatch."""
        if scope["type"] == "lifespan":
            # Lifespan events are for the outer admin_app
            await self.admin_app(scope, receive, send)
            return

        if scope["type"] not in ("http", "websocket"):
            return

        # Strip root_path globally if Uvicorn hasn't done it
        path = scope.get("path", "")
        if args.root_path and path.startswith(args.root_path):
            scope = dict(scope)
            scope["path"] = path[len(args.root_path):]
            scope["root_path"] = args.root_path
            path = scope["path"]

        # Let admin routes go to the admin FastAPI app
        if path.startswith("/admin"):
            await self.admin_app(scope, receive, send)
            return

        # Resolve workspace
        workspace_name = await self._resolve_workspace(scope)
        if workspace_name is None:
            if args.disable_auth:
                response = JSONResponse(
                    {"detail": "Missing or invalid X-Workspace header"},
                    status_code=400,
                )
            else:
                response = JSONResponse(
                    {"detail": "Invalid Credentials"},
                    status_code=401,
                )
            await response(scope, receive, send)
            return

        # Set logging context
        token = current_workspace.set(workspace_name)
        try:
            # Get or create the workspace app
            app = await self.get_app(workspace_name)

            # Rebuild headers to inject the workspace name and optionally the child API key
            new_headers = []
            for k, v in scope.get("headers", []):
                # Strip out the parent auth key and any existing workspace header to prevent spoofing
                if k not in (b"x-api-key", b"lightrag-workspace", b"x-workspace"):
                    new_headers.append((k, v))
            
            # Inject the resolved workspace name so LightRAG's get_workspace() picks it up
            new_headers.append((b"lightrag-workspace", workspace_name.encode("utf-8")))

            # Inject the child API key if auth mode is enabled
            if not args.disable_auth:
                config = await get_workspace_by_name(workspace_name)
                if config and config.api_key:
                    new_headers.append((b"x-api-key", config.api_key.encode("utf-8")))

            # Update the scope with the new headers
            scope = dict(scope)
            scope["headers"] = new_headers

            # Forward directly to the child FastAPI app
            await app(scope, receive, send)
        finally:
            current_workspace.reset(token)
