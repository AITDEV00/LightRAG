import os
import sys
import sqlite3
import asyncio
import subprocess
import shutil
import httpx
import socket
import secrets
import re
import argparse
import time
import signal
from typing import Dict, List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response, Header
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, field_validator
from starlette.background import BackgroundTask
from dotenv import load_dotenv

# --- CLI ARGUMENT PARSING ---
parser = argparse.ArgumentParser(description="LightRAG Enterprise Manager")
parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind the manager to")
parser.add_argument("--port", type=int, default=8000, help="Port to bind the manager to")
parser.add_argument("--disable-auth", action="store_true", help="Disable API Key auth (Plug-and-Play mode)")
parser.add_argument("--auto-create", action="store_true", help="Automatically create workspaces if they don't exist")
args = parser.parse_args()

# --- CONFIGURATION ---
load_dotenv()

DB_FILE = "lightrag_workspaces.db"
DATA_ROOT = os.path.abspath("./data")
LOG_ROOT = os.path.abspath("./logs")
ADMIN_SECRET = "admin-secret-123" 
START_PORT_RANGE = 9000
STARTUP_GRACE_PERIOD = 60
STARTUP_STAGGER = 0.1

os.makedirs(DATA_ROOT, exist_ok=True)
os.makedirs(LOG_ROOT, exist_ok=True)

print(f"🔧 Configuration: Host={args.host}, Port={args.port}")
print(f"🔓 Auth Disabled: {args.disable_auth}")
print(f"✨ Auto-Create Workspaces: {args.auto_create}")
print(f"⏱️ Startup Stagger: {STARTUP_STAGGER}s")

# --- HELPER: KILL ZOMBIES ON PORT ---
def kill_process_on_port(port: int):
    """Finds and kills any process listening on the given port."""
    try:
        result = subprocess.check_output(f"lsof -t -i:{port}", shell=True, stderr=subprocess.DEVNULL)
        pids = result.decode().strip().split('\n')
        for pid in pids:
            if pid:
                print(f"🧹 [Cleanup] Killing zombie process {pid} on port {port}...")
                os.kill(int(pid), signal.SIGKILL)
    except subprocess.CalledProcessError:
        pass
    except Exception as e:
        print(f"⚠️ [Cleanup] Failed to cleanup port {port}: {e}")

# --- DATA MODELS ---
class WorkspaceConfig(BaseModel):
    workspace: str
    api_key: str
    port: int

class WorkspaceCreate(BaseModel):
    workspace: str

    @field_validator('workspace')
    def validate_name(cls, v):
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
            raise ValueError('Workspace name must contain only a-z, A-Z, 0-9, and _')
        return v

# --- DATABASE LAYER (SQLite) ---
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workspaces (
                workspace TEXT PRIMARY KEY,
                api_key TEXT UNIQUE NOT NULL,
                port INTEGER UNIQUE NOT NULL
            )
        """)
        conn.commit()

def get_all_workspaces() -> List[WorkspaceConfig]:
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM workspaces")
        return [WorkspaceConfig(**dict(row)) for row in cursor.fetchall()]

def get_workspace_by_key(api_key: str) -> Optional[WorkspaceConfig]:
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM workspaces WHERE api_key = ?", (api_key,))
        row = cursor.fetchone()
        return WorkspaceConfig(**dict(row)) if row else None

def get_workspace_by_name(name: str) -> Optional[WorkspaceConfig]:
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM workspaces WHERE workspace = ?", (name,))
        row = cursor.fetchone()
        return WorkspaceConfig(**dict(row)) if row else None

def add_workspace_to_db(config: WorkspaceConfig):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO workspaces (workspace, api_key, port) VALUES (?, ?, ?)",
            (config.workspace, config.api_key, config.port)
        )
        conn.commit()

def remove_workspace_from_db(workspace: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM workspaces WHERE workspace = ?", (workspace,))
        conn.commit()

def find_free_port(start_port: int = START_PORT_RANGE) -> int:
    workspaces = get_all_workspaces()
    reserved_ports = {w.port for w in workspaces}
    port = start_port
    while port < 65535:
        if port in reserved_ports:
            port += 1
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    raise RuntimeError("No free ports available.")

# --- PROCESS MANAGER ---
class LightRAGManager:
    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {} 
        self.configs: Dict[str, WorkspaceConfig] = {}
        self.start_times: Dict[str, float] = {}
        self.log_files: Dict[str, any] = {}
        self.running = True

    async def launch_startup_sequence(self):
        """Loads users from DB and starts them sequentially with staggering."""
        workspaces = get_all_workspaces()
        print(f"📂 [DB] Found {len(workspaces)} workspaces. Starting staggered launch sequence...")
        
        for i, ws in enumerate(workspaces):
            if not self.running: break
            
            self.configs[ws.workspace] = ws
            print(f"⏳ [Startup] Launching {i+1}/{len(workspaces)}: '{ws.workspace}'...")
            self.start_process(ws)
            
            if i < len(workspaces) - 1: 
                print(f"zzz [Startup] Cooling down for {STARTUP_STAGGER}s before next launch...")
                await asyncio.sleep(STARTUP_STAGGER)
        
        print("✅ [Startup] Sequence complete.")

    def start_process(self, config: WorkspaceConfig):
        if config.workspace in self.processes and self.processes[config.workspace].poll() is None:
            return 

        # 1. CLEANUP: Ensure port is free
        kill_process_on_port(config.port)

        work_dir = os.path.join(DATA_ROOT, config.workspace)
        os.makedirs(work_dir, exist_ok=True)

        env = os.environ.copy()
        env["WORKSPACE"] = config.workspace
        env["WORKING_DIR"] = work_dir
        
        # 2. AUTH LOGIC
        if not args.disable_auth and config.api_key:
            env["LIGHTRAG_API_KEY"] = config.api_key

        print(f"🚀 [Manager] Spawning '{config.workspace}' on port {config.port}...")
        
        cmd = ["lightrag-server", "--port", str(config.port), "--host", "127.0.0.1"]
        
        try:
            log_path = os.path.join(LOG_ROOT, f"{config.workspace}.log")
            log_file = open(log_path, "a")
            
            proc = subprocess.Popen(
                cmd, 
                env=env, 
                stdout=log_file, 
                stderr=subprocess.STDOUT
            )
            
            self.processes[config.workspace] = proc
            self.configs[config.workspace] = config
            self.start_times[config.workspace] = time.time()
            self.log_files[config.workspace] = log_file
            
        except Exception as e:
            print(f"❌ [Manager] Failed to start {config.workspace}: {e}")

    def stop_process(self, workspace: str):
        if workspace in self.processes:
            print(f"🛑 [Manager] Stopping {workspace}...")
            proc = self.processes[workspace]
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
            del self.processes[workspace]
            
            if workspace in self.log_files:
                self.log_files[workspace].close()
                del self.log_files[workspace]
                
            if workspace in self.configs:
                del self.configs[workspace]
            if workspace in self.start_times:
                del self.start_times[workspace]

    def stop_all(self):
        for ws in list(self.processes.keys()):
            self.stop_process(ws)

    async def check_health(self):
        # Iterate over list to allow modification during iteration if needed (though we modify via methods)
        for ws_name, proc in list(self.processes.items()):
            
            # 1. CHECK: Process Life (Is it dead?)
            exit_code = proc.poll()
            if exit_code is not None:
                print(f"⚠️ [Watchdog] '{ws_name}' crashed (Code: {exit_code}). Restarting...")
                
                # Check uptime to prevent rapid loops
                uptime = time.time() - self.start_times.get(ws_name, 0)
                
                # Fetch config NOW because stop_process will delete it from memory
                config = get_workspace_by_name(ws_name)
                
                # Clean up dead handles
                self.stop_process(ws_name)
                
                if uptime < 5:
                    print(f"zzz [Watchdog] '{ws_name}' died too fast. Pausing restart for 5s...")
                    await asyncio.sleep(5)

                # FIX: Restart using DB config
                if config:
                    self.start_process(config)
                else:
                    print(f"❌ [Watchdog] Cannot restart '{ws_name}': Config missing from DB.")
                continue

            # 2. CHECK: Deep Health (Is it responsive?)
            # Only run deep check if it's "alive" in process list
            config = self.configs.get(ws_name)
            if not config: continue
            
            uptime = time.time() - self.start_times.get(ws_name, 0)
            in_grace_period = uptime < STARTUP_GRACE_PERIOD

            try:
                async with httpx.AsyncClient() as client:
                    headers = {}
                    # Send key regardless of disable_auth mode, IF it exists in config
                    if config.api_key:
                        headers["X-API-Key"] = config.api_key
                        
                    resp = await client.get(f"http://127.0.0.1:{config.port}/health", headers=headers, timeout=3.0)
                    if resp.status_code != 200: raise Exception(f"Bad Status {resp.status_code}")
            except Exception as e:
                if in_grace_period:
                    pass # Still booting, ignore HTTP failures
                else:
                    print(f"🧟 [Watchdog] '{ws_name}' unresponsive: {e}. Restarting...")
                    self.stop_process(ws_name) 
                    self.start_process(config)

    def create_new_workspace(self, workspace_name: str) -> WorkspaceConfig:
        if not re.match(r'^[a-zA-Z0-9_]+$', workspace_name):
            raise ValueError('Invalid workspace name')
            
        api_key = secrets.token_urlsafe(32)
        new_port = find_free_port(START_PORT_RANGE)
        config = WorkspaceConfig(workspace=workspace_name, api_key=api_key, port=new_port)
        add_workspace_to_db(config)
        self.start_process(config)
        return config

manager = LightRAGManager()

async def watchdog_loop():
    await asyncio.sleep(10) 
    print(f"👀 [Watchdog] Active.")
    while manager.running:
        await manager.check_health()
        await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Start loading in background
    startup_task = asyncio.create_task(manager.launch_startup_sequence())
    watchdog_task = asyncio.create_task(watchdog_loop())
    yield
    manager.running = False
    startup_task.cancel()
    watchdog_task.cancel()
    manager.stop_all()

# --- APP SETUP ---
app = FastAPI(
    lifespan=lifespan, 
    title="LightRAG Orchestrator",
    docs_url="/admin/docs",
    redoc_url="/admin/redoc",
    openapi_url="/admin/openapi.json"
)
client = httpx.AsyncClient()

# --- ADMIN API ---

@app.post("/admin/workspaces")
def create_workspace(data: WorkspaceCreate, x_admin_key: str = Header(None, alias="X-Admin-Key")):
    if x_admin_key != ADMIN_SECRET: raise HTTPException(403, "Invalid Key")
    
    try:
        config = manager.create_new_workspace(data.workspace)
        return {"status": "created", "workspace": config.workspace, "api_key": config.api_key}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Workspace already exists")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/admin/workspaces/{workspace}")
def delete_workspace(workspace: str, wipe_data: bool = False, x_admin_key: str = Header(None, alias="X-Admin-Key")):
    if x_admin_key != ADMIN_SECRET: raise HTTPException(403, "Invalid Key")
    
    manager.stop_process(workspace)
    remove_workspace_from_db(workspace)
    if wipe_data:
        shutil.rmtree(os.path.join(DATA_ROOT, workspace), ignore_errors=True)
    return {"status": "deleted"}

@app.get("/admin/workspaces")
def list_workspaces(x_admin_key: str = Header(None, alias="X-Admin-Key")):
    if x_admin_key != ADMIN_SECRET: raise HTTPException(403, "Invalid Key")
    workspaces = get_all_workspaces()
    return [{"workspace": w.workspace, "port": w.port, "api_key": w.api_key} for w in workspaces]

# --- MAIN GATEWAY ---

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def gateway(path: str, request: Request):
    if path.startswith("admin/"):
        return Response(status_code=404)

    target_workspace = None
    target_port = None
    child_api_key = None

    # --- ROUTING LOGIC ---
    if args.disable_auth:
        workspace_name = request.headers.get("X-Workspace")
        if not workspace_name: raise HTTPException(400, "Missing X-Workspace header")
            
        if workspace_name in manager.configs:
            config = manager.configs[workspace_name]
            target_workspace = config.workspace
            target_port = config.port
            child_api_key = config.api_key
        else:
            config = get_workspace_by_name(workspace_name)
            if config:
                target_workspace = config.workspace
                target_port = config.port
                child_api_key = config.api_key
                if config.workspace not in manager.processes:
                    manager.start_process(config)
            elif args.auto_create:
                try:
                    print(f"✨ Auto-creating workspace: {workspace_name}")
                    config = manager.create_new_workspace(workspace_name)
                    target_workspace = config.workspace
                    target_port = config.port
                    child_api_key = config.api_key
                    await asyncio.sleep(1) 
                except ValueError:
                    raise HTTPException(400, "Invalid workspace name")
                except Exception as e:
                    raise HTTPException(500, f"Failed to auto-create: {e}")
            else:
                raise HTTPException(404, f"Workspace '{workspace_name}' not found")
    else:
        api_key = request.headers.get("X-API-Key")
        if not api_key: raise HTTPException(401, "Missing X-API-Key header")

        for ws in manager.configs.values():
            if ws.api_key == api_key:
                target_workspace = ws.workspace
                target_port = ws.port
                child_api_key = ws.api_key
                break
        
        if not target_workspace:
            config = get_workspace_by_key(api_key)
            if config:
                target_workspace = config.workspace
                target_port = config.port
                child_api_key = config.api_key
                if config.workspace not in manager.processes:
                    manager.start_process(config)

    if not target_workspace or not target_port:
        raise HTTPException(401, "Invalid Credentials" if not args.disable_auth else "Workspace not found")

    # --- PROXY LOGIC ---
    url = f"http://127.0.0.1:{target_port}/{path}"

    try:
        headers = dict(request.headers)
        # Inject key ONLY if in auth mode
        if not args.disable_auth and child_api_key:
            headers["X-API-Key"] = child_api_key

        rp_req = client.build_request(
            request.method,
            url,
            headers=headers,
            content=await request.body(),
            params=request.query_params
        )
        
        rp_resp = await client.send(rp_req, stream=True)
        
        excluded_headers = {"content-length", "content-encoding", "transfer-encoding", "connection"}
        resp_headers = {k: v for k, v in rp_resp.headers.items() if k.lower() not in excluded_headers}

        return StreamingResponse(
            rp_resp.aiter_raw(),
            status_code=rp_resp.status_code,
            headers=resp_headers,
            background=BackgroundTask(rp_resp.aclose)
        )
    except httpx.ConnectError:
        raise HTTPException(502, f"Workspace '{target_workspace}' is starting up...")
    except Exception as e:
        print(f"Gateway Error: {e}")
        raise HTTPException(500, "Gateway Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)