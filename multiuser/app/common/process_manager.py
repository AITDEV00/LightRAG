"""
Process Manager for spawning and monitoring LightRAG worker processes.
"""
import os
import subprocess
import signal
import time
import asyncio
import socket
import secrets
import re
import httpx
from datetime import datetime
from typing import Dict, Optional

from app.config.settings import (
    DATA_ROOT,
    LOG_ROOT,
    START_PORT_RANGE,
    STARTUP_GRACE_PERIOD,
    STARTUP_STAGGER,
    args,
)
from app.features.workspaces.schemas import WorkspaceConfig
from app.features.workspaces.repository import get_all_workspaces, get_workspace_by_name


# Log rotation interval in minutes
LOG_ROTATION_INTERVAL = 5


def get_log_timestamp() -> datetime:
    """Round current time to nearest LOG_ROTATION_INTERVAL-minute interval."""
    now = datetime.now()
    minutes = (now.minute // LOG_ROTATION_INTERVAL) * LOG_ROTATION_INTERVAL
    return now.replace(minute=minutes, second=0, microsecond=0)


def get_log_path(workspace: str) -> str:
    """Generate timestamped log path for a workspace."""
    ts = get_log_timestamp()
    workspace_log_dir = os.path.join(LOG_ROOT, workspace)
    os.makedirs(workspace_log_dir, exist_ok=True)
    filename = f"{workspace}_{ts.strftime('%Y-%m-%d_%H-%M')}.log"
    return os.path.join(workspace_log_dir, filename)


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


async def find_free_port(start_port: int = START_PORT_RANGE) -> int:
    """Find a free port starting from the given port."""
    workspaces = await get_all_workspaces()
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


class LightRAGManager:
    """Manages LightRAG worker processes."""
    
    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self.configs: Dict[str, WorkspaceConfig] = {}
        self.start_times: Dict[str, float] = {}
        self.log_files: Dict[str, any] = {}
        self.log_paths: Dict[str, str] = {}  # Track current log path per workspace
        self.running = True

    async def launch_startup_sequence(self):
        """Loads users from DB and starts them sequentially with staggering."""
        workspaces = await get_all_workspaces()
        print(f"📂 [DB] Found {len(workspaces)} workspaces. Starting staggered launch sequence...")
        
        for i, ws in enumerate(workspaces):
            if not self.running:
                break
            
            self.configs[ws.workspace] = ws
            print(f"⏳ [Startup] Launching {i+1}/{len(workspaces)}: '{ws.workspace}'...")
            self.start_process(ws)
            
            if i < len(workspaces) - 1:
                print(f"zzz [Startup] Cooling down for {STARTUP_STAGGER}s before next launch...")
                await asyncio.sleep(STARTUP_STAGGER)
        
        print("✅ [Startup] Sequence complete.")

    def start_process(self, config: WorkspaceConfig):
        """Start a LightRAG worker process for the given workspace."""
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
            log_path = get_log_path(config.workspace)
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
            self.log_paths[config.workspace] = log_path
            
            print(f"📝 [Logs] {config.workspace} -> {log_path}")
            
        except Exception as e:
            print(f"❌ [Manager] Failed to start {config.workspace}: {e}")

    def rotate_logs(self):
        """Rotate log files for all running processes to new timestamped files."""
        for workspace in list(self.processes.keys()):
            proc = self.processes.get(workspace)
            if proc is None or proc.poll() is not None:
                continue  # Process not running
            
            new_log_path = get_log_path(workspace)
            current_log_path = self.log_paths.get(workspace, "")
            
            # Only rotate if path has changed (new 5-min interval)
            if new_log_path != current_log_path:
                print(f"🔄 [Logs] Rotating {workspace} -> {new_log_path}")
                
                try:
                    old_log_file = self.log_files.get(workspace)
                    if old_log_file is None:
                        continue
                    
                    # Get the fd that subprocess is writing to
                    old_fd = old_log_file.fileno()
                    
                    # Open new log file
                    new_log_file = open(new_log_path, "a")
                    
                    # Redirect: make old fd point to new file
                    # This changes where subprocess writes without restarting it
                    os.dup2(new_log_file.fileno(), old_fd)
                    
                    # Close the new file handle (fd is now duplicated to old_fd)
                    new_log_file.close()
                    
                    # Update tracking (old_log_file handle now writes to new file)
                    self.log_paths[workspace] = new_log_path
                    
                except Exception as e:
                    print(f"⚠️ [Logs] Failed to rotate {workspace}: {e}")

    def stop_process(self, workspace: str):
        """Stop a LightRAG worker process."""
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
            
            if workspace in self.log_paths:
                del self.log_paths[workspace]
                
            if workspace in self.configs:
                del self.configs[workspace]
            if workspace in self.start_times:
                del self.start_times[workspace]

    def stop_all(self):
        """Stop all worker processes."""
        for ws in list(self.processes.keys()):
            self.stop_process(ws)

    async def check_health(self):
        """Check health of all worker processes and restart if needed."""
        for ws_name, proc in list(self.processes.items()):
            
            # 1. CHECK: Process Life (Is it dead?)
            exit_code = proc.poll()
            if exit_code is not None:
                print(f"⚠️ [Watchdog] '{ws_name}' crashed (Code: {exit_code}). Restarting...")
                
                # Check uptime to prevent rapid loops
                uptime = time.time() - self.start_times.get(ws_name, 0)
                
                # Fetch config NOW because stop_process will delete it from memory
                config = await get_workspace_by_name(ws_name)
                
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
            config = self.configs.get(ws_name)
            if not config:
                continue
            
            uptime = time.time() - self.start_times.get(ws_name, 0)
            in_grace_period = uptime < STARTUP_GRACE_PERIOD

            try:
                async with httpx.AsyncClient() as client:
                    headers = {}
                    # Send key regardless of disable_auth mode, IF it exists in config
                    if config.api_key:
                        headers["X-API-Key"] = config.api_key
                        
                    resp = await client.get(f"http://127.0.0.1:{config.port}/health", headers=headers, timeout=3.0)
                    if resp.status_code != 200:
                        raise Exception(f"Bad Status {resp.status_code}")
            except Exception as e:
                if in_grace_period:
                    pass  # Still booting, ignore HTTP failures
                else:
                    print(f"🧟 [Watchdog] '{ws_name}' unresponsive: {e}. Restarting...")
                    self.stop_process(ws_name)
                    self.start_process(config)

    async def create_new_workspace(self, workspace_name: str) -> WorkspaceConfig:
        """Create a new workspace with auto-generated API key and port."""
        from app.features.workspaces.create_workspace.repository import add_workspace_to_db
        
        if not re.match(r'^[a-zA-Z0-9_]+$', workspace_name):
            raise ValueError('Invalid workspace name')
            
        api_key = secrets.token_urlsafe(32)
        new_port = await find_free_port(START_PORT_RANGE)
        config = WorkspaceConfig(workspace=workspace_name, api_key=api_key, port=new_port)
        await add_workspace_to_db(config)
        self.start_process(config)
        return config


# Singleton instance
manager = LightRAGManager()


async def watchdog_loop():
    """Background task that monitors worker health."""
    await asyncio.sleep(10)
    print(f"👀 [Watchdog] Active.")
    while manager.running:
        await manager.check_health()
        await asyncio.sleep(5)


async def log_rotation_loop():
    """Background task that rotates logs every minute (checks for 5-min interval change)."""
    await asyncio.sleep(30)  # Initial delay
    print(f"📝 [LogRotation] Active. Rotating every {LOG_ROTATION_INTERVAL} minutes.")
    while manager.running:
        manager.rotate_logs()
        await asyncio.sleep(60)  # Check every minute

