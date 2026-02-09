"""
Configuration settings for LightRAG Enterprise Manager.
Handles CLI arguments, environment variables, and constants.
"""
import os
import argparse
from dotenv import load_dotenv

# --- CLI ARGUMENT PARSING ---
parser = argparse.ArgumentParser(description="LightRAG Enterprise Manager")
parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind the manager to")
parser.add_argument("--port", type=int, default=8000, help="Port to bind the manager to")
parser.add_argument("--disable-auth", action="store_true", help="Disable API Key auth (Plug-and-Play mode)")
parser.add_argument("--auto-create", action="store_true", help="Automatically create workspaces if they don't exist")
parser.add_argument("--root-path", type=str, default="", help="Root path for FastAPI (useful behind proxies)")

# Parse args - allow unknown args for testing frameworks
args, _ = parser.parse_known_args()

# --- ENVIRONMENT VARIABLES ---
load_dotenv()

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")

# Handle Kubernetes/Docker variable collision where POSTGRES_PORT might be "tcp://..."
# We prioritize LIGHTRAG_POSTGRES_PORT, then try parsing POSTGRES_PORT.
# If parsing fails (e.g. it's a URL), we fallback to default 5432.
_pg_port_env = os.getenv("LIGHTRAG_POSTGRES_PORT") or os.getenv("POSTGRES_PORT", "5432")
try:
    POSTGRES_PORT = int(_pg_port_env)
except ValueError:
    print(f"⚠️ [Config] POSTGRES_PORT value '{_pg_port_env}' is not an integer (likely K8s service URL). Defaulting to 5432.")
    POSTGRES_PORT = 5432
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "").strip("'\"")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
POSTGRES_MAX_CONNECTIONS = int(os.getenv("POSTGRES_MAX_CONNECTIONS", "12"))

# --- CONSTANTS ---
DATA_ROOT = os.path.abspath("./data")
LOG_ROOT = os.path.abspath("./logs")
ADMIN_SECRET = "admin-secret-123"
START_PORT_RANGE = 9000
STARTUP_GRACE_PERIOD = 60
STARTUP_STAGGER = 0.1

# Ensure directories exist
os.makedirs(DATA_ROOT, exist_ok=True)
os.makedirs(LOG_ROOT, exist_ok=True)

# Print configuration on import
print(f"🔧 Configuration: Host={args.host}, Port={args.port}")
print(f"🔓 Auth Disabled: {args.disable_auth}")
print(f"✨ Auto-Create Workspaces: {args.auto_create}")
print(f"🗄️ PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")
print(f"⏱️ Startup Stagger: {STARTUP_STAGGER}s")
