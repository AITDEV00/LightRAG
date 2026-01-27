"""
Endpoint for gateway proxy.
Catch-all route that proxies requests to LightRAG worker processes.
"""
import httpx
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from app.config.settings import args
from app.features.gateway.service import (
    wait_for_health,
    resolve_workspace_no_auth,
    resolve_workspace_with_auth,
)

router = APIRouter()

# Shared HTTP client for proxying
client = httpx.AsyncClient()


@router.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def gateway(path: str, request: Request):
    """Gateway proxy that routes requests to workspace worker processes."""
    if path.startswith("admin/"):
        return Response(status_code=404)

    target_workspace = None
    target_port = None
    child_api_key = None
    was_started_just_now = False

    # --- ROUTING LOGIC ---
    try:
        if args.disable_auth:
            workspace_name = request.headers.get("X-Workspace")
            if not workspace_name:
                raise HTTPException(400, "Missing X-Workspace header")
            
            result = await resolve_workspace_no_auth(workspace_name)
            target_workspace = result.workspace
            target_port = result.port
            child_api_key = result.api_key
            was_started_just_now = result.was_started_just_now
        else:
            api_key = request.headers.get("X-API-Key")
            if not api_key:
                raise HTTPException(401, "Missing X-API-Key header")
            
            result = await resolve_workspace_with_auth(api_key)
            target_workspace = result.workspace
            target_port = result.port
            child_api_key = result.api_key
            was_started_just_now = result.was_started_just_now
            
    except ValueError as e:
        raise HTTPException(400, str(e))
    except LookupError as e:
        if args.disable_auth:
            raise HTTPException(404, str(e))
        else:
            raise HTTPException(401, "Invalid Credentials")

    if not target_workspace or not target_port:
        raise HTTPException(401, "Invalid Credentials" if not args.disable_auth else "Workspace not found")

    # Wait for workspace to be ready if just started
    if was_started_just_now:
        try:
            print(f"⏳ Waiting for workspace {target_workspace} to become healthy...")
            await wait_for_health(target_port, timeout=15.0)
            print(f"✅ Workspace {target_workspace} is ready!")
        except Exception:
            print(f"Failed to start workspace {target_workspace} in time.")
            raise HTTPException(504, f"Timeout: Workspace '{target_workspace}' took too long to start.")

    # --- PROXY LOGIC ---
    url = f"http://127.0.0.1:{target_port}/{path}"

    try:
        headers = dict(request.headers)
        # Inject key ONLY if in auth mode
        if not args.disable_auth and child_api_key:
            headers["X-API-Key"] = child_api_key

        # INJECT ROOT PATH via Header
        if args.root_path:
            headers["X-Forwarded-Prefix"] = args.root_path

        request_timeout = 60.0
        rp_req = client.build_request(
            request.method,
            url,
            headers=headers,
            content=await request.body(),
            params=request.query_params,
            timeout=request_timeout
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
        raise HTTPException(502, f"Workspace '{target_workspace}' connection failed immediately after startup. Please retry after 10 seconds.")
    except httpx.TimeoutException:
        print(f"Timeout connecting to {target_workspace}")
        raise HTTPException(504, f"Gateway Timeout: The request took more than 60 seconds.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Gateway Error: {str(e)}")
