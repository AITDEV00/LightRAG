#!/usr/bin/env python3
"""
Concurrent Document Upload Test Script for LightRAG Race Condition Fix

This script uploads 10 documents concurrently to test the race condition fix.
If the fix is working, you should see in the server logs:
    "🔄 [RACE CONDITION FIX] Detected pending documents that were queued while pipeline was busy..."

Usage:
    1. Start the multiuser manager.py server
    2. Run this script:
       python test_concurrent_upload.py --workspace YOUR_WORKSPACE --url http://localhost:8000

Configuration:
    - Edit WORKSPACE_ID to match your X-Workspace header value
    - Edit BASE_URL if your server is running on a different port
"""

import asyncio
import aiohttp
import argparse
import os
from pathlib import Path


# ============== CONFIGURATION ==============
# Change these values to match your setup

WORKSPACE_ID = "test_workspace"  # Your X-Workspace header value
BASE_URL = "http://localhost:8000"  # Your multiuser manager.py URL

# Path to sample documents (relative to this script)
SAMPLE_DOCS_DIR = Path(__file__).parent / "sample_docs"

# ============================================


async def upload_file(
    session: aiohttp.ClientSession,
    file_path: Path,
    workspace: str,
    base_url: str,
    upload_id: int,
):
    """Upload a single file to the LightRAG server."""
    url = f"{base_url}/documents/upload"

    try:
        with open(file_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field(
                "file", f, filename=file_path.name, content_type="text/plain"
            )

            headers = {"X-Workspace": workspace}

            print(f"[{upload_id:02d}] Uploading: {file_path.name}")

            async with session.post(url, data=data, headers=headers) as response:
                status = response.status
                try:
                    result = await response.json()
                except Exception:
                    result = await response.text()

                if status == 200:
                    print(f"[{upload_id:02d}] ✅ Success: {file_path.name}")
                else:
                    print(
                        f"[{upload_id:02d}] ❌ Failed ({status}): {file_path.name} - {result}"
                    )

                return {"file": file_path.name, "status": status, "result": result}

    except Exception as e:
        print(f"[{upload_id:02d}] ❌ Error: {file_path.name} - {e}")
        return {"file": file_path.name, "status": "error", "result": str(e)}


async def run_concurrent_uploads(workspace: str, base_url: str, delay: float = 0.0):
    """
    Upload all sample documents concurrently.

    Args:
        workspace: The X-Workspace header value
        base_url: The base URL of the LightRAG server
        delay: Optional delay between starting uploads (0 = true concurrent)
    """
    # Find all sample documents
    if not SAMPLE_DOCS_DIR.exists():
        print(f"❌ Sample docs directory not found: {SAMPLE_DOCS_DIR}")
        print("   Make sure you're running this from the tests directory")
        return

    files = sorted(SAMPLE_DOCS_DIR.glob("*.txt"))
    if not files:
        print(f"❌ No .txt files found in {SAMPLE_DOCS_DIR}")
        return

    print(f"🚀 Starting concurrent upload test")
    print(f"   Workspace: {workspace}")
    print(f"   Server: {base_url}")
    print(f"   Files: {len(files)}")
    print(f"   Delay: {delay}s between uploads")
    print("-" * 50)

    async with aiohttp.ClientSession() as session:
        if delay > 0:
            # Staggered uploads to increase chance of race condition
            tasks = []
            for i, file_path in enumerate(files):
                task = asyncio.create_task(
                    upload_file(session, file_path, workspace, base_url, i + 1)
                )
                tasks.append(task)
                if i < len(files) - 1:
                    await asyncio.sleep(delay)
            results = await asyncio.gather(*tasks)
        else:
            # True concurrent uploads
            tasks = [
                upload_file(session, file_path, workspace, base_url, i + 1)
                for i, file_path in enumerate(files)
            ]
            results = await asyncio.gather(*tasks)

    print("-" * 50)
    print("📊 Results Summary:")
    success = sum(1 for r in results if r["status"] == 200)
    failed = len(results) - success
    print(f"   ✅ Successful: {success}")
    print(f"   ❌ Failed: {failed}")
    print()
    print("🔍 Check your server logs for:")
    print('   "🔄 [RACE CONDITION FIX] Detected pending documents..."')
    print()
    print("   If you see this message, the race condition occurred and was handled correctly!")


async def check_pipeline_status(workspace: str, base_url: str):
    """Check the current pipeline status."""
    url = f"{base_url}/documents/pipeline-status"
    headers = {"X-Workspace": workspace}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                print("📈 Pipeline Status:")
                print(f"   Busy: {result.get('busy', 'N/A')}")
                print(f"   Request Pending: {result.get('request_pending', 'N/A')}")
                print(f"   Job Name: {result.get('job_name', 'N/A')}")
                print(f"   Latest Message: {result.get('latest_message', 'N/A')}")
                return result
            else:
                print(f"❌ Failed to get pipeline status: {response.status}")
                return None


async def list_documents(workspace: str, base_url: str):
    """List all documents with their processing status."""
    url = f"{base_url}/documents"
    headers = {"X-Workspace": workspace}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                docs = result if isinstance(result, list) else result.get("data", [])
                print(f"📚 Documents ({len(docs)} total):")
                for doc in docs[:20]:  # Limit output
                    status = doc.get("status", "N/A")
                    name = doc.get("file_path", doc.get("id", "Unknown"))
                    if isinstance(name, str) and "/" in name:
                        name = name.split("/")[-1]
                    status_icon = {"PROCESSED": "✅", "PENDING": "⏳", "FAILED": "❌", "PROCESSING": "🔄"}.get(
                        status, "❓"
                    )
                    print(f"   {status_icon} [{status}] {name}")
                if len(docs) > 20:
                    print(f"   ... and {len(docs) - 20} more")
                return docs
            else:
                print(f"❌ Failed to list documents: {response.status}")
                return None


def main():
    parser = argparse.ArgumentParser(
        description="Test concurrent document uploads to LightRAG"
    )
    parser.add_argument(
        "--workspace",
        "-w",
        default=WORKSPACE_ID,
        help=f"Workspace ID for X-Workspace header (default: {WORKSPACE_ID})",
    )
    parser.add_argument(
        "--url",
        "-u",
        default=BASE_URL,
        help=f"Base URL of LightRAG server (default: {BASE_URL})",
    )
    parser.add_argument(
        "--delay",
        "-d",
        type=float,
        default=0.1,
        help="Delay in seconds between uploads (default: 0.1, use 0 for true concurrent)",
    )
    parser.add_argument(
        "--action",
        "-a",
        choices=["upload", "status", "list", "all"],
        default="upload",
        help="Action to perform (default: upload)",
    )

    args = parser.parse_args()

    if args.action == "upload":
        asyncio.run(run_concurrent_uploads(args.workspace, args.url, args.delay))
    elif args.action == "status":
        asyncio.run(check_pipeline_status(args.workspace, args.url))
    elif args.action == "list":
        asyncio.run(list_documents(args.workspace, args.url))
    elif args.action == "all":
        asyncio.run(run_concurrent_uploads(args.workspace, args.url, args.delay))
        print("\n" + "=" * 50 + "\n")
        asyncio.run(check_pipeline_status(args.workspace, args.url))
        print()
        asyncio.run(list_documents(args.workspace, args.url))


if __name__ == "__main__":
    main()
