"""
Load test: fire N parallel requests to /query and report per-request timing.

Usage:
    python load_test.py              # 100 requests (default)
    python load_test.py --count 50   # 50 requests
    python load_test.py --url http://10.0.0.1:8000/query
"""

import asyncio
import time
import argparse
import aiohttp
import json


PAYLOAD = {
    "query": "tell me about the department of health",
    "mode": "auto",
}

HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "X-Workspace": "Excellence",
}


async def send_request(
    session: aiohttp.ClientSession, url: str, request_id: int
) -> dict:
    """Send a single POST request and return timing info."""
    start = time.perf_counter()
    status = None
    error = None
    try:
        async with session.post(url, json=PAYLOAD, headers=HEADERS) as resp:
            status = resp.status
            await resp.read()
    except Exception as e:
        error = str(e)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return {
        "id": request_id,
        "status": status,
        "elapsed_ms": round(elapsed_ms, 1),
        "error": error,
    }


async def main(url: str, count: int):
    print(f"🚀 Firing {count} parallel requests to {url}\n")

    # No timeout — we want to measure, not cut off
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=0, ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        wall_start = time.perf_counter()
        tasks = [send_request(session, url, i) for i in range(count)]
        results = await asyncio.gather(*tasks)
        wall_elapsed = (time.perf_counter() - wall_start) * 1000

    # Sort by request id
    results.sort(key=lambda r: r["id"])

    # Print results table
    print(f"{'#':>4}  {'Status':>6}  {'Time (ms)':>10}  Error")
    print("-" * 50)
    for r in results:
        err_str = r["error"] or ""
        print(f"{r['id']:>4}  {r['status'] or 'ERR':>6}  {r['elapsed_ms']:>10.1f}  {err_str}")

    # Summary
    times = [r["elapsed_ms"] for r in results]
    success = sum(1 for r in results if r["status"] == 200)
    failed = count - success

    print("\n" + "=" * 50)
    print(f"Total requests:  {count}")
    print(f"Successful:      {success}")
    print(f"Failed:          {failed}")
    print(f"Wall clock:      {wall_elapsed:,.0f} ms")
    print(f"Min:             {min(times):,.1f} ms")
    print(f"Max:             {max(times):,.1f} ms")
    print(f"Avg:             {sum(times) / len(times):,.1f} ms")
    times_sorted = sorted(times)
    p50 = times_sorted[len(times_sorted) // 2]
    p95 = times_sorted[int(len(times_sorted) * 0.95)]
    p99 = times_sorted[int(len(times_sorted) * 0.99)]
    print(f"P50:             {p50:,.1f} ms")
    print(f"P95:             {p95:,.1f} ms")
    print(f"P99:             {p99:,.1f} ms")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LightRAG /query load test")
    parser.add_argument("--url", default="http://localhost:8000/query", help="Query endpoint URL")
    parser.add_argument("--count", type=int, default=100, help="Number of parallel requests")
    args = parser.parse_args()
    asyncio.run(main(args.url, args.count))
