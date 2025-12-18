import asyncio
import time
import sys
import os

# Ensure src is in path to import from sibling directory
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.llm_resolution import resolve_variations_batch

async def benchmark():
    # Base Dataset
    base_clusters = [
        ["Apple", "Apple Inc", "Apple Computers"],
        ["Google", "Google LLC", "Alphabet Inc"],
        ["Microsoft", "MSFT", "Microsoft Corp"],
        ["Amazon", "Amazon.com", "AWS"],
        ["Tesla", "Tesla Motors", "TSLA"],
        ["Meta", "Facebook", "Meta Platforms"],
        ["Netflix", "Netflix Inc", "NFLX"],
        ["Nvidia", "NVIDIA Corp", "NVDA"],
        ["Oracle", "Oracle Corporation", "ORCL"],
        ["Salesforce", "Salesforce.com", "CRM"]
    ]
    
    # Scale up to 50 clusters to get a stable measurement
    test_load = base_clusters * 5 
    
    print(f"Starting benchmark with {len(test_load)} clusters using abatch...")
    start_time = time.perf_counter()
    
    results = await resolve_variations_batch(test_load)
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    avg_time = total_time / len(test_load)
    
    success_count = sum(1 for r in results if r is not None)
    
    print(f"\n--- Benchmark Results ---")
    print(f"Total Clusters: {len(test_load)}")
    print(f"Successful Resolutions: {success_count}")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Average Time per Cluster: {avg_time:.4f}s")
    print(f"Throughput: {len(test_load)/total_time:.2f} clusters/sec")

if __name__ == "__main__":
    asyncio.run(benchmark())
