import asyncio
import os
from typing import List, Optional
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

# Configuration
# Loaded from .config import settings 

# 1. Define Pydantic Model



# 1. Define Pydantic Model
class ResolvedEntity(BaseModel):
    canonical_name: str = Field(..., description="The best, most complete, and correct name for the entity.")
    is_wrong_cluster: bool = Field(..., description="Set to True if the input variations contain entities that are clearly different and should NOT be merged. Otherwise False.")

from .config import settings
import httpx
from .logger import logger

# 2. Setup Logic
def get_resolution_chain():
    # Configure custom HTTP clients if SSL verification is disabled
    http_client = None
    http_async_client = None
    
    if not settings.LLM_VERIFY_SSL:
        http_client = httpx.Client(verify=False)
        http_async_client = httpx.AsyncClient(verify=False)

    llm = ChatOpenAI(
        model=settings.LLM_MODEL_NAME,
        openai_api_base=settings.LLM_API_BASE,
        openai_api_key=settings.LLM_API_KEY,
        temperature=0.0,
        max_tokens=512,
        http_client=http_client,
        http_async_client=http_async_client
    )
    


    system_prompt = """You are an expert Data Steward. Your goal is to resolve a list of entity name variations into a single canonical entity.

Instructions:
1. You will be provided with a list of entity names (IDs).
2. Analyze the names to judge if they refer to the SAME real-world entity.
3. If they are ALL the same entity, choose the best canonical name and set 'is_wrong_cluster' to False.
4. If the list contains mixed entities (e.g., 'Google' and 'Amazon', or 'Dept of Energy' and 'Dept of Transport'), set 'is_wrong_cluster' to True. In this case, 'canonical_name' should be 'INVALID_MERGE'."""

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", "Variations: {variations}\n\nResolve this entity:")
    ])
    
    structured_llm = llm.with_structured_output(ResolvedEntity)
    return prompt | structured_llm

async def resolve_variations_batch(variations_list: List[List[dict]]) -> List[Optional[ResolvedEntity]]:
    """
    Resolves a batch of entity variations asynchronously.
    Each item in variations_list is a list of dicts: [{"entity_id": "..."}]
    """
    chain = get_resolution_chain()
    
    # Format inputs for the prompt
    formatted_inputs = []
    for cluster in variations_list:
        # Example format: 
        # - Name: ABC
        # - Name: XYZ
        text_repr = "\n".join([f"- Name: {v['entity_id']}" for v in cluster])
        formatted_inputs.append({"variations": text_repr})
    
    try:
        # Use semaphore to limit concurrency manually since abatch config might be flaky
        semaphore = asyncio.Semaphore(5)
        
        async def _resolve_single(input_data):
            async with semaphore:
                # We use ainvoke for single item
                return await chain.ainvoke(input_data)

        # Create tasks
        tasks = [_resolve_single(inp) for inp in formatted_inputs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions in results
        final_results = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Single resolution failed: {r}")
                final_results.append(None)
            else:
                final_results.append(r)
                
        return final_results
    except Exception as e:
        logger.error(f"Error in batch resolution: {e}")
        # Return Nones on failure
        return [None] * len(variations_list)

async def run_resolution_demo():
    print(f"Initializing ChatOpenAI with model: {settings.LLM_MODEL_NAME} at {settings.LLM_API_BASE}")
    
    # 3. Dummy Data (Clusters without Descriptions)
    clusters = [
        [
            {"entity_id": "Abu Dhabi Dept of Energy"},
            {"entity_id": "Dept of Energy - AD"},
            {"entity_id": "DOE Abu Dhabi"}
        ],

        [
            {"entity_id": "Min. of Finance"},
            {"entity_id": "Ministry of Finance"},
            {"entity_id": "MOF"}
        ],
        [
            {"entity_id": "Google"},
            {"entity_id": "Amazon"},
            {"entity_id": "Microsoft"}
        ]
    ]
    
    print(f"\nProcessing {len(clusters)} clusters using abatch...")
    results = await resolve_variations_batch(clusters)
    
    # 5. Display Results
    print("\n--- Resolution Results ---")
    for vars_in, res in zip(clusters, results):

        print(f"\nInput: {[v['entity_id'] for v in vars_in]}")
        if res:
            print(f"Canonical: {res.canonical_name}")
            print(f"Is Wrong Cluster?: {res.is_wrong_cluster}")
        else:
            print("Failed to resolve.")

if __name__ == "__main__":
    asyncio.run(run_resolution_demo())
