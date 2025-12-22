
from typing import List, Dict, Any
import pandas as pd
from .config import settings
from .logger import logger

def generate_merge_plans(
    df_clusters: pd.DataFrame, 
    reasons_by_cluster: Dict[int, str], 
    llm_map: Dict[int, Any]
) -> pd.DataFrame:
    """
    Generates merge plans based on clusters, existing reasons, and LLM results.
    """
    merge_plans = []
    
    for cluster_id, g in df_clusters.groupby("cluster_id"):
        if len(g) == 1:
            continue
        
        reason_str = reasons_by_cluster.get(cluster_id, "Indirect Link / Unknown")
        
        # Default Fallback: Longest string
        fallback_canonical = (
            g["entity_id"]
            .astype(str)
            .sort_values(key=lambda s: s.str.len(), ascending=False)
            .iloc[0]
        )
        canonical = fallback_canonical
        
        # Check if LLM provided a canonical name
        llm_res = llm_map.get(cluster_id)
        llm_canonical = None
        llm_is_wrong = False
        
        if llm_res:
            llm_canonical = llm_res.canonical_name
            
            # Check if LLM flagged this as a Bad Cluster
            if llm_res.is_wrong_cluster:
                llm_is_wrong = True
                reason_str += f" | LLM Rejected (Wrong Cluster)"
            else:
                # Accept LLM result
                canonical = llm_res.canonical_name
                reason_str += f" | LLM Resolved"

        plan = {
            "cluster_id": int(cluster_id),
            "merged_entity_name": canonical,
            "match_reasons": reason_str,
            "unique_ids": g["unique_id"].tolist(),
            "entity_ids": g["entity_id"].tolist(),
            "entity_types": g["entity_type"].unique().tolist(),
            "col_acronyms": g["col_acronym"].tolist(),
        }
        
        if settings.ENABLE_LLM_MERGE:
            plan["llm_canonical_name"] = llm_canonical
            plan["llm_is_wrong_cluster"] = llm_is_wrong
            
        merge_plans.append(plan)

    if not merge_plans:
        return pd.DataFrame()
        
    return pd.DataFrame(merge_plans)

def format_merge_payload(merge_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Formats the merge dataframe into the LightRAG API payload structure.
    
    Structure:
    [
        {
          "entities_to_change": ["Elonn Musk", "Elon Msk"],
          "entity_to_change_into": "Elon Musk"
        },
        ...
    ]
    """
    payload = []
    
    if merge_df.empty:
        return payload
        
    for _, row in merge_df.iterrows():
        # Skip if flagged as wrong cluster
        if settings.ENABLE_LLM_MERGE and row.get("llm_is_wrong_cluster", False):
            continue
            
        canonical = row["merged_entity_name"]
        variations = row["entity_ids"]
        
        # Filter out the canonical name itself from the changes list
        # strict string equality check
        entities_to_change = [v for v in variations if v != canonical]
        
        if entities_to_change:
            payload.append({
                "entities_to_change": entities_to_change,
                "entity_to_change_into": canonical
            })
            
    return payload
