from typing import List, Dict, Any, Optional
import pandas as pd
import asyncio
from .config import settings
from .logger import logger
from .timer import timer
from .features import generate_hybrid_acronym, normalise_text_preserved
from .data_loader import load_and_filter_data
from .embeddings import add_description_embeddings
from .splink_model import create_linker, train_linker
from .llm_resolution import resolve_variations_batch
from .merging import generate_merge_plans, format_merge_payload

def get_match_reason(gamma_val):
    if gamma_val == 1:
        return "Condition A: Safe Long-Form"
    elif gamma_val == 2:
        return "Condition B: Verified Acronym"
    elif gamma_val == 0: # ElseLevel is usually 0 match weight, but Gamma index might be 0
        return "No Match (Else)"
    return f"Level {gamma_val}"

def run_pipeline() -> Optional[List[Dict[str, Any]]]:
    logger.info("Starting ER Pipeline Analysis...")
    
    # 1. Load Data
    df = load_and_filter_data()
    df["unique_id"] = range(1, len(df) + 1)
    
    # 2. String Features
    timer.start("string_features")
    
    # Generate acronyms
    df["col_acronym"] = df["entity_id"].astype(str).apply(generate_hybrid_acronym)
    
    # Normalise entity_id
    df["col_norm"] = df["entity_id"].astype(str).apply(normalise_text_preserved)
    
    elapsed = timer.stop("string_features")
    logger.info("String feature engineering took %.3f seconds", elapsed)

    # 3. Embeddings
    df = add_description_embeddings(df)
    
    # 4. Splink Setup & Training
    linker = create_linker(df)
    train_linker(linker)
    
    # 5. Inference
    output_lines: List[str] = [] 
    timer.start("inference_predict")
    # threshold_match_weight=0.0 means return all potential matches for inspection/clustering
    pairwise_predictions = linker.inference.predict(threshold_match_weight=0.0)
    elapsed = timer.stop("inference_predict")
    logger.info("inference.predict took %.3f seconds", elapsed)

    # Convert to DF for inspection
    timer.start("pairwise_as_dataframe")
    preds = pairwise_predictions.as_pandas_dataframe()
    timer.stop("pairwise_as_dataframe")

    # Add Reason Column
    if "gamma_entity_id" in preds.columns:
        preds["match_reason"] = preds["gamma_entity_id"].apply(get_match_reason)
    else:
        preds["match_reason"] = "Unknown (Gamma col missing)"

    # Debug: Print logic
    if not preds.empty:
        high_prob_preds = preds[preds["match_probability"] > 0.5].sort_values("match_weight", ascending=True)
        display_limit = 50
        
        print(f"\n--- High Probability Predictions (>0.5) [Top {display_limit} shown] ---")
        cols = ["match_weight", "match_probability", "match_reason"]
        for col in ["entity_type_l", "entity_id_l", "col_acronym_l", "entity_id_r", "col_acronym_r"]:
            if col in preds.columns:
                cols.insert(0, col)

        if not high_prob_preds.empty:
            top_md = high_prob_preds.head(display_limit)[cols].to_markdown(index=False)
            print(top_md)
            output_lines.append(f"\n## High Probability Predictions (>0.5) [Top {display_limit} shown]\n")
            output_lines.append(top_md)
        else:
            print("No predictions found with probability > 0.5")
            output_lines.append("\n## No predictions found with probability > 0.5\n")
    else:
        output_lines.append("\n## No predictions found above threshold\n")

    # 6. Clustering
    timer.start("clustering")
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        pairwise_predictions,
        threshold_match_probability=settings.SPLINK_MATCH_THRESHOLD,
    )
    elapsed = timer.stop("clustering")
    logger.info("cluster_pairwise_predictions_at_threshold took %.3f seconds", elapsed)

    df_clusters = clusters.as_pandas_dataframe()

    # 7. Merge Planning
    timer.start("merge_planning")
    
    # Pre-calc reasons
    cluster_map = df_clusters.set_index("unique_id")["cluster_id"].to_dict()
    reasons_by_cluster = {}
    
    if not preds.empty and "unique_id_l" in preds.columns and "unique_id_r" in preds.columns:
        preds["cluster_id_l"] = preds["unique_id_l"].map(cluster_map)
        preds["cluster_id_r"] = preds["unique_id_r"].map(cluster_map)
        intra_cluster_preds = preds[preds["cluster_id_l"] == preds["cluster_id_r"]].copy()
        
        reasons_by_cluster = (
            intra_cluster_preds.groupby("cluster_id_l")["match_reason"]
            .apply(lambda x: " | ".join(sorted(set(x))))
            .to_dict()
        )

        reasons_by_cluster = (
            intra_cluster_preds.groupby("cluster_id_l")["match_reason"]
            .apply(lambda x: " | ".join(sorted(set(x))))
            .to_dict()
        )

    # --- LLM BATCH RESOLUTION ---
    llm_map = {}
    if settings.ENABLE_LLM_MERGE:
        logger.info("Preparing clusters for LLM resolution...")
        clusters_to_resolve = []
        cluster_ids_for_llm = []
        

        # Pre-pass to collect batches
        for cluster_id, g in df_clusters.groupby("cluster_id"):
            if len(g) > 1:

                # Prepare list of dictionaries for each entity in the cluster
                # Each item: {"entity_id": "Name"}
                cluster_items = (
                    g[["entity_id"]]
                    .astype(str)
                    .to_dict(orient="records")
                )
                clusters_to_resolve.append(cluster_items)
                cluster_ids_for_llm.append(cluster_id)
                
        if clusters_to_resolve:
            logger.info(f"Sending {len(clusters_to_resolve)} clusters to LLM...")
            try:
                # Run the async batch function synchronously
                llm_results = asyncio.run(resolve_variations_batch(clusters_to_resolve))
                for cid, res in zip(cluster_ids_for_llm, llm_results):
                    if res:
                        llm_map[cid] = res
            except Exception as e:
                logger.error(f"LLM resolution failed: {e}")
    else:
        logger.info("LLM Merge is DISABLED. Skipping LLM resolution step.")

    merge_df = generate_merge_plans(df_clusters, reasons_by_cluster, llm_map)
    elapsed = timer.stop("merge_planning")
    logger.info("Merge planning took %.3f seconds", elapsed)

    print("\nSuggested merges (markdown):")
    if not merge_df.empty:
        merge_md = merge_df.to_markdown(index=False)
        print(merge_md)
        output_lines.append("\n## Suggested merges\n")
        output_lines.append(merge_md)
        
        # CSV Export: Merges
        merges_csv_path = settings.run_dir / f"merges_{settings.TIMESTAMP}.csv"
        merge_df.to_csv(merges_csv_path, index=False)
        logger.info(f"Detailed merges exported to {merges_csv_path}")
    else:
        print("No merges suggested.")
        output_lines.append("\nNo merges suggested.\n")

    # CSV Export: Predictions (> 0.5)
    if not preds.empty:
        # Re-filter to ensure we get everything > 0.5
        all_high_prob = preds[preds["match_probability"] > 0.5].sort_values("match_weight", ascending=True)
        if not all_high_prob.empty:
            preds_csv_path = settings.run_dir / f"predictions_{settings.TIMESTAMP}.csv"
            all_high_prob.to_csv(preds_csv_path, index=False)
            logger.info(f"High probability predictions exported to {preds_csv_path}")

    # 8. Save Results
    logger.info("Total script runtime not fully captured by single timer in modular layout, see individual steps.")
    
    output_lines.append("\n## Timing summary (seconds)\n")
    for name, sec in timer.get_all().items():
        output_lines.append(f"- **{name}**: {sec:.3f}")

    with open(settings.output_md_path, "w", encoding="utf-8") as f:
        f.write("# Splink entity resolution debug output\n\n")
        f.write("\n".join(output_lines))
        
    # Write timings file
    with open(settings.timing_path, "w", encoding="utf-8") as tf:
        tf.write("Timing summary (seconds)\n")
        for name, sec in timer.get_all().items():
            tf.write(f"{name}: {sec:.6f}\n")

    logger.info(f"All markdown output written to {settings.output_md_path}")
    logger.info(f"Timings written to {settings.timing_path}")

    # 9. Return Payload if Requested
    if settings.RETURN_MERGE_STRUCTURE:
        merge_payload = format_merge_payload(merge_df)
        return merge_payload
    return None
