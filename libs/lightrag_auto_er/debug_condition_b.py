from src.data_loader import load_and_filter_data
from src.features import generate_hybrid_acronym, normalise_text_preserved
from src.splink_model import acronym_match_score
import pandas as pd

def debug_logic():
    print("Loading data...")
    df = load_and_filter_data()
    
    print("\n--- Generating Features for Sample ---")
    # Take a sample of entities that might be causing issues (e.g. Long ones)
    sample_ids = ["Weatherford Manufacturing", "La Trobe University", "Colorado State University", "Yas Water World"]
    
    # Filter DF to these if they exist, or just take head
    mask = df['entity_id'].isin(sample_ids)
    if mask.sum() > 0:
        sample_df = df[mask].copy()
    else:
        print("Specific samples not found, taking head(5)")
        sample_df = df.head(5).copy()
        
    sample_df["col_acronym"] = sample_df["entity_id"].astype(str).apply(generate_hybrid_acronym)
    sample_df["col_norm"] = sample_df["entity_id"].astype(str).apply(normalise_text_preserved)
    
    print(f"\nAnalyzed {len(sample_df)} records:")
    print(f"{'Entity ID':<30} | {'Norm':<30} | {'Acronym':<10} | {'Len(Norm)':<10} | {'Len(Acr)':<10} | {'Cond B Length Check (<=)'}")
    print("-" * 120)
    
    for _, row in sample_df.iterrows():
        norm = row['col_norm']
        acr = row['col_acronym']
        len_n = len(norm) if norm else 0
        len_a = len(acr) if acr else 0
        
        cond_b_len_check = (len_n <= len_a)
        
        print(f"{str(row['entity_id'])[:30]:<30} | {str(norm)[:30]:<30} | {str(acr)[:10]:<10} | {len_n:<10} | {len_a:<10} | {cond_b_len_check}")

    print("\n--- Testing Custom Score Logic ---")
    # Simulating a pair comparison
    # Weatherford Manufacturing vs Weatherford Manu?
    s1 = "Weatherford Manufacturing"
    s2 = "Weatherford Manu" 
    # (Assuming these might be a pair)
    
    s1_norm = normalise_text_preserved(s1)
    s2_norm = normalise_text_preserved(s2)
    s1_acr = generate_hybrid_acronym(s1)
    s2_acr = generate_hybrid_acronym(s2) # WM
    
    print(f"\nPair: '{s1}' vs '{s2}'")
    print(f"Norms: '{s1_norm}' vs '{s2_norm}'")
    print(f"Acronyms: '{s1_acr}' vs '{s2_acr}'")
    
    score = acronym_match_score(s1_acr.lower(), s2_acr.lower())
    print(f"Acronym Score ({s1_acr} vs {s2_acr}): {score}")
    
    # Calculate Jaro-Winkler
    # Need jaro_winkler implementation or library. 
    # Use DuckDB for simplicity later.
    
    # Check Cond B SQL Logic
    # (len_l <= len_acr_l) OR (len_r <= len_acr_r)
    len_check_l = (len(s1_norm) <= len(s1_acr))
    len_check_r = (len(s2_norm) <= len(s2_acr))
    len_check_combined = len_check_l or len_check_r
    
    print(f"Length Check L ({len(s1_norm)} <= {len(s1_acr)}): {len_check_l}")
    print(f"Length Check R ({len(s2_norm)} <= {len(s2_acr)}): {len_check_r}")
    print(f"Combined Length Check (OR): {len_check_combined}")
    print(f"Condition B Eligible? {len_check_combined} AND (Score > 0.85): {len_check_combined and (score > 0.85)}")

    print("\n--- Testing via DuckDB SQL ---")
    import duckdb
    con = duckdb.connect(":memory:")
    con.create_function("acronym_match_score", acronym_match_score)
    con.register("df", sample_df)
    
    # Check length logic on the sample
    query = """
    SELECT 
        entity_id, 
        col_norm,
        col_acronym,
        length(col_norm) as len_norm,
        length(col_acronym) as len_acr,
        (length(col_norm) <= length(col_acronym)) as is_cond_b_candidate
    FROM df
    """
    print(con.execute(query).df().to_markdown(index=False))

    print("\n--- Testing Pair SQL (Weatherford) ---")
    pair_df = pd.DataFrame([{
        "col_norm_l": "weatherford manufacturing",
        "col_acronym_l": "wm",
        "description_embedding_l": [1.0, 0.0], # Dummy
        "col_norm_r": "weatherford manu",
        "col_acronym_r": "wm",
        "description_embedding_r": [1.0, 0.0]  # Dummy
    }])
    con.register("pair_df", pair_df)
    
    pair_query = """
    SELECT 
        (length(col_norm_l) <= length(col_acronym_l)) as cond_b_len_l,
        acronym_match_score(col_acronym_l, col_acronym_r) as acr_score,
        jaro_winkler_similarity(col_norm_l, col_norm_r) as jaro_score,
        (jaro_winkler_similarity(col_norm_l, col_norm_r) > 0.90) as cond_a_pass
    FROM pair_df
    """
    print(con.execute(pair_query).df().to_markdown(index=False))

    print("\n--- Testing via Full Splink Pipeline ---")
    from src.splink_model import create_linker
    from src.embeddings import add_description_embeddings
    
    # ... (Setup Mock Sample DF reused from before) ...
    # Re-using previous setup logic which is fine.
    
    # ...
    
    # We need to re-import create_linker to get new levels? 
    # Python reload mechanism might be needed if using interactive session, 
    # but here we run script from scratch.

    # ... (Update print logic to interpret Gamma)


    print("\n--- Testing via Full Splink Pipeline ---")
    from src.splink_model import create_linker
    from src.embeddings import add_description_embeddings
    
    # We need embeddings for the linker to work (Condition B uses 'description_embedding')
    # Let's mock them or compute them (mocking is faster)
    print("Mocking embeddings...")
    # add_description_embeddings uses 'description'. Ensure sample has it.
    # sample_df comes from load_and_filter_data which has descriptions.
    
    # Mock embedding function for speed: just random vectors of correct dim? or use actual pipeline
    # To avoid API calls, we'll manually add the column
    # sample_df["description_embedding"] = [[0.1] * 1536 for _ in range(len(sample_df))]
    
    # Ensure blocking rules work. They block on 'entity_type'. 
    # Sample has mixed types? 
    # Colorado: organization? Weatherford: organization?
    # print("Entity types:", sample_df["entity_type"].unique())
    
    # Ensure we have duplicates in sample to trigger comparison?
    # We only have singletons in sample_df. We need PAIRS.
    # Let's create a duplicate of 'Weatherford Manufacturing' with 'Weatherford Manu'
    
    # w_row = sample_df[sample_df["entity_id"] == "Weatherford Manufacturing"].iloc[0].copy()
    # w_dup = w_row.copy()
    # w_dup["unique_id"] = 9999
    # w_dup["entity_id"] = "Weatherford Manu"
    # # Recalculate features for duplicate
    # w_dup["col_norm"] = normalise_text_preserved(w_dup["entity_id"])
    # w_dup["col_acronym"] = generate_hybrid_acronym(w_dup["entity_id"]) # wm
    
    # # Append to sample
    # sample_df = pd.concat([sample_df, pd.DataFrame([w_dup])], ignore_index=True)
    
    # print("Created failing pair sample:")
    # print(sample_df[["entity_id", "col_norm", "col_acronym", "entity_type"]].tail(2).to_markdown())

    # ...
    # Creating mocked dataframe with NEW column names
    
    # Reload sample_df? Just create fresh one
    sample_df = pd.DataFrame([
        {"unique_id": 3, "entity_id": "Yas Water World", "entity_type": "artifact", "description": "...", "description_embedding": [0.1]*1536},
        {"unique_id": 4, "entity_id": "Weatherford Manu", "entity_type": "organization", "description": "...", "description_embedding": [0.1]*1536},
        {"unique_id": 99, "entity_id": "Weatherford Manufacturing", "entity_type": "organization", "description": "...", "description_embedding": [0.1]*1536}
    ])
    
    # Calculate features
    sample_df["col_norm"] = sample_df["entity_id"].apply(normalise_text_preserved)
    sample_df["col_acronym"] = sample_df["entity_id"].apply(generate_hybrid_acronym)
    
    print("Created failing pair sample:")
    print(sample_df[["entity_id", "col_norm", "col_acronym", "entity_type"]].to_markdown())

    linker = create_linker(sample_df)
    
    # Check Gamma mappings
    print("\n--- Comparison Levels Configuration ---")
    levels = linker._settings_obj.comparisons[0].comparison_levels
    for i, level in enumerate(levels):
        # Attribute might be _comparison_level_val or comparison_vector_value
        val = getattr(level, "comparison_vector_value", getattr(level, "_comparison_level_val", -999))
        print(f"Index {i}: {level.label_for_charts} (Level Val: {val})")
        # Also print SQL condition to be sure
        print(f"   SQL: {level.sql_condition[:50]}...")
    
    print("Running predict()...")
    preds = linker.inference.predict(threshold_match_weight=-10.0)
    
    print("Prediction Results:")
    pred_df = preds.as_pandas_dataframe()
    # Check output columns might be prefixed with _l / _r
    cols = ["entity_id_l", "entity_id_r", "match_weight", "match_probability", "match_reason", "gamma_entity_id"]
    available_cols = [c for c in cols if c in pred_df.columns]
    
    results = pred_df[available_cols]
    print(results.to_markdown(index=False))
    
    print("\n--- Inspecting Internal Splink Tables ---")
    # try: removed to avoid syntax error if except is gone
    if True:
        # Check what the linker sees.
        # The input table usually has a specific name or alias.
        # But we can check __splink__df_concat_with_tf or similar if they exist.
        # Or simpler:
        # Linker registers the input dataframe.
        # Let's try to query the input table logic.
        
        # We can create a profile?
        # Or just run a custom SQL using the linker's connection
        
        tables_df = linker._db_api._con.execute("SHOW TABLES").df()
        concat_table = None
        for name in tables_df['name']:
            if name.startswith("__splink__df_concat_with_tf"):
                concat_table = name
                break
        
        if concat_table:
            debug_sql = f"SELECT * FROM {concat_table} LIMIT 5"
            print(f"Querying {concat_table}:")
            print(linker._db_api._con.execute(debug_sql).df().to_markdown())
        else:
            print("Could not find concat table. Available tables:")
            print(tables_df.to_markdown())

if __name__ == "__main__":
    debug_logic()
