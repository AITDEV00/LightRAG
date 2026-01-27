
import json
import re
import unicodedata
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from features import generate_hybrid_acronym, normalise_text_preserved

def acronym_match_score(s1: str, s2: str) -> float:
    # Copy of the UDF from splink_model.py
    def _longest_common_subsequence(s1: str, s2: str) -> int:
        m, n = len(s1), len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i - 1] == s2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                else:
                    dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
        return dp[m][n]

    if not s1 or not s2:
        return 0.0
    
    is_subset = 1.0 if (s1 in s2 or s2 in s1) else 0.0
    lcs_len = _longest_common_subsequence(s1, s2)
    lcs_ratio = (2.0 * lcs_len) / (len(s1) + len(s2))
    
    w_subset = 0.4
    w_lcs = 0.6
    
    return (w_subset * is_subset) + (w_lcs * lcs_ratio)

def load_entities(path, limit=50):
    with open(path, 'r') as f:
        data = json.load(f)
    print(f"Loaded {len(data)} entities. Sampling first {limit}...")
    return data[:limit]

def check_condition_b_pre(norm, acr):
    # (length(col_norm_l) <= length(col_acronym_l))
    return len(norm) <= len(acr)


def load_csv(path):
    import csv
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def parse_embedding(emb_str):
    # "[0.1 0.2 ...]" -> entries
    # Clean brackets and split
    clean = emb_str.replace('[', '').replace(']', '').strip()
    # Handle newlines
    clean = clean.replace('\n', ' ')
    parts = clean.split()
    return [float(x) for x in parts]

def cosine_similarity(v1, v2):
    if not v1 or not v2: return 0.0
    dot = sum(a*b for a, b in zip(v1, v2))
    norm1 = sum(a*a for a in v1) ** 0.5
    norm2 = sum(a*a for a in v2) ** 0.5
    if norm1 == 0 or norm2 == 0: return 0.0
    return dot / (norm1 * norm2)


def main():
    print("Script started.")
    csv_path = "/home/jyao/ait-projects/splink/service-er-lightrag/output/run_2025-12-18_16-29/predictions_2025-12-18_16-29.csv"
    matches = load_csv(csv_path)
    print(f"Loaded {len(matches)} matches from CSV.")
    
    cond_a_count = 0
    cond_a_examples = []
    

    print(f"\nAnalyzing mismatch reasons for Condition A matches...")
    print(f"{'Row':<5} | {'Entity L':<30} | {'Entity R':<30} | {'Is Acr?':<10} | {'Acr Score':<10} | {'Reason for Miss B'}")
    print("-" * 120)

    stats = {
        "len_fail": 0,
        "score_fail": 0,
        "cosine_fail": 0,
        "total_cond_a": 0
    }

    for i, row in enumerate(matches):
        reason = row.get("match_reason", "")
        if "Condition A" in reason:
            stats["total_cond_a"] += 1
            
            ent_l = row["entity_id_l"]
            ent_r = row["entity_id_r"]
            
            acr_l = row.get("col_acronym_l", generate_hybrid_acronym(ent_l))
            norm_l = row.get("col_norm_l", normalise_text_preserved(ent_l))
            
            acr_r = row.get("col_acronym_r", generate_hybrid_acronym(ent_r))
            norm_r = row.get("col_norm_r", normalise_text_preserved(ent_r))
            
            is_acr_l = check_condition_b_pre(norm_l, acr_l)
            is_acr_r = check_condition_b_pre(norm_r, acr_r)
            cond_b_length_met = (is_acr_l or is_acr_r)
            
            # Use lowercase for score to match SQL
            score = acronym_match_score(acr_l.lower(), acr_r.lower())
            
            fail_msg = ""
            if not cond_b_length_met:
                fail_msg = "Failed Length Check"
                stats["len_fail"] += 1
            elif score <= 0.85:
                fail_msg = f"Failed Acronym Score ({score:.2f} <= 0.85)"
                stats["score_fail"] += 1
            else:
                # If length passed AND score passed, it MUST be cosine that failed
                fail_msg = "Failed Cosine Similarity (Logic Inference)"
                stats["cosine_fail"] += 1
            

            if i < 20 or fail_msg.startswith("Failed Cosine"): # Prioritize showing cosine failures
                 if i < 50: # Limit output
                    print(f"{i:<5} | {ent_l[:30]:<30} | {ent_r[:30]:<30} | {str(cond_b_length_met):<10} | {score:<10.2f} | {fail_msg}")

    print("\nSummary of Condition A matches:")
    print(f"Total: {stats['total_cond_a']}")
    print(f"Failed Length Check: {stats['len_fail']}")
    print(f"Failed Acronym Score: {stats['score_fail']}")
    print(f"Failed Cosine Similarity: {stats['cosine_fail']}")

    print("\n\n--- Specific Entity Checks ---")
    
    # Check if ADNOC is in the matches
    adnoc_matches = [r for r in matches if "ADNOC" in r["entity_id_l"] or "ADNOC" in r["entity_id_r"]]
    if adnoc_matches:
        print(f"Found {len(adnoc_matches)} matches involving 'ADNOC':")
        for m in adnoc_matches:
            print(f"  {m['entity_id_l']} <-> {m['entity_id_r']} ({m['match_reason']})")
    else:
        print("No matches found involving 'ADNOC'. (This explains why it didn't show up in Condition B)")

    # Check Weatherford pair analysis
    print("\nAnalyzing 'Weatherford Manufacturing' vs 'Weatherford Manu' Logic:")
    e1 = "Weatherford Manufacturing"
    e2 = "Weatherford Manu"
    
    n1 = normalise_text_preserved(e1)
    a1 = generate_hybrid_acronym(e1)
    is_acr1 = check_condition_b_pre(n1, a1)
    
    n2 = normalise_text_preserved(e2)
    a2 = generate_hybrid_acronym(e2)
    is_acr2 = check_condition_b_pre(n2, a2)
    
    # Check if this pair exists in CSV matches
    w_match = next((r for r in matches if (r["entity_id_l"] == e1 and r["entity_id_r"] == e2) or (r["entity_id_l"] == e2 and r["entity_id_r"] == e1)), None)
    
    print(f"  Entity 1: '{e1}' -> Norm: '{n1}' ({len(n1)}), Acr: '{a1}' ({len(a1)}), Is Acronym? {is_acr1}")
    print(f"  Entity 2: '{e2}' -> Norm: '{n2}' ({len(n2)}), Acr: '{a2}' ({len(a2)}), Is Acronym? {is_acr2}")
    
    if not (is_acr1 or is_acr2):
        print("  -> Neither entity passes the Acronym Length Rule (len(norm) <= len(acr)).")
        print("  -> Therefore, Condition B (Verified Acronym) correctly FAILS.")
        print("  -> System falls back to Condition A (Safe Long-Form), which effectively matches them.")
    else:
        print("  -> At least one entity passes Length Rule. Should test Score/Cosine.")
        
    if w_match:
        print(f"  -> Actual Match Result in CSV: {w_match.get('match_reason', 'Unknown')}")
    else:
        print("  -> Pair not found in CSV matches (maybe filtered by threshold?).")


if __name__ == "__main__":
    main()
