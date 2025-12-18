from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll
import pandas as pd

from .config import settings
from .logger import logger
from .timer import timer

def _longest_common_subsequence(s1: str, s2: str) -> int:
    """Calculates the length of the Longest Common Subsequence (LCS)."""
    m, n = len(s1), len(s2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
                
    return dp[m][n]

def acronym_match_score(s1: str, s2: str) -> float:
    """
    Calculates a weighted score based on Subset/Superset logic and LCS Ratio.
    Weights: 
        - Subset/Superset: 0.4
        - LCS Ratio: 0.6
    """
    if not s1 or not s2:
        return 0.0
        
    # 1. Subset/Superset Check (Binary)
    is_subset = 1.0 if (s1 in s2 or s2 in s1) else 0.0
    
    # 2. LCS Ratio
    lcs_len = _longest_common_subsequence(s1, s2)
    lcs_ratio = (2.0 * lcs_len) / (len(s1) + len(s2))
    
    # Weighted Sum
    w_subset = 0.4
    w_lcs = 0.6
    
    return (w_subset * is_subset) + (w_lcs * lcs_ratio)

def create_linker(df: pd.DataFrame) -> Linker:
    """Initialises the Splink Linker with the specific hybrid strategy."""
    
    db_api = DuckDBAPI()
    
    # Register the UDF
    # Access underlying DuckDB connection to register function
    db_api._con.create_function("acronym_match_score", acronym_match_score)

    # Strategy Definition
    strategy_comparison = cl.CustomComparison(
        output_column_name="entity_id",
        comparison_description="Hybrid Strategy: Safe Long-Form vs Verified Acronyms",
        comparison_levels=[
            cll.NullLevel("entity_id"), # Level -1 (handled by Splink)
            
            # Condition B: Verified Acronym Match (Level 1)
            cll.CustomLevel(
                sql_condition="""
                    (
                        (length(col_norm_l) <= length(col_acronym_l)) OR 
                        (length(col_norm_r) <= length(col_acronym_r))
                    ) AND
                    (acronym_match_score(lower(col_acronym_l), lower(col_acronym_r)) > 0.85) AND 
                    (list_cosine_similarity(description_embedding_l, description_embedding_r) > 0.96)
                """,
                label_for_charts="Condition B: Verified Acronym Match"
            ),

            # Condition A: Safe Long-Form Match (Level 2)
            cll.CustomLevel(
                sql_condition="""
                    (length(col_norm_l) > length(col_acronym_l)) AND
                    (length(col_norm_r) > length(col_acronym_r)) AND
                    (jaro_winkler_similarity(col_norm_l, col_norm_r) > 0.90)
                """,
                label_for_charts="Condition A: Safe Long-Form Match"
            ),
            cll.ElseLevel(),            # Level 0
        ]
    )

    splink_settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            strategy_comparison,
            cl.ExactMatch("entity_type") # Explicit match on entity type
        ],
        blocking_rules_to_generate_predictions=[
            block_on("entity_type"), # Restriction Step
        ],
        retain_intermediate_calculation_columns=True,
        em_convergence=0.01,
    )

    timer.start("linker_init")
    linker = Linker(df, splink_settings, db_api=db_api)
    elapsed = timer.stop("linker_init")
    logger.info("Linker initialisation took %.3f seconds", elapsed)
    
    return linker

def train_linker(linker: Linker):
    """Runs probability estimation steps."""
    
    deterministic_rules = ["""
        l.entity_type = r.entity_type
        AND (
            -- Condition A: Safe Long Form
            (
                (length(l.col_norm) > length(l.col_acronym)) AND 
                (length(r.col_norm) > length(r.col_acronym)) AND 
                (jaro_winkler_similarity(l.col_norm, r.col_norm) > 0.90)
            )
            OR
            -- Condition B: Verified Acronym
            (
                (
                    (length(l.col_norm) <= length(l.col_acronym)) OR 
                    (length(r.col_norm) <= length(r.col_acronym))
                ) AND
                (acronym_match_score(lower(l.col_acronym), lower(r.col_acronym)) > 0.85) AND 
                (list_cosine_similarity(l.description_embedding, r.description_embedding) > 0.96)
            )
        )
    """]

    timer.start("estimate_probability_two_random_records_match")
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules,
        recall=0.7, 
    )
    elapsed = timer.stop("estimate_probability_two_random_records_match")
    logger.info("estimate_probability_two_random_records_match took %.3f seconds", elapsed)

    timer.start("estimate_u_using_random_sampling")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    elapsed = timer.stop("estimate_u_using_random_sampling")
    logger.info("estimate_u_using_random_sampling took %.3f seconds", elapsed)

    # EM Training
    training_blocking_rule = "l.entity_type = r.entity_type"
    timer.start("em_training")
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule
    )
    elapsed = timer.stop("em_training")
    logger.info("EM training took %.3f seconds", elapsed)
