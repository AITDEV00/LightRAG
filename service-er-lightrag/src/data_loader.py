import pandas as pd
from .config import settings
from .logger import logger
from .timer import timer

def load_and_filter_data() -> pd.DataFrame:
    logger.info(f"Loading {settings.INPUT_FILE_PATH}")
    
    timer.start("load_entities")
    
    try:
        df = pd.read_json(settings.INPUT_FILE_PATH)
    except ValueError as e:
        logger.error(f"Failed to load JSON from {settings.INPUT_FILE_PATH}: {e}")
        raise

    # FILTER STEP: Discard 'data' types immediately
    initial_count = len(df)
    df = df[~df['entity_type'].isin(['data', 'year', 'date', 'period', 'timeperiod'])].copy()
    filtered_count = len(df)
    logger.info(f"Filtered out {initial_count - filtered_count} records with entity_type='data', 'year', 'date', 'period', 'timeperiod'")

    df = df.reset_index(drop=True)
    df["unique_id"] = df.index.astype("int64")
    
    elapsed = timer.stop("load_entities")
    logger.info("Loaded entities in %.3f seconds", elapsed)
    
    return df
