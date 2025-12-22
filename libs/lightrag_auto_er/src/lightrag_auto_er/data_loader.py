from pathlib import Path
from typing import Union, List, Dict
import pandas as pd
from .config import settings
from .logger import logger
from .timer import timer

def load_and_filter_data(input_data: Union[str, Path, List[Dict], pd.DataFrame] = None) -> pd.DataFrame:
    timer.start("load_entities")
    
    df = None
    source_desc = "unknown source"

    if input_data is None:
        source_desc = str(settings.INPUT_FILE_PATH)
        logger.info(f"Loading from default path: {source_desc}")
        try:
            df = pd.read_json(settings.INPUT_FILE_PATH)
        except ValueError as e:
            logger.error(f"Failed to load JSON from {source_desc}: {e}")
            raise

    elif isinstance(input_data, (str, Path)):
        source_desc = str(input_data)
        logger.info(f"Loading from specific path: {source_desc}")
        try:
            df = pd.read_json(input_data)
        except ValueError as e:
            logger.error(f"Failed to load JSON from {source_desc}: {e}")
            raise
    
    elif isinstance(input_data, list):
        # List of dicts
        source_desc = "list of python objects"
        logger.info(f"Loading from {source_desc}")
        df = pd.DataFrame(input_data)
        
    elif isinstance(input_data, pd.DataFrame):
        source_desc = "pandas DataFrame"
        logger.info(f"Loading from {source_desc}")
        df = input_data.copy()
        
    else:
        raise ValueError(f"Unsupported input type: {type(input_data)}")

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
