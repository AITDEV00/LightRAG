import pandas as pd
from .config import settings
from .logger import logger
from .timer import timer

def add_description_embeddings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates embeddings for the 'description' column using the configured model.
    Adds a 'description_embedding' column to the DataFrame.
    """
    timer.start("embedding_generation")
    
    desc_texts = []
    for _, row in df.iterrows():
        eid = str(row["entity_id"])
        desc = str(row["description"]) if pd.notna(row["description"]) else ""
        text = f"entity_name: {eid} \n description: {desc}"
        desc_texts.append(text)

    if settings.EMBEDDING_PROVIDER == "openai":
        from langchain_openai import OpenAIEmbeddings
        import httpx

        logger.info(f"Using OpenAI Provider: {settings.EMBEDDING_MODEL_NAME} at {settings.EMBEDDING_API_BASE}")
        
        http_client = httpx.Client(verify=False)
        
        embeddings = OpenAIEmbeddings(
            model=settings.EMBEDDING_MODEL_NAME,
            openai_api_base=settings.EMBEDDING_API_BASE,
            openai_api_key=settings.EMBEDDING_API_KEY,
            check_embedding_ctx_length=False,
            http_client=http_client
        )
        
        logger.info("Encoding descriptions (batch)...")
        results = embeddings.embed_documents(desc_texts)
        df["description_embedding"] = results

    else:
        raise ValueError(f"Provider '{settings.EMBEDDING_PROVIDER}' is not supported in this lightweight version. Only 'openai' is supported.")

    elapsed = timer.stop("embedding_generation")
    logger.info("Embedding generation took %.3f seconds", elapsed)
    return df
