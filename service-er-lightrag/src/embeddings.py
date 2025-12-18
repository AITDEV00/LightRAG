import pandas as pd
from fastembed import TextEmbedding
from fastembed.common.model_description import PoolingType, ModelSource
from sentence_transformers import SentenceTransformer
import torch

from .config import settings
from .logger import logger
from .timer import timer

def add_description_embeddings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates embeddings for the 'description' column using the configured model and device.
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
        # embed_documents takes a list of strings
        # Depending on dataset size, LangChain usually handles batching, but explicit chunking is safer for API limits
        # However, for simplicity/LangChain default, we pass the full list or chunks.
        
        results = embeddings.embed_documents(desc_texts)
        df["description_embedding"] = results

    elif settings.USE_GPU:
        logger.info("Using GPU-accelerated SentenceTransformer...")
        # ... existing GPU logic ...
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Target device: {device}")

        model = SentenceTransformer(settings.EMBEDDING_MODEL_NAME, device=device)

        logger.info("Encoding descriptions...")
        embeddings_numpy = model.encode(
            desc_texts,
            normalize_embeddings=True, 
            batch_size=settings.EMBEDDING_BATCH_SIZE,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        df["description_embedding"] = embeddings_numpy.tolist()

    else:
        logger.info("Using CPU-based FastEmbed...")
        # ... existing CPU logic ...
        # (Assuming model name compatible or using default from fastembed if custom not added)
        # Note: Previous script added custom model. We keep it as is or update if needed.
        # But for 'local' generic usage, we stick to the configured model.
        
        TextEmbedding.add_custom_model(
            model=settings.EMBEDDING_MODEL_NAME,
            pooling=PoolingType.MEAN,
            normalization=True,
            sources=ModelSource(hf=settings.EMBEDDING_MODEL_NAME),
            dim=settings.EMBEDDING_DIM,
            model_file="onnx/model_qint8_avx512_vnni.onnx"
        )

        embedding_model = TextEmbedding(
            model_name=settings.EMBEDDING_MODEL_NAME,
        )

        embeddings_gen = embedding_model.embed(desc_texts)
        desc_embs = [list(e) for e in embeddings_gen]
        
        df["description_embedding"] = pd.Series(desc_embs, index=df.index)

    elapsed = timer.stop("embedding_generation")
    logger.info("Embedding generation took %.3f seconds", elapsed)
    return df
