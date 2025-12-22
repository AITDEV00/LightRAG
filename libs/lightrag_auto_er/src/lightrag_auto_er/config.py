from pathlib import Path
from typing import Any
from datetime import datetime
from pydantic_settings import BaseSettings
from pydantic import Field

class AppConfig(BaseSettings):
    # Data Paths
    INPUT_FILE_PATH: Path = Field(
        default=Path("/home/jyao/ait-projects/GraphRAG/LightRAG/libs/lightrag_auto_er/tests/inputs/all_entities.json"),
        description="Path to the input JSON file."
    )
    OUTPUT_BASE_DIR: Path = Field(
        default=Path("output"),
        description="Base directory for outputs."
    )
    

    # Run Flags
    ENABLE_LLM_MERGE: bool = Field(
        default=True,
        description="Whether to use LLM for entity resolution merging."
    )
    RETURN_MERGE_STRUCTURE: bool = Field(
        default=False,
        description="If True, returns the merge payload object instead of just saving to disk."
    )
    
    # Embedding Settings
    EMBEDDING_PROVIDER: str = Field(
        default="openai",
        description="Provider for embeddings: 'local' (FastEmbed/SentenceTransformer) or 'openai' (LangChain)."
    )

    EMBEDDING_API_BASE: str = Field(
        default="https://inference.ai.ecouncil.ae/models/84452b37-2134-4d8b-a192-ebd569b308e9/proxy/v1",
        description="Base URL for the Embedding API (if provider is openai)."
    )
    EMBEDDING_API_KEY: str = Field(
        default="sk-CDcmT1y0cAVAsK3DwLmvjRKDAq7UtMZVjvpXGtNETYk",
        description="API Key for the Embedding API."
    )
    EMBEDDING_MODEL_NAME: str = Field(
        default="Qwen/Qwen3-Embedding-0.6B",
        description="Model name for embeddings."
    )
    EMBEDDING_DIM: int = Field(
        default=1024,
        description="Dimension of the embedding model."
    )
    EMBEDDING_BATCH_SIZE: int = Field(
        default=32,
        description="Batch size for embedding generation."
    )
    
    # LLM Settings
    LLM_API_BASE: str = Field(
        default="https://inference.ai.ecouncil.ae/models/1144a98d-a5d7-4059-8f5b-e6b8a279955d/proxy/v1",
        description="Base URL for the LLM API."
    )
    LLM_MODEL_NAME: str = Field(
        default="Qwen/Qwen3-Next-80B-A3B-Instruct",
        description="Model name to use."
    )
    LLM_API_KEY: str = Field(
        default="sk-1M-l9wsWGwPjAWItf0_yXHoNE12L2S0pEWhq0TeMeLs",
        description="API Key for the LLM."
    )
    LLM_VERIFY_SSL: bool = Field(
        default=False,
        description="Whether to verify SSL certificates."
    )
    
    # Splink Settings
    SPLINK_MATCH_THRESHOLD: float = Field(
        default=0.80,
        description="Probability threshold for clustering."
    )
    
    # Run Metadata
    TIMESTAMP: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d_%H-%M"),
        description="Run timestamp (YYYY-MM-DD_HH-MM)."
    )

    @property
    def run_dir(self) -> Path:
        """Returns the timestamped run directory."""
        return self.OUTPUT_BASE_DIR / f"run_{self.TIMESTAMP}"

    @property
    def output_md_path(self) -> Path:
        return self.run_dir / f"output_{self.TIMESTAMP}.md"

    @property
    def timing_path(self) -> Path:
        return self.run_dir / f"timings_{self.TIMESTAMP}.txt"

    @property
    def log_path(self) -> Path:
        return self.run_dir / f"log_{self.TIMESTAMP}.log"

    class Config:
        case_sensitive = True

# Global instance
settings = AppConfig()
