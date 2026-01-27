from lightrag_auto_er.pipeline import run_pipeline
from lightrag_auto_er.config import settings as er_settings
from lightrag_auto_er.logger import setup_logging
import asyncio

def main():
    setup_logging()
    asyncio.run(run_pipeline())

if __name__ == "__main__":
    main()
