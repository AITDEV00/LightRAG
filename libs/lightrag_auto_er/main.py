from src.pipeline import run_pipeline
from src.logger import setup_logging

def main():
    setup_logging()
    run_pipeline()

if __name__ == "__main__":
    main()
