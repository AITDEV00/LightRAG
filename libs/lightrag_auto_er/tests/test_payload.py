
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))


from src.config import settings
from src.pipeline import run_pipeline
from src.logger import setup_logging

def test_pipeline_payload():
    setup_logging()
    print("Enabling RETURN_MERGE_STRUCTURE...")
    settings.RETURN_MERGE_STRUCTURE = True
    
    # Optional: reduce threshold or use cached data if possible to speed up
    # For this test, we run as is.
    
    print("Running pipeline...")
    result = run_pipeline()
    
    if result is None:
        print("ERROR: Pipeline returned None!")
        sys.exit(1)
        
    print(f"\nSUCCESS: Pipeline returned {len(result)} merge instructions.")
    
    if len(result) > 0:
        print("\nSample Output:")
        print(result[0])
        
        # Verify keys
        item = result[0]
        if "entities_to_change" in item and "entity_to_change_into" in item:
            print("\nSchema Check: PASS")
        else:
            print("\nSchema Check: FAIL")
    else:
        print("\nNo merges found, cannot verify schema.")

if __name__ == "__main__":
    test_pipeline_payload()
