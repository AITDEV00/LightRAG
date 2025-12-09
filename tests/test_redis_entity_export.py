import os
import redis
import json
import logging

# Configuration
REDIS_URI = os.environ.get("REDIS_URI", "redis://10.34.156.143:6379")
WORKSPACE = "test"
NAMESPACE = "full_entities"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_redis_entity_export():
    """
    Verifies that entity data can be retrieved from Redis for the 'test' workspace.
    """
    logger.info(f"Connecting to Redis at {REDIS_URI}...")
    
    try:
        r = redis.from_url(REDIS_URI, decode_responses=True)
        r.ping()
        logger.info("Successfully connected to Redis.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    # Construct the key pattern
    # LightRAG pattern: "{workspace}_{namespace}:{id}"
    prefix = f"{WORKSPACE}_{NAMESPACE}"
    match_pattern = f"{prefix}:*"
    
    logger.info(f"Scanning for keys with pattern: '{match_pattern}'")
    
    keys = []
    cursor = '0'
    while cursor != 0:
        cursor, batch = r.scan(cursor=cursor, match=match_pattern, count=100)
        keys.extend(batch)
        
    logger.info(f"Found {len(keys)} keys.")
    
    logger.info(f"Found {len(keys)} keys.")
    
    if keys:
        # Print first 10 keys to understand the key structure
        logger.info(f"First 10 keys: {keys[:10]}")
        
        # Try to find a key that doesn't look like a doc-ID
        non_doc_keys = [k for k in keys if not k.split(":")[-1].startswith("doc-")]
        
        target_keys = non_doc_keys[:3] if non_doc_keys else keys[:3]
        
        for key in target_keys:
            logger.info(f"--- Fetching key: {key} ---")
            value = r.get(key)
            if value:
                try:
                    data = json.loads(value)
                    # logger.info(f"Data: {json.dumps(data, indent=2)}")
                    
                    if "entity_name" in data:
                        logger.info(f"VALID ENTITY FOUND: {data['entity_name']}")
                        logger.info(f"Type: {data.get('entity_type')}")
                        logger.info(f"Description: {data.get('description')}")
                    else:
                        logger.warning(f"Key {key} data missing 'entity_name'. Keys found: {list(data.keys())}")
                        
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON for {key}")

    else:
        logger.warning("No entities found. Ensure the 'test' workspace has been populated.")

if __name__ == "__main__":
    test_redis_entity_export()
