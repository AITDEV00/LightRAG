import os
import asyncio
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
import logging

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), ".env")
loaded = load_dotenv(dotenv_path)
print(f"Loaded .env from {dotenv_path}: {loaded}")

# Configuration
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
WORKSPACE = "test"

# Debug
if not NEO4J_PASSWORD:
    print("Available Env Vars (Keys only):", [k for k in os.environ.keys() if "NEO" in k])


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_neo4j_export():
    # Password optional
    auth = (NEO4J_USERNAME, NEO4J_PASSWORD) if NEO4J_PASSWORD else None
    
    logger.info(f"Connecting to Neo4j at {NEO4J_URI}...")
    
    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=auth)

    
    query = f"MATCH (n:`{WORKSPACE}`) RETURN n LIMIT 5"
    
    try:
        async with driver.session() as session:
            result = await session.run(query)
            records = await result.data()
            
            logger.info(f"Found {len(records)} records for workspace '{WORKSPACE}'.")
            
            for i, record in enumerate(records):
                node = record['n']
                logger.info(f"--- Entity {i+1} ---")
                logger.info(f"ID: {node.get('entity_id', 'N/A')}")
                logger.info(f"Name: {node.get('entity_name', 'N/A')}") # Standardize naming check
                logger.info(f"Type: {node.get('entity_type', 'N/A')}")
                # Description might be long, truncate it
                desc = node.get('description', 'N/A')
                logger.info(f"Description: {desc[:100]}...")
                
    except Exception as e:
        logger.error(f"Neo4j Query Failed: {e}")
    finally:
        await driver.close()

if __name__ == "__main__":
    asyncio.run(test_neo4j_export())
