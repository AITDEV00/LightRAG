
import os
import asyncio
import json
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase

# Load .env file
load_dotenv()

async def inspect_neo4j_data(workspace: str = "test"):
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    username = os.environ.get("NEO4J_USERNAME", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")
    
    auth = (username, password) if password else None
    
    print(f"Connecting to Neo4j at {uri}...")
    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    
    try:
        async with driver.session() as session:
            # 1. Count Total Entities
            count_query = f"MATCH (n:`{workspace}`) RETURN count(n) as total"
            result = await session.run(count_query)
            count_record = await result.single()
            total_count = count_record['total']
            print(f"\nTotal nodes in workspace '{workspace}': {total_count}")

            # 2. Check Uniqueness of entity_id
            unique_query = f"MATCH (n:`{workspace}`) RETURN count(distinct n.entity_id) as unique_count"
            result = await session.run(unique_query)
            unique_record = await result.single()
            unique_count = unique_record['unique_count']
            print(f"Unique 'entity_id' count: {unique_count}")
            
            if total_count == unique_count:
                print(">> CONFIRMED: All nodes have unique entity_ids.")
            else:
                print(f">> WARNING: There are {total_count - unique_count} duplicate entity_ids.")

            # 3. Dump Full Properties for a sample
            print(f"\n--- Inspecting First 5 Entities (ALL Properties) ---")
            # Explicitly return labels since result.data() might convert Node to dict
            sample_query = f"MATCH (n:`{workspace}`) RETURN n, labels(n) as labels LIMIT 5"
            result = await session.run(sample_query)
            
            records = await result.data()
            for i, record in enumerate(records):
                node_props = record['n'] # This is a dict of properties
                labels = record['labels']
                
                print(f"\n[Entity {i+1}] Labels: {labels}")
                print("Properties:")
                print(json.dumps(node_props, indent=2))

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await driver.close()

if __name__ == "__main__":
    asyncio.run(inspect_neo4j_data())
