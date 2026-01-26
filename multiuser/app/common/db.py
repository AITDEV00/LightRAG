"""
Database layer for PostgreSQL connection pool management.
"""
from typing import Optional
import asyncpg

from app.config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DATABASE,
    POSTGRES_MAX_CONNECTIONS,
)

# Global connection pool
db_pool: Optional[asyncpg.Pool] = None


async def init_db():
    """Initialize the PostgreSQL connection pool and create tables."""
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
        min_size=2,
        max_size=POSTGRES_MAX_CONNECTIONS
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lightrag_workspaces (
                workspace TEXT PRIMARY KEY,
                api_key TEXT UNIQUE NOT NULL,
                port INTEGER UNIQUE NOT NULL
            )
        """)
    print("✅ [DB] PostgreSQL connection pool initialized.")


async def close_db():
    """Close the PostgreSQL connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()
        print("🔒 [DB] PostgreSQL connection pool closed.")


def get_pool() -> asyncpg.Pool:
    """Get the database connection pool. Raises if not initialized."""
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")
    return db_pool
