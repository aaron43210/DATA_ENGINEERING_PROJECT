import os
import json
import asyncpg
from fastapi import Request

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql+asyncpg://geoplatform:geoplatform@postgres:5432/geoplatform",
)
# asyncpg expects the scheme to be 'postgresql://', not 'postgresql+asyncpg://'
if POSTGRES_DSN.startswith("postgresql+asyncpg://"):
    POSTGRES_DSN = POSTGRES_DSN.replace("postgresql+asyncpg://", "postgresql://")


async def _init_connection(conn):
    """Register a JSONB codec so JSONB columns decode to dict/list, not str."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def get_db_pool():
    """Create and return an asyncpg connection pool."""
    return await asyncpg.create_pool(dsn=POSTGRES_DSN, init=_init_connection)


async def get_db(request: Request):
    """Dependency to get a connection from the pool."""
    pool = request.app.state.pool
    async with pool.acquire() as connection:
        yield connection
