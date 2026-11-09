import os
import json
import ssl

import asyncpg
from fastapi import Request


async def init_connection(conn):
    """Register JSONB codec."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


def ssl_setting():
    """POSTGRES_SSLMODE: verify-full checks the server certificate and hostname against
    POSTGRES_SSLROOTCERT (the RDS CA bundle baked into the image); require only encrypts.
    """
    mode = os.getenv("POSTGRES_SSLMODE", "require")
    if mode == "verify-full":
        return ssl.create_default_context(cafile=os.environ["POSTGRES_SSLROOTCERT"])
    return mode


async def get_db_pool():
    """Create PostgreSQL connection pool."""
    return await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "geoplatform"),
        user=os.getenv("POSTGRES_USER", "geoplatform"),
        password=os.environ["POSTGRES_PASSWORD"],
        ssl=ssl_setting(),
        init=init_connection,
    )


async def get_db(request: Request):
    """Dependency to get a connection from the pool."""
    pool = request.app.state.pool
    async with pool.acquire() as connection:
        yield connection
