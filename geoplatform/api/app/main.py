from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter
from prometheus_client import make_asgi_app

from app.db.postgis import get_db_pool
from app.routers import datasets, imagery, weather, sensors, vector, lineage
from app.graphql_schema import schema
from app.auth import verify_access


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Create DB pool
    app.state.pool = await get_db_pool()
    yield
    # Shutdown: Close DB pool
    await app.state.pool.close()


app = FastAPI(
    title="Cloud-Native Geospatial Data Platform API",
    description="Unified API serving Satellite, Weather, and IoT Data Products",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration — explicit allowlist from env (never "*" with credentials).
# Set CORS_ALLOW_ORIGINS to a comma-separated list of trusted origins.
_cors_origins = [
    o.strip()
    for o in os.environ.get("CORS_ALLOW_ORIGINS", "http://localhost:3000").split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "X-API-Key", "Content-Type"],
)

# Prometheus metrics endpoint (/metrics)
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include REST Routers
app.include_router(datasets.router)
app.include_router(imagery.router)
app.include_router(weather.router)
app.include_router(sensors.router)
app.include_router(vector.router)
app.include_router(lineage.router)


# Custom context dependency for GraphQL to inject DB connection and user info
async def get_context(request: Request, user_info: dict = Depends(verify_access)):
    request.state.user_info = user_info
    pool = request.app.state.pool
    async with pool.acquire() as connection:
        yield {"request": request, "db": connection}


# Mount Strawberry GraphQL
graphql_app = GraphQLRouter(schema, context_getter=get_context)

app.include_router(graphql_app, prefix="/graphql", tags=["GraphQL"])


@app.get("/health", tags=["System"])
async def health_check():
    """Liveness probe for Kubernetes / Docker Compose."""
    return {"status": "healthy"}
