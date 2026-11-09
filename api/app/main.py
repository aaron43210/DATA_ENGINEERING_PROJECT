from contextlib import asynccontextmanager
import os

import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Histogram, make_asgi_app

from app.db.postgis import get_db_pool
from app.routers import (
    air_quality,
    auth_router,
    datasets,
    imagery,
    sensors,
    vector,
    weather,
)


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
    # Behind CloudFront the API lives under /api; docs need that prefix to find openapi.json.
    root_path=os.environ.get("ROOT_PATH", ""),
    # Behind two proxies Starlette's slash redirects point at http:// and drop /api,
    # so SlashTolerance below resolves the alternate form internally instead.
    redirect_slashes=False,
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
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "X-API-Key", "Content-Type"],
)

REQUEST_LATENCY = Histogram(
    "request_latency_seconds", "HTTP request latency", ["method", "path", "status"]
)


@app.middleware("http")
async def record_metrics(request, call_next):
    t = time.time()
    response = await call_next(request)
    # Label by route template, never the raw URL, so random paths cannot create
    # unbounded metric series.
    route = request.scope.get("route")
    REQUEST_LATENCY.labels(
        request.method, getattr(route, "path", "unmatched"), response.status_code
    ).observe(time.time() - t)
    return response


# Prometheus metrics endpoint (/metrics). Internal only: CloudFront blocks /api/metrics.
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include REST Routers
app.include_router(datasets.router)
app.include_router(imagery.router)
app.include_router(weather.router)
app.include_router(air_quality.router)
app.include_router(sensors.router)
app.include_router(vector.router)
app.include_router(auth_router.router)


@app.get("/health", tags=["System"])
async def health_check():
    """Liveness probe for Kubernetes / Docker Compose."""
    return {"status": "healthy"}


class SlashTolerance:
    """Serve /imagery and /imagery/ alike by rewriting to the registered form."""

    def __init__(self, inner):
        self.inner = inner
        self.paths = None

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            if self.paths is None:
                self.paths = set(app.openapi()["paths"]) | {"/metrics/"}
            path = scope["path"]
            alt = path[:-1] if path.endswith("/") and path != "/" else path + "/"
            if path not in self.paths and alt in self.paths:
                scope = dict(scope, path=alt, raw_path=alt.encode())
        await self.inner(scope, receive, send)


app.add_middleware(SlashTolerance)
