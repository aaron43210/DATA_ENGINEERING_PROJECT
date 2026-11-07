import os

from fastapi.testclient import TestClient

from app.main import app
from app.db.postgis import get_db

API_KEY = os.environ["API_KEY"]


# Fake database connection for testing
class FakeDB:
    async def fetch(self, query):
        return []


# Override the real database dependency
async def override_get_db():
    yield FakeDB()


app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)


def test_health_check():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_datasets_unauthorized():
    response = client.get("/datasets/")

    assert response.status_code == 401


def test_datasets_authorized():
    response = client.get("/datasets/", headers={"X-API-Key": API_KEY})

    assert response.status_code == 200
    assert response.json() == []


def test_only_rest_routes_exposed():
    paths = {r.path for r in app.routes}
    assert "/graphql" not in paths
    assert not any(p.startswith("/lineage") for p in paths)
    assert {
        "/health",
        "/datasets/",
        "/imagery/",
        "/weather/",
        "/sensors/",
        "/vector/features",
    } <= paths


def test_datasets_catalog_allows_product_without_rest_port():
    row = {
        "product_id": "climate_spatial_mart",
        "name": "Spatial climate mart",
        "domain": "Environmental Intelligence",
        "owner_team": "Angel Team",
        "description": "One row per OSM feature",
        "sla_freshness_hours": 24,
        "output_port_rest": None,
    }

    class RowDB:
        async def fetch(self, query):
            return [row]

    async def row_db():
        yield RowDB()

    app.dependency_overrides[get_db] = row_db
    try:
        response = client.get("/datasets/", headers={"X-API-Key": API_KEY})
    finally:
        app.dependency_overrides[get_db] = override_get_db

    assert response.status_code == 200
    assert response.json() == [row]
