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
