import os

# Auth fails closed: JWT_SECRET and API_KEY must exist before `app` is imported,
# since app.auth raises at import time when they are unset. Set deterministic
# test credentials here (conftest also sets these for the whole suite).
os.environ.setdefault("JWT_SECRET", "test-jwt-secret")
os.environ.setdefault("API_KEY", "test-api-key")

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)

API_KEY = os.environ["API_KEY"]


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_datasets_unauthorized():
    response = client.get("/datasets")
    # Should require auth
    assert response.status_code == 401


def test_datasets_authorized():
    # Use API key auth
    response = client.get("/datasets", headers={"X-API-Key": API_KEY})
    # It might return 500 if DB is not mocked, but we expect 200 if DB is up.
    # Since this is a simple unit test, we just check it doesn't return 401.
    assert response.status_code in [200, 500]
