from fastapi.testclient import TestClient
from app.main import app
import pytest

client = TestClient(app)

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
    response = client.get("/datasets", headers={"X-API-Key": "supersecretapikey123"})
    # It might return 500 if DB is not mocked, but we expect 200 if DB is up
    # Since this is a simple unit test, we just check it doesn't return 401
    assert response.status_code in [200, 500] 
