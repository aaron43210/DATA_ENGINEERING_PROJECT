from fastapi.testclient import TestClient

from app.main import app
from app.db.postgis import get_db

from app.routers.auth_router import _make_jwt

SESSION = {"Authorization": f"Bearer {_make_jwt(1, 'reader@example.com', 'reader')}"}


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
    response = client.get("/datasets/", headers=SESSION)

    assert response.status_code == 200
    assert response.json() == []


def test_only_rest_routes_exposed():
    paths = set(app.openapi()["paths"])
    assert "/graphql" not in paths
    assert not any(p.startswith("/lineage") for p in paths)
    assert {
        "/health",
        "/datasets/",
        "/imagery/",
        "/weather/",
        "/sensors/",
        "/vector/features",
        "/air-quality/",
    } <= paths


def test_air_quality_latest_per_station():
    row = {
        "station_id": "WX_COK",
        "city_name": "Kochi",
        "event_time": "2026-11-09 10:00:00+00",
        "aqi": 3,
        "aqi_label": "Moderate",
        "pm2_5": 31.2,
        "pm10": 48.0,
        "no2": None,
        "o3": 60.1,
        "so2": 4.0,
        "co": 310.5,
        "nh3": 1.2,
        "no": 0.1,
        "latitude": 9.9312,
        "longitude": 76.2673,
    }
    seen = []

    class AirDB:
        async def fetch(self, query, *args):
            seen.append((query, args))
            return [row]

    async def air_db():
        yield AirDB()

    app.dependency_overrides[get_db] = air_db
    try:
        response = client.get("/air-quality/?latest=true&limit=5", headers=SESSION)
        plain = client.get("/air-quality?station_id=WX_COK", headers=SESSION)
        ranged = client.get(
            "/air-quality/?from_time=2026-11-08T00:00:00Z&to_time=2026-11-09T00:00:00Z",
            headers=SESSION,
        )
        bad = client.get("/air-quality/?from_time=yesterday", headers=SESSION)
    finally:
        app.dependency_overrides[get_db] = override_get_db

    assert response.status_code == 200
    assert response.json() == [row]
    assert "DISTINCT ON (station_id)" in seen[0][0]
    assert seen[0][1] == (None, None, None, 5, 0)
    assert plain.status_code == 200
    assert "DISTINCT ON" not in seen[1][0]
    assert seen[1][1] == ("WX_COK", None, None, 100, 0)
    assert ranged.status_code == 200
    _, start, end, _, _ = seen[2][1]
    assert (start.isoformat(), end.isoformat()) == (
        "2026-11-08T00:00:00+00:00",
        "2026-11-09T00:00:00+00:00",
    )
    assert bad.status_code == 422


def test_datasets_catalog_allows_product_without_rest_port():
    row = {
        "product_id": "climate_spatial_mart",
        "name": "Spatial climate mart",
        "domain": "Environmental Intelligence",
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
        response = client.get("/datasets/", headers=SESSION)
    finally:
        app.dependency_overrides[get_db] = override_get_db

    assert response.status_code == 200
    assert response.json() == [row]


def test_legacy_shared_api_key_is_gone(monkeypatch):
    monkeypatch.setenv("API_KEY", "old-shared-key")

    class NoKeyPool:
        def acquire(self):
            class Ctx:
                async def __aenter__(self_inner):
                    class DB:
                        async def fetchrow(self, *a):
                            return None

                    return DB()

                async def __aexit__(self_inner, *a):
                    return False

            return Ctx()

    app.state.pool = NoKeyPool()
    assert (
        client.get("/datasets/", headers={"X-API-Key": "old-shared-key"}).status_code
        == 401
    )


def test_session_cookie_is_accepted():
    client.cookies.set("gp_session", SESSION["Authorization"].split()[1])
    try:
        assert client.get("/datasets/").status_code == 200
    finally:
        client.cookies.clear()


def test_token_without_audience_is_rejected():
    import jwt
    from datetime import datetime, timedelta, timezone
    from app.auth import JWT_SECRET

    bare = jwt.encode(
        {"sub": "x@y.z", "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
        JWT_SECRET,
        algorithm="HS256",
    )
    assert (
        client.get(
            "/datasets/", headers={"Authorization": f"Bearer {bare}"}
        ).status_code
        == 401
    )


def test_trailing_slash_served_without_redirect():
    r = client.get("/datasets", headers=SESSION, follow_redirects=False)
    assert r.status_code == 200


def test_metrics_label_by_route_not_raw_path():
    client.get("/no-such-path-12345")
    body = client.get("/metrics/").text
    assert "no-such-path-12345" not in body
    assert 'path="unmatched"' in body
