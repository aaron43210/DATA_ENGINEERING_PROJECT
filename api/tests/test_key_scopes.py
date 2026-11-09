import asyncio
import hashlib

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.auth import _verify_db_api_key, require_scope
from app.db.postgis import get_db
from app.main import app
from app.routers.auth_router import KeyCreate, MAX_ACTIVE_KEYS, _make_jwt

SESSION = {"Authorization": f"Bearer {_make_jwt(1, 'owner@example.com', 'reader')}"}


def test_scoped_key_only_reaches_its_product():
    check = require_scope("satellite_imagery")

    assert (
        asyncio.run(check(user={"scope": "satellite_imagery"}))["scope"]
        == "satellite_imagery"
    )
    assert asyncio.run(check(user={"scope": "all"}))["scope"] == "all"
    with pytest.raises(HTTPException) as exc:
        asyncio.run(check(user={"scope": "live_weather_stream"}))
    assert exc.value.status_code == 403


def test_key_create_rejects_unknown_scope_and_expiry():
    assert KeyCreate(name="app").scope == "all"
    assert KeyCreate(name="app", scope="live_air_quality").scope == "live_air_quality"
    assert KeyCreate(name="app", expires_in_days=None).expires_in_days is None
    with pytest.raises(ValidationError):
        KeyCreate(name="app", scope="iot_sensor_stream")
    with pytest.raises(ValidationError):
        KeyCreate(name="app", expires_in_days=5)


class _Pool:
    """Fake pool: the atomic UPDATE returns `ok`; the follow-up status SELECT returns `status`."""

    def __init__(self, ok=None, status=None):
        self.ok, self.status = ok, status

    def acquire(self):
        pool = self

        class Ctx:
            async def __aenter__(self):
                return pool

            async def __aexit__(self, *a):
                return False

        return Ctx()

    async def fetchrow(self, query, key_hash):
        assert key_hash == hashlib.sha256(b"k").hexdigest()
        return self.ok if "UPDATE" in query else self.status


def _request(pool):
    class Req:
        class app:
            class state:
                pass

    Req.app.state.pool = pool
    return Req()


def _verify(pool):
    return asyncio.run(_verify_db_api_key("k", _request(pool)))


def test_valid_key_carries_its_scope():
    pool = _Pool(ok={"scope": "osm_vector_product", "email": "a@b.c", "role": "reader"})
    assert _verify(pool)["scope"] == "osm_vector_product"


@pytest.mark.parametrize(
    "status,code,detail",
    [
        (None, 401, "Invalid or inactive API key"),
        ({"is_active": False, "expired": False}, 401, "Invalid or inactive API key"),
        ({"is_active": True, "expired": True}, 401, "API key expired"),
        ({"is_active": True, "expired": False}, 429, "Daily quota exceeded"),
    ],
)
def test_rejected_key_reports_why(status, code, detail):
    with pytest.raises(HTTPException) as exc:
        _verify(_Pool(ok=None, status=status))
    assert (exc.value.status_code, exc.value.detail) == (code, detail)


def test_data_routes_require_auth():
    client = TestClient(app)
    for path in ("/imagery/", "/vector/features", "/weather/", "/sensors/"):
        assert client.get(path).status_code == 401


def test_api_keys_cannot_manage_keys():
    client = TestClient(app)
    key = {"X-API-Key": "any-key"}
    assert client.get("/keys", headers=key).status_code == 401
    assert client.post("/keys", headers=key, json={"name": "x"}).status_code == 401
    assert client.delete("/keys/abcdefgh", headers=key).status_code == 401
    assert client.get("/admin/usage", headers=key).status_code == 401


def test_key_creation_capped_per_user():
    class FullDB:
        async def fetchval(self, query, email):
            return MAX_ACTIVE_KEYS

    async def full_db():
        yield FullDB()

    previous = app.dependency_overrides.get(get_db)
    app.dependency_overrides[get_db] = full_db
    try:
        r = TestClient(app).post("/keys", headers=SESSION, json={"name": "x"})
    finally:
        if previous:
            app.dependency_overrides[get_db] = previous
        else:
            app.dependency_overrides.pop(get_db, None)
    assert r.status_code == 409
