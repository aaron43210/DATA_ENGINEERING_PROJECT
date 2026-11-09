import asyncio
import hashlib

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.auth import _verify_db_api_key, require_scope
from app.main import app
from app.routers.auth_router import KeyCreate


def test_scoped_key_only_reaches_its_product():
    check = require_scope("satellite_imagery")

    assert asyncio.run(check(user={"scope": "satellite_imagery"}))["scope"] == "satellite_imagery"
    assert asyncio.run(check(user={"scope": "all"}))["scope"] == "all"
    with pytest.raises(HTTPException) as exc:
        asyncio.run(check(user={"scope": "live_weather_stream"}))
    assert exc.value.status_code == 403


def test_key_create_rejects_unknown_scope_and_expiry():
    assert KeyCreate(name="app").scope == "all"
    assert KeyCreate(name="app", expires_in_days=None).expires_in_days is None
    with pytest.raises(ValidationError):
        KeyCreate(name="app", scope="iot_sensor_stream")
    with pytest.raises(ValidationError):
        KeyCreate(name="app", expires_in_days=5)


class _Pool:
    def __init__(self, row):
        self.row = row

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
        return self.row

    async def execute(self, *a):
        return "UPDATE 1"


def _request(row):
    class Req:
        class app:
            class state:
                pool = _Pool(row)

    return Req()


def _row(**over):
    from datetime import date

    return {"id": 1, "user_id": 1, "is_active": True, "quota_day": 500, "calls_today": 0,
            "quota_reset_at": date.today(), "scope": "osm_vector_product", "expired": False,
            "email": "a@b.c", "role": "reader", **over}


def test_expired_key_is_rejected():
    with pytest.raises(HTTPException) as exc:
        asyncio.run(_verify_db_api_key("k", _request(_row(expired=True))))
    assert exc.value.status_code == 401
    assert exc.value.detail == "API key expired"


def test_valid_key_carries_its_scope():
    assert asyncio.run(_verify_db_api_key("k", _request(_row())))["scope"] == "osm_vector_product"


def test_data_routes_require_auth():
    client = TestClient(app)
    for path in ("/imagery/", "/vector/features", "/weather/", "/sensors/"):
        assert client.get(path).status_code == 401
