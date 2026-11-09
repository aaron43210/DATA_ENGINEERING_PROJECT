import hashlib
import logging
import os

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
import jwt

logger = logging.getLogger(__name__)

JWT_SECRET = os.environ.get("JWT_SECRET")
# Deprecated single-key escape hatch — kept for the transition period only.
# New consumers must use per-user DB-issued keys (POST /keys).
API_KEY = os.environ.get("API_KEY")
ALGORITHM = "HS256"

if not JWT_SECRET:
    raise RuntimeError(
        "JWT_SECRET must be set in the environment. "
        "Refusing to start with insecure default credentials."
    )

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)

# GDPR: fields that must be masked for non-privileged users
GDPR_MASKED_FIELDS = {
    "sensor_observations": ["latitude", "longitude"],
    "weather_observations": ["location"],
    "weather_stream_observations": ["latitude", "longitude"],
}


def mask_pii(data: dict, table: str, is_privileged: bool = False) -> dict:
    """Apply GDPR field-level masking for non-privileged users."""
    if is_privileged:
        return data

    fields_to_mask = GDPR_MASKED_FIELDS.get(table, [])
    for field in fields_to_mask:
        if field in data and data[field] is not None:
            if isinstance(data[field], float):
                data[field] = round(data[field], 2)
            else:
                data[field] = "***MASKED***"
    return data


async def _verify_db_api_key(api_key: str, request: Request) -> dict:
    """Look up a presented API key in the DB; enforce quota; update usage stats."""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    pool = request.app.state.pool
    async with pool.acquire() as db:
        row = await db.fetchrow(
            """
            SELECT k.id, k.user_id, k.is_active, k.quota_day,
                   k.calls_today, k.quota_reset_at,
                   u.email, u.role
            FROM api_keys k
            JOIN api_users u ON u.id = k.user_id
            WHERE k.key_hash = $1
            """,
            key_hash,
        )
        if not row or not row["is_active"]:
            raise HTTPException(status_code=401, detail="Invalid or inactive API key")

        from datetime import date

        today = date.today()
        calls = row["calls_today"]
        if row["quota_reset_at"] < today:
            calls = 0
            await db.execute(
                "UPDATE api_keys SET calls_today=0, quota_reset_at=$1 WHERE id=$2",
                today,
                row["id"],
            )

        if calls >= row["quota_day"]:
            raise HTTPException(status_code=429, detail="Daily quota exceeded")

        await db.execute(
            "UPDATE api_keys SET calls_today=$1, last_used_at=now() WHERE id=$2",
            calls + 1,
            row["id"],
        )

    return {
        "user": row["email"],
        "role": row["role"],
        "privileged": row["role"] == "admin",
    }


async def verify_access(
    request: Request,
    api_key: str = Security(api_key_header),
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> dict:
    """Accept API key (DB-issued) or JWT Bearer token."""
    # API Key path
    if api_key:
        # Deprecated single env-var key kept as admin escape hatch.
        if API_KEY and api_key == API_KEY:
            logger.warning(
                "Request authenticated with deprecated API_KEY env var. "
                "Migrate to per-user DB keys."
            )
            return {"user": "api-key-user", "role": "admin", "privileged": True}
        # Normal path: DB lookup with quota enforcement.
        return await _verify_db_api_key(api_key, request)

    # JWT Bearer path
    if credentials:
        try:
            payload = jwt.decode(
                credentials.credentials, JWT_SECRET, algorithms=[ALGORITHM]
            )
            return {
                "user": payload["sub"],
                "role": payload.get("role", "reader"),
                "privileged": payload.get("role") == "admin",
            }
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")

    raise HTTPException(status_code=401, detail="No authentication provided")
