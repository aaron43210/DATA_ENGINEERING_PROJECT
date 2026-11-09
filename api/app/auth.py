import hashlib
import logging
import os

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
import jwt

logger = logging.getLogger(__name__)

JWT_SECRET = os.environ.get("JWT_SECRET")
ALGORITHM = "HS256"
JWT_ISSUER = "geoplatform-api"
JWT_AUDIENCE = "geoplatform-web"
SESSION_COOKIE = "gp_session"
SESSION_HOURS = 12

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


def decode_session(token: str) -> dict:
    """Validate a platform JWT (signature, exp, issuer, audience)."""
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[ALGORITHM],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
            options={"require": ["exp", "sub", "iss", "aud"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid session")
    role = payload.get("role", "reader")
    return {
        "user": payload["sub"],
        "role": role,
        "privileged": role == "admin",
        "scope": "all",
    }


async def _verify_db_api_key(api_key: str, request: Request) -> dict:
    """Look up a presented API key and count the call, atomically, in one statement."""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    pool = request.app.state.pool
    async with pool.acquire() as db:
        # Only succeeds for an active, unexpired key that still has quota today, so
        # concurrent requests cannot overshoot the daily quota.
        row = await db.fetchrow(
            """
            UPDATE api_keys k SET
                calls_today = CASE WHEN k.quota_reset_at < CURRENT_DATE THEN 1
                                   ELSE k.calls_today + 1 END,
                quota_reset_at = CURRENT_DATE,
                last_used_at = now()
            FROM api_users u
            WHERE k.key_hash = $1 AND u.id = k.user_id AND k.is_active
              AND (k.expires_at IS NULL OR k.expires_at > now())
              AND (k.quota_reset_at < CURRENT_DATE OR k.calls_today < k.quota_day)
            RETURNING k.scope, u.email, u.role
            """,
            key_hash,
        )
        if row is None:
            status = await db.fetchrow(
                """
                SELECT is_active,
                       expires_at IS NOT NULL AND expires_at <= now() AS expired
                FROM api_keys WHERE key_hash = $1
                """,
                key_hash,
            )
            if not status or not status["is_active"]:
                raise HTTPException(
                    status_code=401, detail="Invalid or inactive API key"
                )
            if status["expired"]:
                raise HTTPException(status_code=401, detail="API key expired")
            raise HTTPException(status_code=429, detail="Daily quota exceeded")

    return {
        "user": row["email"],
        "role": row["role"],
        "privileged": row["role"] == "admin",
        "scope": row["scope"],
    }


def _session_token(request: Request, credentials) -> str | None:
    if credentials:
        return credentials.credentials
    return request.cookies.get(SESSION_COOKIE)


async def verify_access(
    request: Request,
    api_key: str = Security(api_key_header),
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> dict:
    """Accept a DB-issued API key, or a web session (cookie or Bearer JWT)."""
    if api_key:
        return await _verify_db_api_key(api_key, request)
    token = _session_token(request, credentials)
    if token:
        return decode_session(token)
    raise HTTPException(status_code=401, detail="No authentication provided")


async def require_session(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> dict:
    """Signed-in web users only. API keys cannot manage keys, so a scoped or
    leaked key can never mint a broader or longer-lived one."""
    token = _session_token(request, credentials)
    if not token:
        raise HTTPException(status_code=401, detail="Sign in to manage API keys")
    return decode_session(token)


def require_scope(product: str):
    """Dependency: like verify_access, but the API key must cover `product`.
    Web sessions have no scope and see everything."""

    async def check(user: dict = Depends(verify_access)) -> dict:
        scope = user.get("scope", "all")
        if scope not in ("all", product):
            raise HTTPException(
                status_code=403, detail=f"This API key is limited to {scope}"
            )
        return user

    return check
