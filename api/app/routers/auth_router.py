import hashlib
import logging
import os
import secrets

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, Response
from typing import Literal, Optional

from pydantic import BaseModel, Field
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import jwt
from datetime import datetime, timedelta, timezone

from app.auth import (
    ALGORITHM,
    JWT_AUDIENCE,
    JWT_ISSUER,
    JWT_SECRET,
    SESSION_COOKIE,
    SESSION_HOURS,
    require_session,
)
from app.db.postgis import get_db

logger = logging.getLogger(__name__)

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
# Browsers accept Secure cookies on http://localhost; set false only for other plain-HTTP hosts.
COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "true").lower() != "false"
MAX_ACTIVE_KEYS = 5

router = APIRouter(tags=["auth"])


# ── helpers ───────────────────────────────────────────────────────────────────


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _make_jwt(user_id: int, email: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": email,
        "user_id": user_id,
        "role": role,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "exp": now + timedelta(hours=SESSION_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)


# ── schemas ───────────────────────────────────────────────────────────────────


class GoogleCredential(BaseModel):
    credential: str = Field(max_length=4096)


class KeyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    scope: Literal[
        "all",
        "satellite_imagery",
        "osm_vector_product",
        "live_weather_stream",
        "live_air_quality",
    ] = "all"
    expires_in_days: Optional[Literal[7, 30, 90, 365]] = 30  # None = never expires


# ── endpoints ─────────────────────────────────────────────────────────────────


@router.post("/auth/google")
async def google_auth(body: GoogleCredential, db=Depends(get_db)):
    """Verify a Google id_token, upsert the user, and start an HttpOnly cookie session."""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=503, detail="Google OAuth not configured")
    try:
        info = id_token.verify_oauth2_token(
            body.credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except ValueError:
        logger.info("Rejected Google sign-in token", exc_info=True)
        raise HTTPException(status_code=401, detail="Invalid Google sign-in token")

    google_sub = info["sub"]
    email = info.get("email", "")
    name = info.get("name", "")

    row = await db.fetchrow(
        """
        INSERT INTO api_users (google_sub, email, name)
        VALUES ($1, $2, $3)
        ON CONFLICT (google_sub) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name
        RETURNING id, role
        """,
        google_sub,
        email,
        name,
    )
    response = JSONResponse(
        {"user": {"email": email, "name": name, "role": row["role"]}}
    )
    response.set_cookie(
        SESSION_COOKIE,
        _make_jwt(row["id"], email, row["role"]),
        max_age=SESSION_HOURS * 3600,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="strict",
        path="/",
    )
    return response


@router.post("/auth/logout", status_code=204)
async def logout():
    response = Response(status_code=204)
    response.delete_cookie(
        SESSION_COOKIE, path="/", secure=COOKIE_SECURE, httponly=True, samesite="strict"
    )
    return response


@router.get("/auth/me")
async def me(user: dict = Depends(require_session)):
    return {"email": user["user"], "role": user["role"]}


@router.get("/keys")
async def list_keys(db=Depends(get_db), user: dict = Depends(require_session)):
    """List the caller's API keys."""
    rows = await db.fetch(
        """
        SELECT id, key_prefix, name, quota_day, calls_today,
               quota_reset_at, is_active, last_used_at, created_at,
               scope, expires_at
        FROM api_keys
        WHERE user_id = (SELECT id FROM api_users WHERE email = $1)
        ORDER BY created_at DESC
        """,
        user["user"],
    )
    return [dict(r) for r in rows]


@router.post("/keys", status_code=201)
async def create_key(
    body: KeyCreate, db=Depends(get_db), user: dict = Depends(require_session)
):
    """Generate a new API key. Raw key returned exactly once."""
    active = await db.fetchval(
        """
        SELECT COUNT(*) FROM api_keys
        WHERE user_id = (SELECT id FROM api_users WHERE email = $1)
          AND is_active AND (expires_at IS NULL OR expires_at > now())
        """,
        user["user"],
    )
    if active >= MAX_ACTIVE_KEYS:
        raise HTTPException(
            status_code=409,
            detail=f"You already have {MAX_ACTIVE_KEYS} active keys. Revoke one first.",
        )
    expires_at = (
        datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)
        if body.expires_in_days
        else None
    )
    raw_key = secrets.token_urlsafe(32)
    prefix = raw_key[:8]
    key_hash = _sha256(raw_key)

    await db.execute(
        """
        INSERT INTO api_keys (user_id, key_prefix, key_hash, name, scope, expires_at)
        VALUES ((SELECT id FROM api_users WHERE email = $1), $2, $3, $4, $5, $6)
        """,
        user["user"],
        prefix,
        key_hash,
        body.name,
        body.scope,
        expires_at,
    )
    return {
        "key": raw_key,
        "scope": body.scope,
        "expires_at": expires_at,
        "warning": "Store this now. It will not be shown again.",
    }


@router.delete("/keys/{key_prefix}", status_code=204)
async def revoke_key(
    key_prefix: str, db=Depends(get_db), user: dict = Depends(require_session)
):
    """Deactivate a key. Only the owner can revoke their own key."""
    result = await db.execute(
        """
        UPDATE api_keys SET is_active = FALSE
        WHERE key_prefix = $1
          AND user_id = (SELECT id FROM api_users WHERE email = $2)
          AND is_active = TRUE
        """,
        key_prefix,
        user["user"],
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Key not found or already revoked")


@router.get("/admin/usage")
async def admin_usage(db=Depends(get_db), user: dict = Depends(require_session)):
    """Admin-only: all users with key count and total calls today."""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    rows = await db.fetch(
        """
        SELECT u.id, u.email, u.name, u.role,
               COUNT(k.id)          AS key_count,
               COALESCE(SUM(k.calls_today), 0) AS total_calls_today
        FROM api_users u
        LEFT JOIN api_keys k ON k.user_id = u.id AND k.is_active = TRUE
        GROUP BY u.id
        ORDER BY u.created_at DESC
        """
    )
    return [dict(r) for r in rows]
