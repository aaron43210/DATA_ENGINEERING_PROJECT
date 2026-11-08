import hashlib
import logging
import os
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import jwt
from datetime import datetime, timedelta, timezone

from app.auth import verify_access, JWT_SECRET, ALGORITHM
from app.db.postgis import get_db

logger = logging.getLogger(__name__)

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")

router = APIRouter(tags=["auth"])


# ── helpers ───────────────────────────────────────────────────────────────────

def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _make_jwt(user_id: int, email: str, role: str) -> str:
    payload = {
        "sub": email,
        "user_id": user_id,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=24),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)


# ── schemas ───────────────────────────────────────────────────────────────────

class GoogleCredential(BaseModel):
    credential: str


class KeyCreate(BaseModel):
    name: str


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post("/auth/google")
async def google_auth(body: GoogleCredential, db=Depends(get_db)):
    """Verify Google id_token, upsert user, return platform JWT."""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=503, detail="Google OAuth not configured")
    try:
        info = id_token.verify_oauth2_token(
            body.credential,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {exc}")

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
        google_sub, email, name,
    )
    token = _make_jwt(row["id"], email, row["role"])
    return {"token": token, "user": {"email": email, "name": name, "role": row["role"]}}


@router.get("/keys")
async def list_keys(db=Depends(get_db), user: dict = Depends(verify_access)):
    """List the caller's API keys."""
    rows = await db.fetch(
        """
        SELECT id, key_prefix, name, quota_day, calls_today,
               quota_reset_at, is_active, last_used_at, created_at
        FROM api_keys
        WHERE user_id = (SELECT id FROM api_users WHERE email = $1)
        ORDER BY created_at DESC
        """,
        user["user"],
    )
    return [dict(r) for r in rows]


@router.post("/keys", status_code=201)
async def create_key(body: KeyCreate, db=Depends(get_db), user: dict = Depends(verify_access)):
    """Generate a new API key. Raw key returned exactly once."""
    raw_key = secrets.token_urlsafe(32)
    prefix = raw_key[:8]
    key_hash = _sha256(raw_key)

    await db.execute(
        """
        INSERT INTO api_keys (user_id, key_prefix, key_hash, name)
        VALUES ((SELECT id FROM api_users WHERE email = $1), $2, $3, $4)
        """,
        user["user"], prefix, key_hash, body.name,
    )
    return {
        "key": raw_key,
        "warning": "Store this now. It will not be shown again.",
    }


@router.delete("/keys/{key_prefix}", status_code=204)
async def revoke_key(key_prefix: str, db=Depends(get_db), user: dict = Depends(verify_access)):
    """Deactivate a key. Only the owner can revoke their own key."""
    result = await db.execute(
        """
        UPDATE api_keys SET is_active = FALSE
        WHERE key_prefix = $1
          AND user_id = (SELECT id FROM api_users WHERE email = $2)
          AND is_active = TRUE
        """,
        key_prefix, user["user"],
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Key not found or already revoked")


@router.get("/admin/usage")
async def admin_usage(db=Depends(get_db), user: dict = Depends(verify_access)):
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
