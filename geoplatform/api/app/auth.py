import os
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
import jwt
from datetime import datetime, timedelta, timezone

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
API_KEY = os.environ.get("API_KEY", "supersecretapikey123")
ALGORITHM = "HS256"

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
                # Truncate to 3 decimal places (city-level precision only)
                data[field] = round(data[field], 3)
            else:
                data[field] = "***MASKED***"
    return data

def verify_access(
    api_key: str = Security(api_key_header),
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> dict:
    """Accept either API Key or JWT Bearer token."""
    # API Key auth
    if api_key and api_key == API_KEY:
        return {"user": "api-key-user", "role": "admin", "privileged": True}
    
    # JWT auth
    if credentials:
        try:
            payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[ALGORITHM])
            return {
                "user": payload["sub"],
                "role": payload.get("role", "reader"),
                "privileged": payload.get("role") == "admin"
            }
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
            
    raise HTTPException(status_code=401, detail="No authentication provided")
