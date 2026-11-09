from fastapi import APIRouter, Depends, Query
from typing import List, Optional
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import require_scope

router = APIRouter(prefix="/air-quality", tags=["Air Quality"])


class AirQualityRecord(BaseModel):
    station_id: str
    city_name: Optional[str] = None
    event_time: str
    aqi: int
    aqi_label: str
    pm2_5: Optional[float] = None
    pm10: Optional[float] = None
    no2: Optional[float] = None
    o3: Optional[float] = None
    so2: Optional[float] = None
    co: Optional[float] = None
    nh3: Optional[float] = None
    no: Optional[float] = None
    latitude: float
    longitude: float


@router.get("/", response_model=List[AirQualityRecord])
async def get_air_quality(
    station_id: Optional[str] = Query(default=None, max_length=100),
    latest: bool = Query(
        default=False, description="Only the newest reading per station"
    ),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
    user_info: dict = Depends(require_scope("live_air_quality")),
):
    """Hourly air quality per Kerala station. AQI scale: 1 Good .. 5 Very Poor."""
    records = await db.fetch(
        f"""
        SELECT {"DISTINCT ON (station_id)" if latest else ""}
               station_id, city_name, event_time::text, aqi,
               (ARRAY['Good','Fair','Moderate','Poor','Very Poor'])[aqi] AS aqi_label,
               pm2_5, pm10, no2, o3, so2, co, nh3, no, latitude, longitude
        FROM air_quality_observations
        WHERE $1::text IS NULL OR station_id = $1
        ORDER BY {"station_id, " if latest else ""}event_time DESC
        LIMIT $2 OFFSET $3
        """,
        station_id,
        limit,
        offset,
    )
    return [dict(r) for r in records]
