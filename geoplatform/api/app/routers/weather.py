from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access, mask_pii

router = APIRouter(prefix="/weather", tags=["Weather"])

class WeatherRecord(BaseModel):
    station_id: str
    city_name: Optional[str] = None
    event_time: str
    temperature_c: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_speed_ms: Optional[float] = None
    cloud_cover_pct: Optional[float] = None
    rainfall_1h_mm: Optional[float] = None
    weather_condition: Optional[str] = None
    quality_flag: Optional[str] = None
    is_anomaly: Optional[bool] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

@router.get("/", response_model=List[WeatherRecord])
async def get_weather(
    station_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=1000),
    anomalies_only: bool = Query(default=False),
    db=Depends(get_db),
    user_info: dict = Depends(verify_access)
):
    """Live weather stream observations (from Flink-processed Redpanda stream)."""
    conditions = ["1=1"]
    args = []

    if station_id:
        args.append(station_id)
        conditions.append(f"station_id = ${len(args)}")

    if anomalies_only:
        conditions.append("is_anomaly = TRUE")

    args.append(limit)
    where = " AND ".join(conditions)
    query = f"""
        SELECT station_id, city_name, event_time::text, temperature_c,
               humidity_pct, wind_speed_ms, cloud_cover_pct, rainfall_1h_mm,
               weather_condition, quality_flag, is_anomaly, latitude, longitude
        FROM weather_stream_observations
        WHERE {where}
        ORDER BY event_time DESC LIMIT ${len(args)}
    """
    records = await db.fetch(query, *args)
    results = []
    for r in records:
        data = mask_pii(dict(r), "weather_stream_observations", user_info["privileged"])
        results.append(WeatherRecord(**data))
    return results
