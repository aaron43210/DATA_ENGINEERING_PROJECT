from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access, mask_pii

router = APIRouter(prefix="/sensors", tags=["IoT Sensors"])

class SensorRecord(BaseModel):
    sensor_id: str
    event_time: str
    measurement_type: str
    value: float
    unit: str
    quality_flag: Optional[str] = None
    window_avg: Optional[float] = None
    is_anomaly: bool
    nearest_weather_station: Optional[str] = None
    ambient_temperature_c: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

@router.get("/", response_model=List[SensorRecord])
async def get_sensors(
    sensor_id: Optional[str] = Query(default=None),
    measurement_type: Optional[str] = Query(default=None, description="temperature, humidity, soil_moisture, air_quality"),
    anomalies_only: bool = Query(default=False),
    limit: int = Query(default=100, le=1000),
    db=Depends(get_db),
    user_info: dict = Depends(verify_access)
):
    """Real-time IoT sensor observations with ambient weather join and anomaly flags."""
    conditions = ["1=1"]
    args = []

    if sensor_id:
        args.append(sensor_id)
        conditions.append(f"sensor_id = ${len(args)}")
    if measurement_type:
        args.append(measurement_type)
        conditions.append(f"measurement_type = ${len(args)}")
    if anomalies_only:
        conditions.append("is_anomaly = TRUE")

    args.append(limit)
    where = " AND ".join(conditions)
    query = f"""
        SELECT sensor_id, event_time::text, measurement_type, value, unit,
               quality_flag, window_avg, is_anomaly,
               nearest_weather_station, ambient_temperature_c,
               latitude, longitude
        FROM sensor_observations
        WHERE {where}
        ORDER BY event_time DESC LIMIT ${len(args)}
    """
    records = await db.fetch(query, *args)
    results = []
    for r in records:
        data = mask_pii(dict(r), "sensor_observations", user_info["privileged"])
        results.append(SensorRecord(**data))
    return results
