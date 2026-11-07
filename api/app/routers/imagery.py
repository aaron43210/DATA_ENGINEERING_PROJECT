from fastapi import APIRouter, Depends, Query
from typing import List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access

router = APIRouter(prefix="/imagery", tags=["Satellite Imagery"])


class SatelliteRecord(BaseModel):
    scene_id: str
    satellite: str
    acquisition_time: str
    cloud_percentage: float
    image_quality: str


@router.get("/", response_model=List[SatelliteRecord])
async def get_imagery(
    max_cloud_pct: float = Query(default=50.0, le=100.0),
    limit: int = Query(default=50, le=500),
    db=Depends(get_db),
    user_info: dict = Depends(verify_access),
):
    """Satellite observations from the Gold Data Product mart."""
    records = await db.fetch(
        """SELECT scene_id, satellite_name AS satellite,
                  acquisition_time::text, cloud_percentage, image_quality
           FROM marts.satellite_observation
           WHERE cloud_percentage <= $1
           ORDER BY acquisition_time DESC LIMIT $2""",
        max_cloud_pct,
        limit,
    )
    return [dict(r) for r in records]
