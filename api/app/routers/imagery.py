from fastapi import APIRouter, Depends, Query
from typing import List, Optional
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import require_scope

router = APIRouter(prefix="/imagery", tags=["Satellite Imagery"])


class SatelliteRecord(BaseModel):
    scene_id: str
    satellite: str
    acquisition_time: str
    cloud_percentage: float
    image_quality: str
    footprint: Optional[dict] = None


@router.get("/", response_model=List[SatelliteRecord])
async def get_imagery(
    max_cloud_pct: float = Query(default=50.0, le=100.0),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
    user_info: dict = Depends(require_scope("satellite_imagery")),
):
    """Sentinel-2 scenes; quality bands match the dbt satellite_observation mart."""
    records = await db.fetch(
        """SELECT scene_id, satellite,
                  acquisition_time::text, cloud_percentage,
                  CASE WHEN cloud_percentage < 10 THEN 'Excellent'
                       WHEN cloud_percentage < 30 THEN 'Good'
                       ELSE 'Poor' END AS image_quality,
                  ST_AsGeoJSON(bbox)::jsonb AS footprint
           FROM satellite_observations
           WHERE cloud_percentage <= $1
           ORDER BY acquisition_time DESC LIMIT $2 OFFSET $3""",
        max_cloud_pct,
        limit,
        offset,
    )
    return [dict(r) for r in records]
