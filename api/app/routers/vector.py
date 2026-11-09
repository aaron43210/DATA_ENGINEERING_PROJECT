from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import require_scope

router = APIRouter(prefix="/vector", tags=["Vector Features"])


class VectorRecord(BaseModel):
    id: int
    osm_type: Optional[str] = None
    osm_id: Optional[int] = None
    feature_type: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[dict] = None
    geometry: Optional[dict] = None


@router.get("/features", response_model=List[VectorRecord])
async def get_vector_features(
    feature_type: Optional[str] = Query(
        default=None, description="hospital, trunk, etc."
    ),
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
    user_info: dict = Depends(require_scope("osm_vector_product")),
):
    """OSM vector features (hospitals, roads) loaded from the Bronze layer."""
    conditions = ["1=1"]
    args = []

    if feature_type:
        args.append(feature_type)
        conditions.append(f"feature_type = ${len(args)}")

    args.extend([limit, offset])
    where = " AND ".join(conditions)
    query = f"""
        SELECT id, osm_type, osm_id, feature_type, name, tags,
               ST_AsGeoJSON(geometry)::jsonb AS geometry
        FROM vector_features
        WHERE {where}
        ORDER BY id DESC LIMIT ${len(args) - 1} OFFSET ${len(args)}
    """
    records = await db.fetch(query, *args)
    return [dict(r) for r in records]
