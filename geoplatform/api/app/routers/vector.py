from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access

router = APIRouter(prefix="/vector", tags=["Vector Features"])


class VectorRecord(BaseModel):
    id: int
    osm_id: Optional[int] = None
    feature_type: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[dict] = None


@router.get("/features", response_model=List[VectorRecord])
async def get_vector_features(
    feature_type: Optional[str] = Query(
        default=None, description="hospital, trunk, etc."
    ),
    limit: int = Query(default=100, le=500),
    db=Depends(get_db),
    user_info: dict = Depends(verify_access),
):
    """OSM vector features (hospitals, roads) loaded from the Bronze layer."""
    conditions = ["1=1"]
    args = []

    if feature_type:
        args.append(feature_type)
        conditions.append(f"feature_type = ${len(args)}")

    args.append(limit)
    where = " AND ".join(conditions)
    query = f"""
        SELECT id, osm_id, feature_type, name, tags
        FROM vector_features
        WHERE {where}
        ORDER BY id DESC LIMIT ${len(args)}
    """
    records = await db.fetch(query, *args)
    return [dict(r) for r in records]
