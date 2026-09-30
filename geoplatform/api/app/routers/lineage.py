from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access

router = APIRouter(prefix="/lineage", tags=["Data Lineage"])

class LineageEvent(BaseModel):
    run_id: str
    job_name: str
    job_namespace: str
    event_type: str
    event_time: str
    input_datasets: Optional[list] = None
    output_datasets: Optional[list] = None

@router.get("/{job_name}", response_model=List[LineageEvent])
async def get_lineage(
    job_name: str,
    limit: int = Query(default=20, le=100),
    db=Depends(get_db),
    user_info: dict = Depends(verify_access)
):
    """
    Query data lineage events for a specific job.
    Primary lineage is in DataHub at http://localhost:9002.
    This endpoint exposes a lightweight queryable copy stored in PostgreSQL.
    """
    records = await db.fetch(
        """SELECT run_id, job_name, job_namespace, event_type,
                  event_time::text, input_datasets, output_datasets
           FROM lineage_events
           WHERE job_name ILIKE $1
           ORDER BY event_time DESC LIMIT $2""",
        f"%{job_name}%", limit
    )
    return [dict(r) for r in records]
