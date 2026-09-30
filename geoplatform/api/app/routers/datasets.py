from fastapi import APIRouter, Depends, Request
from typing import List
from pydantic import BaseModel
from app.db.postgis import get_db
from app.auth import verify_access

router = APIRouter(prefix="/datasets", tags=["Data Products"])

class DataProduct(BaseModel):
    product_id: str
    name: str
    domain: str
    owner_team: str
    description: str
    sla_freshness_hours: int
    output_port_rest: str
    output_port_graphql: str

@router.get("/", response_model=List[DataProduct])
async def list_data_products(
    request: Request,
    db = Depends(get_db),
    user_info: dict = Depends(verify_access)
):
    """
    Data Mesh Data-as-a-Product Catalog.
    Returns the registry of all available data products and their output ports.
    """
    query = """
        SELECT product_id, name, domain, owner_team, description, 
               sla_freshness_hours, output_port_rest, output_port_graphql 
        FROM data_products 
        WHERE status = 'active'
    """
    records = await db.fetch(query)
    return [dict(r) for r in records]
