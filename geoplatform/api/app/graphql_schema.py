import strawberry
from strawberry.types import Info
from typing import Optional, List
from app.auth import mask_pii

@strawberry.type
class SatelliteObservation:
    scene_id: str
    satellite: str
    acquisition_time: str
    cloud_percentage: float
    image_quality: str

@strawberry.type
class WeatherObservation:
    station_id: str
    city_name: str
    event_time: str
    temperature_c: Optional[float]
    humidity_pct: Optional[float]
    wind_speed_ms: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]

@strawberry.type
class SensorReading:
    sensor_id: str
    event_time: str
    measurement_type: str
    value: float
    unit: str
    is_anomaly: bool
    latitude: Optional[float]
    longitude: Optional[float]

def get_auth_status(info: Info) -> dict:
    """Extract auth info injected by the FastAPI route dependency."""
    request = info.context["request"]
    return request.state.user_info

@strawberry.type
class Query:
    @strawberry.field
    async def imagery(
        self, info: Info,
        max_cloud_pct: float = 100.0,
        limit: int = 50
    ) -> List[SatelliteObservation]:
        """Query the satellite_observation Data Product."""
        db = info.context["db"]
        
        query = """
            SELECT scene_id, satellite_name as satellite, acquisition_time, 
                   cloud_percentage, image_quality 
            FROM marts.satellite_observation
            WHERE cloud_percentage <= $1
            ORDER BY acquisition_time DESC
            LIMIT $2
        """
        records = await db.fetch(query, max_cloud_pct, limit)
        return [SatelliteObservation(**dict(r)) for r in records]

    @strawberry.field
    async def weather(
        self, info: Info,
        station_id: Optional[str] = None,
        limit: int = 50
    ) -> List[WeatherObservation]:
        """Query the live weather stream observations (with GDPR masking)."""
        db = info.context["db"]
        user_info = get_auth_status(info)
        
        query = """
            SELECT station_id, city_name, event_time, temperature_c, 
                   humidity_pct, wind_speed_ms, latitude, longitude
            FROM weather_stream_observations
        """
        args = []
        if station_id:
            query += " WHERE station_id = $1"
            args.append(station_id)
        
        query += f" ORDER BY event_time DESC LIMIT ${len(args)+1}"
        args.append(limit)
        
        records = await db.fetch(query, *args)
        
        results = []
        for r in records:
            data = dict(r)
            # Apply GDPR masking
            masked_data = mask_pii(data, "weather_stream_observations", user_info["privileged"])
            results.append(WeatherObservation(**masked_data))
            
        return results

schema = strawberry.Schema(query=Query)
