"""Loads public source data straight into PostGIS for the AWS deployment.

Airflow, Redpanda and Flink run only in the local Docker stack, so AWS runs
this on a schedule instead (see infra/terraform/ingest.tf).

    python ingest.py catalog satellite osm weather
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

from app.db.postgis import get_db_pool

# west, south, east, north — same Kerala box as the Airflow DAGs
BBOX = (74.8, 8.1, 77.5, 12.8)
STATIONS = [
    {
        "station_id": "WX_TVM",
        "city_name": "Thiruvananthapuram",
        "lat": 8.5241,
        "lon": 76.9366,
    },
    {"station_id": "WX_COK", "city_name": "Kochi", "lat": 9.9312, "lon": 76.2673},
    {"station_id": "WX_CCJ", "city_name": "Kozhikode", "lat": 11.2588, "lon": 75.7804},
    {"station_id": "WX_TCR", "city_name": "Thrissur", "lat": 10.5276, "lon": 76.2144},
    {"station_id": "WX_QLN", "city_name": "Kollam", "lat": 8.8932, "lon": 76.6141},
]
# Products with a live source in AWS; the rest need the local streaming/dbt stack.
SHOWCASED = ["satellite_imagery", "osm_vector_product", "live_weather_stream"]
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
STAC_URL = "https://earth-search.aws.element84.com/v1/search"
OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

http = requests.Session()
http.headers["User-Agent"] = "GeospatialDataPlatform/2.0"


class IngestError(RuntimeError):
    """Failure whose message is safe to log (no URLs, no keys)."""


def classify(tags: dict) -> str:
    if tags.get("amenity"):
        return tags["amenity"]
    if tags.get("emergency"):
        return tags["emergency"]
    if tags.get("highway"):
        return f"highway_{tags['highway']}"
    return "other"


def osm_row(el: dict):
    """Overpass element -> (osm_type, osm_id, feature_type, name, tags, wkt), or None."""
    tags = el.get("tags", {})
    if el.get("geometry"):
        coords = ", ".join(f"{p['lon']} {p['lat']}" for p in el["geometry"])
        wkt = f"LINESTRING({coords})"
    elif "center" in el:
        wkt = f"POINT({el['center']['lon']} {el['center']['lat']})"
    elif "lat" in el:
        wkt = f"POINT({el['lon']} {el['lat']})"
    else:
        return None
    return el["type"], el["id"], classify(tags), tags.get("name"), tags, wkt


def weather_row(station: dict, d: dict):
    return (
        station["station_id"],
        station["city_name"],
        datetime.fromtimestamp(d["dt"], timezone.utc),
        station["lat"],
        station["lon"],
        d["main"].get("temp"),
        d["main"].get("humidity"),
        d.get("wind", {}).get("speed"),
        d.get("clouds", {}).get("all"),
        d.get("rain", {}).get("1h", 0.0),
        d["weather"][0]["main"] if d.get("weather") else None,
    )


def satellite_row(item: dict):
    props = item.get("properties", {})
    if (
        props.get("eo:cloud_cover") is None
        or not props.get("datetime")
        or len(item.get("bbox", [])) != 4
    ):
        return None
    return (
        item["id"],
        props.get("platform", "sentinel-2"),
        datetime.fromisoformat(props["datetime"].replace("Z", "+00:00")),
        float(props["eo:cloud_cover"]),
        *item["bbox"],
    )


async def catalog(conn) -> int:
    result = await conn.execute(
        "UPDATE data_products SET status = CASE WHEN product_id = ANY($1::text[]) "
        "THEN 'active' ELSE 'inactive' END, updated_at = NOW()",
        SHOWCASED,
    )
    return int(result.split()[-1])


async def weather(conn) -> int:
    key = os.environ.get("OPENWEATHER_API_KEY", "")
    if not key or key.startswith("YOUR_"):
        raise IngestError("OPENWEATHER_API_KEY is not set")
    rows = []
    for station in STATIONS:
        try:
            r = http.get(
                OWM_URL,
                params={
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "appid": key,
                    "units": "metric",
                },
                timeout=15,
            )
        except requests.RequestException as exc:
            # requests' messages include the URL, which carries the appid.
            raise IngestError(
                f"OpenWeather unreachable ({type(exc).__name__})"
            ) from None
        if r.status_code != 200:
            raise IngestError(
                f"OpenWeather HTTP {r.status_code} for {station['station_id']}"
            )
        rows.append(weather_row(station, r.json()))
    await conn.executemany(
        """INSERT INTO weather_stream_observations
               (station_id, city_name, event_time, latitude, longitude, temperature_c,
                humidity_pct, wind_speed_ms, cloud_cover_pct, rainfall_1h_mm,
                weather_condition, source_topic)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'openweathermap')
           ON CONFLICT (station_id, event_time) DO NOTHING""",
        rows,
    )
    return len(rows)


async def satellite(conn) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    r = http.post(
        STAC_URL,
        json={
            "collections": ["sentinel-2-c1-l2a"],
            "bbox": list(BBOX),
            "datetime": f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}",
            "limit": 100,
        },
        timeout=60,
    )
    r.raise_for_status()
    rows = [row for row in map(satellite_row, r.json().get("features", [])) if row]
    await conn.executemany(
        """INSERT INTO satellite_observations (scene_id, satellite, acquisition_time, cloud_percentage, bbox)
           VALUES ($1, $2, $3, $4, ST_MakeEnvelope($5, $6, $7, $8, 4326))
           ON CONFLICT (scene_id) DO NOTHING""",
        rows,
    )
    return len(rows)


async def osm(conn) -> int:
    w, s, e, n = BBOX
    box = f"{s},{w},{n},{e}"
    query = f"""[out:json][timeout:180];
(nwr["amenity"~"^(hospital|clinic|doctors)$"]({box}); node["emergency"="ambulance_station"]({box}););
out center tags;
way["highway"~"^(trunk|primary)$"]({box});
out geom tags;"""
    elements = None
    for url in OVERPASS_URLS:
        try:
            r = http.post(url, data={"data": query}, timeout=240)
            r.raise_for_status()
            elements = r.json().get("elements", [])
            break
        except (requests.RequestException, ValueError) as exc:
            print(
                f"osm: {url} failed ({type(exc).__name__}), trying next mirror",
                flush=True,
            )
    if elements is None:
        raise IngestError("all Overpass mirrors failed")
    rows = [row for row in map(osm_row, elements) if row]
    await conn.executemany(
        """INSERT INTO vector_features (osm_type, osm_id, feature_type, name, tags, geometry)
           VALUES ($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326))
           ON CONFLICT (osm_type, osm_id) DO UPDATE SET
               feature_type = EXCLUDED.feature_type, name = EXCLUDED.name,
               tags = EXCLUDED.tags, geometry = EXCLUDED.geometry, ingested_at = NOW()""",
        rows,
    )
    return len(rows)


JOBS = {"catalog": catalog, "satellite": satellite, "osm": osm, "weather": weather}


async def main(names) -> int:
    unknown = [n for n in names if n not in JOBS]
    if not names or unknown:
        print(f"usage: python ingest.py {' '.join(JOBS)}", file=sys.stderr)
        return 2
    pool = await get_db_pool()
    failed = []
    try:
        async with pool.acquire() as conn:
            for name in names:
                try:
                    print(f"{name}: {await JOBS[name](conn)} rows", flush=True)
                except Exception as exc:
                    failed.append(name)
                    detail = exc if isinstance(exc, IngestError) else type(exc).__name__
                    print(f"{name}: FAILED ({detail})", flush=True)
    finally:
        await pool.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
