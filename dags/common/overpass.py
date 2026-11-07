"""
Overpass API client and OSM element -> WKT conversion.

The public Overpass servers regularly return 502/504 under load, so every
query walks a list of mirrors before giving up.

Query with ``out geom`` so ways and relations carry their own coordinates.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

DEFAULT_MIRRORS = "https://overpass.kumi.systems/api/interpreter"
USER_AGENT = "GeospatialDataPlatform/2.0"

# Rebuilds a (multi)polygon from a relation's member ways, sent as a
# MULTILINESTRING. ST_LineMerge joins the ways into rings; ST_BuildArea turns the
# rings into polygons, treating nested rings as holes.
AREA_FROM_WAYS_SQL = (
    "ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_BuildArea(ST_LineMerge("
    "ST_GeomFromText(%s, 4326)))), 3))"
)


def overpass_urls() -> List[str]:
    primary = os.environ.get(
        "OVERPASS_API_URL", "https://overpass-api.de/api/interpreter"
    )
    mirrors = os.environ.get("OVERPASS_MIRROR_URLS", DEFAULT_MIRRORS)
    urls = [primary] + [u.strip() for u in mirrors.split(",") if u.strip()]
    return list(dict.fromkeys(urls))


def run_overpass(query: str, timeout_seconds: int) -> List[Dict[str, Any]]:
    """Run a query, trying each mirror in turn. Returns the ``elements`` list."""
    errors = []
    for url in overpass_urls():
        try:
            logger.info("Querying Overpass at %s", url)
            response = requests.post(
                url,
                data={"data": query},
                timeout=timeout_seconds + 30,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            payload = response.json()
            # Overpass reports server-side timeouts in a 200 response body.
            remark = payload.get("remark", "")
            if "runtime error" in remark:
                raise RuntimeError(remark)
            return payload.get("elements", [])
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            logger.warning("Overpass mirror %s failed: %s", url, exc)
            errors.append(f"{url}: {exc}")
    raise RuntimeError("All Overpass mirrors failed: " + " | ".join(errors))


def _coords(points: List[Dict[str, float]]) -> str:
    return ", ".join(f"{p['lon']} {p['lat']}" for p in points)


def to_wkt(element: Dict[str, Any]) -> Optional[Tuple[str, bool]]:
    """
    Convert an OSM element to ``(wkt, is_relation)``, or None if unusable.

    Relations come back as a MULTILINESTRING of their member ways and must be
    inserted through AREA_FROM_WAYS_SQL.
    """
    kind = element.get("type")

    if kind == "node":
        if "lon" in element and "lat" in element:
            return f"POINT({element['lon']} {element['lat']})", False
        return None

    if kind == "way":
        points = element.get("geometry") or []
        if len(points) < 2:
            return None
        is_closed = len(points) >= 4 and points[0] == points[-1]
        if is_closed and "highway" not in element.get("tags", {}):
            return f"POLYGON(({_coords(points)}))", False
        return f"LINESTRING({_coords(points)})", False

    if kind == "relation":
        lines = [
            f"({_coords(member['geometry'])})"
            for member in element.get("members", [])
            if member.get("type") == "way"
            and member.get("role") in ("outer", "inner", "")
            and len(member.get("geometry") or []) >= 2
        ]
        if lines:
            return f"MULTILINESTRING({', '.join(lines)})", True
    return None
