from datetime import datetime, timezone

from ingest import osm_row, satellite_row, weather_row


def test_osm_row_handles_points_centers_and_lines():
    node = {"type": "node", "id": 1, "lat": 10.0, "lon": 76.0, "tags": {"amenity": "hospital", "name": "GH"}}
    way = {"type": "way", "id": 2, "center": {"lat": 9.5, "lon": 76.5}, "tags": {"amenity": "clinic"}}
    road = {"type": "way", "id": 3, "tags": {"highway": "trunk"},
            "geometry": [{"lat": 8.0, "lon": 77.0}, {"lat": 8.1, "lon": 77.1}]}

    assert osm_row(node) == ("node", 1, "hospital", "GH", node["tags"], "POINT(76.0 10.0)")
    assert osm_row(way)[2:] == ("clinic", None, way["tags"], "POINT(76.5 9.5)")
    assert osm_row(road)[2] == "highway_trunk"
    assert osm_row(road)[5] == "LINESTRING(77.0 8.0, 77.1 8.1)"
    assert osm_row({"type": "relation", "id": 4, "tags": {}}) is None


def test_weather_row_defaults_missing_rain_to_zero():
    station = {"station_id": "WX_COK", "city_name": "Kochi", "lat": 9.9, "lon": 76.2}
    row = weather_row(station, {"dt": 0, "main": {"temp": 30.1, "humidity": 80},
                                "wind": {"speed": 3.2}, "clouds": {"all": 40},
                                "weather": [{"main": "Clouds"}]})

    assert row == ("WX_COK", "Kochi", datetime(1970, 1, 1, tzinfo=timezone.utc), 9.9, 76.2,
                   30.1, 80, 3.2, 40, 0.0, "Clouds")


def test_satellite_row_skips_scenes_without_cloud_cover():
    item = {"id": "S2A_X", "bbox": [76, 9, 77, 10],
            "properties": {"platform": "sentinel-2a", "datetime": "2026-11-01T05:10:00Z", "eo:cloud_cover": 12.5}}

    assert satellite_row(item) == ("S2A_X", "sentinel-2a",
                                   datetime(2026, 11, 1, 5, 10, tzinfo=timezone.utc), 12.5, 76, 9, 77, 10)
    assert satellite_row({**item, "properties": {"datetime": "2026-11-01T05:10:00Z"}}) is None
