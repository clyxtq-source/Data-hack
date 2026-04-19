from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any
from zoneinfo import ZoneInfo

import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from shapely import wkb


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DRIVE_GRAPH_PATH = DATA_DIR / "sydney_drive.graphml"
WALK_GRAPH_PATH = DATA_DIR / "sydney_walk.graphml"
NETWORK_GEOJSON_PATH = DATA_DIR / "sydney_drive_network.geojson"
TRAFFIC_CSV_PATH = BASE_DIR / "road_traffic_counts_hourly_sample_0.csv"
TRAFFIC_MODEL_PATH = DATA_DIR / "traffic_station_profiles.json"
SYDNEY_PLACE = "Sydney, New South Wales, Australia"
SYDNEY_TZ = ZoneInfo("Australia/Sydney")
HTTP_HEADERS = {"User-Agent": "SydneyTrafficPlanner/1.0"}
WALKING_SPEED_KPH = 5.0
EFFECTIVE_WALKING_SPEED_KPH = 4.5
DEFAULT_DRIVE_SPEED_KPH = 32.0
OFF_ROUTE_THRESHOLD_METERS = 120.0
TFNSW_BASE_URL = "https://api.transport.nsw.gov.au/v1/tp"
SYDNEY_VIEWBOX = "150.52,-34.35,151.55,-33.10"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
ROUTE_DEADLINE_SECONDS = 15

FAMOUS_SYDNEY_PLACES = [
    "Sydney Opera House",
    "Bondi Beach",
    "Sydney Harbour Bridge",
    "Darling Harbour",
    "Circular Quay",
    "Taronga Zoo Sydney",
    "The Rocks",
    "Manly Beach",
    "Royal Botanic Garden Sydney",
    "University of Sydney",
    "Sydney Airport",
    "Barangaroo Reserve",
    "Luna Park Sydney",
    "Australian Museum",
    "Coogee Beach",
    "Hyde Park Sydney",
]

KNOWN_PLACE_COORDINATES = {
    "sydney opera house": (-33.857198, 151.2151234),
    "bondi beach": (-33.890842, 151.274292),
    "sydney harbour bridge": (-33.8523063, 151.2107876),
    "darling harbour": (-33.87488, 151.19871),
    "circular quay": (-33.8610, 151.2127),
    "taronga zoo sydney": (-33.8430, 151.2410),
    "the rocks": (-33.8599, 151.2090),
    "manly beach": (-33.7972, 151.2887),
    "royal botanic garden sydney": (-33.8642, 151.2166),
    "university of sydney": (-33.8887, 151.1876),
    "sydney airport": (-33.9399, 151.1753),
    "barangaroo reserve": (-33.8541, 151.2012),
    "luna park sydney": (-33.8473, 151.2107),
    "australian museum": (-33.8748, 151.2130),
    "coogee beach": (-33.9206, 151.2552),
    "hyde park sydney": (-33.8731, 151.2110),
}

ox.settings.use_cache = True
ox.settings.log_console = False
load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Sydney Traffic")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_graph_lock = Lock()
_drive_graph: nx.MultiDiGraph | None = None
_walk_graph: nx.MultiDiGraph | None = None
_traffic_lock = Lock()
_traffic_model: "TrafficModel | None" = None
_transit_cache_lock = Lock()
_transit_cache: dict[str, dict[str, Any]] = {}
_weather_cache_lock = Lock()
_weather_cache: dict[str, dict[str, Any]] = {}
_place_name_cache_lock = Lock()
_place_name_cache: dict[str, str] = {}
_preload_status_lock = Lock()
_preload_status: dict[str, Any] = {"started": False, "finished": False, "errors": []}


@app.on_event("startup")
def preload_application_assets() -> None:
    warm_runtime_assets()


class RouteRequest(BaseModel):
    origin: str = Field(..., description="Start point address or coordinates")
    destination: str = Field(..., description="Destination address or coordinates")
    profile: str = Field(default="drive", pattern="^(drive|walk|transit|combined)$")
    max_walk_distance_km: float | None = Field(default=None, ge=0)


class LocationRequest(BaseModel):
    lat: float
    lng: float


class ReverseGeocodeRequest(BaseModel):
    lat: float
    lng: float


@dataclass
class TrafficModel:
    station_keys: np.ndarray
    latitudes: np.ndarray
    longitudes: np.ndarray
    weekday_profile: np.ndarray
    weekend_profile: np.ndarray
    baseline_hourly_volume: np.ndarray

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "TrafficModel":
        stations = payload.get("stations", [])
        return cls(
            station_keys=np.array([int(item["station_key"]) for item in stations], dtype=np.int64),
            latitudes=np.array([float(item["lat"]) for item in stations], dtype=np.float64),
            longitudes=np.array([float(item["lng"]) for item in stations], dtype=np.float64),
            weekday_profile=np.array([item["weekday_profile"] for item in stations], dtype=np.float64),
            weekend_profile=np.array([item["weekend_profile"] for item in stations], dtype=np.float64),
            baseline_hourly_volume=np.array([float(item["baseline_hourly_volume"]) for item in stations], dtype=np.float64),
        )

    def nearest_station_index(self, lat: float, lng: float) -> tuple[int, float]:
        lat1 = np.radians(lat)
        lng1 = np.radians(lng)
        lat2 = np.radians(self.latitudes)
        lng2 = np.radians(self.longitudes)

        dlat = lat2 - lat1
        dlng = lng2 - lng1
        hav = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2) ** 2
        distances_km = 6371.0088 * 2 * np.arctan2(np.sqrt(hav), np.sqrt(1 - hav))
        best_index = int(np.argmin(distances_km))
        return best_index, float(distances_km[best_index])

    def congestion_multiplier(self, lat: float, lng: float, current_dt: datetime) -> float:
        if self.station_keys.size == 0:
            return 1.0

        station_index, distance_km = self.nearest_station_index(lat, lng)
        if distance_km > 4.0:
            return 1.0

        hour_index = current_dt.hour
        profile = self.weekend_profile if current_dt.weekday() >= 5 else self.weekday_profile
        ratio = float(profile[station_index, hour_index])

        # Counts dataset contains volumes, not observed speeds, so we convert demand intensity
        # into a conservative travel-time multiplier.
        if ratio <= 0:
            ratio = 1.0

        if ratio >= 1.0:
            multiplier = 1.0 + 0.34 * min(ratio - 1.0, 2.0)
        else:
            multiplier = max(0.9, 1.0 - 0.1 * (1.0 - ratio))

        distance_decay = max(0.3, 1.0 - distance_km / 6.0)
        return round(1.0 + (multiplier - 1.0) * distance_decay, 4)


def parse_coordinate_query(query: str) -> tuple[float, float] | None:
    parts = [part.strip() for part in query.split(",")]
    if len(parts) != 2:
        return None

    try:
        lat = float(parts[0])
        lng = float(parts[1])
    except ValueError:
        return None

    if -90 <= lat <= 90 and -180 <= lng <= 180:
        return lat, lng
    return None


def normalize_location_query(query: str) -> str:
    cleaned = query.strip()
    cleaned = re.sub(r"[\[\]\{\}\(\)]+", " ", cleaned)
    cleaned = cleaned.replace("|", " ").replace(";", ",")
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"(,\s*){2,}", ", ", cleaned)
    return cleaned.strip(" ,")


def build_location_variants(query: str) -> list[str]:
    normalized = normalize_location_query(query)
    if not normalized:
        return []

    variants: list[str] = []

    def add_variant(value: str) -> None:
        candidate = normalize_location_query(value)
        if candidate and candidate not in variants:
            variants.append(candidate)

    add_variant(normalized)

    raw_parts = [part.strip() for part in normalized.split(",") if part.strip()]
    parts = [
        part
        for part in raw_parts
        if "council of" not in part.lower()
        and "city of" not in part.lower()
        and "municipal" not in part.lower()
    ]
    if len(parts) >= 3:
        first, second, third, *rest = parts
        if any(char.isdigit() for char in second) and not any(char.isdigit() for char in first):
            add_variant(", ".join([f"{second} {third}", first, *rest]))
            add_variant(", ".join([f"{second} {third}", *rest]))
            add_variant(", ".join([third, first, *rest]))
        if any(char.isdigit() for char in third) and not any(char.isdigit() for char in first):
            add_variant(", ".join([f"{third} {second}", first, *rest]))

    if len(parts) >= 4 and not any(char.isdigit() for char in parts[0]) and any(char.isdigit() for char in parts[1]):
        add_variant(", ".join(parts[1:]))

    if len(parts) >= 2 and any(char.isdigit() for char in parts[0]) and not any(char.isdigit() for char in parts[1]):
        add_variant(", ".join([f"{parts[0]} {parts[1]}", *parts[2:]]))

    # Trim noisy prefixes/suffixes so detailed user-entered addresses still resolve.
    if len(parts) >= 4:
        add_variant(", ".join(parts[:6]))
        add_variant(", ".join(parts[1:6]))
        add_variant(", ".join(parts[-5:]))

    compact_parts = ["NSW" if part.lower() == "new south wales" else part for part in parts]
    if compact_parts != parts:
        add_variant(", ".join(compact_parts))

    # Common pattern: Building name, street number, street, suburb, city, state, postcode, country
    if len(parts) >= 6 and any(char.isdigit() for char in parts[1]):
        street_variant = [f"{parts[1]} {parts[2]}", *parts[3:7]]
        add_variant(", ".join(street_variant))
        street_variant_nsw = ["NSW" if part.lower() == "new south wales" else part for part in street_variant]
        add_variant(", ".join(street_variant_nsw))

    # Try a Sydney-scoped version for suburban addresses that omit the state or country.
    if "sydney" not in normalized.lower():
        add_variant(f"{normalized}, Sydney, New South Wales, Australia")
    if "new south wales" not in normalized.lower():
        add_variant(f"{normalized}, New South Wales, Australia")

    return variants


def nominatim_search(query: str, limit: int = 1, bounded: bool = False) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "format": "jsonv2",
        "q": query,
        "countrycodes": "au",
        "limit": limit,
        "addressdetails": 1,
        "dedupe": 1,
    }
    if bounded:
        params["viewbox"] = SYDNEY_VIEWBOX
        params["bounded"] = 1

    response = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params=params,
        headers=HTTP_HEADERS,
        timeout=8,
    )
    response.raise_for_status()
    return response.json()


def meters_to_minutes(meters: float, speed_kph: float) -> float:
    meters_per_minute = speed_kph * 1000 / 60
    return meters / meters_per_minute if meters_per_minute else 0.0


def current_sydney_time() -> datetime:
    return datetime.now(tz=SYDNEY_TZ)


def tfnsw_headers() -> dict[str, str]:
    api_key = os.getenv("TFNSW_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="TFNSW_API_KEY is missing from the server configuration.")
    return {"Authorization": f"apikey {api_key}", **HTTP_HEADERS}


def resolve_graph(profile: str) -> nx.MultiDiGraph:
    global _drive_graph, _walk_graph

    if profile == "drive" and _drive_graph is not None:
        return _drive_graph
    if profile == "walk" and _walk_graph is not None:
        return _walk_graph

    with _graph_lock:
        if profile == "drive" and _drive_graph is not None:
            return _drive_graph
        if profile == "walk" and _walk_graph is not None:
            return _walk_graph

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        graph_path = DRIVE_GRAPH_PATH if profile == "drive" else WALK_GRAPH_PATH
        network_type = "drive" if profile == "drive" else "walk"

        if graph_path.exists():
            try:
                graph = ox.load_graphml(graph_path)
            except Exception:
                try:
                    graph_path.unlink(missing_ok=True)
                except OSError:
                    pass
                graph = None
        else:
            graph = ox.graph_from_place(SYDNEY_PLACE, network_type=network_type)
            if profile == "drive":
                graph = ox.add_edge_speeds(graph)
                graph = ox.add_edge_travel_times(graph)
            ox.save_graphml(graph, graph_path)

        if graph is None:
            graph = ox.graph_from_place(SYDNEY_PLACE, network_type=network_type)
            if profile == "drive":
                graph = ox.add_edge_speeds(graph)
                graph = ox.add_edge_travel_times(graph)
            ox.save_graphml(graph, graph_path)

        if profile == "drive" and not all("travel_time" in data for _, _, _, data in graph.edges(keys=True, data=True)):
            graph = ox.add_edge_speeds(graph)
            graph = ox.add_edge_travel_times(graph)
            ox.save_graphml(graph, graph_path)

        if profile == "drive":
            _drive_graph = graph
            return _drive_graph

        _walk_graph = graph
        return _walk_graph


def localized_route_graph(graph: nx.MultiDiGraph, origin_node: int, destination_node: int, buffer_km: float = 1.6) -> nx.MultiDiGraph:
    origin = graph.nodes[origin_node]
    destination = graph.nodes[destination_node]
    origin_lat = float(origin["y"])
    origin_lng = float(origin["x"])
    destination_lat = float(destination["y"])
    destination_lng = float(destination["x"])

    direct_distance_km = segment_distance_m((origin_lat, origin_lng), (destination_lat, destination_lng)) / 1000
    margin_km = max(buffer_km, direct_distance_km * 0.35)
    lat_padding = margin_km / 111.0
    avg_lat = (origin_lat + destination_lat) / 2
    lng_padding = margin_km / max(20.0, 111.0 * abs(np.cos(np.radians(avg_lat))))

    north = max(origin_lat, destination_lat) + lat_padding
    south = min(origin_lat, destination_lat) - lat_padding
    east = max(origin_lng, destination_lng) + lng_padding
    west = min(origin_lng, destination_lng) - lng_padding

    try:
        subgraph = ox.truncate.truncate_graph_bbox(graph, north, south, east, west, retain_all=False)
    except Exception:
        return graph

    if origin_node in subgraph and destination_node in subgraph and subgraph.number_of_nodes() > 0:
        return subgraph
    return graph


def resolve_network_geojson() -> dict[str, Any]:
    graph = resolve_graph("drive")

    if NETWORK_GEOJSON_PATH.exists():
        return json.loads(NETWORK_GEOJSON_PATH.read_text(encoding="utf-8"))

    edges = ox.graph_to_gdfs(graph, nodes=False, fill_edge_geometry=True)
    network_geojson = jsonable_encoder(edges.__geo_interface__)
    NETWORK_GEOJSON_PATH.write_text(json.dumps(network_geojson, ensure_ascii=False), encoding="utf-8")
    return network_geojson


def build_traffic_model_file() -> None:
    if not TRAFFIC_CSV_PATH.exists():
        payload = {"generated_at": current_sydney_time().isoformat(), "stations": []}
        TRAFFIC_MODEL_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return

    hour_cols = [f"hour_{hour:02d}" for hour in range(24)]
    usecols = ["the_geom", "station_key", "day_of_week", "public_holiday", "daily_total", *hour_cols]
    df = pd.read_csv(TRAFFIC_CSV_PATH, usecols=usecols, low_memory=False)

    df = df.dropna(subset=["station_key", "the_geom"]).copy()
    df["station_key"] = df["station_key"].astype(int)
    df["public_holiday"] = df["public_holiday"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["day_of_week"] = pd.to_numeric(df["day_of_week"], errors="coerce").fillna(0).astype(int)
    df["daily_total"] = pd.to_numeric(df["daily_total"], errors="coerce").fillna(0)

    lower = float(df["daily_total"].quantile(0.005))
    upper = float(df["daily_total"].quantile(0.995))
    df = df[(df["daily_total"] >= lower) & (df["daily_total"] <= upper) & (df["daily_total"] > 0)].copy()

    for column in hour_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).clip(lower=0)

    df["day_type"] = np.where(df["public_holiday"] | df["day_of_week"].isin([6, 7]), "weekend", "weekday")
    df["baseline_hourly_volume"] = (df["daily_total"] / 24).clip(lower=1)

    base_profile = df.groupby("station_key")["baseline_hourly_volume"].median()
    weekday_agg = df.loc[df["day_type"] == "weekday"].groupby("station_key")[hour_cols].median()
    weekend_agg = df.loc[df["day_type"] == "weekend"].groupby("station_key")[hour_cols].median()

    geometry_frame = df.drop_duplicates(subset=["station_key"]).loc[:, ["station_key", "the_geom"]].copy()

    station_rows: list[dict[str, Any]] = []
    for row in geometry_frame.itertuples(index=False):
        station_key = int(row.station_key)
        baseline = float(base_profile.get(station_key, 0.0))
        if baseline <= 0:
            continue

        try:
            point = wkb.loads(bytes.fromhex(str(row.the_geom)))
            lng = float(point.x)
            lat = float(point.y)
        except Exception:
            continue

        weekday_series = weekday_agg.loc[station_key] if station_key in weekday_agg.index else pd.Series(1.0, index=hour_cols)
        weekend_series = weekend_agg.loc[station_key] if station_key in weekend_agg.index else pd.Series(1.0, index=hour_cols)

        weekday_profile = np.clip((weekday_series / baseline).to_numpy(dtype=float), 0.35, 3.0).round(4).tolist()
        weekend_profile = np.clip((weekend_series / baseline).to_numpy(dtype=float), 0.35, 3.0).round(4).tolist()

        station_rows.append(
            {
                "station_key": station_key,
                "lat": lat,
                "lng": lng,
                "baseline_hourly_volume": round(baseline, 4),
                "weekday_profile": weekday_profile,
                "weekend_profile": weekend_profile,
            }
        )

    payload = {
        "generated_at": current_sydney_time().isoformat(),
        "station_count": len(station_rows),
        "stations": station_rows,
    }
    TRAFFIC_MODEL_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def resolve_traffic_model() -> TrafficModel:
    global _traffic_model

    if _traffic_model is not None:
        return _traffic_model

    with _traffic_lock:
        if _traffic_model is not None:
            return _traffic_model

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if not TRAFFIC_MODEL_PATH.exists():
            build_traffic_model_file()

        payload = json.loads(TRAFFIC_MODEL_PATH.read_text(encoding="utf-8"))
        _traffic_model = TrafficModel.from_json(payload)
        return _traffic_model


def warm_runtime_assets() -> None:
    errors: list[str] = []
    with _preload_status_lock:
        _preload_status["started"] = True
        _preload_status["finished"] = False
        _preload_status["errors"] = []

    warm_steps = [
        ("drive_graph", lambda: resolve_graph("drive")),
        ("walk_graph", lambda: resolve_graph("walk")),
        ("traffic_model", resolve_traffic_model),
        ("network_geojson", resolve_network_geojson),
    ]

    for name, step in warm_steps:
        try:
            step()
        except Exception as exc:
            errors.append(f"{name}: {exc}")

    with _preload_status_lock:
        _preload_status["finished"] = True
        _preload_status["errors"] = errors


def reverse_geocode(lat: float, lng: float) -> str:
    cache_key = f"{lat:.4f},{lng:.4f}"
    with _place_name_cache_lock:
        cached = _place_name_cache.get(cache_key)
        if cached is not None:
            return cached

    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"format": "jsonv2", "lat": lat, "lon": lng, "zoom": 18, "addressdetails": 1},
            headers=HTTP_HEADERS,
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        fallback = f"{lat:.6f},{lng:.6f}"
        with _place_name_cache_lock:
            _place_name_cache[cache_key] = fallback
        return fallback

    address = payload.get("address", {})
    name_parts = [
        payload.get("name"),
        address.get("attraction"),
        address.get("building"),
        address.get("amenity"),
        address.get("road"),
        address.get("suburb"),
    ]
    for part in name_parts:
        if part:
            name = str(part)
            with _place_name_cache_lock:
                _place_name_cache[cache_key] = name
            return name

    display_name = payload.get("display_name", f"{lat:.6f},{lng:.6f}")
    with _place_name_cache_lock:
        _place_name_cache[cache_key] = display_name
    return display_name


def search_places(query: str, limit: int, viewbox: str | None = None, bounded: bool = False) -> list[str]:
    params: dict[str, Any] = {
        "format": "jsonv2",
        "q": query,
        "countrycodes": "au",
        "limit": limit,
        "addressdetails": 1,
        "dedupe": 1,
    }
    if viewbox:
        params["viewbox"] = viewbox
        params["bounded"] = int(bounded)

    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params=params,
            headers=HTTP_HEADERS,
            timeout=8,
        )
        response.raise_for_status()
        items = response.json()
    except requests.RequestException:
        return []

    suggestions: list[str] = []
    seen: set[str] = set()
    for item in items:
        display_name = item.get("display_name")
        if display_name and display_name not in seen:
            seen.add(display_name)
            suggestions.append(display_name)
    return suggestions


def geocode_with_search(query: str) -> tuple[float, float] | None:
    for variant in build_location_variants(query):
        for bounded in (True, False):
            try:
                items = nominatim_search(variant, limit=3, bounded=bounded)
            except requests.RequestException:
                continue

            if items:
                best = items[0]
                return float(best["lat"]), float(best["lon"])

    return None


def geocode_point(query: str) -> tuple[float, float]:
    coordinates = parse_coordinate_query(query)
    if coordinates is not None:
        return coordinates

    cleaned_query = normalize_location_query(query)
    normalized_query = cleaned_query.lower()
    if normalized_query in KNOWN_PLACE_COORDINATES:
        return KNOWN_PLACE_COORDINATES[normalized_query]

    for place_name, coords in KNOWN_PLACE_COORDINATES.items():
        if normalized_query in place_name or place_name in normalized_query:
            return coords

    for place in FAMOUS_SYDNEY_PLACES:
        if normalized_query == place.lower():
            try:
                lat, lng = ox.geocode(f"{place}, Sydney")
                return lat, lng
            except Exception:
                break

    looks_like_address = (
        "," in cleaned_query
        or any(char.isdigit() for char in cleaned_query)
        or any(token in normalized_query for token in ["street", "st", "road", "rd", "avenue", "ave", "parade", "lane", "close", "drive", "court", "place"])
    )

    if looks_like_address:
        fallback = geocode_with_search(cleaned_query)
        if fallback is not None:
            return fallback

    try:
        lat, lng = ox.geocode(cleaned_query)
        return lat, lng
    except Exception:
        fallback = geocode_with_search(cleaned_query)
        if fallback is not None:
            return fallback
        raise HTTPException(status_code=400, detail=f"Could not recognize location: {query}")


def iso_to_sydney(iso_text: str | None) -> datetime | None:
    if not iso_text:
        return None
    return datetime.fromisoformat(iso_text.replace("Z", "+00:00")).astimezone(SYDNEY_TZ)


def display_time(iso_text: str | None) -> str | None:
    dt = iso_to_sydney(iso_text)
    return dt.strftime("%H:%M") if dt else None


def minutes_difference(start_iso: str | None, end_iso: str | None) -> float:
    start_dt = iso_to_sydney(start_iso)
    end_dt = iso_to_sydney(end_iso)
    if not start_dt or not end_dt:
        return 0.0
    return max(0.0, (end_dt - start_dt).total_seconds() / 60)


def coord_string(lat: float, lng: float) -> str:
    return f"{lng}:{lat}:EPSG:4326"


def extract_stop_name(stop: dict[str, Any]) -> str:
    if stop.get("parent") and stop["parent"].get("name"):
        return str(stop["parent"]["name"])
    return str(stop.get("name", "Unknown stop"))


def extract_platform_name(stop: dict[str, Any]) -> str | None:
    properties = stop.get("properties", {})
    return properties.get("plannedPlatformName") or properties.get("platformName") or properties.get("platform")


def journey_step_distance_m(step: dict[str, Any]) -> float:
    if "distance" in step and step["distance"] is not None:
        return float(step["distance"])

    sequence = step.get("stopSequence", [])
    if len(sequence) >= 2:
        distance_m = 0.0
        for start, end in zip(sequence[:-1], sequence[1:]):
            start_coord = start.get("coord")
            end_coord = end.get("coord")
            if start_coord and end_coord:
                distance_m += segment_distance_m((float(start_coord[0]), float(start_coord[1])), (float(end_coord[0]), float(end_coord[1])))
        return round(distance_m, 2)

    origin_coord = step.get("origin", {}).get("coord")
    destination_coord = step.get("destination", {}).get("coord")
    if origin_coord and destination_coord:
        return round(segment_distance_m((float(origin_coord[0]), float(origin_coord[1])), (float(destination_coord[0]), float(destination_coord[1]))), 2)

    return 0.0


def journey_step_polyline(step: dict[str, Any]) -> list[list[float]]:
    sequence = step.get("stopSequence", [])
    if len(sequence) >= 2:
        return [[float(item["coord"][0]), float(item["coord"][1])] for item in sequence if item.get("coord")]

    origin_coord = step.get("origin", {}).get("coord")
    destination_coord = step.get("destination", {}).get("coord")
    if origin_coord and destination_coord:
        return [[float(origin_coord[0]), float(origin_coord[1])], [float(destination_coord[0]), float(destination_coord[1])]]

    return []


def build_trace_from_polyline(polyline: list[list[float]], total_minutes: float) -> list[dict[str, float]]:
    if not polyline:
        return []
    if len(polyline) == 1:
        return [{"lat": polyline[0][0], "lng": polyline[0][1], "distance_m": 0.0, "distance_km": 0.0, "minutes": 0.0}]

    segment_lengths: list[float] = []
    total_distance_m = 0.0
    for start, end in zip(polyline[:-1], polyline[1:]):
        segment_length = segment_distance_m((float(start[0]), float(start[1])), (float(end[0]), float(end[1])))
        segment_lengths.append(segment_length)
        total_distance_m += segment_length

    if total_distance_m <= 0:
        total_distance_m = 1.0

    trace = [{"lat": float(polyline[0][0]), "lng": float(polyline[0][1]), "distance_m": 0.0, "distance_km": 0.0, "minutes": 0.0}]
    cumulative_distance = 0.0
    cumulative_minutes = 0.0
    for point, segment_length in zip(polyline[1:], segment_lengths):
        cumulative_distance += segment_length
        cumulative_minutes += total_minutes * (segment_length / total_distance_m)
        trace.append(
            {
                "lat": float(point[0]),
                "lng": float(point[1]),
                "distance_m": round(cumulative_distance, 2),
                "distance_km": round(cumulative_distance / 1000, 3),
                "minutes": round(cumulative_minutes, 2),
            }
        )
    return trace


def tfnsw_departure_options(stop_id: str | None, departure_iso: str | None) -> list[str]:
    if not stop_id:
        return []

    departure_dt = iso_to_sydney(departure_iso) or current_sydney_time()
    params = {
        "outputFormat": "rapidJSON",
        "coordOutputFormat": "EPSG:4326",
        "mode": "direct",
        "type_dm": "stop",
        "name_dm": stop_id,
        "depArrMacro": "dep",
        "itdDate": departure_dt.strftime("%Y%m%d"),
        "itdTime": departure_dt.strftime("%H%M"),
        "TfNSWDM": "true",
    }

    try:
        response = requests.get(f"{TFNSW_BASE_URL}/departure_mon", headers=tfnsw_headers(), params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        return []

    options: list[str] = []
    for event in payload.get("stopEvents", []):
        transportation = event.get("transportation", {})
        label = transportation.get("number") or transportation.get("disassembledName") or transportation.get("name")
        if label and label not in options:
            options.append(str(label))
        if len(options) >= 5:
            break
    return options


def parse_transit_leg(leg: dict[str, Any], previous_arrival_iso: str | None) -> dict[str, Any]:
    transportation = leg.get("transportation", {})
    product = transportation.get("product", {})
    product_name = str(product.get("name", "")).lower()

    if product_name == "footpath":
        distance_m = journey_step_distance_m(leg)
        return {
            "mode": "walk",
            "instruction": f"Walk to {extract_stop_name(leg.get('destination', {}))}",
            "from": leg.get("origin", {}).get("name"),
            "to": leg.get("destination", {}).get("name"),
            "from_coord": leg.get("origin", {}).get("coord"),
            "to_coord": leg.get("destination", {}).get("coord"),
            "distance_m": round(distance_m, 2),
            "distance_km": round(distance_m / 1000, 3),
            "duration_minutes": round(float(leg.get("duration", 0)) / 60, 2),
            "wait_minutes": 0.0,
            "stops_count": 0,
            "depart_time": display_time(leg.get("origin", {}).get("departureTimeEstimated") or leg.get("origin", {}).get("departureTimePlanned")),
            "arrive_time": display_time(leg.get("destination", {}).get("arrivalTimeEstimated") or leg.get("destination", {}).get("arrivalTimePlanned")),
            "polyline": journey_step_polyline(leg),
        }

    departure_iso = leg.get("origin", {}).get("departureTimeEstimated") or leg.get("origin", {}).get("departureTimePlanned")
    arrival_iso = leg.get("destination", {}).get("arrivalTimeEstimated") or leg.get("destination", {}).get("arrivalTimePlanned")
    wait_minutes = minutes_difference(previous_arrival_iso, departure_iso) if previous_arrival_iso else 0.0
    stop_sequence = leg.get("stopSequence", [])
    distance_m = journey_step_distance_m(leg)
    stop_id = (
        leg.get("origin", {}).get("parent", {}).get("properties", {}).get("stopId")
        or leg.get("origin", {}).get("properties", {}).get("stopId")
    )
    alternatives = tfnsw_departure_options(stop_id, departure_iso)

    return {
        "mode": str(product.get("name", "transit")),
        "line": transportation.get("number") or transportation.get("disassembledName") or transportation.get("name"),
        "operator": transportation.get("operator", {}).get("name"),
        "instruction": f"Take {transportation.get('number') or transportation.get('name', 'service')}",
        "from": extract_stop_name(leg.get("origin", {})),
        "from_platform": extract_platform_name(leg.get("origin", {})),
        "to": extract_stop_name(leg.get("destination", {})),
        "to_platform": extract_platform_name(leg.get("destination", {})),
        "headsign": transportation.get("destination", {}).get("name"),
        "from_coord": leg.get("origin", {}).get("coord"),
        "to_coord": leg.get("destination", {}).get("coord"),
        "wait_minutes": round(wait_minutes, 2),
        "distance_m": round(distance_m, 2),
        "distance_km": round(distance_m / 1000, 3),
        "duration_minutes": round(float(leg.get("duration", 0)) / 60, 2),
        "stops_count": max(0, len(stop_sequence) - 1),
        "depart_time": display_time(departure_iso),
        "arrive_time": display_time(arrival_iso),
        "alternatives": alternatives,
        "polyline": journey_step_polyline(leg),
    }


def transit_route_polyline(steps: list[dict[str, Any]]) -> list[list[float]]:
    polyline: list[list[float]] = []
    for step in steps:
        for point in step.get("polyline", []):
            if not polyline or polyline[-1] != point:
                polyline.append(point)
    return polyline


def nearest_point_on_polyline(polyline: list[list[float]], coord: list[float] | tuple[float, float] | None) -> tuple[float, float] | None:
    if not polyline or coord is None:
        return None

    target = (float(coord[0]), float(coord[1]))
    best_point: tuple[float, float] | None = None
    best_distance = float("inf")

    for point in polyline:
        candidate = (float(point[0]), float(point[1]))
        distance = segment_distance_m(target, candidate)
        if distance < best_distance:
            best_distance = distance
            best_point = candidate

    return best_point


def tfnsw_trip_query(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    max_walk_distance_km: float | None = None,
    timeout_seconds: int = 30,
    calc_number_of_trips: int = 6,
) -> dict[str, Any]:
    current_dt = current_sydney_time()
    cache_key = transit_cache_key(origin_lat, origin_lng, destination_lat, destination_lng, max_walk_distance_km, current_dt)

    with _transit_cache_lock:
        cached = _transit_cache.get(cache_key)
        if cached is not None:
            return cached

    params: dict[str, Any] = {
        "outputFormat": "rapidJSON",
        "coordOutputFormat": "EPSG:4326",
        "depArrMacro": "dep",
        "itdDate": current_dt.strftime("%Y%m%d"),
        "itdTime": current_dt.strftime("%H%M"),
        "type_origin": "coord",
        "name_origin": coord_string(origin_lat, origin_lng),
        "type_destination": "coord",
        "name_destination": coord_string(destination_lat, destination_lng),
        "calcNumberOfTrips": calc_number_of_trips,
        "TfNSWTR": "true",
    }
    if max_walk_distance_km is not None:
        params["changeSpeed"] = "normal"
        params["maxWalkingDistance"] = int(max_walk_distance_km * 1000)

    try:
        response = requests.get(f"{TFNSW_BASE_URL}/trip", headers=tfnsw_headers(), params=params, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        with _transit_cache_lock:
            _transit_cache[cache_key] = payload
        return payload
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail="TfNSW trip planner request failed.") from exc


def transit_cache_key(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    max_walk_distance_km: float | None,
    current_dt: datetime | None = None,
) -> str:
    current_dt = current_dt or current_sydney_time()
    time_bucket = current_dt.strftime("%Y%m%d%H%M")[:-1]
    return "|".join(
        [
            f"{origin_lat:.5f}",
            f"{origin_lng:.5f}",
            f"{destination_lat:.5f}",
            f"{destination_lng:.5f}",
            f"{max_walk_distance_km if max_walk_distance_km is not None else 'none'}",
            time_bucket,
        ]
    )


def weather_cache_key(samples: list[dict[str, float]], current_dt: datetime) -> str:
    bucket = current_dt.strftime("%Y%m%d%H")
    point_key = ";".join(f"{sample['lat']:.3f},{sample['lng']:.3f}" for sample in samples)
    return f"{bucket}|{point_key}"


def weather_label_from_code(code: int) -> str:
    if code in {0, 1}:
        return "sunny"
    if code in {2, 3, 45, 48}:
        return "cloudy"
    if code in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}:
        return "rain"
    if code in {71, 73, 75, 77, 85, 86}:
        return "snow"
    if code in {95, 96, 99}:
        return "storm"
    return "mixed"


def format_clock(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")


def sample_trace_for_weather(trace: list[dict[str, float]]) -> list[dict[str, float]]:
    if not trace:
        return []

    total_distance_m = float(trace[-1]["distance_m"])
    if total_distance_m <= 0:
        return [trace[0]]

    target_count = min(6, max(3, int(total_distance_m // 2500) + 1))
    samples: list[dict[str, float]] = []
    for index in range(target_count):
        target_ratio = index / max(1, target_count - 1)
        target_distance = total_distance_m * target_ratio
        best_point = min(trace, key=lambda point: abs(float(point["distance_m"]) - target_distance))
        sample = {
            "lat": float(best_point["lat"]),
            "lng": float(best_point["lng"]),
            "minutes": float(best_point["minutes"]),
            "distance_m": float(best_point["distance_m"]),
        }
        if not samples or (abs(samples[-1]["distance_m"] - sample["distance_m"]) > 25):
            samples.append(sample)
    return samples


def fetch_route_weather_samples(samples: list[dict[str, float]], current_dt: datetime) -> list[dict[str, Any]]:
    if not samples:
        return []

    cache_key = weather_cache_key(samples, current_dt)
    with _weather_cache_lock:
        cached = _weather_cache.get(cache_key)
        if cached is not None:
            return cached["samples"]

    params = {
        "latitude": ",".join(f"{sample['lat']:.6f}" for sample in samples),
        "longitude": ",".join(f"{sample['lng']:.6f}" for sample in samples),
        "hourly": "weather_code,precipitation_probability,precipitation,rain,showers",
        "forecast_days": 2,
        "timezone": "Australia/Sydney",
    }

    response = requests.get(OPEN_METEO_URL, params=params, headers=HTTP_HEADERS, timeout=12)
    response.raise_for_status()
    payload = response.json()
    locations = payload if isinstance(payload, list) else [payload]

    enriched: list[dict[str, Any]] = []
    for sample, location in zip(samples, locations):
        arrival_dt = current_dt + timedelta(minutes=float(sample["minutes"]))
        hourly = location.get("hourly", {})
        times = hourly.get("time", [])
        if not times:
            continue

        arrival_iso = arrival_dt.strftime("%Y-%m-%dT%H:00")
        best_index = min(
            range(len(times)),
            key=lambda idx: abs(datetime.fromisoformat(times[idx]).replace(tzinfo=SYDNEY_TZ) - arrival_dt),
        )
        if arrival_iso in times:
            best_index = times.index(arrival_iso)

        weather_code = int(hourly.get("weather_code", [0])[best_index] or 0)
        precipitation_probability = float(hourly.get("precipitation_probability", [0])[best_index] or 0)
        precipitation = float(hourly.get("precipitation", [0])[best_index] or 0)
        rain = float(hourly.get("rain", [0])[best_index] or 0)
        showers = float(hourly.get("showers", [0])[best_index] or 0)
        label = weather_label_from_code(weather_code)
        is_rain = label in {"rain", "storm"} or precipitation_probability >= 45 or precipitation >= 0.2 or rain > 0 or showers > 0
        is_sunny = label == "sunny" and precipitation_probability < 20 and precipitation < 0.1

        enriched.append(
            {
                **sample,
                "arrival_time": arrival_dt.isoformat(),
                "weather_code": weather_code,
                "precipitation_probability": round(precipitation_probability, 1),
                "precipitation": round(precipitation, 2),
                "label": label,
                "is_rain": is_rain,
                "is_sunny": is_sunny,
            }
        )

    with _weather_cache_lock:
        _weather_cache[cache_key] = {"samples": enriched}
    return enriched


def build_route_weather_summary(route: dict[str, Any], current_dt: datetime) -> dict[str, Any] | None:
    try:
        sample_points = sample_trace_for_weather(route.get("trace", []))
        if not sample_points:
            return None

        weather_samples = fetch_route_weather_samples(sample_points, current_dt)
        if not weather_samples:
            return None

        route_start = current_dt
        route_end = current_dt + timedelta(minutes=float(route.get("estimated_minutes", 0)))
        if all(sample["is_sunny"] for sample in weather_samples):
            return {
                "icon": "sun",
                "headline": f"Sunny from {format_clock(route_start)} to {format_clock(route_end)}.",
                "details": [],
                "samples": weather_samples,
            }

        rain_intervals: list[dict[str, Any]] = []
        current_interval: dict[str, Any] | None = None
        for sample in weather_samples:
            if sample["is_rain"]:
                if current_interval is None:
                    current_interval = {"start": sample, "end": sample}
                else:
                    current_interval["end"] = sample
            elif current_interval is not None:
                rain_intervals.append(current_interval)
                current_interval = None
        if current_interval is not None:
            rain_intervals.append(current_interval)

        details: list[str] = []
        for interval in rain_intervals:
            start_sample = interval["start"]
            end_sample = interval["end"]
            place_name = reverse_geocode(float(start_sample["lat"]), float(start_sample["lng"]))
            start_dt = datetime.fromisoformat(str(start_sample["arrival_time"]))
            end_dt = datetime.fromisoformat(str(end_sample["arrival_time"]))
            if end_dt <= start_dt:
                end_dt = min(route_end, start_dt + timedelta(minutes=20))
            details.append(f"Rain near {place_name} from {format_clock(start_dt)} to {format_clock(end_dt)}.")

        if details:
            return {
                "icon": "rain",
                "headline": "Rain may affect part of this trip.",
                "details": details,
                "samples": weather_samples,
            }

        return {
            "icon": "mixed",
            "headline": f"Mixed weather from {format_clock(route_start)} to {format_clock(route_end)}.",
            "details": [],
            "samples": weather_samples,
        }
    except requests.RequestException:
        return None


def parse_transit_journey(journey: dict[str, Any], label: str, color: str) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    transfer_points: list[dict[str, Any]] = []
    previous_arrival_iso: str | None = None
    total_walk_m = 0.0
    total_wait_minutes = 0.0
    total_distance_m = 0.0
    total_leg_minutes = 0.0

    for leg in journey.get("legs", []):
        step = parse_transit_leg(leg, previous_arrival_iso)
        if steps:
            previous_step = steps[-1]
            if previous_step["mode"] != step["mode"]:
                previous_polyline = previous_step.get("polyline", [])
                current_polyline = step.get("polyline", [])
                raw_coord = step.get("from_coord") or previous_step.get("to_coord")
                anchored_coord = (
                    nearest_point_on_polyline(previous_polyline, raw_coord)
                    or nearest_point_on_polyline(current_polyline, raw_coord)
                    or (tuple(previous_polyline[-1]) if previous_polyline else None)
                    or (tuple(current_polyline[0]) if current_polyline else None)
                )
                coord = anchored_coord
                if coord:
                    transfer_points.append(
                        {
                            "lat": float(coord[0]),
                            "lng": float(coord[1]),
                            "title": "Transfer point",
                            "summary": f"Change from {previous_step.get('line') or previous_step['mode']} to {step.get('line') or step['mode']}",
                            "from": previous_step.get("to") or previous_step.get("from"),
                            "to": step.get("from") or step.get("to"),
                        }
                    )
        steps.append(step)
        if step["mode"] == "walk":
            total_walk_m += step["distance_m"]
        total_wait_minutes += step.get("wait_minutes", 0.0)
        total_distance_m += step["distance_m"]
        total_leg_minutes += step["duration_minutes"]
        previous_arrival_iso = leg.get("destination", {}).get("arrivalTimeEstimated") or leg.get("destination", {}).get("arrivalTimePlanned")

    journey_duration = journey.get("duration")
    total_minutes = (
        round(float(journey_duration) / 60, 2)
        if journey_duration not in (None, "", 0)
        else round(total_leg_minutes + total_wait_minutes, 2)
    )
    polyline = transit_route_polyline(steps)
    trace = build_trace_from_polyline(polyline, total_minutes)

    return {
        "label": label,
        "color": color,
        "profile": "transit",
        "distance_m": round(total_distance_m, 2),
        "distance_km": round(total_distance_m / 1000, 3),
        "estimated_minutes": total_minutes,
        "walk_distance_km": round(total_walk_m / 1000, 3),
        "wait_minutes": round(total_wait_minutes, 2),
        "interchanges": int(journey.get("interchanges", 0)),
        "details": {"steps": steps, "transfer_points": transfer_points},
        "trace": trace,
        "polyline": polyline,
    }


def build_transit_routes(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    max_walk_distance_km: float | None = None,
    timeout_seconds: int = 30,
    calc_number_of_trips: int = 6,
) -> list[dict[str, Any]]:
    payload = tfnsw_trip_query(
        origin_lat,
        origin_lng,
        destination_lat,
        destination_lng,
        max_walk_distance_km=max_walk_distance_km,
        timeout_seconds=timeout_seconds,
        calc_number_of_trips=calc_number_of_trips,
    )
    journeys = payload.get("journeys", [])
    if not journeys:
        raise HTTPException(status_code=404, detail="No public transport route was returned by TfNSW.")

    parsed = [parse_transit_journey(journey, "Transit option", "#5f3dc4") for journey in journeys]
    fastest = min(parsed, key=lambda item: (item["estimated_minutes"], item["walk_distance_km"]))
    shortest = min(parsed, key=lambda item: (item["distance_km"], item["estimated_minutes"]))

    fastest = {**fastest, "label": "Fastest transit", "color": "#5f3dc4"}
    shortest = {**shortest, "label": "Shortest transit", "color": "#0b7285"}

    if fastest["polyline"] == shortest["polyline"]:
        return [fastest]

    return [shortest, fastest]


def build_combined_routes(
    origin_node: int,
    destination_node: int,
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    current_dt: datetime,
    max_walk_distance_km: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    current_cache_key = transit_cache_key(origin_lat, origin_lng, destination_lat, destination_lng, max_walk_distance_km, current_dt)
    with _transit_cache_lock:
        cached_transit_payload = _transit_cache.get(current_cache_key)

    max_possible_walk_m = max_walk_distance_km * 1000
    direct_distance_m = segment_distance_m((origin_lat, origin_lng), (destination_lat, destination_lng))
    should_compute_walk = direct_distance_m <= max_possible_walk_m * 1.15

    executor = ThreadPoolExecutor(max_workers=1 if not should_compute_walk else 2)
    try:
        walk_future = executor.submit(build_walk_routes, origin_node, destination_node, current_dt) if should_compute_walk else None

        walk_routes = walk_future.result() if walk_future is not None else []
        warnings: list[str] = []
        if cached_transit_payload is not None:
            journeys = cached_transit_payload.get("journeys", [])
            parsed = [parse_transit_journey(journey, "Transit option", "#5f3dc4") for journey in journeys] if journeys else []
            if parsed:
                fastest = min(parsed, key=lambda item: (item["estimated_minutes"], item["walk_distance_km"]))
                shortest = min(parsed, key=lambda item: (item["distance_km"], item["estimated_minutes"]))
                transit_routes = [{**shortest, "label": "Shortest transit", "color": "#0b7285"}]
                if fastest["polyline"] != shortest["polyline"]:
                    transit_routes.append({**fastest, "label": "Fastest transit", "color": "#5f3dc4"})
                elif fastest["label"] != "Shortest transit":
                    transit_routes = [{**fastest, "label": "Fastest transit", "color": "#5f3dc4"}]
            else:
                transit_routes = []
        else:
            try:
                transit_routes = build_transit_routes(
                    origin_lat,
                    origin_lng,
                    destination_lat,
                    destination_lng,
                    max_walk_distance_km=max_walk_distance_km,
                    timeout_seconds=8,
                    calc_number_of_trips=4,
                )
                warnings.append("Combined mode used a live TfNSW transit query.")
            except HTTPException:
                transit_routes = []
                warnings.append("Live transit data was unavailable for Combined mode, so only walking-compatible options could be shown.")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    candidates: list[dict[str, Any]] = []
    candidates.extend(transit_routes)
    if walk_routes and walk_routes[0]["distance_km"] <= max_walk_distance_km:
        candidates.extend(walk_routes)

    filtered = []
    for route in candidates:
        walk_km = route.get("walk_distance_km", route["distance_km"])
        if walk_km <= max_walk_distance_km:
            filtered.append(route)

    if not filtered:
        transit_walk_requirements = [route.get("walk_distance_km", 0.0) for route in transit_routes]
        if transit_walk_requirements:
            required_km = round(min(transit_walk_requirements), 2)
            raise HTTPException(
                status_code=404,
                detail=f"No transit-and-walk route satisfies the current walking limit. Try at least {required_km} km.",
            )
        raise HTTPException(status_code=404, detail="No transit-and-walk route is currently available for this trip.")

    fastest = min(filtered, key=lambda item: (item["estimated_minutes"], item["distance_km"]))
    shortest = min(filtered, key=lambda item: (item["distance_km"], item["estimated_minutes"]))

    fastest = {**fastest, "label": f"Fastest combined ({fastest['label']})"}
    shortest = {**shortest, "label": f"Shortest combined ({shortest['label']})"}

    if fastest["polyline"] == shortest["polyline"] and fastest["label"] == shortest["label"]:
        return [fastest], warnings

    return [shortest, fastest], warnings


def nearest_node_info(graph: nx.MultiDiGraph, lat: float, lng: float) -> dict[str, Any]:
    node_id = ox.distance.nearest_nodes(graph, lng, lat)
    node_data = graph.nodes[node_id]
    return {"id": int(node_id), "lat": float(node_data["y"]), "lng": float(node_data["x"])}


def pick_edge_data(graph: nx.MultiDiGraph, source: int, target: int, weight: str | None = None) -> dict[str, Any]:
    edge_bundle = graph.get_edge_data(source, target)
    if not edge_bundle:
        return {}

    if weight is None:
        first_key = next(iter(edge_bundle))
        return edge_bundle[first_key]

    key = min(edge_bundle, key=lambda edge_key: float(edge_bundle[edge_key].get(weight, float("inf"))))
    return edge_bundle[key]


def segment_distance_m(start: tuple[float, float], end: tuple[float, float]) -> float:
    return float(ox.distance.great_circle(start[0], start[1], end[0], end[1]))


def node_distance_m(graph: nx.MultiDiGraph, source_node: int, destination_node: int) -> float:
    source = graph.nodes[source_node]
    destination = graph.nodes[destination_node]
    return segment_distance_m(
        (float(source["y"]), float(source["x"])),
        (float(destination["y"]), float(destination["x"])),
    )


def edge_coordinates(graph: nx.MultiDiGraph, source: int, target: int, edge: dict[str, Any]) -> list[tuple[float, float]]:
    geometry = edge.get("geometry")
    if geometry is not None and hasattr(geometry, "coords"):
        coordinates = [(float(lat), float(lng)) for lng, lat in geometry.coords]
        if coordinates:
            return coordinates

    source_node = graph.nodes[source]
    target_node = graph.nodes[target]
    return [(float(source_node["y"]), float(source_node["x"])), (float(target_node["y"]), float(target_node["x"]))]


def edge_highway_tag(edge: dict[str, Any]) -> str:
    highway = edge.get("highway", "residential")
    if isinstance(highway, list):
        highway = highway[0]
    return str(highway)


def road_time_parameters(edge: dict[str, Any]) -> tuple[float, float, float]:
    highway = edge_highway_tag(edge)
    if highway in {"motorway", "motorway_link"}:
        return 78.0, 1.03, 0.03
    if highway in {"trunk", "trunk_link"}:
        return 62.0, 1.05, 0.05
    if highway in {"primary", "primary_link"}:
        return 48.0, 1.08, 0.08
    if highway in {"secondary", "secondary_link"}:
        return 42.0, 1.12, 0.11
    if highway in {"tertiary", "tertiary_link"}:
        return 36.0, 1.15, 0.14
    if highway in {"living_street", "service", "track"}:
        return 22.0, 1.18, 0.16
    return 28.0, 1.16, 0.15


def drive_edge_minutes(edge: dict[str, Any], traffic_multiplier: float = 1.0) -> float:
    length = float(edge.get("length", 0.0))
    default_speed_kph, road_buffer, junction_delay_min = road_time_parameters(edge)
    observed_minutes = float(edge.get("travel_time", 0.0)) / 60 if edge.get("travel_time") else 0.0
    base_minutes = observed_minutes if observed_minutes > 0 else meters_to_minutes(length, default_speed_kph)
    return base_minutes * road_buffer * traffic_multiplier + junction_delay_min


def path_weight_drive(_: int, __: int, edge_bundle: dict[str, Any], current_dt: datetime, traffic_model: TrafficModel) -> float:
    best_weight = float("inf")

    for edge in edge_bundle.values():
        geometry = edge.get("geometry")
        if geometry is not None and hasattr(geometry, "coords"):
            midpoint = geometry.interpolate(0.5, normalized=True)
            mid_lat = float(midpoint.y)
            mid_lng = float(midpoint.x)
        else:
            mid_lat = 0.0
            mid_lng = 0.0
        multiplier = traffic_model.congestion_multiplier(mid_lat, mid_lng, current_dt) if (mid_lat or mid_lng) else 1.0
        candidate = drive_edge_minutes(edge, multiplier)
        if candidate < best_weight:
            best_weight = candidate

    return best_weight if best_weight < float("inf") else 1.0


def edge_midpoint(graph: nx.MultiDiGraph, source: int, target: int, edge: dict[str, Any]) -> tuple[float, float]:
    coordinates = edge_coordinates(graph, source, target, edge)
    middle_index = len(coordinates) // 2
    return coordinates[middle_index]


def edge_travel_minutes(
    graph: nx.MultiDiGraph,
    source: int,
    target: int,
    edge: dict[str, Any],
    profile: str,
    current_dt: datetime,
    traffic_model: TrafficModel | None,
) -> float:
    length = float(edge.get("length", 0.0))

    if profile == "walk":
        return meters_to_minutes(length, EFFECTIVE_WALKING_SPEED_KPH)

    base_minutes = drive_edge_minutes(edge, 1.0)
    if traffic_model is None:
        return base_minutes

    mid_lat, mid_lng = edge_midpoint(graph, source, target, edge)
    multiplier = traffic_model.congestion_multiplier(mid_lat, mid_lng, current_dt)
    return drive_edge_minutes(edge, multiplier)


def build_route_trace(
    graph: nx.MultiDiGraph,
    route: list[int],
    profile: str,
    current_dt: datetime,
    traffic_model: TrafficModel | None,
    selection_weight: str,
) -> list[dict[str, float]]:
    trace: list[dict[str, float]] = []
    cumulative_distance_m = 0.0
    cumulative_minutes = 0.0

    first_node = graph.nodes[route[0]]
    trace.append({"lat": float(first_node["y"]), "lng": float(first_node["x"]), "distance_m": 0.0, "distance_km": 0.0, "minutes": 0.0})

    for source, target in zip(route[:-1], route[1:]):
        edge = pick_edge_data(graph, source, target, selection_weight)
        coordinates = edge_coordinates(graph, source, target, edge)
        edge_length = float(edge.get("length", 0.0))
        edge_minutes = edge_travel_minutes(graph, source, target, edge, profile, current_dt, traffic_model)

        partial_lengths: list[float] = []
        total_geometry_length = 0.0
        for start, end in zip(coordinates[:-1], coordinates[1:]):
            length = segment_distance_m(start, end)
            partial_lengths.append(length)
            total_geometry_length += length

        if total_geometry_length <= 0:
            total_geometry_length = edge_length if edge_length > 0 else 1.0
            partial_lengths = [total_geometry_length]
            coordinates = [coordinates[0], coordinates[-1]]

        for coordinate_index, coordinate in enumerate(coordinates[1:], start=1):
            segment_length = partial_lengths[coordinate_index - 1]
            ratio = segment_length / total_geometry_length if total_geometry_length else 0.0
            cumulative_distance_m += edge_length * ratio
            cumulative_minutes += edge_minutes * ratio
            trace.append(
                {
                    "lat": coordinate[0],
                    "lng": coordinate[1],
                    "distance_m": round(cumulative_distance_m, 2),
                    "distance_km": round(cumulative_distance_m / 1000, 3),
                    "minutes": round(cumulative_minutes, 2),
                }
            )

    return trace


def build_route_payload(
    graph: nx.MultiDiGraph,
    route: list[int],
    label: str,
    color: str,
    profile: str,
    selection_weight: str,
    current_dt: datetime,
    traffic_model: TrafficModel | None,
) -> dict[str, Any]:
    trace = build_route_trace(graph, route, profile, current_dt, traffic_model, selection_weight)
    total_distance_m = trace[-1]["distance_m"]
    total_minutes = trace[-1]["minutes"]

    return {
        "label": label,
        "color": color,
        "profile": profile,
        "distance_m": round(total_distance_m, 2),
        "distance_km": round(total_distance_m / 1000, 3),
        "estimated_minutes": round(total_minutes, 2),
        "trace": trace,
        "polyline": [[point["lat"], point["lng"]] for point in trace],
    }


def build_approximate_route(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    label: str,
    color: str,
    profile: str,
) -> dict[str, Any]:
    graph = resolve_graph("drive" if profile == "walk" else "drive")
    origin_node = ox.distance.nearest_nodes(graph, origin_lng, origin_lat)
    destination_node = ox.distance.nearest_nodes(graph, destination_lng, destination_lat)

    try:
        coarse_graph = localized_route_graph(graph, origin_node, destination_node, buffer_km=3.2)
        coarse_route = nx.astar_path(
            coarse_graph,
            origin_node,
            destination_node,
            heuristic=lambda node_a, node_b: node_distance_m(coarse_graph, node_a, node_b),
            weight="length",
        )
        payload = build_route_payload(
            graph=coarse_graph,
            route=coarse_route,
            label=label,
            color=color,
            profile=profile,
            selection_weight="length",
            current_dt=current_sydney_time(),
            traffic_model=resolve_traffic_model() if profile == "drive" else None,
        )
        payload["is_approximate"] = True
        return payload
    except Exception:
        direct_distance_m = segment_distance_m((origin_lat, origin_lng), (destination_lat, destination_lng))
        if profile == "walk":
            distance_factor = 1.22
            speed_kph = EFFECTIVE_WALKING_SPEED_KPH
        else:
            distance_factor = 1.16
            speed_kph = 24.0

        adjusted_distance_m = max(1.0, direct_distance_m * distance_factor)
        estimated_minutes = round(meters_to_minutes(adjusted_distance_m, speed_kph), 2)
        polyline = [[origin_lat, origin_lng], [destination_lat, destination_lng]]
        trace = build_trace_from_polyline(polyline, estimated_minutes)

        return {
            "label": label,
            "color": color,
            "profile": profile,
            "distance_m": round(adjusted_distance_m, 2),
            "distance_km": round(adjusted_distance_m / 1000, 3),
            "estimated_minutes": estimated_minutes,
            "trace": trace,
            "polyline": polyline,
            "is_approximate": True,
            "is_straight_fallback": True,
        }


def build_approximate_transit_route(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    label: str,
    color: str,
    walk_limit_km: float | None = None,
) -> dict[str, Any]:
    base_route = build_approximate_route(
        origin_lat,
        origin_lng,
        destination_lat,
        destination_lng,
        label,
        color,
        "drive",
    )
    adjusted_distance_m = float(base_route["distance_m"])
    total_distance_km = float(base_route["distance_km"])
    walk_distance_km = min(walk_limit_km if walk_limit_km is not None else 1.0, max(0.35, total_distance_km * 0.12))
    in_vehicle_km = max(0.1, total_distance_km - walk_distance_km)
    wait_minutes = 6.0
    estimated_minutes = round(meters_to_minutes(walk_distance_km * 1000, EFFECTIVE_WALKING_SPEED_KPH) + meters_to_minutes(in_vehicle_km * 1000, 22.0) + wait_minutes, 2)
    trace = build_trace_from_polyline(base_route["polyline"], estimated_minutes)

    midpoint_lat = (origin_lat + destination_lat) / 2
    midpoint_lng = (origin_lng + destination_lng) / 2

    return {
        "label": label,
        "color": color,
        "profile": "transit",
        "distance_m": round(adjusted_distance_m, 2),
        "distance_km": round(total_distance_km, 3),
        "estimated_minutes": estimated_minutes,
        "walk_distance_km": round(walk_distance_km, 3),
        "wait_minutes": wait_minutes,
        "interchanges": 1,
        "details": {
            "steps": [
                {
                    "mode": "walk",
                    "from": "Origin",
                    "to": "Nearest stop",
                    "distance_km": round(walk_distance_km * 0.5, 3),
                    "duration_minutes": round(meters_to_minutes(walk_distance_km * 500, EFFECTIVE_WALKING_SPEED_KPH), 2),
                },
                {
                    "mode": "transit",
                    "line": "Transit service",
                    "from": "Nearest stop",
                    "to": "Destination stop",
                    "wait_minutes": wait_minutes,
                    "distance_km": round(in_vehicle_km, 3),
                    "duration_minutes": round(meters_to_minutes(in_vehicle_km * 1000, 24.0), 2),
                    "stops_count": max(2, int(in_vehicle_km // 1.2)),
                    "alternatives": [],
                },
                {
                    "mode": "walk",
                    "from": "Destination stop",
                    "to": "Destination",
                    "distance_km": round(walk_distance_km * 0.5, 3),
                    "duration_minutes": round(meters_to_minutes(walk_distance_km * 500, EFFECTIVE_WALKING_SPEED_KPH), 2),
                },
            ],
            "transfer_points": [
                {
                    "lat": midpoint_lat,
                    "lng": midpoint_lng,
                    "title": "Approximate transfer point",
                    "summary": "This is an approximate public transport handoff used to keep results within 15 seconds.",
                    "from": "Walk",
                    "to": "Transit",
                }
            ],
        },
        "trace": trace,
        "polyline": base_route["polyline"],
        "is_approximate": True,
    }


def build_drive_routes(origin_node: int, destination_node: int, current_dt: datetime) -> list[dict[str, Any]]:
    graph = resolve_graph("drive")
    traffic_model = resolve_traffic_model()

    shortest_route = nx.astar_path(
        graph,
        origin_node,
        destination_node,
        heuristic=lambda node_a, node_b: node_distance_m(graph, node_a, node_b),
        weight="length",
    )
    fastest_route = nx.astar_path(
        graph,
        origin_node,
        destination_node,
        heuristic=lambda node_a, node_b: node_distance_m(graph, node_a, node_b) / (110000 / 60),
        weight=lambda u, v, data: path_weight_drive(u, v, data, current_dt, traffic_model),
    )

    return [
        build_route_payload(
            graph=graph,
            route=shortest_route,
            label="Shortest drive",
            color="#d9480f",
            profile="drive",
            selection_weight="length",
            current_dt=current_dt,
            traffic_model=traffic_model,
        ),
        build_route_payload(
            graph=graph,
            route=fastest_route,
            label="Fastest drive",
            color="#1971c2",
            profile="drive",
            selection_weight="travel_time",
            current_dt=current_dt,
            traffic_model=traffic_model,
        ),
    ]


def build_walk_routes(origin_node: int, destination_node: int, current_dt: datetime) -> list[dict[str, Any]]:
    graph = resolve_graph("walk")
    graph = localized_route_graph(graph, origin_node, destination_node)
    walking_route = nx.astar_path(
        graph,
        origin_node,
        destination_node,
        heuristic=lambda node_a, node_b: node_distance_m(graph, node_a, node_b),
        weight="length",
    )

    return [
        build_route_payload(
            graph=graph,
            route=walking_route,
            label="Walking route",
            color="#2b8a3e",
            profile="walk",
            selection_weight="length",
            current_dt=current_dt,
            traffic_model=None,
        )
    ]


def resolve_routes_with_fallback(
    profile: str,
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    current_dt: datetime,
    max_walk_km: float | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []

    def run_builder() -> tuple[list[dict[str, Any]], list[str]]:
        if profile == "drive":
            graph = resolve_graph("drive")
            origin_node = ox.distance.nearest_nodes(graph, origin_lng, origin_lat)
            destination_node = ox.distance.nearest_nodes(graph, destination_lng, destination_lat)
            return build_drive_routes(origin_node, destination_node, current_dt), []
        if profile == "walk":
            graph = resolve_graph("walk")
            origin_node = ox.distance.nearest_nodes(graph, origin_lng, origin_lat)
            destination_node = ox.distance.nearest_nodes(graph, destination_lng, destination_lat)
            return build_walk_routes(origin_node, destination_node, current_dt), []
        if profile == "transit":
            return build_transit_routes(origin_lat, origin_lng, destination_lat, destination_lng), []
        if max_walk_km is None:
            raise HTTPException(status_code=400, detail="Combined mode requires max_walk_distance_km.")
        graph = resolve_graph("drive")
        origin_node = ox.distance.nearest_nodes(graph, origin_lng, origin_lat)
        destination_node = ox.distance.nearest_nodes(graph, destination_lng, destination_lat)
        return build_combined_routes(
            origin_node,
            destination_node,
            origin_lat,
            origin_lng,
            destination_lat,
            destination_lng,
            current_dt,
            max_walk_km,
        )

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(run_builder)
    try:
        return future.result(timeout=ROUTE_DEADLINE_SECONDS)
    except FutureTimeoutError:
        future.cancel()
        if profile == "drive":
            approx_shortest = build_approximate_route(origin_lat, origin_lng, destination_lat, destination_lng, "Shortest drive", "#d9480f", "drive")
            approx_fastest = {**approx_shortest, "label": "Fastest drive", "color": "#1971c2"}
            warnings.append("Drive routing reached the 15-second limit, so a coarse road-based route was returned.")
            return [approx_shortest, approx_fastest], warnings
        if profile == "walk":
            approx_walk = build_approximate_route(origin_lat, origin_lng, destination_lat, destination_lng, "Walking route", "#2b8a3e", "walk")
            warnings.append("Walking routing reached the 15-second limit, so a coarse road-based route was returned.")
            return [approx_walk], warnings
        if profile == "transit":
            approx_fastest = build_approximate_transit_route(origin_lat, origin_lng, destination_lat, destination_lng, "Fastest transit", "#5f3dc4")
            approx_shortest = {**approx_fastest, "label": "Shortest transit", "color": "#0b7285"}
            warnings.append("Transit routing reached the 15-second limit, so an approximate route was returned.")
            return [approx_shortest, approx_fastest], warnings
        approx_fastest_combined = build_approximate_transit_route(
            origin_lat,
            origin_lng,
            destination_lat,
            destination_lng,
            "Fastest combined (Approximate transit)",
            "#5f3dc4",
            max_walk_km,
        )
        approx_shortest_combined = {
            **approx_fastest_combined,
            "label": "Shortest combined (Approximate transit)",
            "color": "#0b7285",
        }
        approx_fastest_combined["walk_distance_km"] = round(min(max_walk_km or 1.0, approx_fastest_combined.get("walk_distance_km", 1.0)), 3)
        approx_shortest_combined["walk_distance_km"] = approx_fastest_combined["walk_distance_km"]
        warnings.append("Combined routing reached the 15-second limit, so an approximate route was returned.")
        return [approx_shortest_combined, approx_fastest_combined], warnings
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, Any]:
    with _preload_status_lock:
        preload_snapshot = dict(_preload_status)
    return {"status": "ok", "preload": preload_snapshot}


@app.get("/api/network")
def network_overview() -> dict[str, Any]:
    graph = resolve_graph("drive")
    latitudes = [float(data["y"]) for _, data in graph.nodes(data=True)]
    longitudes = [float(data["x"]) for _, data in graph.nodes(data=True)]

    return {
        "city": SYDNEY_PLACE,
        "bounds": {"south": min(latitudes), "north": max(latitudes), "west": min(longitudes), "east": max(longitudes)},
        "geojson": resolve_network_geojson(),
    }


@app.get("/api/suggest")
def suggest_places(
    q: str = Query(..., min_length=1),
    mode: str = Query(..., pattern="^(origin|destination)$"),
    user_lat: float | None = None,
    user_lng: float | None = None,
) -> dict[str, list[str]]:
    query = q.strip()
    if not query:
        return {"suggestions": []}

    suggestions: list[str] = []
    seen: set[str] = set()

    if mode == "destination":
        for place in FAMOUS_SYDNEY_PLACES:
            if place.lower().startswith(query.lower()):
                seen.add(place)
                suggestions.append(place)

    viewbox = None
    if mode == "origin" and user_lat is not None and user_lng is not None:
        delta = 0.12
        viewbox = f"{user_lng - delta},{user_lat + delta},{user_lng + delta},{user_lat - delta}"

    remote_results = search_places(
        query=f"{query}, Sydney",
        limit=6 if mode == "origin" else 8,
        viewbox=viewbox,
        bounded=mode == "origin" and viewbox is not None,
    )
    for result in remote_results:
        if result not in seen:
            seen.add(result)
            suggestions.append(result)

    if mode == "destination":
        for place in FAMOUS_SYDNEY_PLACES:
            if query.lower() in place.lower() and place not in seen:
                seen.add(place)
                suggestions.append(place)

    return {"suggestions": suggestions[:8]}


@app.post("/api/snap-location")
def snap_location(payload: LocationRequest) -> dict[str, Any]:
    graph = resolve_graph("drive")
    snapped_node = nearest_node_info(graph, payload.lat, payload.lng)

    return {
        "user_location": {"lat": payload.lat, "lng": payload.lng, "query": f"{payload.lat:.6f},{payload.lng:.6f}", "coordinates": f"{payload.lat:.6f},{payload.lng:.6f}"},
        "nearest_road_node": {
            "id": snapped_node["id"],
            "lat": snapped_node["lat"],
            "lng": snapped_node["lng"],
            "query": f"{snapped_node['lat']:.6f},{snapped_node['lng']:.6f}",
            "coordinates": f"{snapped_node['lat']:.6f},{snapped_node['lng']:.6f}",
        },
    }


@app.post("/api/reverse-geocode")
def resolve_place_name(payload: ReverseGeocodeRequest) -> dict[str, str]:
    return {"query": reverse_geocode(payload.lat, payload.lng)}


@app.post("/api/route")
def calculate_route(payload: RouteRequest) -> dict[str, Any]:
    profile = payload.profile

    origin_lat, origin_lng = geocode_point(payload.origin)
    destination_lat, destination_lng = geocode_point(payload.destination)

    try:
        current_dt = current_sydney_time()
        routes, warnings = resolve_routes_with_fallback(
            profile,
            origin_lat,
            origin_lng,
            destination_lat,
            destination_lng,
            current_dt,
            payload.max_walk_distance_km,
        )
    except nx.NetworkXNoPath as exc:
        raise HTTPException(status_code=404, detail="No valid path exists between these two locations.") from exc

    for route in routes:
        route["weather_summary"] = build_route_weather_summary(route, current_dt)

    return {
        "city": SYDNEY_PLACE,
        "profile": profile,
        "generated_at": current_dt.isoformat(),
        "origin": {"query": payload.origin, "lat": origin_lat, "lng": origin_lng},
        "destination": {"query": payload.destination, "lat": destination_lat, "lng": destination_lng},
        "off_route_threshold_m": OFF_ROUTE_THRESHOLD_METERS,
        "warnings": warnings,
        "routes": routes,
    }
