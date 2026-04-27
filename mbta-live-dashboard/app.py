import asyncio
import csv
import io
import ssl
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import certifi
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from google.transit import gtfs_realtime_pb2


BASE_DIR = Path(__file__).resolve().parent

VEHICLE_FEED_URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
TRIP_FEED_URL = "https://cdn.mbta.com/realtime/TripUpdates.pb"
ALERTS_FEED_URL = "https://cdn.mbta.com/realtime/Alerts.pb"
STATIC_GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"

METERS_PER_SECOND_TO_MPH = 2.2369362921
FRESH_VEHICLE_SECONDS = 90
STATIC_GTFS_TTL_SECONDS = 24 * 60 * 60


app = FastAPI(title="5114 Live MBTA Bus Tracker")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


class FeedCache:
    def __init__(self, url: str, ttl_seconds: int):
        self.url = url
        self.ttl_seconds = ttl_seconds
        self._feed: gtfs_realtime_pb2.FeedMessage | None = None
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self) -> gtfs_realtime_pb2.FeedMessage:
        now = time.time()
        if self._feed and now - self._fetched_at < self.ttl_seconds:
            return self._feed

        async with self._lock:
            now = time.time()
            if self._feed and now - self._fetched_at < self.ttl_seconds:
                return self._feed

            self._feed = await asyncio.to_thread(fetch_feed, self.url)
            self._fetched_at = time.time()
            return self._feed


vehicle_cache = FeedCache(VEHICLE_FEED_URL, ttl_seconds=3)
trip_cache = FeedCache(TRIP_FEED_URL, ttl_seconds=5)
alerts_cache = FeedCache(ALERTS_FEED_URL, ttl_seconds=15)


class StaticGtfsCache:
    def __init__(self, url: str, ttl_seconds: int):
        self.url = url
        self.ttl_seconds = ttl_seconds
        self._data: dict[str, Any] = {}
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self) -> dict[str, Any]:
        now = time.time()
        if self._data and now - self._fetched_at < self.ttl_seconds:
            return self._data

        async with self._lock:
            now = time.time()
            if self._data and now - self._fetched_at < self.ttl_seconds:
                return self._data

            try:
                self._data = await asyncio.to_thread(fetch_static_gtfs, self.url)
                self._fetched_at = time.time()
            except (OSError, TimeoutError, URLError, zipfile.BadZipFile, KeyError, csv.Error):
                if not self._data:
                    self._data = {"stops": {}, "direction_labels": {}, "trip_headsigns": {}, "route_shapes": {}}
            return self._data


static_gtfs_cache = StaticGtfsCache(STATIC_GTFS_URL, ttl_seconds=STATIC_GTFS_TTL_SECONDS)


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    request = Request(url, headers={"User-Agent": "mbta-live-fastapi/1.0"})
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with urlopen(request, timeout=20, context=ssl_context) as response:
            payload = response.read()
    except (OSError, TimeoutError, URLError) as exc:
        raise HTTPException(status_code=503, detail=f"Unable to fetch MBTA feed: {exc}") from exc

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def fetch_static_gtfs(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "mbta-live-fastapi/1.0"})
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(request, timeout=30, context=ssl_context) as response:
        payload = response.read()

    with zipfile.ZipFile(io.BytesIO(payload)) as gtfs_zip:
        with gtfs_zip.open("stops.txt") as stops_file:
            rows = csv.DictReader(io.TextIOWrapper(stops_file, encoding="utf-8-sig"))
            stops = {
                row["stop_id"]: row.get("stop_name", "").strip()
                for row in rows
                if row.get("stop_id") and row.get("stop_name")
            }

        shape_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        headsign_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        trip_headsigns = {}
        with gtfs_zip.open("trips.txt") as trips_file:
            rows = csv.DictReader(io.TextIOWrapper(trips_file, encoding="utf-8-sig"))
            for row in rows:
                route_id = row.get("route_id", "").strip()
                if not is_bus_route(route_id):
                    continue

                direction_id = row.get("direction_id", "").strip()
                shape_id = row.get("shape_id", "").strip()
                headsign = row.get("trip_headsign", "").strip()
                trip_id = row.get("trip_id", "").strip()

                if trip_id and headsign:
                    trip_headsigns[trip_id] = headsign
                if headsign:
                    headsign_counts[(route_id, direction_id)][headsign] += 1
                if shape_id:
                    shape_counts[(route_id, direction_id)][shape_id] += 1

        direction_labels: dict[str, dict[str, str]] = defaultdict(dict)
        for (route_id, direction_id), counts in headsign_counts.items():
            direction_labels[route_id][direction_id] = counts.most_common(1)[0][0]

        selected_shapes = {}
        for (route_id, direction_id), counts in shape_counts.items():
            direction_label = direction_labels.get(route_id, {}).get(direction_id, f"Direction {direction_id}")
            for shape_id, trip_count in counts.most_common(3):
                selected_shapes[shape_id] = {
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "direction_label": direction_label,
                    "trip_count": trip_count,
                }

        shape_points: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
        with gtfs_zip.open("shapes.txt") as shapes_file:
            rows = csv.DictReader(io.TextIOWrapper(shapes_file, encoding="utf-8-sig"))
            for row in rows:
                shape_id = row.get("shape_id", "").strip()
                if shape_id not in selected_shapes:
                    continue

                try:
                    sequence = int(row.get("shape_pt_sequence", "0"))
                    lat = float(row["shape_pt_lat"])
                    lon = float(row["shape_pt_lon"])
                except (TypeError, ValueError, KeyError):
                    continue
                shape_points[shape_id].append((sequence, lon, lat))

        route_shapes: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for shape_id, points in shape_points.items():
            if len(points) < 2:
                continue

            meta = selected_shapes[shape_id]
            coordinates = [[lon, lat] for _, lon, lat in sorted(points)]
            route_shapes[meta["route_id"]].append(
                {
                    "type": "Feature",
                    "id": shape_id,
                    "geometry": {"type": "LineString", "coordinates": coordinates},
                    "properties": {
                        "shape_id": shape_id,
                        "route_id": meta["route_id"],
                        "direction_id": meta["direction_id"],
                        "direction_label": meta["direction_label"],
                        "trip_count": meta["trip_count"],
                    },
                }
            )

        for route_id in route_shapes:
            route_shapes[route_id].sort(key=lambda item: (item["properties"]["direction_id"], -item["properties"]["trip_count"]))

        return {
            "stops": stops,
            "direction_labels": dict(direction_labels),
            "trip_headsigns": trip_headsigns,
            "route_shapes": dict(route_shapes),
        }


def iso_from_unix(timestamp: int | float | None) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def age_seconds(timestamp: int | float | None) -> int | None:
    if not timestamp:
        return None
    return max(0, int(time.time() - timestamp))


def enum_name(enum_type: Any, value: int) -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return str(value)


def is_bus_route(route_id: str | None) -> bool:
    """MBTA bus routes are mostly numeric, CT*, or SL*. Exclude known rail/ferry ids."""
    if not route_id:
        return False

    non_bus_prefixes = ("CR-", "Boat-", "Ferry-")
    non_bus_ids = {
        "Red",
        "Orange",
        "Blue",
        "Mattapan",
        "Green-B",
        "Green-C",
        "Green-D",
        "Green-E",
    }
    if route_id in non_bus_ids or route_id.startswith(non_bus_prefixes):
        return False

    return route_id.isdigit() or route_id.startswith("CT") or route_id.startswith("SL")


def natural_route_key(route_id: str) -> tuple[int, Any]:
    if route_id.isdigit():
        return (0, int(route_id))
    if route_id.startswith("SL"):
        return (1, route_id)
    if route_id.startswith("CT"):
        return (2, route_id)
    return (3, route_id)


def parse_vehicle_feature(entity: Any, feed_timestamp: int | None, static_data: dict[str, Any]) -> dict[str, Any] | None:
    if not entity.HasField("vehicle"):
        return None

    vehicle = entity.vehicle
    if not vehicle.HasField("position"):
        return None

    route_id = vehicle.trip.route_id if vehicle.HasField("trip") else ""
    if not is_bus_route(route_id):
        return None

    position = vehicle.position
    if not position.HasField("latitude") or not position.HasField("longitude"):
        return None

    vehicle_id = vehicle.vehicle.id if vehicle.HasField("vehicle") else entity.id
    vehicle_label = vehicle.vehicle.label if vehicle.HasField("vehicle") else ""
    trip_id = vehicle.trip.trip_id if vehicle.HasField("trip") else ""
    direction_id = vehicle.trip.direction_id if vehicle.HasField("trip") and vehicle.trip.HasField("direction_id") else None
    direction_key = str(direction_id) if direction_id is not None else ""
    stop_names = static_data.get("stops", {})
    direction_label = (
        static_data.get("trip_headsigns", {}).get(trip_id)
        or static_data.get("direction_labels", {}).get(route_id, {}).get(direction_key)
        or (f"Direction {direction_id}" if direction_id is not None else "")
    )
    updated_ts = vehicle.timestamp or feed_timestamp
    speed_mph = None
    if position.HasField("speed"):
        speed_mph = round(position.speed * METERS_PER_SECOND_TO_MPH, 1)

    props = {
        "vehicle_id": vehicle_id,
        "vehicle_label": vehicle_label,
        "route_id": route_id,
        "trip_id": trip_id,
        "direction_id": direction_id,
        "direction_label": direction_label,
        "status": enum_name(gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus, vehicle.current_status),
        "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else "",
        "stop_name": stop_names.get(vehicle.stop_id, "") if vehicle.HasField("stop_id") else "",
        "current_stop_sequence": vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None,
        "speed_mph": speed_mph,
        "updated_at": iso_from_unix(updated_ts),
        "updated_timestamp": int(updated_ts) if updated_ts else None,
        "age_seconds": age_seconds(updated_ts),
        "is_fresh": bool(updated_ts and age_seconds(updated_ts) <= FRESH_VEHICLE_SECONDS),
    }

    return {
        "type": "Feature",
        "id": vehicle_id,
        "geometry": {
            "type": "Point",
            "coordinates": [position.longitude, position.latitude],
        },
        "properties": props,
    }


async def vehicle_features(route_id: str | None = None) -> list[dict[str, Any]]:
    feed, static_data = await asyncio.gather(vehicle_cache.get(), static_gtfs_cache.get())
    features = []
    for entity in feed.entity:
        feature = parse_vehicle_feature(entity, feed.header.timestamp, static_data)
        if not feature:
            continue
        if route_id and feature["properties"]["route_id"] != route_id:
            continue
        features.append(feature)

    features.sort(key=lambda item: item["properties"].get("updated_timestamp") or 0, reverse=True)
    return features


async def find_vehicle(vehicle_id: str) -> dict[str, Any]:
    for feature in await vehicle_features():
        if feature["properties"]["vehicle_id"] == vehicle_id:
            return feature
    raise HTTPException(status_code=404, detail="Vehicle is not currently visible in the live feed")


def stop_time_to_dict(stop_time: Any, stop_names: dict[str, str]) -> dict[str, Any]:
    arrival = stop_time.arrival.time if stop_time.HasField("arrival") and stop_time.arrival.HasField("time") else None
    departure = stop_time.departure.time if stop_time.HasField("departure") and stop_time.departure.HasField("time") else None
    return {
        "stop_sequence": stop_time.stop_sequence if stop_time.HasField("stop_sequence") else None,
        "stop_id": stop_time.stop_id if stop_time.HasField("stop_id") else "",
        "stop_name": stop_names.get(stop_time.stop_id, "") if stop_time.HasField("stop_id") else "",
        "arrival_time": iso_from_unix(arrival),
        "departure_time": iso_from_unix(departure),
    }


def active_alert(alert: Any) -> bool:
    if not alert.active_period:
        return True
    now = int(time.time())
    for period in alert.active_period:
        start_ok = not period.HasField("start") or period.start <= now
        end_ok = not period.HasField("end") or period.end >= now
        if start_ok and end_ok:
            return True
    return False


def translated_text(text: Any) -> str:
    for translation in text.translation:
        if not translation.language or translation.language.lower().startswith("en"):
            return translation.text
    return text.translation[0].text if text.translation else ""


def feed_meta(feed: gtfs_realtime_pb2.FeedMessage) -> dict[str, Any]:
    timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else None
    return {
        "timestamp": iso_from_unix(timestamp),
        "age_seconds": age_seconds(timestamp),
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return (BASE_DIR / "templates" / "index.html").read_text(encoding="utf-8")


@app.get("/api/live/routes")
async def live_routes() -> list[dict[str, str]]:
    routes = sorted(
        {feature["properties"]["route_id"] for feature in await vehicle_features()},
        key=natural_route_key,
    )
    return [{"route_id": route_id, "label": route_id} for route_id in routes]


@app.get("/api/live/vehicles")
async def live_vehicles(route_id: str | None = Query(default=None)) -> dict[str, Any]:
    features = await vehicle_features(route_id)
    return {
        "type": "FeatureCollection",
        "features": features,
    }


@app.get("/api/live/vehicle/{vehicle_id}")
async def live_vehicle(vehicle_id: str) -> dict[str, Any]:
    feature = await find_vehicle(vehicle_id)
    return feature["properties"]


@app.get("/api/live/vehicle/{vehicle_id}/upcoming-stops")
async def upcoming_stops(vehicle_id: str) -> dict[str, Any]:
    vehicle = (await find_vehicle(vehicle_id))["properties"]
    trip_id = vehicle.get("trip_id")
    current_sequence = vehicle.get("current_stop_sequence")
    now = int(time.time())
    feed, static_data = await asyncio.gather(trip_cache.get(), static_gtfs_cache.get())
    stop_names = static_data.get("stops", {})

    best_update = None
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        update = entity.trip_update
        update_vehicle_id = update.vehicle.id if update.HasField("vehicle") else ""
        update_trip_id = update.trip.trip_id if update.HasField("trip") else ""
        if update_vehicle_id == vehicle_id or (trip_id and update_trip_id == trip_id):
            best_update = update
            break

    if not best_update:
        return {"vehicle_id": vehicle_id, "stops": [], "message": "Upcoming stops are unavailable for this bus right now."}

    stops = []
    for stop_time in best_update.stop_time_update:
        sequence = stop_time.stop_sequence if stop_time.HasField("stop_sequence") else None
        arrival = stop_time.arrival.time if stop_time.HasField("arrival") and stop_time.arrival.HasField("time") else None
        departure = stop_time.departure.time if stop_time.HasField("departure") and stop_time.departure.HasField("time") else None
        event_time = arrival or departure

        if current_sequence is not None and sequence is not None:
            if sequence <= current_sequence:
                continue
        elif event_time and event_time < now:
            continue

        stops.append(stop_time_to_dict(stop_time, stop_names))
        if len(stops) >= 8:
            break

    return {"vehicle_id": vehicle_id, "trip_id": trip_id, "stops": stops}


@app.get("/api/live/route-shapes")
async def live_route_shapes(route_id: str = Query(...)) -> dict[str, Any]:
    static_data = await static_gtfs_cache.get()
    return {
        "type": "FeatureCollection",
        "features": static_data.get("route_shapes", {}).get(route_id, []),
    }


@app.get("/api/live/alerts")
async def live_alerts(route_id: str | None = Query(default=None)) -> list[dict[str, Any]]:
    feed = await alerts_cache.get()
    results = []
    for entity in feed.entity:
        if not entity.HasField("alert") or not active_alert(entity.alert):
            continue

        routes_affected = sorted(
            {
                informed.route_id
                for informed in entity.alert.informed_entity
                if informed.HasField("route_id") and is_bus_route(informed.route_id)
            },
            key=natural_route_key,
        )
        if route_id and route_id not in routes_affected:
            continue
        if not routes_affected:
            continue

        results.append(
            {
                "alert_id": entity.id,
                "header": translated_text(entity.alert.header_text),
                "description": translated_text(entity.alert.description_text),
                "cause": enum_name(gtfs_realtime_pb2.Alert.Cause, entity.alert.cause),
                "effect": enum_name(gtfs_realtime_pb2.Alert.Effect, entity.alert.effect),
                "routes_affected": routes_affected,
            }
        )
    return results


@app.get("/api/live/meta")
async def live_meta() -> dict[str, Any]:
    vehicle_feed, trip_feed, alerts_feed = await asyncio.gather(
        vehicle_cache.get(),
        trip_cache.get(),
        alerts_cache.get(),
    )
    vehicle = feed_meta(vehicle_feed)
    trip = feed_meta(trip_feed)
    alerts = feed_meta(alerts_feed)
    return {
        "vehicle_feed_timestamp": vehicle["timestamp"],
        "vehicle_feed_age_seconds": vehicle["age_seconds"],
        "trip_feed_timestamp": trip["timestamp"],
        "trip_feed_age_seconds": trip["age_seconds"],
        "alerts_feed_timestamp": alerts["timestamp"],
        "alerts_feed_age_seconds": alerts["age_seconds"],
    }
