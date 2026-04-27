import gzip
import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import boto3
from google.transit import gtfs_realtime_pb2


VEHICLE_POSITIONS_URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
REQUEST_TIMEOUT_SECONDS = 20

SNAPSHOT_BUCKET = os.environ.get("SNAPSHOT_BUCKET", "5114-transit-project-data")
SNAPSHOT_PREFIX = os.environ.get("SNAPSHOT_PREFIX", "realtime_snapshots/vehicle_positions").strip("/")
BUS_ONLY = os.environ.get("BUS_ONLY", "true").lower() == "true"
PULLS_PER_INVOCATION = int(os.environ.get("PULLS_PER_INVOCATION", "6"))
SECONDS_BETWEEN_PULLS = int(os.environ.get("SECONDS_BETWEEN_PULLS", "10"))

s3 = boto3.client("s3")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def compact_utc(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def age_seconds(timestamp: int | None, now: datetime) -> int | None:
    if not timestamp:
        return None
    return max(0, int(now.timestamp()) - int(timestamp))


def enum_name(enum_type: Any, value: int, default: str = "UNKNOWN") -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return default


def is_bus_route(route_id: str | None) -> bool:
    if not route_id:
        return False

    route_id = str(route_id).strip()
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

    return route_id.isdigit() or route_id.startswith("SL") or route_id.startswith("CT")


def fetch_vehicle_feed() -> gtfs_realtime_pb2.FeedMessage:
    request = Request(VEHICLE_POSITIONS_URL, headers={"User-Agent": "mbta-vehicle-snapshot-lambda/1.0"})
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = response.read()
    except (OSError, TimeoutError, URLError) as exc:
        raise RuntimeError(f"Unable to fetch MBTA VehiclePositions feed: {exc}") from exc

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def parse_vehicle_rows(feed: gtfs_realtime_pb2.FeedMessage, snapshot_dt: datetime) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    feed_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else None
    rows = []
    skipped_non_bus = 0
    skipped_no_position = 0

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        if not vehicle.HasField("position"):
            skipped_no_position += 1
            continue

        trip = vehicle.trip if vehicle.HasField("trip") else None
        route_id = trip.route_id if trip else ""

        if BUS_ONLY and not is_bus_route(route_id):
            skipped_non_bus += 1
            continue

        position = vehicle.position
        if not position.HasField("latitude") or not position.HasField("longitude"):
            skipped_no_position += 1
            continue

        vehicle_descriptor = vehicle.vehicle if vehicle.HasField("vehicle") else None
        vehicle_timestamp = vehicle.timestamp if vehicle.HasField("timestamp") else None

        rows.append(
            {
                "snapshot_ts": iso_utc(snapshot_dt),
                "snapshot_unix": int(snapshot_dt.timestamp()),
                "feed_timestamp": int(feed_timestamp) if feed_timestamp else None,
                "feed_age_seconds": age_seconds(feed_timestamp, snapshot_dt),
                "entity_id": entity.id,
                "vehicle_id": vehicle_descriptor.id if vehicle_descriptor and vehicle_descriptor.HasField("id") else entity.id,
                "vehicle_label": vehicle_descriptor.label if vehicle_descriptor and vehicle_descriptor.HasField("label") else "",
                "route_id": str(route_id) if route_id else "",
                "trip_id": trip.trip_id if trip and trip.HasField("trip_id") else "",
                "direction_id": trip.direction_id if trip and trip.HasField("direction_id") else None,
                "latitude": position.latitude,
                "longitude": position.longitude,
                "bearing": position.bearing if position.HasField("bearing") else None,
                "speed_mps": position.speed if position.HasField("speed") else None,
                "current_stop_sequence": vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None,
                "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else "",
                "status": enum_name(
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus,
                    vehicle.current_status,
                    default="NO_DATA",
                )
                if vehicle.HasField("current_status")
                else "NO_DATA",
                "vehicle_timestamp": int(vehicle_timestamp) if vehicle_timestamp else None,
                "vehicle_age_seconds": age_seconds(vehicle_timestamp, snapshot_dt),
            }
        )

    meta = {
        "feed_timestamp": int(feed_timestamp) if feed_timestamp else None,
        "feed_age_seconds": age_seconds(feed_timestamp, snapshot_dt),
        "row_count": len(rows),
        "skipped_non_bus_count": skipped_non_bus,
        "skipped_no_position_count": skipped_no_position,
    }
    return rows, meta


def s3_key_for_snapshot(snapshot_dt: datetime) -> str:
    return (
        f"{SNAPSHOT_PREFIX}/"
        f"dt={snapshot_dt.strftime('%Y-%m-%d')}/"
        f"hour={snapshot_dt.strftime('%H')}/"
        f"vehicle_positions_{compact_utc(snapshot_dt)}.json.gz"
    )


def gzip_ndjson(rows: list[dict[str, Any]]) -> bytes:
    payload = "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    if payload:
        payload += "\n"
    return gzip.compress(payload.encode("utf-8"))


def write_rows_to_s3(rows: list[dict[str, Any]], snapshot_dt: datetime) -> str:
    key = s3_key_for_snapshot(snapshot_dt)
    s3.put_object(
        Bucket=SNAPSHOT_BUCKET,
        Key=key,
        Body=gzip_ndjson(rows),
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
        Metadata={
            "feed": "vehicle_positions",
            "snapshot_ts": iso_utc(snapshot_dt),
            "row_count": str(len(rows)),
            "bus_only": str(BUS_ONLY).lower(),
        },
    )
    return key


def capture_once() -> dict[str, Any]:
    snapshot_dt = utc_now()
    feed = fetch_vehicle_feed()
    rows, meta = parse_vehicle_rows(feed, snapshot_dt)
    key = write_rows_to_s3(rows, snapshot_dt)

    result = {
        "feed": "vehicle_positions",
        "bucket": SNAPSHOT_BUCKET,
        "key": key,
        "snapshot_ts": iso_utc(snapshot_dt),
        **meta,
    }
    print(json.dumps(result, sort_keys=True))
    return result


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    event = event or {}
    pulls = int(event.get("pulls", PULLS_PER_INVOCATION))
    seconds_between_pulls = int(event.get("seconds_between_pulls", SECONDS_BETWEEN_PULLS))

    results = []
    for index in range(pulls):
        results.append(capture_once())
        if index < pulls - 1 and seconds_between_pulls > 0:
            time.sleep(seconds_between_pulls)

    return {
        "statusCode": 200,
        "body": {
            "pull_count": len(results),
            "results": results,
        },
    }
