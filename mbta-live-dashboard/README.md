# 5114 Live MBTA Bus Tracker

A small FastAPI and vanilla JavaScript app for live MBTA bus tracking. The backend fetches MBTA GTFS-Realtime protobuf feeds, parses vehicles, trip updates, and alerts, enriches them with MBTA static GTFS stop names, direction labels, and route shapes, then serves JSON to a MapLibre GL JS frontend.

The map is created once on page load. Polling refreshes the existing `vehicles` GeoJSON source with `map.getSource("vehicles").setData(...)`, so panning and zooming are preserved while live bus positions update in place. Static GTFS route shapes are drawn under the bus markers so users can see the route path before interpreting live vehicle positions.

## What Users Can Check

- Choose an active MBTA bus route.
- See the selected route's path on the map.
- See live buses on top of the route path.
- Filter the map and bus list to fresh vehicle positions only.
- Select a bus from the map, dropdown, or active bus cards.
- Check the selected bus's current stop name, status, speed, update age, and direction/headsign.
- See upcoming stop names and estimated arrival/departure times for the selected bus.
- Check active route alerts and feed freshness.

## Run Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

Open `http://127.0.0.1:8000`.

## Endpoints

- `GET /` serves the live tracker page.
- `GET /api/live/routes` returns active bus routes from the vehicle feed.
- `GET /api/live/vehicles?route_id=...` returns live bus positions as a GeoJSON FeatureCollection.
- `GET /api/live/vehicle/{vehicle_id}` returns current details for one bus.
- `GET /api/live/vehicle/{vehicle_id}/upcoming-stops` returns upcoming stops from trip updates when available.
- `GET /api/live/route-shapes?route_id=...` returns representative static GTFS route shapes as GeoJSON LineString features.
- `GET /api/live/alerts?route_id=...` returns active bus route alerts.
- `GET /api/live/meta` returns feed timestamps and ages.

## Realtime Flow

The backend keeps short feed caches only at the protobuf fetch layer:

- Vehicle positions: 3 seconds
- Trip updates: 5 seconds
- Alerts: 15 seconds

The frontend polls vehicle, selected vehicle, and upcoming stop data every 5 seconds, and route alerts every 15 seconds. A failed request leaves the last rendered map and panels in place.

## Static GTFS Enrichment

The app also downloads MBTA static GTFS from `https://cdn.mbta.com/MBTA_GTFS.zip` and caches parsed data for 24 hours.

It uses:

- `stops.txt` for human-readable stop names.
- `trips.txt` for trip headsigns and direction labels.
- `shapes.txt` for route path lines drawn on the map.

For each route and direction, the backend picks the most common shapes from static GTFS so the map shows the main route path without sending every possible shape variant to the browser.

## Recent Snapshot Layer

This project also includes a lightweight AWS Lambda snapshot writer for the MBTA `VehiclePositions.pb` feed:

```text
lambda_vehicle_positions/
```

That Lambda is separate from the FastAPI UI and from the existing batch/history pipeline. It captures recent operational state only:

```text
MBTA VehiclePositions.pb -> Lambda -> S3 JSON.gz snapshots
```

Default target:

```text
s3://5114-transit-project-data/realtime_snapshots/vehicle_positions/
```

The Lambda writes one gzip-compressed newline-delimited JSON file per feed pull, partitioned by date and hour. With the default settings, one EventBridge invocation per minute loops internally and captures roughly one snapshot every 10 seconds.

Use this layer for short route replay, vehicle trails, feed freshness checks, and debugging disappearing vehicles. Keep retention short with an S3 lifecycle rule, for example 3 days, so this does not duplicate the longer-term Spark/Airflow/Snowflake history pipeline.

## Notes

The server filters non-bus routes before sending route options, vehicle features, or route shapes to the browser. MBTA bus route IDs are treated as numeric routes plus `CT*` and `SL*`; known subway, commuter rail, and ferry route IDs are excluded.
