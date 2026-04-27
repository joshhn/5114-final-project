# VehiclePositions Recent Snapshot Lambda

This Lambda captures only the MBTA `VehiclePositions.pb` feed and writes parsed, compressed JSON snapshots to S3. It is intended as a short-retention recent-snapshot layer, separate from the existing Spark/Airflow/Snowflake batch/history pipeline.

## Target Bucket

```text
s3://5114-transit-project-data/realtime_snapshots/vehicle_positions/
```

## Output Format

The Lambda writes newline-delimited JSON compressed with gzip:

```text
realtime_snapshots/vehicle_positions/dt=YYYY-MM-DD/hour=HH/vehicle_positions_YYYYMMDDTHHMMSSZ.json.gz
```

Each line is one parsed vehicle row with fields such as:

- `snapshot_ts`
- `feed_timestamp`
- `entity_id`
- `vehicle_id`
- `route_id`
- `trip_id`
- `direction_id`
- `latitude`
- `longitude`
- `bearing`
- `speed_mps`
- `stop_id`
- `status`
- `vehicle_timestamp`
- `vehicle_age_seconds`

By default, the Lambda keeps bus-like routes only: numeric routes, `SL*`, and `CT*`.

## Lambda Environment Variables

```text
SNAPSHOT_BUCKET=5114-transit-project-data
SNAPSHOT_PREFIX=realtime_snapshots/vehicle_positions
BUS_ONLY=true
PULLS_PER_INVOCATION=6
SECONDS_BETWEEN_PULLS=10
```

With these defaults, one EventBridge invocation per minute produces roughly one VehiclePositions snapshot every 10 seconds.

## Build Deployment Zip

From the project root:

```bash
chmod +x lambda_vehicle_positions/build_zip.sh
lambda_vehicle_positions/build_zip.sh
```

The script defaults to Linux x86_64 wheels for a Python 3.12 Lambda runtime:

```text
LAMBDA_PLATFORM=manylinux2014_x86_64
LAMBDA_PYTHON_VERSION=312
```

For an arm64 Lambda, build with:

```bash
LAMBDA_PLATFORM=manylinux2014_aarch64 lambda_vehicle_positions/build_zip.sh
```

Or run the packaging commands manually:

```bash
mkdir -p /tmp/mbta_vehicle_lambda
python3 -m pip install \
  --platform manylinux2014_x86_64 \
  --python-version 312 \
  --implementation cp \
  --only-binary=:all: \
  -r lambda_vehicle_positions/requirements.txt \
  -t /tmp/mbta_vehicle_lambda
cp lambda_vehicle_positions/lambda_function.py /tmp/mbta_vehicle_lambda/
cd /tmp/mbta_vehicle_lambda
zip -r /tmp/mbta_vehicle_positions_lambda.zip .
```

Upload `/tmp/mbta_vehicle_positions_lambda.zip` to AWS Lambda.

## Lambda Settings

Recommended settings:

```text
Runtime: Python 3.12
Handler: lambda_function.lambda_handler
Timeout: 90 seconds
Memory: 256 MB
Architecture: x86_64
```

## IAM Permission

Attach this to the Lambda execution role, replacing the bucket name only if needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::5114-transit-project-data/realtime_snapshots/vehicle_positions/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

## Manual Test Event

Use this first so it writes one file and returns quickly:

```json
{
  "pulls": 1,
  "seconds_between_pulls": 0
}
```

Then check S3 under:

```text
realtime_snapshots/vehicle_positions/
```

## EventBridge Schedule

Create one EventBridge schedule:

```text
Name: mbta-vehicle-positions-every-minute
Schedule: rate(1 minute)
Target: this Lambda
Input: {}
```

The Lambda loops internally six times with a 10-second wait between pulls.

## S3 Lifecycle

Add a lifecycle rule to the bucket:

```text
Prefix: realtime_snapshots/vehicle_positions/
Expire current objects after: 3 days
```

That keeps this as a recent-snapshot layer instead of duplicating long-term history.
