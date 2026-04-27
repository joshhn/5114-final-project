#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-/tmp/mbta_vehicle_lambda}"
ZIP_PATH="${ZIP_PATH:-/tmp/mbta_vehicle_positions_lambda.zip}"
LAMBDA_PLATFORM="${LAMBDA_PLATFORM:-manylinux2014_x86_64}"
LAMBDA_PYTHON_VERSION="${LAMBDA_PYTHON_VERSION:-312}"

rm -rf "$BUILD_DIR" "$ZIP_PATH"
mkdir -p "$BUILD_DIR"

python3 -m pip install \
  --platform "$LAMBDA_PLATFORM" \
  --python-version "$LAMBDA_PYTHON_VERSION" \
  --implementation cp \
  --only-binary=:all: \
  -r "$(dirname "$0")/requirements.txt" \
  -t "$BUILD_DIR"
cp "$(dirname "$0")/lambda_function.py" "$BUILD_DIR/"

cd "$BUILD_DIR"
zip -r "$ZIP_PATH" .

echo "$ZIP_PATH"
