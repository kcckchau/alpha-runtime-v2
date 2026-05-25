#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="$(basename "$ROOT_DIR")"
TIMESTAMP="$(date +"%Y%m%d-%H%M%S")"
OUTPUT_DIR="${1:-$ROOT_DIR/dist}"
ARCHIVE_PATH="$OUTPUT_DIR/${PROJECT_NAME}-${TIMESTAMP}.zip"

mkdir -p "$OUTPUT_DIR"

cd "$ROOT_DIR"

zip -r "$ARCHIVE_PATH" . \
  -x ".git/*" \
  -x ".DS_Store" \
  -x ".env" \
  -x ".venv/*" \
  -x "__pycache__/*" \
  -x "*.pyc" \
  -x ".pytest_cache/*" \
  -x ".mypy_cache/*" \
  -x ".ruff_cache/*" \
  -x "data/*" \
  -x "dist/*"

echo "Created archive: $ARCHIVE_PATH"
