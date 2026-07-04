#!/usr/bin/env bash
# Deploy the bundle and generate the default (sf1) datasets.
#   ./scripts/setup.sh [PROFILE] [TARGET]
set -euo pipefail
PROFILE="${1:-fe-vm-zh-serverless}"
TARGET="${2:-dev}"
cd "$(dirname "$0")/.."

echo "==> Validating bundle"
databricks bundle validate -t "$TARGET" -p "$PROFILE"

echo "==> Deploying (serverless SQL warehouse + jobs + dashboard)"
databricks bundle deploy -t "$TARGET" -p "$PROFILE"

echo "==> Generating sf1 datasets (products / profiles / orders / reviews)"
databricks bundle run momos_generate_data -t "$TARGET" -p "$PROFILE"

echo "==> Done. Next: ./scripts/run_benchmark.sh smoke   (or 'full' for the 2M run)"
