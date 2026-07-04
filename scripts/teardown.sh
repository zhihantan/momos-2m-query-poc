#!/usr/bin/env bash
# Remove all deployed resources to stop any billing.
#   ./scripts/teardown.sh [PROFILE] [TARGET]
# The serverless warehouse auto-stops after auto_stop_mins, but this deletes it
# (and the jobs + dashboard) outright. Generated data in the schema is NOT dropped
# by default — uncomment the DROP SCHEMA line if you want it gone.
set -euo pipefail
PROFILE="${1:-fe-vm-zh-serverless}"
TARGET="${2:-dev}"
CATALOG="${3:-zh_serverless_ws}"
SCHEMA="${4:-momos_cx}"
cd "$(dirname "$0")/.."

# Optional: drop the generated data first (needs a running warehouse).
# WID="$(databricks warehouses list -p "$PROFILE" -o json | python3 -c 'import sys,json;print([w["id"] for w in json.load(sys.stdin) if w["name"]=="momos-benchmark-wh"][0])')"
# databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "{\"warehouse_id\":\"$WID\",\"statement\":\"DROP SCHEMA IF EXISTS $CATALOG.$SCHEMA CASCADE\",\"wait_timeout\":\"30s\"}"

echo "==> Destroying bundle resources (warehouse + jobs + dashboard)"
databricks bundle destroy -t "$TARGET" -p "$PROFILE" --auto-approve
echo "==> Done."
