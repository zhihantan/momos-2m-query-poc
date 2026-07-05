#!/usr/bin/env bash
# Run the benchmark job. Usage:
#   ./scripts/run_benchmark.sh smoke                 # 5-min validation vs sf1 data
#   ./scripts/run_benchmark.sh full                  # the 2M-in-60-min showcase (compute mode)
#   ./scripts/run_benchmark.sh full serving          # 2M in serving/cache mode
#
# 'full' assumes you have generated 'full'-scale data and set a warm floor on the
# warehouse (see docs/tuning.md). It overrides the job's notebook widgets via
# run-now notebook-params.
set -euo pipefail
KIND="${1:-smoke}"
MODE="${2:-compute}"
PROFILE="${3:-your-profile}"
cd "$(dirname "$0")/.."

JOB_ID="$(databricks jobs list --name momos_benchmark -p "$PROFILE" -o json \
          | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["job_id"])')"
echo "==> momos_benchmark job_id=$JOB_ID  kind=$KIND mode=$MODE"

if [[ "$KIND" == "full" ]]; then
  PARAMS=$(cat <<JSON
{"use_smoke":"false","mode":"$MODE","scale":"full","template_profile":"mixed",
 "duration_seconds":"3600","target_qps":"560","num_partitions":"32",
 "threads_per_partition":"8","pace":"true","client_log_sample_rate":"0.1"}
JSON
)
else
  PARAMS='{"use_smoke":"true","scale":"sf1","mode":"'"$MODE"'"}'
fi

RUN_ID="$(databricks jobs run-now --job-id "$JOB_ID" \
          --notebook-params "$PARAMS" -p "$PROFILE" -o json \
          | python3 -c 'import sys,json;print(json.load(sys.stdin)["run_id"])')"
echo "==> Launched run_id=$RUN_ID. Follow it:"
echo "    databricks jobs get-run $RUN_ID -p $PROFILE"
