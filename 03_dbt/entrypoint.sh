#!/bin/bash
set -e
set -o pipefail


start_time=$(date +%s.%N)

# Docker path
CONFIG_PATH="/app/config/config.json"


# -------------------------
# Docker
# -------------------------
export DBT_GCP_PROJECT=$(jq -r '.gcs["big-query-project"]' "$CONFIG_PATH")
export DBT_DATASET=$(jq -r '.gcs["big-query-dataset"]' "$CONFIG_PATH")
export BQ_REGION=$(jq -r '.gcs["region"]' "$CONFIG_PATH")
export GCS_BUCKET=$(jq -r '.gcs["bucket"]' "$CONFIG_PATH")
export DBT_THREADS="1"


echo "[DBT] Running dbt"
uv run dbt run

echo "[DBT] Generating PDF report..."
uv run python report.py

end_time=$(date +%s.%N)
execution_time=$(awk "BEGIN {print $end_time - $start_time}")

printf "[DBT] Execution time: %.2f seconds\n" "$execution_time"
