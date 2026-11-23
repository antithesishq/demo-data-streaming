#!/usr/bin/env bash
# Build, validate and launch the streaming workload on Antithesis.
# Usage: scripts/launch.sh <max-compute-hours> [description]
# Check the task's compute budget before each launch.
set -euo pipefail
hours=${1:?usage: launch.sh <max-compute-hours> [description]}
cd "$(dirname "$0")/../config"
rev=$(git rev-parse --short HEAD)
desc=${2:-"plugin workload @ $rev, ${hours}h"}
docker compose -p dds build
snouty validate .
snouty launch \
  --launcher basic_test \
  --config . \
  --test-name demo-data-streaming-plugin \
  --description "$desc" \
  --source demo-data-streaming-plugin \
  --max-compute-hours "$hours" \
  --fault-tolerance high_availability \
  --fault-exclusions '^(driver|state-tracker)$' \
  --param attrs.workload=streaming \
  --param attrs.workload_rev="$rev" \
  --param attrs.plugin_version=0.2.0
