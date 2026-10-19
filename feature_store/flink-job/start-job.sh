#!/usr/bin/env bash
# Start the job from the newest savepoint or retained checkpoint, so a redeploy or
# container restart never runs with empty state (which would overwrite correct bucket
# totals in Scylla with partial ones). With no state at all the Kafka source starts
# from the earliest offset and rebuilds state by replay.
set -euo pipefail

latest=$(ls -t /flink-data/savepoints/*/_metadata /flink-data/checkpoints/*/chk-*/_metadata 2>/dev/null | head -1 || true)
args=(standalone-job --job-classname com.example.fs.FeatureJob)
if [ -n "$latest" ]; then
  echo "Restoring state from $(dirname "$latest")"
  args+=(--fromSavepoint "file://$(dirname "$latest")" --allowNonRestoredState)
else
  echo "No savepoint or checkpoint found: starting with empty state (replay from earliest offsets)"
fi
exec /docker-entrypoint.sh "${args[@]}"
