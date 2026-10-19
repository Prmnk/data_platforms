#!/usr/bin/env bash
# Stop the running Flink job with a savepoint (state + source offsets), so the next
# start resumes exactly where this one stopped. Used by `make redeploy-job`.
set -euo pipefail
FLINK=${FLINK:-http://localhost:8081}

jid=$(curl -sf "$FLINK/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["state"] == "RUNNING"]
print(jobs[0]["jid"] if jobs else "")')
if [ -z "$jid" ]; then
  echo "no running job; nothing to stop" >&2
  exit 0
fi

rid=$(curl -sf -XPOST "$FLINK/jobs/$jid/stop" -H 'content-type: application/json' \
  -d '{"targetDirectory": "file:///flink-data/savepoints", "drain": false}' |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["request-id"])')

for _ in $(seq 1 60); do
  out=$(curl -s "$FLINK/jobs/$jid/savepoints/$rid" || true)
  status=$(echo "$out" | python3 -c 'import json, sys; print(json.load(sys.stdin)["status"]["id"])' 2>/dev/null || echo PENDING)
  if [ "$status" = "COMPLETED" ]; then
    echo "$out" | python3 -c '
import json, sys
op = json.load(sys.stdin)["operation"]
if "failure-cause" in op:
    sys.exit("savepoint failed: " + op["failure-cause"]["stack-trace"].splitlines()[0])
print("savepoint:", op["location"])'
    exit 0
  fi
  sleep 1
done
echo "timed out waiting for savepoint" >&2
exit 1
