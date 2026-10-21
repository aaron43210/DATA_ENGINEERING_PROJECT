#!/bin/bash
set -euo pipefail

REST=http://flink-jobmanager:8081
REGISTRY=http://redpanda:8081/subjects
JOB_NAME=lubaba-sensor-stream
CLIENT_CONF="$(mktemp -d)"
cp -a /opt/flink/conf/. "$CLIENT_CONF/"
sed -i \
  -e 's/^jobmanager\.rpc\.address:.*/jobmanager.rpc.address: flink-jobmanager/' \
  -e 's/^rest\.address:.*/rest.address: flink-jobmanager/' \
  -e 's/^rest\.bind-address:.*/rest.bind-address: 0.0.0.0/' \
  "$CLIENT_CONF/flink-conf.yaml"
export FLINK_CONF_DIR="$CLIENT_CONF"

while true; do
  until curl -fsS "$REST/overview" >/dev/null; do sleep 2; done
  until curl -fsS "$REGISTRY" >/dev/null; do sleep 2; done

  if ! curl -fsS "$REST/overview" | python3 -c \
    'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("taskmanagers",0)>0 and d.get("slots-available",0)>0 else 1)'; then
    echo "Waiting for a registered TaskManager slot"
    sleep 3
    continue
  fi

  if curl -fsS "$REST/jobs/overview" | python3 -c \
    'import json,sys; jobs=json.load(sys.stdin).get("jobs", []); terminal={"FAILED","CANCELED","FINISHED"}; sys.exit(0 if any(j.get("name")=="lubaba-sensor-stream" and j.get("state") not in terminal for j in jobs) else 1)'; then
    sleep 5
    continue
  fi

  restore_args=()
  latest="$(find /opt/flink/checkpoints -type f -name _metadata -printf '%T@ %h\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
  if [ -n "$latest" ]; then
    echo "Restoring $JOB_NAME from completed checkpoint $latest"
    restore_args=(-s "$latest")
  else
    echo "No retained checkpoint; starting with Kafka group offsets (earliest on first run)"
  fi

  if flink run -d -m flink-jobmanager:8081 \
      "${restore_args[@]}" -py /opt/flink/jobs/sensor_flink_job.py \
      -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3; then
    echo "Submitted $JOB_NAME; continuing to monitor Flink REST"
    sleep 3
  else
    echo "Job submission failed; retrying after 5 seconds" >&2
    sleep 5
  fi
done
