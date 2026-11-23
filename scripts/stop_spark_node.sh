#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
export SPARK_HOME=/opt/bigdata/spark
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
if [[ "$ROLE" == "master" ]]; then
  "${SPARK_HOME}/sbin/stop-master.sh"
elif [[ "$ROLE" == "worker1" || "$ROLE" == "worker2" ]]; then
  "${SPARK_HOME}/sbin/stop-worker.sh"
else
  echo "Usage: $0 {master|worker1|worker2}" >&2
  exit 2
fi
