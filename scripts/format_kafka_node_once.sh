#!/usr/bin/env bash
set -euo pipefail
if [[ "${EUID}" -ne 0 ]]; then exec sudo -E bash "$0" "$@"; fi
CLUSTER_ID="${1:-}"
if [[ -z "$CLUSTER_ID" ]]; then
  echo "Usage: $0 <same-kafka-cluster-id-on-all-three-nodes>" >&2
  exit 2
fi
source /etc/bigdata-cluster.env
RUN_USER="${SUDO_USER:-}"
if [[ -z "$RUN_USER" || "$RUN_USER" == "root" ]]; then
  echo "Run via sudo from your normal cluster account; do not format as root." >&2
  exit 2
fi
LOG_DIR="${DATA_ROOT}/kafka/logs"
if [[ -f "${LOG_DIR}/meta.properties" ]]; then
  echo "Kafka storage already formatted; leaving it untouched."
  exit 0
fi
if [[ -d "$LOG_DIR" ]] && find "$LOG_DIR" -mindepth 1 -print -quit | grep -q .; then
  echo "Kafka log directory is non-empty but unformatted. Refusing to format." >&2
  exit 1
fi
sudo -u "$RUN_USER" env \
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
  KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" \
  /opt/bigdata/kafka/bin/kafka-storage.sh format \
  --cluster-id "$CLUSTER_ID" \
  --config /opt/bigdata/kafka/config/kraft/server.properties
