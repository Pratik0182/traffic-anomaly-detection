#!/usr/bin/env bash
set -euo pipefail
source /etc/bigdata-cluster.env
if [[ "$(hostname -s)" != "$MASTER_HOST" ]]; then
  echo "Run this on the master node only." >&2; exit 2
fi
/opt/bigdata/kafka/bin/kafka-storage.sh random-uuid
