#!/usr/bin/env bash
set -euo pipefail
source /etc/bigdata-cluster.env
if [[ "$(hostname -s)" != "$MASTER_HOST" ]]; then
  echo "Run this on the master node only." >&2; exit 2
fi
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${MASTER_HOST}:9092" \
  --create --if-not-exists \
  --topic "${KAFKA_TOPIC}" \
  --partitions "${KAFKA_TOPIC_PARTITIONS}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION}" \
  --config "min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS}"
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${MASTER_HOST}:9092" --describe --topic "${KAFKA_TOPIC}"
