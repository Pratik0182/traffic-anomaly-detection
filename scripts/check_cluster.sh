#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source /etc/bigdata-cluster.env
echo "== Host resolution =="
getent hosts "$MASTER_HOST" "$WORKER1_HOST" "$WORKER2_HOST"
echo "== HDFS report =="
/opt/bigdata/hadoop/bin/hdfs dfsadmin -report
echo "== Kafka topic =="
/opt/bigdata/kafka/bin/kafka-topics.sh --bootstrap-server "${MASTER_HOST}:9092" --describe --topic "$KAFKA_TOPIC"
echo "== Spark master =="
curl -fsS "http://${MASTER_HOST}:8080" >/dev/null && echo "Spark master UI reachable"
echo "== HBase Thrift =="
timeout 3 bash -c ":</dev/tcp/${MASTER_HOST}/9090" && echo "HBase Thrift TCP reachable"
echo "== Application HBase check =="
source "${ROOT}/.venv/bin/activate"
python -m hbase.database_operations check
