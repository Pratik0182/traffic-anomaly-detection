#!/usr/bin/env bash
set -euo pipefail
source /etc/bigdata-cluster.env
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export KAFKA_HEAP_OPTS="-Xms512m -Xmx768m"
exec /opt/bigdata/kafka/bin/kafka-server-start.sh -daemon \
  /opt/bigdata/kafka/config/kraft/server.properties
