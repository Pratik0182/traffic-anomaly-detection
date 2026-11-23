#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
if [[ "$ROLE" != "master" && "$ROLE" != "worker1" && "$ROLE" != "worker2" ]]; then
  echo "Usage: $0 {master|worker1|worker2}" >&2; exit 2
fi
source /etc/bigdata-cluster.env
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export SPARK_HOME=/opt/bigdata/spark
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
if [[ "$ROLE" == "master" ]]; then
  "${SPARK_HOME}/sbin/start-master.sh"
else
  "${SPARK_HOME}/sbin/start-worker.sh" "spark://${MASTER_HOST}:7077"
fi
jps
