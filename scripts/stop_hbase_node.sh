#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HBASE_HOME=/opt/bigdata/hbase
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HBASE_HOME}/bin:${PATH}"
if [[ "$ROLE" == "master" ]]; then
  hbase-daemon.sh stop thrift || true
  hbase-daemon.sh stop master || true
  hbase zookeeper stop || true
elif [[ "$ROLE" == "worker1" || "$ROLE" == "worker2" ]]; then
  hbase-daemon.sh stop regionserver || true
else
  echo "Usage: $0 {master|worker1|worker2}" >&2
  exit 2
fi
