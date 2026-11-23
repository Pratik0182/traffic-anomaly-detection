#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
source /etc/bigdata-cluster.env
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${PATH}"
if [[ "$ROLE" == "master" ]]; then
  hdfs --daemon stop namenode
elif [[ "$ROLE" == "worker2" ]]; then
  hdfs --daemon stop secondarynamenode || true
  hdfs --daemon stop datanode
else
  hdfs --daemon stop datanode
fi
