#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
if [[ "$ROLE" != "master" && "$ROLE" != "worker1" && "$ROLE" != "worker2" ]]; then
  echo "Usage: $0 {master|worker1|worker2}" >&2; exit 2
fi
source /etc/bigdata-cluster.env
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"
if [[ "$ROLE" == "master" ]]; then
  if [[ ! -f "${DATA_ROOT}/hadoop/dfs/name/current/VERSION" ]]; then
    echo "NameNode is not formatted. Run scripts/format_hdfs_namenode_once.sh once on master." >&2
    exit 1
  fi
  hdfs --daemon start namenode
elif [[ "$ROLE" == "worker2" ]]; then
  hdfs --daemon start datanode
  hdfs --daemon start secondarynamenode
else
  hdfs --daemon start datanode
fi
jps
