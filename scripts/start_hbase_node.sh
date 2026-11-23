#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:-}"
if [[ "$ROLE" != "master" && "$ROLE" != "worker1" && "$ROLE" != "worker2" ]]; then
  echo "Usage: $0 {master|worker1|worker2}" >&2; exit 2
fi
source /etc/bigdata-cluster.env
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HBASE_HOME=/opt/bigdata/hbase
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HBASE_HOME}/bin:${PATH}"
if [[ "$ROLE" == "master" ]]; then
  hbase zookeeper start
  zk_ready=false
  for _ in $(seq 1 30); do
    if timeout 2 bash -c ':</dev/tcp/127.0.0.1/2181' 2>/dev/null; then
      zk_ready=true; break
    fi
    sleep 2
  done
  if [[ "$zk_ready" != true ]]; then
    echo "ZooKeeper did not become ready on port 2181; inspect HBase logs." >&2
    exit 1
  fi
  hbase-daemon.sh start master
  master_ready=false
  for _ in $(seq 1 60); do
    if timeout 2 bash -c ':</dev/tcp/127.0.0.1/16000' 2>/dev/null; then
      master_ready=true; break
    fi
    sleep 2
  done
  if [[ "$master_ready" != true ]]; then
    echo "HBase Master did not become ready on port 16000; inspect HBase logs." >&2
    exit 1
  fi
  hbase-daemon.sh start thrift
  thrift_ready=false
  for _ in $(seq 1 30); do
    if timeout 2 bash -c ':</dev/tcp/127.0.0.1/9090' 2>/dev/null; then
      thrift_ready=true; break
    fi
    sleep 2
  done
  if [[ "$thrift_ready" != true ]]; then
    echo "HBase Thrift did not become ready on port 9090; inspect HBase logs." >&2
    exit 1
  fi
else
  hbase-daemon.sh start regionserver
fi
jps
