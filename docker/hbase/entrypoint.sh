#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for ZooKeeper..."
until nc -z zookeeper 2181; do
    sleep 2
done

echo "Waiting for HDFS NameNode..."
until nc -z namenode 9000; do
    sleep 2
done

echo "Waiting for HDFS DataNode..."
until nc -z datanode 9867; do
    sleep 2
done

echo "Starting HBase Master..."
/opt/hbase/bin/hbase-daemon.sh start master

echo "Waiting for HBase Master RPC..."
for i in $(seq 1 60); do
    if nc -z hbase 16000; then
        break
    fi
    sleep 2
done

if ! nc -z hbase 16000; then
    echo "ERROR: HBase Master did not start."
    exit 1
fi

echo "Starting HBase RegionServer..."
/opt/hbase/bin/hbase-daemon.sh start regionserver

echo "Starting HBase Thrift on port 9090..."
exec /opt/hbase/bin/hbase thrift start -b 0.0.0.0 -p 9090
