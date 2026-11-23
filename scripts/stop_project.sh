#!/usr/bin/env bash
set -euo pipefail

echo "=== Stopping Website Traffic Dashboard ==="

# Stop processing and presentation first
for name in website-traffic-dashboard website-traffic-streaming hbase broker spark-master hdfs-datanode hdfs-namenode traffic-zookeeper; do
    if docker container inspect "$name" >/dev/null 2>&1; then
        echo "Stopping $name..."
        docker stop "$name" >/dev/null || true
    fi
done

echo
echo "Project containers stopped."
echo "Persistent Docker volumes have NOT been deleted."
