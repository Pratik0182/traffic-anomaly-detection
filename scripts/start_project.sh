#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting Website Traffic Dashboard ==="

docker info >/dev/null

start() {
    local name="$1"
    echo "Starting $name..."
    docker start "$name" >/dev/null
}

# Start infrastructure in dependency order
start traffic-zookeeper
sleep 5

start broker
start hdfs-namenode

# Wait for NameNode health
echo "Waiting for HDFS NameNode..."
for i in {1..60}; do
    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' hdfs-namenode)
    [[ "$health" == "healthy" ]] && break
    sleep 2
done

if [[ "$health" != "healthy" ]]; then
    echo "ERROR: NameNode did not become healthy."
    exit 1
fi

start hdfs-datanode
sleep 5

start hbase
start spark-master
sleep 10

# Start the verified local-Spark streaming container
start website-traffic-streaming
sleep 5

start website-traffic-dashboard

echo
echo "=== Startup complete ==="
docker ps --format 'table {{.Names}}\t{{.Status}}'
echo
echo "Dashboard: http://localhost:8501"
