#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROLE="${1:-}"
ENV_FILE="${2:-${ROOT}/config/cluster.env}"

if [[ "$ROLE" != "master" && "$ROLE" != "worker1" && "$ROLE" != "worker2" ]]; then
  echo "Usage: $0 {master|worker1|worker2} [config/cluster.env]" >&2
  exit 2
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing ${ENV_FILE}. Copy config/cluster.env.example and edit the Tailscale IPs." >&2
  exit 2
fi
# shellcheck disable=SC1090
set -a
source "$ENV_FILE"
set +a

case "$ROLE" in
  master)
    NODE_HOST="${MASTER_HOST}"; NODE_TAILSCALE_IP="${MASTER_TAILSCALE_IP}"; NODE_ID=1 ;;
  worker1)
    NODE_HOST="${WORKER1_HOST}"; NODE_TAILSCALE_IP="${WORKER1_TAILSCALE_IP}"; NODE_ID=2 ;;
  worker2)
    NODE_HOST="${WORKER2_HOST}"; NODE_TAILSCALE_IP="${WORKER2_TAILSCALE_IP}"; NODE_ID=3 ;;
esac

for name in MASTER_TAILSCALE_IP WORKER1_TAILSCALE_IP WORKER2_TAILSCALE_IP; do
  value="${!name}"
  if ! python3 - "$value" <<'PYIP'
import ipaddress
import sys
try:
    address = ipaddress.ip_address(sys.argv[1])
except ValueError:
    raise SystemExit(1)
allowed = (
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
if address.version != 4 or not any(address in network for network in allowed):
    raise SystemExit(1)
PYIP
  then
    echo "${name}='${value}' must be a Tailscale or RFC1918 private IPv4 address." >&2
    exit 2
  fi
done

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$ROLE" "$ENV_FILE"
fi

export ROLE NODE_HOST NODE_TAILSCALE_IP NODE_ID
ACTUAL_HOST="$(hostname -s)"
if [[ "$ACTUAL_HOST" != "$NODE_HOST" ]]; then
  echo "This VM hostname is '${ACTUAL_HOST}', but role '${ROLE}' expects '${NODE_HOST}'." >&2
  echo "Set it with: sudo hostnamectl set-hostname ${NODE_HOST}, then reconnect and retry." >&2
  exit 2
fi
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME=/opt/bigdata/hadoop
export SPARK_HOME=/opt/bigdata/spark
export KAFKA_HOME=/opt/bigdata/kafka
export HBASE_HOME=/opt/bigdata/hbase
export DATA_ROOT="${DATA_ROOT:-/data/bigdata}"

RUN_USER="${SUDO_USER:-}"
if [[ -z "$RUN_USER" || "$RUN_USER" == "root" ]]; then
  echo "Run this script as your normal cluster user (it will request sudo)." >&2
  exit 2
fi
RUN_GROUP="$(id -gn "$RUN_USER")"

for path in \
  "${DATA_ROOT}/hadoop/tmp" "${DATA_ROOT}/hadoop/dfs/name" \
  "${DATA_ROOT}/hadoop/dfs/data" "${DATA_ROOT}/kafka/logs" \
  "${DATA_ROOT}/zookeeper" "${DATA_ROOT}/logs/hadoop" \
  "${DATA_ROOT}/logs/spark" "${DATA_ROOT}/logs/hbase" "${DATA_ROOT}/spark/work" \
  "${DATA_ROOT}/pids/hadoop" "${DATA_ROOT}/pids/spark" \
  "${DATA_ROOT}/pids/hbase"; do
  mkdir -p "$path"
done
chown -R "${RUN_USER}:${RUN_GROUP}" "${DATA_ROOT}"

# Add managed hostname mappings without disturbing unrelated /etc/hosts entries.
sed -i '/# traffic-cluster-managed$/d' /etc/hosts
{
  echo "${MASTER_TAILSCALE_IP} ${MASTER_HOST} # traffic-cluster-managed"
  echo "${WORKER1_TAILSCALE_IP} ${WORKER1_HOST} # traffic-cluster-managed"
  echo "${WORKER2_TAILSCALE_IP} ${WORKER2_HOST} # traffic-cluster-managed"
} >> /etc/hosts

install -d "${HADOOP_HOME}/etc/hadoop" "${SPARK_HOME}/conf" \
  "${KAFKA_HOME}/config/kraft" "${HBASE_HOME}/conf"

render() {
  local input="$1"
  local output="$2"
  envsubst < "$input" > "$output"
}

render "${ROOT}/config/hadoop/core-site.xml.template" "${HADOOP_HOME}/etc/hadoop/core-site.xml"
render "${ROOT}/config/hadoop/hdfs-site.xml.template" "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml"
render "${ROOT}/config/hadoop/hadoop-env.sh.template" "${HADOOP_HOME}/etc/hadoop/hadoop-env.sh"
render "${ROOT}/config/hadoop/workers.template" "${HADOOP_HOME}/etc/hadoop/workers"

render "${ROOT}/config/spark/spark-env.sh.template" "${SPARK_HOME}/conf/spark-env.sh"
render "${ROOT}/config/spark/spark-defaults.conf.template" "${SPARK_HOME}/conf/spark-defaults.conf"
render "${ROOT}/config/spark/workers.template" "${SPARK_HOME}/conf/workers"

render "${ROOT}/config/hbase/hbase-site.xml.template" "${HBASE_HOME}/conf/hbase-site.xml"
render "${ROOT}/config/hbase/hbase-env.sh.template" "${HBASE_HOME}/conf/hbase-env.sh"
render "${ROOT}/config/hbase/regionservers.template" "${HBASE_HOME}/conf/regionservers"

render "${ROOT}/config/kafka/server.properties.template" "${KAFKA_HOME}/config/kraft/server.properties"

chmod +x "${HADOOP_HOME}/etc/hadoop/hadoop-env.sh" "${SPARK_HOME}/conf/spark-env.sh" "${HBASE_HOME}/conf/hbase-env.sh"
cp "$ENV_FILE" /etc/bigdata-cluster.env
chmod 0644 /etc/bigdata-cluster.env

echo "Configured role=${ROLE}, host=${NODE_HOST}, Tailscale IP=${NODE_TAILSCALE_IP}"
echo "Host resolution:"
getent hosts "${MASTER_HOST}" "${WORKER1_HOST}" "${WORKER2_HOST}"
echo "Review generated configuration before starting any daemon."
