#!/usr/bin/env bash
set -euo pipefail
if [[ "${EUID}" -ne 0 ]]; then exec sudo -E bash "$0" "$@"; fi
source /etc/bigdata-cluster.env
RUN_USER="${SUDO_USER:-}"
if [[ -z "$RUN_USER" || "$RUN_USER" == "root" ]]; then
  echo "Run via sudo from your normal cluster account; do not format as root." >&2
  exit 2
fi
NAME_DIR="${DATA_ROOT}/hadoop/dfs/name"
if [[ -f "${NAME_DIR}/current/VERSION" ]]; then
  echo "NameNode already formatted; leaving it untouched."
  exit 0
fi
if [[ -d "$NAME_DIR" ]] && find "$NAME_DIR" -mindepth 1 -print -quit | grep -q .; then
  echo "NameNode directory is non-empty but has no current/VERSION. Refusing to format." >&2
  exit 1
fi
echo "Formatting a new NameNode directory. This is an initial-cluster-only operation."
sudo -u "$RUN_USER" env \
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
  HADOOP_HOME=/opt/bigdata/hadoop \
  HADOOP_CONF_DIR=/opt/bigdata/hadoop/etc/hadoop \
  /opt/bigdata/hadoop/bin/hdfs namenode -format -nonInteractive
