#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source /etc/bigdata-cluster.env
source "${ROOT}/.venv/bin/activate"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export SPARK_HOME=/opt/bigdata/spark
export PYSPARK_DRIVER_PYTHON="${ROOT}/.venv/bin/python"
export PYSPARK_PYTHON=/usr/bin/python3.10
exec "${SPARK_HOME}/bin/spark-submit" \
  --master "spark://${MASTER_HOST}:7077" \
  --deploy-mode client \
  --conf "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.10" \
  "${ROOT}/spark/batch_processor.py" "$@"
