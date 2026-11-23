#!/usr/bin/env bash
set -euo pipefail
source /etc/bigdata-cluster.env
export HADOOP_HOME=/opt/bigdata/hadoop
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${PATH}"

hdfs dfs -mkdir -p /traffic/raw/events /traffic/checkpoints /traffic/reports
hdfs dfs -chmod -R 775 /traffic
hdfs dfs -ls -R /traffic
