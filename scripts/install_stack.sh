#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

BASE=/opt/bigdata
DIST="${BASE}/dist"
mkdir -p "$DIST" "$BASE"

download_verify() {
  local base_url="$1"
  local filename="$2"
  local archive="${DIST}/${filename}"
  local checksum="${archive}.sha512"

  if [[ ! -f "$archive" ]]; then
    echo "Downloading ${filename}"
    curl -fL --retry 4 --retry-delay 2 "${base_url}/${filename}" -o "$archive"
  else
    echo "Using cached ${archive}"
  fi

  if [[ ! -f "$checksum" ]]; then
    curl -fL --retry 4 --retry-delay 2 "${base_url}/${filename}.sha512" -o "$checksum"
  fi

  local checksum_text payload expected
  checksum_text="$(cat "$checksum")"
  if [[ "$checksum_text" == *:* ]]; then
    # Apache checksum files may wrap the digest over several lines.
    payload="${checksum_text#*:}"
  else
    payload="$(printf '%s\n' "$checksum_text" | head -n 1 | awk '{print $1}')"
  fi
  expected="$(printf '%s' "$payload" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
  if [[ ! "$expected" =~ ^[[:xdigit:]]{128}$ ]]; then
    echo "Invalid SHA-512 file: ${checksum}" >&2
    exit 1
  fi
  echo "${expected}  ${archive}" | sha512sum --check --status || {
    echo "SHA-512 verification failed for ${filename}" >&2
    exit 1
  }
  echo "Verified ${filename}"
}

HADOOP_VERSION=3.3.6
SPARK_VERSION=3.5.3
KAFKA_VERSION=3.7.2
KAFKA_SCALA=2.13
HBASE_VERSION=2.5.16

download_verify \
  "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}" \
  "hadoop-${HADOOP_VERSION}.tar.gz"
download_verify \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}" \
  "spark-${SPARK_VERSION}-bin-hadoop3.tgz"
download_verify \
  "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}" \
  "kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
download_verify \
  "https://downloads.apache.org/hbase/${HBASE_VERSION}" \
  "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz"

extract_and_link() {
  local archive="$1"
  local link_name="$2"
  local top_dir
  top_dir="$(tar -tzf "${DIST}/${archive}" | sed -n '1p' | cut -d/ -f1)"
  if [[ -z "$top_dir" ]]; then
    echo "Could not determine archive root directory for ${archive}" >&2
    exit 1
  fi
  if [[ ! -d "${BASE}/${top_dir}" ]]; then
    tar -xf "${DIST}/${archive}" -C "$BASE"
  fi
  if [[ ! -d "${BASE}/${top_dir}" ]]; then
    echo "Extraction did not create ${BASE}/${top_dir}" >&2
    exit 1
  fi
  ln -sfn "${BASE}/${top_dir}" "${BASE}/${link_name}"
}

extract_and_link "hadoop-${HADOOP_VERSION}.tar.gz" "hadoop"
extract_and_link "spark-${SPARK_VERSION}-bin-hadoop3.tgz" "spark"
extract_and_link "kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" "kafka"
extract_and_link "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" "hbase"

echo "Installed software links:"
ls -ld "${BASE}/hadoop" "${BASE}/spark" "${BASE}/kafka" "${BASE}/hbase"
echo "Do not run this script if you intend to replace an existing production installation."
