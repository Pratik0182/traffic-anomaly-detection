FROM eclipse-temurin:17-jre

ARG HBASE_VERSION=2.6.7
ARG HBASE_FILE=hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz
ARG HBASE_URL=https://dlcdn.apache.org/hbase/${HBASE_VERSION}

ENV JAVA_HOME=/opt/java/openjdk
ENV HBASE_HOME=/opt/hbase
ENV HADOOP_CONF_DIR=/opt/hbase/conf
ENV PATH="${JAVA_HOME}/bin:${HBASE_HOME}/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        netcat-openbsd \
        procps && \
    rm -rf /var/lib/apt/lists/*
# Match the Hadoop container's Linux user and group
RUN curl -fSL "${HBASE_URL}/${HBASE_FILE}" \
        -o "/tmp/${HBASE_FILE}" && \
    curl -fSL "${HBASE_URL}/${HBASE_FILE}.sha512" \
        -o "/tmp/${HBASE_FILE}.sha512" && \
    cd /tmp && \
    EXPECTED_SHA512="$(sed 's/^[^:]*:[[:space:]]*//' "${HBASE_FILE}.sha512" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')" && \
    ACTUAL_SHA512="$(sha512sum "${HBASE_FILE}" | awk '{print $1}')" && \
    test "${#EXPECTED_SHA512}" -eq 128 && \
    test "$EXPECTED_SHA512" = "$ACTUAL_SHA512" && \
    echo "HBase SHA-512 verification passed" && \
    tar -xzf "${HBASE_FILE}" -C /opt && \
    ln -s "/opt/hbase-${HBASE_VERSION}" /opt/hbase && \
    rm -f "/tmp/${HBASE_FILE}" "/tmp/${HBASE_FILE}.sha512"

RUN rm -f /opt/hbase && \
    ln -s "/opt/hbase-${HBASE_VERSION}-hadoop3" /opt/hbase

COPY docker/hadoop/core-site.xml /opt/hbase/conf/core-site.xml
COPY docker/hadoop/hdfs-site.xml /opt/hbase/conf/hdfs-site.xml
COPY docker/hbase/hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY docker/hbase/hbase-env.sh /opt/hbase/conf/hbase-env.sh

COPY docker/hbase/entrypoint.sh /usr/local/bin/hbase-entrypoint.sh

RUN chmod +x /usr/local/bin/hbase-entrypoint.sh

EXPOSE 9090 16000 16010 16020 16030
# Prepare writable directories for the existing ubuntu user
RUN mkdir -p /opt/hbase/logs /tmp/hbase-pids /data/tmp /data/local && \
    chown -R 1000:100 /opt/hbase/logs /tmp/hbase-pids /data

ENTRYPOINT ["/usr/local/bin/hbase-entrypoint.sh"]
