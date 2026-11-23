# Java 17 runtime
FROM eclipse-temurin:17-jre AS java

# Python application
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy Java runtime from Temurin
COPY --from=java /opt/java/openjdk /opt/java/openjdk

# C compiler and build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

# Project source
COPY . .

CMD ["python", "-m", "pytest", "-v"]
