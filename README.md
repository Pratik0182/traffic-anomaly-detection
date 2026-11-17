# Real-Time Website Traffic Analytics & Anomaly Detection

**Course:** CSBB422 – Big Data Analytics Lab Project  
**Repository:** [https://github.com/Pratik0182/traffic-anomaly-detection](https://github.com/Pratik0182/traffic-anomaly-detection)  
**Team Size:** 4 Members  
**Deployment Target:** 3-Node Physical Distributed Cluster (1 Master + 2 Workers)

---

## 1. Project Overview

This project implements an end-to-end distributed Big Data pipeline that:
1. Simulates realistic website traffic events (normal navigation, repeated visits, error bursts, and latency spikes).
2. Ingests events in real time using a 3-broker **Apache Kafka** cluster running in KRaft mode.
3. Processes streaming event windows using **Apache Spark Structured Streaming** with event-time watermarking and checkpoints.
4. Archives raw immutable event logs in **Hadoop HDFS** as partitioned Parquet datasets.
5. Upserts rolling window aggregates and anomaly alerts into **Apache HBase** via deterministic row keys.
6. Computes historical batch aggregations using **Apache Spark Batch Processing**.
7. Visualizes real-time metrics and historical trends across an 8-page interactive **Streamlit + Plotly** dashboard.

```
       [ Website Traffic Simulator ]
                     │
                     ▼
          [ Apache Kafka Topic ]
            (website_traffic)
                     │
                     ▼
    [ Apache Spark Structured Streaming ]
        (1-min windows, 2-min watermark)
             │                      │
             ▼                      ▼
      [ Hadoop HDFS ]        [ Apache HBase ]
   (Raw Parquet Logs)       (Metrics & Alerts)
             │                      │
             ▼                      ▼
    [ Spark Batch Job ]     [ Streamlit Dashboard ]
   (Historical Reports)     (Plotly Interactive UI)
```

---

## 2. Team Member Responsibilities

| Role | Primary Responsibilities |
|---|---|
| **Member 1 – Infrastructure & Networking** | 3-PC private networking (Tailscale / LAN), Hadoop HDFS configuration (NameNode/DataNodes), Spark cluster setup (Master/Workers), Kafka KRaft quorum, and node verification scripts. |
| **Member 2 – Data Engineering & Streaming** | Traffic event contract & generator, Kafka producer integration, Spark Structured Streaming pipelines, HBase schema & deterministic row keys, and statistical anomaly detection algorithms. |
| **Member 3 – Application & Integration** | Streamlit multi-view dashboard, Plotly interactive data visualizations, Spark batch analytics, HDFS historical report ingestion, and documentation. |

---

## 3. Technology Stack & Compatibility Baseline

All three nodes run identical pinned versions inside Ubuntu Server 22.04 LTS VMs:

| Component | Pinned Version | Architectural Role |
|---|---|---|
| **Host Operating System** | Windows 10 / 11 | Physical machine layer hosting the virtual nodes |
| **Guest OS / Runtime** | Ubuntu Server 22.04 LTS | Standardized Linux environment across all 3 PCs |
| **Java** | OpenJDK 11 | Required runtime for Hadoop 3.3.6 and Spark 3.5.3 |
| **Python** | Python 3.10 | Native Ubuntu 22.04 Python runtime |
| **Hadoop HDFS** | 3.3.6 | Distributed storage (1 NameNode on PC1, 2 DataNodes on PC2 & PC3) |
| **Apache Kafka** | 3.7.2 (KRaft mode) | Real-time event ingestion; eliminates ZooKeeper for Kafka |
| **Apache Spark** | 3.5.3 (Scala 2.12) | Structured Streaming engine and offline batch processor |
| **Kafka Connector** | `spark-sql-kafka-0-10_2.12:3.5.3` | Spark-to-Kafka streaming ingestion connector |
| **Apache HBase** | 2.5.16 (`-hadoop3-bin`) | Real-time NoSQL storage for aggregated metrics and alerts |
| **Streamlit & Plotly** | Streamlit 1.41.1, Plotly 5.24.1 | Interactive operational dashboard |

---

## 4. Distributed Cluster Topology (3 Physical Nodes)

```
=============================================================================
PC 1: MASTER NODE (`master`)
  • Hadoop NameNode (:9000, Web UI :9870)
  • Spark Master (:7077, Web UI :8080)
  • Kafka Broker & KRaft Controller 1 (:9092, :9093)
  • HBase Master (:16000, Web UI :16010) + HBase ZooKeeper (:2181)
  • HBase Thrift Gateway (:9090)
  • Streamlit Web Dashboard (:8501)
-----------------------------------------------------------------------------
PC 2: WORKER NODE 1 (`worker1`)
  • HDFS DataNode (:9864)
  • Spark Worker (2 cores, 2 GB RAM)
  • Kafka Broker & KRaft Controller 2 (:9092, :9093)
  • HBase RegionServer (:16020, Web UI :16030)
-----------------------------------------------------------------------------
PC 3: WORKER NODE 2 (`worker2`)
  • HDFS DataNode (:9864) + HDFS SecondaryNameNode (:9868)
  • Spark Worker (2 cores, 2 GB RAM)
  • Kafka Broker & KRaft Controller 3 (:9092, :9093)
  • HBase RegionServer (:16020, Web UI :16030)
  • Traffic Simulator (publishes to Kafka cluster)
=============================================================================
```

---

## 5. Repository Structure

```text
traffic-anomaly-detection/
├── common/
│   ├── event_schema.py             # Event data contract, validation, user agents & methods
│   └── __init__.py
├── data-generator/
│   └── traffic_generator.py        # Realistic multi-session simulator with anomaly injection
├── kafka_client/
│   ├── producer.py                 # KafkaProducer wrapper with retry & exponential backoff
│   └── __init__.py
├── spark/
│   ├── streaming_processor.py      # Structured Streaming: 4 queries (HDFS Parquet + HBase)
│   ├── batch_processor.py          # Offline batch aggregations over HDFS raw Parquet
│   └── anomaly_detector.py         # Multi-criteria statistical & threshold anomaly rules
├── hbase/
│   ├── database_operations.py      # HappyBase / Thrift table init, upserts, and scanning
│   └── __init__.py
├── dashboard/
│   └── app.py                      # 8-section interactive Streamlit & Plotly application
├── config/
│   ├── cluster.env.example         # 3-node network IPs & host configuration
│   ├── app.env.distributed.example # Master node runtime environment template
│   ├── hadoop/                     # core-site.xml, hdfs-site.xml, workers templates
│   ├── spark/                      # spark-env.sh, spark-defaults.conf, workers templates
│   ├── kafka/                      # server.properties KRaft template
│   └── hbase/                      # hbase-site.xml, hbase-env.sh, regionservers templates
├── scripts/                        # Automated node setup, formatting, startup, and checks
│   ├── check_cluster.sh            # One-click health check for all cluster services
│   ├── configure_node.sh           # Renders configuration files for master/worker1/worker2
│   ├── install_stack.sh            # Downloads & verifies Hadoop, Spark, Kafka, HBase
│   ├── start_hdfs_node.sh          # Starts NameNode, SecondaryNameNode, or DataNodes
│   ├── start_kafka_node.sh         # Starts KRaft broker daemon
│   ├── start_spark_node.sh         # Starts Spark Master or Workers
│   ├── start_hbase_node.sh         # Starts HBase Master, RegionServers, Thrift
│   ├── start_streaming.sh          # Submits Spark streaming job to cluster
│   ├── run_batch.sh                # Submits Spark batch analytics job
│   └── run_dashboard.sh            # Launches Streamlit dashboard
├── docs/
│   └── THREE_NODE_DEPLOYMENT.md    # Comprehensive step-by-step 3-node cluster guide
├── tests/
│   ├── test_event_schema.py        # Event schema and validation unit tests
│   ├── test_hbase_keys.py          # Deterministic row-key generation tests
│   ├── test_streaming_helpers.py   # ForeachBatch Spark upsert callback tests
│   ├── test_anomaly_detector.py    # Anomaly detection rule tests
│   └── test_config_templates.py    # XML and property file template rendering tests
├── .env.example                    # Local single-machine environment template
├── .gitignore                      # Git exclusion rules
├── requirements.txt                # Pinned Python dependencies
└── README.md                       # Project documentation and viva guide
```

---

## 6. Getting Started

### Option A: Local Single-Machine Development

1. **Clone the repository and prepare the virtual environment:**
   ```bash
   git clone https://github.com/Pratik0182/traffic-anomaly-detection.git
   cd traffic-anomaly-detection
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   cp .env.example .env
   ```

2. **Run all unit tests:**
   ```bash
   pytest tests -v
   ```

3. **Start local Kafka & initialize HBase tables:**
   ```bash
   # Create Kafka topic
   kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
     --topic website_traffic --partitions 3 --replication-factor 1

   # Initialize HBase tables via Thrift
   hbase-daemon.sh start thrift
   python -m hbase.database_operations init
   python -m hbase.database_operations check
   ```

4. **Launch Spark Structured Streaming:**
   ```bash
   spark-submit \
     --master 'local[2]' \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
     spark/streaming_processor.py
   ```

5. **Generate test traffic:**
   ```bash
   # Normal traffic
   python data-generator/traffic_generator.py --rate 5

   # Traffic spike demo
   python data-generator/traffic_generator.py --rate 5 --spike-after-seconds 10 --spike-rate 45

   # Error surge demo
   python data-generator/traffic_generator.py --rate 5 --inject-errors

   # Latency spike demo
   python data-generator/traffic_generator.py --rate 5 --slow-responses
   ```

6. **Run batch job & launch dashboard:**
   ```bash
   spark-submit --master 'local[2]' spark/batch_processor.py
   streamlit run dashboard/app.py
   ```

---

### Option B: 3-Node Physical Cluster Deployment

Follow the complete, ordered guide in [`docs/THREE_NODE_DEPLOYMENT.md`](docs/THREE_NODE_DEPLOYMENT.md):
1. **Host Configuration:** Assign static hostnames (`master`, `worker1`, `worker2`) and configure private network IP addresses.
2. **Software Stack Installation:** Execute `sudo bash scripts/install_stack.sh` on all nodes to install Hadoop 3.3.6, Spark 3.5.3, Kafka 3.7.2, and HBase 2.5.16 with verified checksums.
3. **Template Rendering:** Render cluster configuration files on each node via `bash scripts/configure_node.sh <node-role> config/cluster.env`.
4. **One-Time Formatting:**
   - On master: Format HDFS NameNode (`bash scripts/format_hdfs_namenode_once.sh`).
   - On all nodes: Format Kafka KRaft storage with the shared cluster ID (`bash scripts/format_kafka_node_once.sh <ID>`).
5. **Daemon Startup Sequence:**
   1. HDFS: `scripts/start_hdfs_node.sh` (Master first, then workers).
   2. Kafka: `scripts/start_kafka_node.sh` on all 3 nodes; create topic with `scripts/create_kafka_topic.sh`.
   3. Spark: `scripts/start_spark_node.sh` (Master first, then workers).
   4. HBase: `scripts/start_hbase_node.sh` (Master first, then workers).
6. **Streaming & Analytics:** Run `scripts/start_streaming.sh` on master and launch the traffic generator on PC3.

---

## 7. Streamlit Dashboard (8 Analytical Views)

The dashboard (`dashboard/app.py`) provides 8 dedicated tabs built with Plotly:

1. **Overview:** Executive KPIs (Requests/min, Unique Visitors, Error Percentage, Avg Response Time, Traffic State) with side-by-side volume and HTTP status donut charts.
2. **Live Traffic Monitoring:** Event-time 1-minute window area chart with visible spike threshold annotations and recent window telemetry tables.
3. **Visitor Analytics:** Unique visitors vs total requests, user engagement depth (requests/visitor ratio), and HyperLogLog methodology notes.
4. **Page Analytics:** Horizontal bar chart of top requested endpoints and route traffic share percentages.
5. **HTTP Error Monitoring:** Error percentage over time, 4xx client errors vs 5xx server errors comparison, and status distribution breakdown.
6. **Anomaly Detection:** Real-time alert log from HBase displaying `TRAFFIC_SPIKE`, `HIGH_ERROR_RATE`, and `HIGH_LATENCY` alerts with `CRITICAL`/`WARNING` severity badges, observed values, thresholds, and diagnostic reasons.
7. **Historical Reports (HDFS):** Date range filtering over offline Spark batch reports (`daily_summary`, `hourly_summary`, `top_pages`, `response_time_trend` with average and p95 latency).
8. **System / Cluster Status:** Physical 3-node topology overview, daemon endpoint directory, live HBase Thrift connection probe, and viva terminal commands.

---

## 8. Academic Viva / Oral Exam Preparation Guide

### Viva Questions for Member 1 (Infrastructure & Cluster)

- **Q: Why was Kafka KRaft mode chosen instead of ZooKeeper?**
  *A:* Starting with Kafka 3.3, KRaft (Kafka Raft Metadata mode) is production-ready. It manages cluster metadata internally using an active controller quorum, removing the operational overhead and failure modes of maintaining a separate external ZooKeeper cluster for Kafka.
- **Q: Why is `dfs.replication` set to 2 instead of the Hadoop default of 3?**
  *A:* Our physical cluster has exactly two DataNodes (`worker1` and `worker2`). Setting replication to 3 would cause HDFS to continuously report under-replicated block warnings because a block cannot place two replicas on the same physical DataNode.
- **Q: What happens if one of the worker nodes goes offline during the demonstration?**
  *A:* Because HDFS block replication is 2, all blocks remain available on the surviving DataNode. For Kafka, since `min.insync.replicas` is 2 and replication is 3, the cluster continues acknowledging writes as long as 2 brokers remain synchronized. Spark continues scheduling tasks on the remaining active worker.

### Viva Questions for Member 2 (Data Engineering & Streaming)

- **Q: Why use `approx_count_distinct` instead of `countDistinct` in Spark Streaming?**
  *A:* Exact distinct counting requires storing all unique keys seen across unbounded streaming state in memory, which quickly causes JVM `OutOfMemoryError`. Spark Structured Streaming prohibits non-watermarked exact distinct aggregations on streaming datasets. We use `approx_count_distinct` based on the HyperLogLog algorithm, which provides a guaranteed standard deviation error (0.05) with constant, bounded memory usage.
- **Q: How does the system handle duplicate writes if a Spark micro-batch fails and retries?**
  *A:* HBase writes are designed to be idempotent. We do not use atomic increments. Instead, we use deterministic compound row keys (`metric_type#window_key#dimension`) and write absolute metrics via HBase `PUT` semantics. A retried micro-batch simply overwrites the exact same row.
- **Q: What anomaly detection rules are evaluated?**
  *A:* Three statistical and threshold rules are evaluated per 1-minute window:
  1. *Traffic Spike:* Requests/min exceed the configured baseline threshold (e.g., > 600 req/min).
  2. *Error Rate Surge:* Combined 4xx + 5xx error rate exceeds 15% with >= 20 total requests.
  3. *Latency Anomaly:* Average server response latency exceeds 600 ms.

### Viva Questions for Member 3 (Application & Dashboard)

- **Q: How does the dashboard read live metrics vs historical reports?**
  *A:* Live metrics are queried from HBase through the HappyBase Thrift gateway every 10 seconds for real-time responsiveness. Historical reports are precomputed by Spark batch jobs over raw Parquet logs stored in HDFS and written as JSON summaries, which Streamlit loads via the Hadoop CLI or local path fallback.
- **Q: What is the difference between event time and processing time?**
  *A:* Processing time is the wall-clock time on the computer running the Spark streaming job. Event time is the actual timestamp embedded in the event when the user made the web request. We group by event-time windows and use a 2-minute watermark to accurately place late-arriving events into their correct historical time window.
