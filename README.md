# Real-Time Website Traffic Analytics & Anomaly Detection

A CSBB 422 Big Data project that generates synthetic website request events, ingests
them through Apache Kafka, processes them with Spark Structured Streaming, stores raw
events in Hadoop HDFS and processed metrics in HBase, and presents live/historical
analytics in Streamlit.

> Traffic is simulated for a repeatable academic demonstration; it is not captured
> from a real production website.

## Project status

- [x] Event schema, synthetic traffic generator and Kafka producer
- [x] Spark Structured Streaming: HDFS raw-event archive + HBase summary/page/status metrics
- [x] Separate checkpoint locations for the four streaming queries
- [x] HBase Thrift helpers, deterministic row keys and retry-safe absolute upserts
- [x] Spark batch analytics over HDFS Parquet
- [x] Streamlit live dashboard and HDFS historical reports
- [x] Unit tests for event schema, row keys, Spark callbacks and rendered cluster templates
- [x] Three-node Hadoop/Spark/Kafka/HBase configuration templates
- [x] Three-node installation, formatting, startup and verification scripts
- [ ] End-to-end execution on the team's actual three PCs

## Compatibility baseline

Keep the same versions on all three Ubuntu VMs. This is a pinned academic-project
baseline, not a claim that every component is the newest available release.

| Component | Target |
|---|---|
| Host OS | Windows 10/11 |
| Linux runtime | Ubuntu Server 22.04 LTS VM on each PC |
| Java | OpenJDK 11 |
| Python | 3.10 |
| Hadoop | 3.3.6 |
| Spark | 3.5.3, Scala 2.12 build |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Kafka | 3.7.2, KRaft mode |
| HBase | 2.5.16, Hadoop 3 binary |
| Streamlit | 1.41.1 |

Compatibility-sensitive choices:
- Spark 3.5.3 and the Kafka connector must use the same Spark version and Scala binary
  version (`_2.12`).
- HBase 2.5.x supports Hadoop 3.3.5+; the deployment uses Hadoop 3.3.6.
- Use the HBase archive ending in `-hadoop3-bin.tar.gz`, not the Hadoop 2 build.
- Python 3.10 is the Ubuntu 22.04 system version and is supported by PySpark 3.5.3.
- Kafka uses three KRaft controllers and three brokers; all three nodes share one
  generated Kafka cluster ID.

Official references:
- [Spark 3.5.3](https://spark.apache.org/docs/3.5.3/)
- [Spark Structured Streaming](https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html)
- [Hadoop 3.3.6 cluster setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/ClusterSetup.html)
- [HBase compatibility prerequisites](https://hbase.apache.org/docs/configuration/basic-prerequisites/)
- [Kafka 3.7 KRaft mode](https://kafka.apache.org/37/operations/kraft/)

## Repository layout

```text
website-traffic-dashboard/
├── common/                         # Shared event contract
├── data-generator/                 # Synthetic Kafka traffic generator
├── kafka_client/                   # kafka-python producer wrapper
├── spark/
│   ├── streaming_processor.py      # Kafka -> HDFS + HBase
│   └── batch_processor.py          # HDFS historical reports
├── hbase/
│   └── database_operations.py      # Thrift table and row operations
├── dashboard/
│   └── app.py                      # Streamlit UI
├── config/
│   ├── cluster.env.example
│   ├── app.env.distributed.example
│   ├── hadoop/                     # Hadoop XML and workers templates
│   ├── spark/                      # Spark env/defaults/workers templates
│   ├── kafka/                      # KRaft broker/controller template
│   └── hbase/                      # HBase site/env/regionservers templates
├── scripts/                        # Node prep, config, startup, checks
├── docs/
│   └── THREE_NODE_DEPLOYMENT.md
├── hadoop/configuration/
├── tests/
├── .env.example
├── requirements.txt
└── README.md
```

## 1. Python environment

Python 3.10 is the target for the Ubuntu VMs and local Windows development.

### Windows PowerShell

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

### Ubuntu

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

## 2. Single-machine smoke test

For the first application test, run Kafka and HBase locally, then:

1. Create the Kafka topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic website_traffic \
  --partitions 3 --replication-factor 1
```

2. Start HBase and its Thrift service, then create the application tables:

```bash
hbase-daemon.sh start thrift
python -m hbase.database_operations init
python -m hbase.database_operations check
```

3. Start Spark Structured Streaming:

```bash
spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark/streaming_processor.py
```

4. In another terminal, generate traffic:

```bash
python data-generator/traffic_generator.py --rate 5
```

To trigger a spike:

```bash
python data-generator/traffic_generator.py \
  --rate 5 --spike-after-seconds 15 \
  --spike-duration-seconds 30 --spike-rate 40
```

5. Inspect HBase and HDFS output, then run batch analytics:

```bash
python -m hbase.database_operations scan --type summary --limit 10
spark-submit --master 'local[2]' spark/batch_processor.py
streamlit run dashboard/app.py
```

The `.env.example` defaults use local filesystem paths for raw events, checkpoints and
reports. For HDFS, use the distributed environment template described below.

## 3. Three-PC deployment

The recommended final cluster has:

- PC1 / `master`: HDFS NameNode, Spark Master, HBase Master, HBase-managed ZooKeeper,
  HBase Thrift, and Streamlit/Spark driver.
- PC2 / `worker1`: HDFS DataNode, Spark Worker, HBase RegionServer, Kafka broker/controller.
- PC3 / `worker2`: HDFS DataNode + SecondaryNameNode, Spark Worker, HBase RegionServer,
  Kafka broker/controller, and traffic generator.
- All three PCs: Kafka broker + KRaft controller.

The Kafka topic uses 3 partitions, replication factor 3 and minimum in-sync replicas 2.
HDFS has two DataNodes and replication factor 2.

Read the complete, ordered instructions in
[`docs/THREE_NODE_DEPLOYMENT.md`](docs/THREE_NODE_DEPLOYMENT.md). It covers:
- Ubuntu VM preparation and Tailscale/private networking;
- verified software downloads;
- configuration rendering for each node;
- safe one-time HDFS/Kafka formatting;
- daemon startup order;
- Spark/HBase configuration;
- traffic generation, dashboard and batch report commands;
- demo sequence and recovery cautions.

Important: do not run HDFS NameNode format or Kafka storage format on an existing cluster.

## 4. Streaming design

The Spark application starts four independent streaming queries:

| Query | Output | Checkpoint |
|---|---|---|
| Raw events | Partitioned Parquet in HDFS | `raw_events` |
| Summary metrics | HBase `traffic_metrics` | `summary_metrics` |
| Popular pages | HBase `traffic_metrics` | `page_metrics` |
| HTTP status counts | HBase `traffic_metrics` | `status_metrics` |

All queries use one-minute event-time windows and a two-minute watermark by default.
Unique visitors are estimated with Spark `approx_count_distinct`; the field is explicitly
named `unique_visitors_approx`.

HBase row keys are deterministic by metric type, window and dimension. The Spark
`foreachBatch` callbacks write absolute values with HBase PUT semantics rather than
incrementing counters, so a retried batch overwrites the same row. Each query has a
separate checkpoint path.

The default spike threshold is 600 requests/minute:
- Normal generator rate: 5 events/second (about 300/minute).
- Spike rate: 40 events/second (about 2,400/minute).

## 5. Historical batch reports

After raw Parquet files exist, run:

```bash
spark-submit --master 'local[2]' spark/batch_processor.py
```

The distributed helper is `scripts/run_batch.sh`. Five JSON report directories are
written below `HDFS_REPORTS_PATH`: `daily_summary`, `hourly_summary`, `top_pages`,
`status_distribution`, and `response_time_trend`.

## 6. Dashboard

Run locally:

```bash
streamlit run dashboard/app.py
```

On the cluster, use `scripts/run_dashboard.sh`. The live HBase panel refreshes every
10 seconds; historical charts read the latest JSON reports from HDFS using the Hadoop
CLI. The dashboard host therefore needs `HADOOP_CONF_DIR` and the `hdfs` command.

## 7. Tests

```bash
python -m pytest tests -v
```

Unit tests do not require live Kafka, Spark, HDFS or HBase services. They validate the
event contract, HBase row keys, Spark callback behavior and rendered cluster templates.
They are not an end-to-end integration test.

## 8. Known limitations

- The final three-PC deployment has not yet been executed on the team's actual PCs.
- HDFS NameNode, HBase Master and the single HBase ZooKeeper instance are single points
  of failure; this is a distributed demonstration, not HA.
- Kafka uses combined broker/controller roles for a small academic cluster. Apache
  recommends separating roles for critical production environments.
- No Kerberos, TLS or application-level authentication is configured. Keep all cluster
  traffic on a private LAN or restricted Tailscale tailnet.
- The four Spark streaming queries each read the Kafka topic independently. This is
  acceptable for the project's synthetic demo rate, not an optimized high-throughput
  production design.
