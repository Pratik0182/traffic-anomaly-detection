# Three-PC distributed deployment guide

This guide turns the application into a small, real distributed cluster. The three
Windows PCs run Ubuntu Server 22.04 LTS virtual machines. The master hosts the control
services; the two workers host HDFS DataNodes, Spark workers and HBase RegionServers.
Kafka is distributed across all three PCs as a three-broker KRaft cluster.

> **Scope:** this is a student demonstration cluster, not a production-secure or
> high-availability platform. It has one HDFS NameNode, one HBase Master, and one
> ZooKeeper instance. It has no Kerberos, TLS, or disaster-recovery setup.

## 1. Final node layout

| Service | PC1 `master` | PC2 `worker1` | PC3 `worker2` |
|---|---|---|---|
| Kafka broker | Yes | Yes | Yes |
| KRaft controller | Yes | Yes | Yes |
| HDFS NameNode | Yes | – | – |
| HDFS DataNode | – | Yes | Yes |
| HDFS SecondaryNameNode | – | – | Yes |
| Spark Master | Yes | – | – |
| Spark Worker | – | Yes | Yes |
| HBase Master | Yes | – | – |
| HBase RegionServer | – | Yes | Yes |
| HBase-managed ZooKeeper | Yes | – | – |
| HBase Thrift | Yes | – | – |
| Streamlit + Spark driver | Yes | – | – |
| Traffic generator | Optional | Optional | Recommended |

Kafka's three-node KRaft quorum can tolerate one controller failure. Its broker and
controller roles are combined on each machine to reduce service count; Apache does not
recommend combined mode for critical production environments. The project uses it only
for a small academic cluster.

## 2. VM and network preparation

### Suggested VM allocation

For each Windows host with approximately 16 GB RAM:

- Ubuntu Server 22.04 LTS, minimal installation
- 4 virtual CPUs
- 8 GB RAM (keep the Windows host responsive; reduce Spark limits if memory pressure occurs)
- 60 GB virtual disk, dynamically allocated
- OpenSSH Server enabled
- Hostnames exactly `master`, `worker1`, `worker2`

Use a bridged adapter if all PCs are on the same LAN. If they are on different networks,
install Tailscale inside each Ubuntu VM and use its private 100.64.0.0/10 IPv4 address.
Do not expose Hadoop, Kafka, HBase or Spark ports to the public Internet.

Set each VM hostname:

```bash
# Run the appropriate one on each VM:
sudo hostnamectl set-hostname master
# or worker1 / worker2
```

Reboot or reconnect after changing the hostname.

### Install base packages

Copy/clone the repository on all three VMs, then from its root run:

```bash
bash scripts/prepare_ubuntu_node.sh
```

This installs OpenJDK 11, Python 3.10 (Ubuntu 22.04's native Python), SSH and utilities.
The Python 3.10 choice avoids adding an external Python PPA; PySpark 3.5.3 supports
Python 3.8 and newer.

### Private network setup

For Tailscale, install and authenticate it on each VM using the official Linux guide:
https://tailscale.com/kb/1031/install-linux

On each node:

```bash
tailscale up
tailscale ip -4
```

Record all three IPs. Test name resolution and reachability after configuration:

```bash
ping -c 3 master
ping -c 3 worker1
ping -c 3 worker2
```

If using Tailscale, make sure the devices are approved in the tailnet and its ACL policy
allows the three VMs to communicate. The project config script writes a managed block
to `/etc/hosts`, so MagicDNS is not required.

## 3. Prepare the cluster configuration file

On one machine, copy:

```bash
cp config/cluster.env.example config/cluster.env
```

Edit `config/cluster.env` and replace all three `REPLACE_WITH_...` values with the
actual Tailscale IPs (or stable private-LAN IPs if using a LAN). Keep these names:

```dotenv
MASTER_HOST=master
WORKER1_HOST=worker1
WORKER2_HOST=worker2
```

Copy the same `config/cluster.env` file to the same repository-relative location on
all three VMs. Do not commit it if it contains private network details.

## 4. Install the same software on all three VMs

Run on each VM:

```bash
sudo bash scripts/install_stack.sh
```

The script downloads and verifies SHA-512 checksums for:

| Component | Version | Archive |
|---|---|---|
| Hadoop | 3.3.6 | `hadoop-3.3.6.tar.gz` |
| Spark | 3.5.3 | `spark-3.5.3-bin-hadoop3.tgz` |
| Kafka | 3.7.2 | `kafka_2.13-3.7.2.tgz` |
| HBase | 2.5.16 | `hbase-2.5.16-hadoop3-bin.tar.gz` |

The HBase archive is deliberately the Hadoop 3 build. HBase 2.5.x supports Hadoop 3.3.5+
and Java 11; the official HBase compatibility guide identifies the Hadoop 3 artifact by
the `-hadoop3` suffix:
https://hbase.apache.org/docs/configuration/basic-prerequisites/

If the archive server or checksum format changes, stop and verify the download rather
than bypassing checksum validation.

## 5. Render node-specific configuration

Copy `config/cluster.env` to each VM. Run exactly one command per VM from the project
root:

```bash
# PC1
bash scripts/configure_node.sh master config/cluster.env

# PC2
bash scripts/configure_node.sh worker1 config/cluster.env

# PC3
bash scripts/configure_node.sh worker2 config/cluster.env
```

The script checks that the Ubuntu hostname matches the selected role, adds the three
private IP/name mappings to `/etc/hosts`, creates persistent data/log directories, and
renders Hadoop, Spark, Kafka and HBase configuration files under `/opt/bigdata`.

Review generated files before starting services:

```bash
cat /opt/bigdata/hadoop/etc/hadoop/core-site.xml
cat /opt/bigdata/hadoop/etc/hadoop/hdfs-site.xml
cat /opt/bigdata/spark/conf/spark-env.sh
cat /opt/bigdata/kafka/config/kraft/server.properties
cat /opt/bigdata/hbase/conf/hbase-site.xml
```

The default resource limits are intentionally modest: each Spark worker advertises
2 cores and 2 GB RAM; each Spark executor uses 1 GB. Increase them only after observing
the VMs' actual memory usage.

## 6. Network ports

All of these ports must be reachable between the relevant VMs over the private LAN or
Tailscale network. The web UI ports are also needed from the device used for the demo.

| Port | Service | Where |
|---:|---|---|
| 22/tcp | SSH administration | All nodes |
| 9000/tcp | HDFS NameNode RPC | Master |
| 9870/tcp | HDFS NameNode UI | Master |
| 9864/tcp, 9866-9868/tcp | DataNode transfer/UI/IPC and SecondaryNameNode UI | Workers |
| 9092/tcp | Kafka broker | All nodes |
| 9093/tcp | Kafka KRaft controller | All nodes |
| 7077/tcp | Spark Master | Master |
| 7078/tcp | Spark Worker RPC | Workers |
| 8080/tcp, 8081/tcp | Spark Master/Worker UIs | Master/workers |
| 39000-39064/tcp | Spark driver and block-manager communication | Master/workers |
| 2181/tcp | HBase-managed ZooKeeper | Master |
| 16000/tcp, 16010/tcp | HBase Master RPC/UI | Master |
| 16020/tcp, 16030/tcp | HBase RegionServer RPC/UI | Workers |
| 9090/tcp | HBase Thrift | Master |
| 8501/tcp | Streamlit dashboard | Master |

Spark can use additional retry ports within the configured 39000 range; the worker
configuration and Spark's `spark.port.maxRetries` control those retries. Do not publish
these ports on a public interface.

## 7. Optional private-interface firewall

After Tailscale is connected and `tailscale0` exists, run on each VM:

```bash
sudo bash scripts/configure_tailscale_firewall.sh
```

This denies unsolicited inbound traffic on other interfaces and allows traffic arriving
through `tailscale0`. Restrict the tailnet with Tailscale ACLs to the three VMs and the
authorized users. This is a network boundary for a demo, not application-level
authentication or encryption.

## 8. Format HDFS once, then start HDFS daemons

**Formatting is only for a brand-new cluster. Never reformat an existing NameNode.**

On `master`, as the normal Linux account:

```bash
bash scripts/format_hdfs_namenode_once.sh
```

The script refuses to format a non-empty directory and skips formatting if a NameNode
`VERSION` file already exists.

Start the daemons manually, one machine at a time:

```bash
# PC1
bash scripts/start_hdfs_node.sh master

# PC2
bash scripts/start_hdfs_node.sh worker1

# PC3
bash scripts/start_hdfs_node.sh worker2
```

PC3 also starts the SecondaryNameNode. On master, verify:

```bash
hdfs dfsadmin -report
```

Expected: one NameNode, two live DataNodes, and configured replication factor 2.
Open the NameNode UI at `http://master:9870`.

Create the project HDFS paths:

```bash
bash scripts/prepare_hdfs_paths.sh
```

## 9. Format Kafka KRaft storage once, then start all brokers

Kafka storage must be formatted once on each empty broker/controller using the **same
cluster ID**. Never format a node that already has `meta.properties`.

On master, generate one ID:

```bash
bash scripts/generate_kafka_cluster_id.sh
```

Copy the printed ID. Then run the following on each VM, replacing `PASTE_THE_SAME_ID`:

```bash
bash scripts/format_kafka_node_once.sh PASTE_THE_SAME_ID
```

Start Kafka on each VM:

```bash
bash scripts/start_kafka_node.sh
```

After all three brokers are running, create the replicated topic on master:

```bash
bash scripts/create_kafka_topic.sh
```

Expected topic description: 3 partitions, replication factor 3, min ISR 2, with replicas
distributed across broker IDs 1, 2 and 3. Check the KRaft controller quorum and brokers
using Kafka's CLI tools if startup fails.

Kafka KRaft documentation:
https://kafka.apache.org/37/operations/kraft/

## 10. Start Spark

Start the Spark master on PC1:

```bash
bash scripts/start_spark_node.sh master
```

Start a Spark worker on each worker VM:

```bash
# PC2
bash scripts/start_spark_node.sh worker1

# PC3
bash scripts/start_spark_node.sh worker2
```

Open `http://master:8080`. Expected: one Spark master and two registered workers, each
advertising 2 cores and 2 GB memory.

The workers are started manually, so passwordless SSH is not required for this step.
Hadoop and HBase daemons are also started manually below.

## 11. Start HBase

HDFS must be healthy before HBase starts. HBase's single ZooKeeper instance is managed
on master for this small demonstration.

```bash
# PC1
bash scripts/start_hbase_node.sh master

# PC2
bash scripts/start_hbase_node.sh worker1

# PC3
bash scripts/start_hbase_node.sh worker2
```

Master starts ZooKeeper, HBase Master and Thrift; workers start RegionServers. Verify
the HBase Master UI at `http://master:16010` and check that two RegionServers register.

On master, create/check the application's tables after completing the Python setup:

```bash
python -m hbase.database_operations init
python -m hbase.database_operations check
```

Expected tables:
- `traffic_metrics`, family `m`
- `traffic_spike_alerts`, family `a`

## 12. Install the Python application environment

On master and whichever VM will generate traffic, run:

```bash
bash scripts/setup_python_env.sh
```

The Spark driver and Streamlit run from the virtual environment on master. Spark executor
nodes use Ubuntu's `/usr/bin/python3.10`; all three nodes installed that interpreter in
step 2.

On master, create the distributed application environment:

```bash
cp config/app.env.distributed.example .env
```

If PC3 will run the traffic generator, copy the same distributed template to PC3's
repository root as `.env` too (or pass `--bootstrap-servers
master:9092,worker1:9092,worker2:9092` to the generator).

Review the values. In particular, all Kafka broker addresses must resolve from every
Spark worker, and HDFS/HBase must point to the master host. Do not use `localhost` for a
remote service.

## 13. Run the full application

Start the streaming job on master:

```bash
bash scripts/start_streaming.sh
```

It starts four independent Spark queries with separate HDFS checkpoints:
- raw events to partitioned Parquet in HDFS;
- one-minute summary metrics to HBase;
- one-minute page counts to HBase;
- one-minute HTTP status counts to HBase.

The Spark Kafka connector coordinate is pinned to Spark 3.5.3 and Scala 2.12:
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`.

On PC3, generate normal traffic:

```bash
python data-generator/traffic_generator.py --rate 5
```

For a visible spike:

```bash
python data-generator/traffic_generator.py \
  --rate 5 --spike-after-seconds 15 \
  --spike-duration-seconds 30 --spike-rate 40
```

On master, inspect HBase:

```bash
python -m hbase.database_operations scan --type summary --limit 10
python -m hbase.database_operations scan --type page --limit 20
```

Inspect HDFS raw events:

```bash
hdfs dfs -ls -R /traffic/raw/events
```

## 14. Run batch analytics and dashboard

After raw events have accumulated, run the batch job on master:

```bash
bash scripts/run_batch.sh
```

Expected HDFS output directories:

```text
/traffic/reports/daily_summary
/traffic/reports/hourly_summary
/traffic/reports/top_pages
/traffic/reports/status_distribution
/traffic/reports/response_time_trend
/traffic/reports/run_info
```

Start Streamlit on master:

```bash
bash scripts/run_dashboard.sh
```

Open `http://master:8501` from a device on the private network. The dashboard's live
panel refreshes every 10 seconds. Historical charts appear after the batch job finishes.

## 15. Useful verification commands

```bash
# HDFS: NameNode + two DataNodes
hdfs dfsadmin -report

# Kafka: three brokers / replicated topic
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server master:9092,worker1:9092,worker2:9092 \
  --describe --topic website_traffic

# Spark: two registered workers
curl -s http://master:8080 | grep -E 'Spark Master|worker'

# HBase: tables and recent metric rows
python -m hbase.database_operations check
python -m hbase.database_operations scan --type summary --limit 10

# HDFS: raw events and checkpoints
hdfs dfs -ls -R /traffic
```

The project-level check script is:

```bash
bash scripts/check_cluster.sh
```

## 16. Demo sequence

1. Show the three Ubuntu VMs and private host-to-host connectivity.
2. Show HDFS NameNode UI and two live DataNodes.
3. Show Kafka topic with 3 partitions and replication factor 3.
4. Show Spark UI with two workers.
5. Show HBase Master UI and two RegionServers.
6. Start Spark Structured Streaming and show its four active queries in logs.
7. Start traffic generation and show Kafka console-consumer JSON events.
8. Show HDFS Parquet files and HBase metric rows.
9. Open Streamlit and show the live charts.
10. Trigger spike mode and show the HBase alert row/dashboard alert.
11. Run batch analytics and show the historical reports.
12. Show the repository, README and deployment guide.

## 17. Recovery and safety rules

- **Do not** run the HDFS format script after data has been written.
- **Do not** run Kafka storage format on a broker with existing `meta.properties`.
- **Do not** delete streaming checkpoint directories to "fix" a restart. Checkpoints
  preserve offsets and state; deleting them can cause reprocessing or data loss.
- For a changed stateful Spark query, use a deliberately new checkpoint directory and
  document the cutover.
- If a VM reboots, start services in this order: HDFS, Kafka, Spark, HBase, streaming
  processor, traffic generator, batch job/dashboard.
- For a planned shutdown, stop the traffic generator and streaming job first, then stop
  the dashboard, HBase on all nodes, Spark on all nodes, Kafka on all nodes, and HDFS
  last. Use the matching `scripts/stop_*_node.sh` helper on each node.
- If one worker is offline, HDFS replication factor 2 means some writes may fail when
  both replicas are required. Restore the worker before continuing the demo.
- Kafka min ISR 2 means acknowledged writes may stop if fewer than two replicas are in
  sync. This is intentional for the replicated demo topic.

## 18. Known limitations

- HDFS NameNode, HBase Master, and the HBase ZooKeeper instance are single points of
  failure. The cluster demonstrates distributed storage and processing, not HA.
- Kafka uses three combined broker/controller nodes; this is for a student cluster and
  is not the production-recommended separated controller topology.
- No Kerberos, TLS, or application-level authentication is configured. Keep the VMs on
  a private network and do not publish service ports to the Internet.
- HBase Thrift is the application's Python integration point. It runs on master and is
  not load-balanced.
- The Spark streaming job has four Kafka source queries. This is simple and easy to
  demonstrate at the low synthetic event rate, but it reads the topic four times.
- The final deployment still needs to be run on the actual PCs. A successful script
  syntax check does not prove the cluster services have started correctly.
