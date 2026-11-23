# Hadoop configuration notes

Spark loads Hadoop settings from `HADOOP_CONF_DIR` (recommended) or the Spark
installation's `conf/` directory. Keep `core-site.xml` and `hdfs-site.xml` identical
on the Spark driver and every worker.

For the planned cluster:
- `fs.defaultFS` points to the NameNode RPC endpoint, e.g. `hdfs://master:9000`.
- Raw traffic events: `/traffic/raw/events`.
- Structured Streaming checkpoints: `/traffic/checkpoints`.
- With two DataNodes (PC2 and PC3), set `dfs.replication` to `2` rather than the
  default `3`, unless PC1 is also configured as a DataNode.

Example `.env` values:
```dotenv
HDFS_RAW_EVENTS_PATH=hdfs://master:9000/traffic/raw/events
STREAMING_CHECKPOINT_BASE=hdfs://master:9000/traffic/checkpoints
```

Do not use `localhost` in distributed configuration. On a worker, localhost refers to
that worker, not the master. The exact NameNode RPC port must match `fs.defaultFS` in
`core-site.xml`. Do not manually create the HBase root directory in HDFS; HBase
initializes its own root path during first startup.
