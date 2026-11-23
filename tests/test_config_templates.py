from pathlib import Path
from string import Template
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
VALUES = {
    "MASTER_HOST": "master",
    "WORKER1_HOST": "worker1",
    "WORKER2_HOST": "worker2",
    "MASTER_TAILSCALE_IP": "100.100.1.1",
    "WORKER1_TAILSCALE_IP": "100.100.1.2",
    "WORKER2_TAILSCALE_IP": "100.100.1.3",
    "NODE_HOST": "worker1",
    "NODE_TAILSCALE_IP": "100.100.1.2",
    "NODE_ID": "2",
    "DATA_ROOT": "/data/bigdata",
    "HDFS_REPLICATION": "2",
    "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
    "HADOOP_HOME": "/opt/bigdata/hadoop",
    "SPARK_WORKER_CORES": "2",
    "SPARK_WORKER_MEMORY": "2g",
    "SPARK_EXECUTOR_MEMORY": "1g",
    "SPARK_MAX_CORES": "4",
    "KAFKA_BROKER_PORT": "9092",
    "KAFKA_CONTROLLER_PORT": "9093",
    "KAFKA_TOPIC": "website_traffic",
    "KAFKA_TOPIC_PARTITIONS": "3",
    "KAFKA_TOPIC_REPLICATION": "3",
    "KAFKA_MIN_INSYNC_REPLICAS": "2",
}

def render(relative_path: str) -> str:
    template = (ROOT / relative_path).read_text(encoding="utf-8")
    return Template(template).substitute(VALUES)

def properties(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result

def test_hadoop_and_hbase_xml_templates_render_as_valid_xml():
    for path in [
        "config/hadoop/core-site.xml.template",
        "config/hadoop/hdfs-site.xml.template",
        "config/hbase/hbase-site.xml.template",
    ]:
        ET.fromstring(render(path))

def test_hdfs_template_uses_two_data_node_replication():
    xml = ET.fromstring(render("config/hadoop/hdfs-site.xml.template"))
    values = {
        prop.findtext("name"): prop.findtext("value")
        for prop in xml.findall("property")
    }
    assert values["dfs.replication"] == "2"
    assert values["dfs.namenode.rpc-address"] == "master:9000"
    assert values["dfs.datanode.hostname"] == "worker1"

def test_kafka_template_has_three_node_kraft_quorum_and_replication():
    config = properties(render("config/kafka/server.properties.template"))
    assert config["process.roles"] == "broker,controller"
    assert config["node.id"] == "2"
    assert config["controller.quorum.voters"] == (
        "1@master:9093,2@worker1:9093,3@worker2:9093"
    )
    assert config["default.replication.factor"] == "3"
    assert config["min.insync.replicas"] == "2"
