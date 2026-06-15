import argparse
import os
import sys
from uuid import uuid4

from confluent_kafka import KafkaError, avro
from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_BOOTSTRAP_SERVERS = "46.225.20.89:9092"
_SASL_CONFIG_KEYS = (
    "security.protocol",
    "sasl.mechanisms",
    "sasl.username",
    "sasl.password",
)
_ENV_TO_CONFIG_KEY = {
    "BOOTSTRAP_SERVERS": "bootstrap.servers",
    "KAFKA_SECURITY_PROTOCOL": "security.protocol",
    "KAFKA_SASL_MECHANISMS": "sasl.mechanisms",
    "KAFKA_SASL_USERNAME": "sasl.username",
    "KAFKA_SASL_PASSWORD": "sasl.password",
    "KAFKA_CLIENT_ID": "client.id",
    "KAFKA_ACKS": "acks",
    "KAFKA_SESSION_TIMEOUT_MS": "session.timeout.ms",
    "KAFKA_HEARTBEAT_INTERVAL_MS": "heartbeat.interval.ms",
    "KAFKA_SCHEMA_REGISTRY_URL": "schema.registry.url",
    "KAFKA_BASIC_AUTH_USER_INFO": "basic.auth.user.info",
    "KAFKA_BASIC_AUTH_CREDENTIALS_SOURCE": "basic.auth.credentials.source",
}

name_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "Name",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    }
"""


class Name(object):
    """
        Name stores the deserialized Avro record for the Kafka key.
    """

    __slots__ = ["name", "id"]

    def __init__(self, name=None):
        self.name = name
        self.id = uuid4()

    @staticmethod
    def dict_to_name(obj, ctx):
        return Name(obj["name"])

    @staticmethod
    def name_to_dict(name, ctx):
        return Name.to_dict(name)

    def to_dict(self):
        return dict(name=self.name)


count_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "Count",
        "type": "record",
        "fields": [
            {"name": "count", "type": "int"}
        ]
    }
"""


class Count(object):
    """
        Count stores the deserialized Avro record for the Kafka value.
    """

    __slots__ = ["count", "id"]

    def __init__(self, count=None):
        self.count = count
        self.id = uuid4()

    @staticmethod
    def dict_to_count(obj, ctx):
        return Count(obj["count"])

    @staticmethod
    def count_to_dict(count, ctx):
        return Count.to_dict(count)

    def to_dict(self):
        return dict(count=self.count)


def parse_args():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(
        description="Kafka/Redpanda producer and consumer example"
    )
    parser._action_groups.pop()
    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "-t",
        "--topic",
        dest="topic",
        help="topic name",
        required=True,
    )
    optional = parser.add_argument_group("optional arguments")
    optional.add_argument(
        "-f",
        "--config-file",
        dest="config_file",
        help="path to a Kafka configuration file",
    )
    optional.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Kafka/Redpanda bootstrap servers override",
    )
    return parser.parse_args()


def _read_config_file(config_file):
    conf = {}
    if not config_file or not os.path.exists(config_file):
        return conf

    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                conf[parameter] = value.strip()
    return conf


def _should_reset_auth(bootstrap_servers):
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "").upper()
    if security_protocol and not security_protocol.startswith("SASL"):
        return True

    if not (bootstrap_servers or os.getenv("BOOTSTRAP_SERVERS")):
        return False

    return not any(
        os.getenv(env_name)
        for env_name in (
            "KAFKA_SASL_MECHANISMS",
            "KAFKA_SASL_USERNAME",
            "KAFKA_SASL_PASSWORD",
        )
    )


def read_ccloud_config(config_file=None, bootstrap_servers=None):
    """
    Read Kafka client configuration from file and environment.

    Redpanda is treated as the default runtime profile, so when a bootstrap
    override is provided without explicit SASL settings, old Confluent auth
    parameters are removed automatically.
    """

    conf = _read_config_file(config_file)

    if _should_reset_auth(bootstrap_servers):
        for key in _SASL_CONFIG_KEYS:
            conf.pop(key, None)

    for env_name, config_key in _ENV_TO_CONFIG_KEY.items():
        value = os.getenv(env_name)
        if value:
            conf[config_key] = value.strip()

    if bootstrap_servers:
        conf["bootstrap.servers"] = bootstrap_servers.strip()

    conf.setdefault("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS)
    return {key: value for key, value in conf.items() if value not in (None, "")}


read_kafka_config = read_ccloud_config


def pop_schema_registry_params_from_config(conf):
    """Remove potential Schema Registry related configurations from dictionary."""

    conf.pop("schema.registry.url", None)
    conf.pop("basic.auth.user.info", None)
    conf.pop("basic.auth.credentials.source", None)
    return conf


def pop_admin_client_only_params_from_config(conf):
    """Remove settings that the Kafka admin client does not use."""

    conf.pop("group.id", None)
    conf.pop("auto.offset.reset", None)
    conf.pop("session.timeout.ms", None)
    conf.pop("heartbeat.interval.ms", None)
    return conf


def get_topic_partitions(default=1):
    return int(os.getenv("KAFKA_TOPIC_PARTITIONS", str(default)))


def get_topic_replication_factor(default=1):
    return int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", str(default)))


def uses_sasl(conf):
    security_protocol = conf.get("security.protocol", "").upper()
    if security_protocol:
        return security_protocol.startswith("SASL")

    return bool(conf.get("sasl.username") and conf.get("sasl.password"))


def uses_ssl(conf):
    security_protocol = conf.get("security.protocol", "").upper()
    return security_protocol.endswith("SSL")


def create_topic(
    conf,
    topic,
    num_partitions=None,
    replication_factor=None,
    config=None,
):
    """Create a topic if needed."""

    admin_client_conf = pop_schema_registry_params_from_config(conf.copy())
    admin_client_conf = pop_admin_client_only_params_from_config(admin_client_conf)
    admin_client = AdminClient(admin_client_conf)
    topic_partitions = num_partitions or get_topic_partitions()
    topic_replication_factor = replication_factor or get_topic_replication_factor()

    topic_kwargs = {
        "num_partitions": topic_partitions,
        "replication_factor": topic_replication_factor,
    }
    if config is not None:
        topic_kwargs["config"] = config

    futures = admin_client.create_topics([NewTopic(topic, **topic_kwargs)])
    for created_topic, future in futures.items():
        try:
            future.result()
            print("Topic {} created".format(created_topic))
        except Exception as exc:
            error = exc.args[0] if exc.args else exc
            code = error.code() if hasattr(error, "code") else None
            if code == KafkaError.TOPIC_ALREADY_EXISTS:
                print("Topic {} already exists".format(created_topic))
                continue

            print("Failed to create topic {}: {}".format(created_topic, exc))
            sys.exit(1)


ensure_topic = create_topic
