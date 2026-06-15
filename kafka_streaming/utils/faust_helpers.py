from pathlib import Path
import os
import ssl

import faust

from utils import ccloud_lib


def build_broker_credentials(config):
    if not ccloud_lib.uses_sasl(config):
        return None

    ssl_context = ssl.create_default_context() if ccloud_lib.uses_ssl(config) else None
    return faust.SASLCredentials(
        username=config["sasl.username"],
        password=config["sasl.password"],
        mechanism=config.get("sasl.mechanisms", "PLAIN"),
        ssl_context=ssl_context,
    )


def load_kafka_config(config_file: Path):
    return ccloud_lib.read_ccloud_config(str(config_file))


def resolve_store():
    configured_store = os.getenv("FAUST_STORE")
    if configured_store:
        return configured_store

    try:
        import rocksdb  # noqa: F401
    except Exception:
        return "memory://"

    return "rocksdb://"


def build_app_kwargs(config):
    app_kwargs = {
        "topic_replication_factor": ccloud_lib.get_topic_replication_factor(),
        "topic_partitions": ccloud_lib.get_topic_partitions(),
        "topic_allow_declare": False,
        "topic_disable_leader": True,
        "broker": f"kafka://{config['bootstrap.servers']}",
        "value_serializer": "json",
        "store": resolve_store(),
    }
    broker_credentials = build_broker_credentials(config)
    if broker_credentials is not None:
        app_kwargs["broker_credentials"] = broker_credentials
    return app_kwargs


def ensure_faust_topics(config, app_id, source_topics, table_names):
    for topic_name in source_topics:
        ccloud_lib.ensure_topic(config, topic_name)

    changelog_config = {"cleanup.policy": "compact"}
    for table_name in table_names:
        ccloud_lib.ensure_topic(
            config,
            f"{app_id}-{table_name}-changelog",
            config=changelog_config,
        )
