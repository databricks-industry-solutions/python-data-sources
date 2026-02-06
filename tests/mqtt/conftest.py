"""
MQTT-specific pytest fixtures.
"""

import os
import pytest
from pyspark.sql import SparkSession

from python_data_sources.mqtt import MqttDataSource


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """
    Create a SparkSession with MQTT data source registered.
    """
    spark_session = SparkSession.builder.master("local[2]").appName("mqtt-tests").getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    spark_session.dataSource.register(MqttDataSource)
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def mqtt_config():
    """Configuration for local MQTT broker."""
    return {
        "host": os.getenv("MQTT_LOCAL_BROKER_HOST", "localhost"),
        "port": int(os.getenv("MQTT_LOCAL_BROKER_PORT", 1883)),
        "username": os.getenv("MQTT_LOCAL_USERNAME", "root"),
        "password": os.getenv("MQTT_LOCAL_PASSWORD", "<PASSWORD>"),
        "topic_prefix": os.getenv("MQTT_LOCAL_BROKER_TOPIC_PREFIX", "test/pyspark"),
    }


@pytest.fixture(scope="module")
def mqtt_server_config():
    """Configuration for remote MQTT broker (e.g., HiveMQ)."""
    return {
        "host": os.getenv("MQTT_REMOTE_BROKER_HOST", ""),
        "port": int(os.getenv("MQTT_REMOTE_BROKER_PORT", 883)),
        "username": os.getenv("MQTT_REMOTE_USERNAME", ""),
        "password": os.getenv("MQTT_REMOTE_PASSWORD", "<PASSWORD>"),
        "topic_prefix": os.getenv("MQTT_REMOTE_BROKER_TOPIC_PREFIX", "test/pyspark"),
    }
