"""
Test suite for the MQTT streaming data source.

Note: These tests require an MQTT broker to be running.
Set environment variables for broker configuration:
- MQTT_LOCAL_BROKER_HOST
- MQTT_LOCAL_BROKER_PORT
- MQTT_LOCAL_USERNAME
- MQTT_LOCAL_PASSWORD
"""

import datetime
import os
import ssl
import time

import pytest
from paho.mqtt import client as mqtt

from python_data_sources.mqtt import MqttDataSource


@pytest.fixture
def mqtt_client(mqtt_config):
    """Create an MQTT client for publishing test messages."""
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)

    if mqtt_config["username"] and mqtt_config["password"]:
        client.username_pw_set(username=mqtt_config["username"], password=mqtt_config["password"])

    try:
        client.connect(mqtt_config["host"], mqtt_config["port"], 60)
        client.loop_start()
        yield client
        client.loop_stop()
        client.disconnect()
    except Exception:
        pytest.skip("MQTT broker not available")


@pytest.fixture
def mqtt_remote_client(mqtt_server_config):
    """Create an MQTT client for remote broker."""
    if not mqtt_server_config["host"]:
        pytest.skip("Remote MQTT broker not configured")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)

    if mqtt_server_config["username"] and mqtt_server_config["password"]:
        client.username_pw_set(username=mqtt_server_config["username"], password=mqtt_server_config["password"])

    try:
        client.connect(mqtt_server_config["host"], mqtt_server_config["port"], 60)
        ssl_settings = ssl.SSLContext(ssl.PROTOCOL_TLS)
        client.tls_set_context(ssl_settings)
        client.loop_start()
        yield client
        client.loop_stop()
        client.disconnect()
    except Exception:
        pytest.skip("Remote MQTT broker not available")


@pytest.mark.skipif(not os.getenv("MQTT_LOCAL_BROKER_HOST"), reason="MQTT_LOCAL_BROKER_HOST not set")
def test_mqtt_local_read_stream(spark, mqtt_client, mqtt_config):
    """Test reading from a local MQTT broker."""
    spark.dataSource.register(MqttDataSource)

    # Prepare Test Messages
    test_messages = [
        (mqtt_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=10", 2, False),
        (mqtt_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=11", 2, False),
        (mqtt_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=12", 2, False),
        (mqtt_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=13", 2, False),
        (mqtt_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=14", 2, False),
    ]

    # Start the streaming query
    query = (
        spark.readStream.format("mqtt_pub_sub")
        .option("broker_address", mqtt_config["host"])
        .option("username", mqtt_config["username"])
        .option("port", mqtt_config["port"])
        .option("password", mqtt_config["password"])
        .option("topic", mqtt_config["topic_prefix"])
        .option("qos", 2)
        .option("require_tls", False)
        .load()
        .writeStream.format("memory")
        .queryName("mqtt_results")
        .start()
    )

    time.sleep(5)

    # Publish the test messages
    for topic, payload, _qos, _is_persisted in test_messages:
        mqtt_client.publish(topic, payload, qos=2)

    time.sleep(10)

    # Assert Results
    results = spark.sql("select * from mqtt_results").collect()
    query.stop()

    assert len(results) == len(test_messages)
    received = {(row.topic, row.message) for row in results}
    expected = set((item[0], item[1]) for item in test_messages)
    assert received == expected


@pytest.mark.skipif(not os.getenv("MQTT_REMOTE_BROKER_HOST"), reason="MQTT_REMOTE_BROKER_HOST not set")
def test_hivemq_read_stream(spark, mqtt_remote_client, mqtt_server_config):
    """
    Test reading from a remote MQTT broker (e.g., HiveMQ).

    Uses the "availableNow" trigger which pulls whatever is present in the MQTT topic.
    MQTT retains only one message per topic, so we expect only the last message.
    """
    spark.dataSource.register(MqttDataSource)

    # Prepare the Test Messages
    test_messages = [
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=0", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=1", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=2", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=3", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=4", 2, False),
    ]

    # Publish the test messages with retain=True
    for topic, payload, _qos, _is_persisted in test_messages:
        mqtt_remote_client.publish(topic, payload, qos=2, retain=True)

    time.sleep(5)

    # Start the streaming query
    query = (
        spark.readStream.format("mqtt_pub_sub")
        .option("broker_address", mqtt_server_config["host"])
        .option("username", mqtt_server_config["username"])
        .option("port", mqtt_server_config["port"])
        .option("password", mqtt_server_config["password"])
        .option("topic", mqtt_server_config["topic_prefix"])
        .option("qos", 2)
        .option("require_tls", True)
        .load()
        .writeStream.format("memory")
        .trigger(availableNow=True)
        .queryName("mqtt_results")
        .start()
    )
    query.awaitTermination()

    # Assert Results
    results = spark.sql("select * from mqtt_results").collect()

    # Since we're testing retained messages, expect only the last one
    assert len(results) == 1
    received = {(row.topic, row.message) for row in results}
    expected = set((item[0], item[1]) for item in test_messages[-1:])
    assert received == expected
