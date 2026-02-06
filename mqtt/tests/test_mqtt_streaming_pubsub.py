import ssl

import pytest
import os

from pyspark.sql import SparkSession
from paho.mqtt import client as mqtt
import datetime
import time

from python_datasource_connectors import MqttDataSource


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    yield spark


@pytest.fixture(scope="module")
def mqtt_config():
    return {
        "host": os.getenv("MQTT_LOCAL_BROKER_HOST", "localhost"),
        "port": int(os.getenv("MQTT_LOCAL_BROKER_PORT", 1883)),
        "username": os.getenv("MQTT_LOCAL_USERNAME", "root"),
        "password": os.getenv("MQTT_LOCAL_PASSWORD", "<PASSWORD>"),
        "topic_prefix": os.getenv("MQTT_LOCAL_BROKER_TOPIC_PREFIX", "test/pyspark"),
    }


@pytest.fixture(scope="module")
def mqtt_server_config():
    return {
        "host": os.getenv("MQTT_REMOTE_BROKER_HOST", ""),
        "port": int(os.getenv("MQTT_REMOTE_BROKER_PORT", 883)),
        "username": os.getenv("MQTT_REMOTE_USERNAME", ""),
        "password": os.getenv("MQTT_REMOTE_PASSWORD", "<PASSWORD>"),
        "topic_prefix": os.getenv("MQTT_REMOTE_BROKER_TOPIC_PREFIX", "test/pyspark"),
    }


@pytest.fixture
def mqtt_remote_client(mqtt_server_config):
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)

    if mqtt_server_config["username"] and mqtt_server_config["password"]:
        client.username_pw_set(username=mqtt_server_config["username"], password=mqtt_server_config["password"])

    client.connect(mqtt_server_config["host"], mqtt_server_config["port"], 60)
    sslSettings = ssl.SSLContext(ssl.PROTOCOL_TLS)
    client.tls_set_context(sslSettings)
    client.loop_start()
    yield client
    client.loop_stop()
    client.disconnect()


@pytest.fixture
def mqtt_client(mqtt_config):
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)

    if mqtt_config["username"] and mqtt_config["password"]:
        client.username_pw_set(username=mqtt_config["username"], password=mqtt_config["password"])

    client.connect(mqtt_config["host"], mqtt_config["port"], 60)
    client.loop_start()
    yield client
    client.loop_stop()
    client.disconnect()


def test_hivemq_read_stream(spark, mqtt_remote_client, mqtt_server_config):
    """
    This test implements a slightly different logic than the local one. Here we use the "availableNow" trigger,
    which will pull whatever is present in the MQTT topic when the streaming query starts.
    MQTT will retain only one message per topic. So we send the four messages first, then set up the streaming
    query, then expect only the last message to be pulled by our connector.
    """
    spark.dataSource.register(MqttDataSource)
    # Prepare the Test Messages for HiveMQ Remote Service
    test_messages = [
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=0", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=1", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=2", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=3", 2, False),
        (mqtt_server_config["topic_prefix"], f"test message at {str(datetime.datetime.now())} and value=4", 2, False),
    ]

    # Publish the test messages to remote server
    # Let's retain them so that this new subscriber can catch them
    for topic, payload, qos, is_persisted in test_messages:
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
    # No need to stop the query anymore, since we're using the availableNow trigger
    # Assert Results
    results = spark.sql("select * from mqtt_results").collect()
    # Since we're testing retained messages, we expect only the last one to be pulled by our connector
    assert len(results) == 1
    received = {(row.topic, row.message) for row in results}
    # Pull only the topic and the payload to perform the assertion, compare against last message sent
    expected = set((item[0], item[1]) for item in test_messages[-1:])
    assert received == expected


def test_mqtt_local_read_stream(spark, mqtt_client, mqtt_config):
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
    for topic, payload, qos, is_persisted in test_messages:
        mqtt_client.publish(topic, payload, qos=2)

    time.sleep(10)

    # Assert Results
    results = spark.sql("select * from mqtt_results").collect()
    query.stop()

    assert len(results) == len(test_messages)
    received = {(row.topic, row.message) for row in results}
    # Pull only the topic and the payload to perform the assertion
    expected = set((item[0], item[1]) for item in test_messages)
    assert received == expected
