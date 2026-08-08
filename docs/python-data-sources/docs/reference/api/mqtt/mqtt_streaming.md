---
sidebar_label: mqtt_streaming
title: python_data_sources.mqtt.mqtt_streaming
---

## MqttDataSource Objects

```python
class MqttDataSource(DataSource)
```

A PySpark DataSource for reading MQTT messages from a broker.

This data source allows you to stream MQTT messages into Spark DataFrames,
supporting various MQTT broker configurations including authentication,
SSL/TLS encryption, and different quality of service levels.

Input validation is performed on critical parameters to ensure connection reliability.

Supported options:
- broker_address: MQTT broker hostname or IP address (required, validated)
* Must be a valid hostname or IP address format
* Cannot be None, empty, or whitespace-only
- port: Broker port number (default: 8883, validated)
* Must be an integer in range 1-65535
- username: Authentication username (optional)
- password: Authentication password (optional)
- topic: MQTT topic to subscribe to (default: &quot;#&quot; for all topics)
- qos: Quality of Service level 0-2 (default: 0, validated)
* Must be 0, 1, or 2 (standard MQTT QoS levels)
- require_tls: Enable SSL/TLS encryption (default: true)
- keepalive: Keep alive interval in seconds (default: 60)

Example usage:
spark.readStream.format(&quot;mqtt_pub_sub&quot;)
.option(&quot;broker_address&quot;, &quot;mqtt.example.com&quot;)
.option(&quot;topic&quot;, &quot;sensors/+/temperature&quot;)
.option(&quot;username&quot;, &quot;user&quot;)
.option(&quot;password&quot;, &quot;pass&quot;)
.load()

**Raises**:

- `ValueError` - If broker_address, port, clean_session, or qos parameters are invalid.

### name

```python
@classmethod
def name(cls)
```

Returns the name of the data source.

### \_\_init\_\_

```python
def __init__(options)
```

Initialize the MQTT data source with configuration options.

**Arguments**:

- `options` _dict_ - Configuration options for the MQTT connection.
  See class docstring for supported options.

### schema

```python
def schema()
```

Define the schema of the data source.

**Returns**:

- `StructType` - The schema of the data source.

### streamReader

```python
def streamReader(schema: StructType)
```

Create and return a stream reader for MQTT data.

**Arguments**:

- `schema` _StructType_ - The schema for the streaming data.
  

**Returns**:

- `MqttSimpleStreamReader` - A stream reader instance configured for MQTT.

## MqttSimpleStreamReader Objects

```python
class MqttSimpleStreamReader(SimpleDataSourceStreamReader)
```

### \_\_init\_\_

```python
def __init__(_schema, options)
```

Initialize the MQTT simple stream reader with configuration options.

**Arguments**:

- `_schema` _StructType_ - The schema for the streaming data.
- `options` _dict_ - Configuration options for the MQTT connection.
  See class docstring for supported options.

### latestOffset

```python
def latestOffset() -> dict
```

Returns the current latest offset that the next microbatch will read to.

### partitions

```python
def partitions(start: dict, end: dict)
```

Plans the partitioning of the current microbatch defined by start and end offset. It
needs to return a sequence of :class:`InputPartition` objects.

### read

```python
def read(_)
```

Read MQTT messages from the broker.

**Returns**:

- `Iterator[list]` - An iterator of lists containing the MQTT message data.
  The list contains the following elements:
  - received_time: The time the message was received.
  - topic: The topic of the message.
  - message: The payload of the message.
  - is_duplicate: Whether the message is a duplicate.
  - qos: The quality of service level of the message.
  - is_retained: Whether the message is retained.
  

**Raises**:

- `Exception` - If the connection to the broker fails.

