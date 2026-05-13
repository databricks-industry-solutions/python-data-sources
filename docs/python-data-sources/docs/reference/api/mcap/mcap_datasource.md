---
sidebar_label: mcap_datasource
title: python_data_sources.mcap.mcap_datasource
---

### path\_handler

```python
def path_handler(path: str,
                 glob_pattern: str,
                 recursive: bool = False) -> list
```

Discover files matching the glob pattern in the given path.

**Arguments**:

- `path` - Path to search for files
- `glob_pattern` - Glob pattern to match files (e.g., &quot;*.mcap&quot;)
- `recursive` - If True, recursively search subdirectories using rglob
  

**Returns**:

  List of file paths matching the pattern

### decode\_protobuf\_message

```python
def decode_protobuf_message(message, schema, _reader)
```

Decode protobuf messages.

### decode\_json\_message

```python
def decode_json_message(message, _schema, _reader)
```

Decode JSON messages.

### decode\_fallback

```python
def decode_fallback(message, _schema, _reader)
```

Fallback decoder for unknown formats.

## MCAPDataSourceReader Objects

```python
class MCAPDataSourceReader(DataSourceReader)
```

Facilitate reading MCAP (ROS 2 bag) files.

### partitions

```python
def partitions() -> Sequence[RangePartition]
```

Compute &#x27;splits&#x27; of the data to read.

**Returns**:

  List of RangePartition objects

### read

```python
def read(partition: InputPartition) -> Iterator[tuple]
```

Executor level method, performs read by Range Partition.

**Arguments**:

- `partition` - The partition to read
  

**Returns**:

  Iterator of tuples (sequence, topic, schema, encoding, log_time, data_json)

## MCAPDataSource Objects

```python
class MCAPDataSource(DataSource)
```

A data source for batch query over MCAP (ROS 2 bag) files.

Usage:
    # Read all topics
    df = spark.read.format(&quot;mcap&quot;).option(&quot;path&quot;, &quot;/path/to/mcap/files&quot;).load()

    # Filter by specific topic at read time (more efficient than DataFrame filter)
    df = spark.read.format(&quot;mcap&quot;)             .option(&quot;path&quot;, &quot;/path/to/mcap/files&quot;)             .option(&quot;topicFilter&quot;, &quot;pose&quot;)             .load()

Options:
    - path: Path to MCAP file(s) or directory (required)
    - pathGlobFilter: Glob pattern for file matching (default: &quot;*.mcap&quot;)
    - numPartitions: Number of partitions to split files across (default: 4)
    - recursiveFileLookup: Recursively search subdirectories (default: false)
    - topicFilter: Filter messages by topic name (optional). Use &quot;*&quot; or omit to read all topics.

Schema:
    - sequence: BIGINT - The message sequence number from MCAP
    - topic: STRING - The message topic
    - schema: STRING - The schema name
    - encoding: STRING - The encoding type (protobuf, json, etc.)
    - log_time: BIGINT - The message timestamp in nanoseconds
    - data: STRING - JSON string containing all message fields

