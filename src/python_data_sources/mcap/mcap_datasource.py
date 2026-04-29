import json
import logging
from collections.abc import Iterator, Sequence
from pathlib import Path

from google.protobuf.json_format import MessageToDict
from mcap.reader import McapReader, make_reader
from mcap.records import Channel, Message, Schema
from mcap_protobuf.decoder import DecoderFactory
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from python_data_sources.common.range_partition import RangePartition
from python_data_sources.common.utils import get_range_partitions

logger = logging.getLogger(__name__)

DEFAULT_NUM_PARTITIONS = 4
DEFAULT_PATH_GLOB_FILTER = "*.mcap"


def path_handler(path: str, glob_pattern: str, recursive: bool = False) -> list:
    """
    Discover files matching the glob pattern in the given path.

    Args:
        path: Path to search for files
        glob_pattern: Glob pattern to match files (e.g., "*.mcap")
        recursive: If True, recursively search subdirectories using rglob

    Returns:
        List of file paths matching the pattern
    """
    path_obj = Path(path)

    if path_obj.is_file():
        return [str(path_obj)]
    if path_obj.is_dir():
        # Use rglob for recursive search, glob for non-recursive
        if recursive:
            files = sorted(path_obj.rglob(glob_pattern))
        else:
            files = sorted(path_obj.glob(glob_pattern))
        return [str(f) for f in files if f.is_file()]
    # Try glob pattern on parent directory
    parent = path_obj.parent
    if parent.exists():
        files = sorted(parent.glob(path_obj.name))
        return [str(f) for f in files if f.is_file()]
    return []


def decode_protobuf_message(message, schema, _reader):
    """Decode protobuf messages."""
    decoder_factory = DecoderFactory()
    decoder = decoder_factory.decoder_for(message.log_time, schema)
    if decoder:
        decoded_message = decoder(message.data)
        return MessageToDict(decoded_message)
    return {"raw_data": message.data.hex()}


def decode_json_message(message, _schema, _reader):
    """Decode JSON messages."""
    return json.loads(message.data.decode("utf-8"))


def decode_fallback(message, _schema, _reader):
    """Fallback decoder for unknown formats."""
    return {"raw_data": message.data.hex()}


DECODERS = {
    "protobuf": decode_protobuf_message,
    "json": decode_json_message,
    "jsonschema": decode_json_message,
    "fallback": decode_fallback,
}


def _decode_message(message: Message, schema: Schema, channel: Channel, reader: McapReader) -> tuple:
    """Decode a single MCAP message and return a row tuple."""
    enc_raw = channel.message_encoding or getattr(schema, "encoding", None)
    if not enc_raw:
        enc = "fallback"
    else:
        enc = enc_raw.lower()
        if enc not in DECODERS:
            enc = "fallback"

    decoded_fn = DECODERS.get(enc, decode_fallback)

    try:
        msg_dict = decoded_fn(message, schema, reader)
    except Exception as e:
        logger.warning(f"Error decoding message: {e}")
        msg_dict = {"error": str(e)}

    data_json = json.dumps(msg_dict)
    return (int(message.sequence), channel.topic, schema.name, enc, int(message.log_time), data_json)


def _read_mcap_file(file_path: str, topic_filter: str = None) -> Iterator[tuple]:
    """
    Read a single MCAP file and yield rows.

    Args:
        file_path: Path to the MCAP file
        topic_filter: Optional topic name to filter messages. If None or "*", read all topics.

    Yields:
        Tuples of (sequence, topic, schema, encoding, log_time, data_json)
    """
    logger.debug(f"Reading MCAP file: {file_path}, topic_filter: {topic_filter}")

    # Treat "*" as no filter
    if topic_filter == "*":
        topic_filter = None

    try:
        with open(file_path, "rb") as f:
            reader = make_reader(f)

            for schema, channel, message in reader.iter_messages():
                if topic_filter and channel.topic != topic_filter:
                    continue
                yield _decode_message(message, schema, channel, reader)
    except Exception as e:
        logger.error(f"Error reading MCAP file {file_path}: {e}")
        raise


def _read_mcap_partition(partition: RangePartition, paths: list, topic_filter: str = None) -> Iterator[tuple]:
    """
    Read MCAP files for a given partition range.

    Args:
        partition: RangePartition with start and end indices
        paths: List of file paths to process
        topic_filter: Optional topic name to filter messages. If None or "*", read all topics.

    Yields:
        Tuples of (sequence, topic, schema, encoding, log_time, data_json)
    """
    logger.debug(
        f"Processing partition: {partition}, paths subset: {paths[partition.start:partition.end]}, topic_filter: {topic_filter}"
    )

    for file_path in paths[partition.start : partition.end]:
        yield from _read_mcap_file(file_path, topic_filter=topic_filter)


class MCAPDataSourceReader(DataSourceReader):
    """
    Facilitate reading MCAP (ROS 2 bag) files.
    """

    def __init__(self, schema, options):
        logger.debug(f"MCAPDataSourceReader(schema: {schema}, options: {options})")
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.pathGlobFilter = self.options.get("pathGlobFilter", DEFAULT_PATH_GLOB_FILTER)
        self.recursiveFileLookup = self.options.get("recursiveFileLookup", "false").strip().lower() == "true"
        self.numPartitions = int(self.options.get("numPartitions", DEFAULT_NUM_PARTITIONS))
        self.topicFilter = self.options.get("topicFilter", None)

        # Treat "*" as no filter
        if self.topicFilter == "*":
            self.topicFilter = None

        if self.path is None:
            raise ValueError("No value for required option 'path'")
        self.paths = path_handler(self.path, self.pathGlobFilter, recursive=self.recursiveFileLookup)

        if not self.paths:
            logger.warning(f"No MCAP files found at path: {self.path} with filter: {self.pathGlobFilter}")

        if self.recursiveFileLookup:
            logger.info(f"Recursive file lookup enabled, found {len(self.paths)} files")

        if self.topicFilter:
            logger.info(f"Topic filter enabled: {self.topicFilter}")

    def partitions(self) -> Sequence[RangePartition]:
        """
        Compute 'splits' of the data to read.

        Returns:
            List of RangePartition objects
        """
        logger.debug(f"MCAPDataSourceReader.partitions({self.numPartitions}, {self.path}, paths: {self.paths})")

        length = len(self.paths)
        if length == 0:
            return [RangePartition(0, 0)]

        return get_range_partitions(length, self.numPartitions)

    def read(self, partition: InputPartition) -> Iterator[tuple]:
        """
        Executor level method, performs read by Range Partition.

        Args:
            partition: The partition to read

        Returns:
            Iterator of tuples (sequence, topic, schema, encoding, log_time, data_json)
        """
        logger.debug(
            f"MCAPDataSourceReader.read({partition}, {self.path}, paths: {self.paths}, topicFilter: {self.topicFilter})"
        )

        if self.path is None:
            raise ValueError(f"Missing path '{self.path}'")
        if self.paths is None:
            raise ValueError(f"Missing paths '{self.paths}'")

        # Library imports must be within the method for executor-level execution
        return _read_mcap_partition(partition, self.paths, topic_filter=self.topicFilter)


class MCAPDataSource(DataSource):
    """
    A data source for batch query over MCAP (ROS 2 bag) files.
    
    Usage:
        # Read all topics
        df = spark.read.format("mcap").option("path", "/path/to/mcap/files").load()
        
        # Filter by specific topic at read time (more efficient than DataFrame filter)
        df = spark.read.format("mcap") \
            .option("path", "/path/to/mcap/files") \
            .option("topicFilter", "pose") \
            .load()
        
    Options:
        - path: Path to MCAP file(s) or directory (required)
        - pathGlobFilter: Glob pattern for file matching (default: "*.mcap")
        - numPartitions: Number of partitions to split files across (default: 4)
        - recursiveFileLookup: Recursively search subdirectories (default: false)
        - topicFilter: Filter messages by topic name (optional). Use "*" or omit to read all topics.
    
    Schema:
        - sequence: BIGINT - The message sequence number from MCAP
        - topic: STRING - The message topic
        - schema: STRING - The schema name
        - encoding: STRING - The encoding type (protobuf, json, etc.)
        - log_time: BIGINT - The message timestamp in nanoseconds
        - data: STRING - JSON string containing all message fields
    """

    @classmethod
    def name(cls):
        datasource_type = "mcap"
        logger.debug(f"MCAPDataSource.name({datasource_type})")
        return datasource_type

    def schema(self):
        schema = "sequence BIGINT, topic STRING, schema STRING, encoding STRING, log_time BIGINT, data STRING"
        logger.debug(f"MCAPDataSource.schema({schema})")
        return schema

    def reader(self, schema: StructType):
        logger.debug(f"MCAPDataSource.reader({schema}, options={self.options})")
        return MCAPDataSourceReader(schema, self.options)
