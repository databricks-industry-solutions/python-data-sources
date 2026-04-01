import datetime
import ipaddress
import logging
import random
import re
import time

import paho.mqtt.client as mqttClient
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import StringType, StructField, StructType
from python_data_sources.common.range_partition import RangePartition

logger = logging.getLogger(__name__)


class MqttDataSource(DataSource):
    """
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
    - topic: MQTT topic to subscribe to (default: "#" for all topics)
    - qos: Quality of Service level 0-2 (default: 0, validated)
        * Must be 0, 1, or 2 (standard MQTT QoS levels)
    - require_tls: Enable SSL/TLS encryption (default: true)
    - keepalive: Keep alive interval in seconds (default: 60)

    Example usage:
        spark.readStream.format("mqtt_pub_sub")
            .option("broker_address", "mqtt.example.com")
            .option("topic", "sensors/+/temperature")
            .option("username", "user")
            .option("password", "pass")
            .load()

    Raises:
        ValueError: If broker_address, port, clean_session, or qos parameters are invalid.
    """

    @classmethod
    def name(cls):
        """Returns the name of the data source."""
        return "mqtt_pub_sub"

    def __init__(self, options):
        """
        Initialize the MQTT data source with configuration options.

        Args:
            options (dict): Configuration options for the MQTT connection.
                          See class docstring for supported options.
        """
        self.options = options

    def schema(self):
        """
        Define the schema of the data source.

        Returns:
            StructType: The schema of the data source.
        """
        return StructType(
            [
                StructField("received_time", StringType(), True),
                StructField("topic", StringType(), True),
                StructField("message", StringType(), True),
                StructField("is_duplicate", StringType(), True),
                StructField("qos", StringType(), True),
                StructField("is_retained", StringType(), True),
            ]
        )

    def streamReader(self, schema: StructType):
        """
        Create and return a stream reader for MQTT data.

        Args:
            schema (StructType): The schema for the streaming data.

        Returns:
            MqttSimpleStreamReader: A stream reader instance configured for MQTT.
        """
        return MqttSimpleStreamReader(schema, self.options)


class MqttSimpleStreamReader(SimpleDataSourceStreamReader):

    def __init__(self, _schema, options):
        """
        Initialize the MQTT simple stream reader with configuration options.

        Args:
            _schema (StructType): The schema for the streaming data.
            options (dict): Configuration options for the MQTT connection.
                          See class docstring for supported options.
        """
        super().__init__()
        self.topic = self._parse_topic(options.get("topic", "#"))
        self.broker_address = options.get("broker_address")
        self.require_tls = options.get("require_tls", True)
        self.port = int(options.get("port", 8883))
        self.username = options.get("username", "")
        self.password = options.get("password", "")
        self.qos = int(options.get("qos", 2))
        self.keep_alive = int(options.get("keepalive", 60))
        self.clean_session = options.get("clean_session", False)
        self.conn_timeout = int(options.get("conn_time", 1))
        self.tls_certs = {
            "ca_certs": options.get("ca_certs", None),
            "certfile": options.get("certfile", None),
            "keyfile": options.get("keyfile", None),
            "disable_certs": options.get("tls_disable_certs", None),
        }

        # Validate all input parameters
        self._validate_input_parameters()

        if self.clean_session not in [True, False]:
            raise ValueError(f"Unsupported session: {self.clean_session}")
        self.client_id = f'spark-data-source-mqtt-{random.randint(0, 1000000)}'
        self.current = 0
        self.new_data = []

    def _validate_input_parameters(self):
        """
        Validate all input parameters for the MQTT connection.

        Raises:
            ValueError: If any parameter is invalid.
        """
        # Validate broker address
        self._validate_broker_address()

        # Validate port range
        self._validate_port()

        # Validate QoS level
        self._validate_qos()

    def _validate_broker_address(self):
        """
        Validate that the broker address is provided and properly formatted.

        Raises:
            ValueError: If broker address is None, empty, or improperly formatted.
        """
        if not self.broker_address:
            raise ValueError("broker_address is required and cannot be None or empty")

        if not isinstance(self.broker_address, str):
            raise ValueError("broker_address must be a string")

        self.broker_address = self.broker_address.strip()
        if not self.broker_address:
            raise ValueError("broker_address cannot be empty or just whitespace")

        # Check if it's a valid IP address
        try:
            ipaddress.ip_address(self.broker_address)
            return  # Valid IP address
        except ValueError:
            pass  # Not an IP address, check if it's a valid hostname

        # Validate hostname format
        if not self._is_valid_hostname(self.broker_address):
            raise ValueError(f"broker_address '{self.broker_address}' is not a valid hostname or IP address")

    def _is_valid_hostname(self, hostname):
        """
        Check if a string is a valid hostname according to RFC standards.

        Args:
            hostname (str): The hostname to validate.

        Returns:
            bool: True if valid hostname, False otherwise.
        """
        if len(hostname) > 253:
            return False

        # Remove trailing dot if present
        if hostname.endswith('.'):
            hostname = hostname[:-1]

        # Hostname regex pattern
        # Allows letters, numbers, hyphens, and dots
        # Must start and end with alphanumeric characters
        hostname_pattern = re.compile(r'^(?!-)(?:[a-zA-Z0-9-]{1,63}(?<!-)\.)*[a-zA-Z0-9-]{1,63}(?<!-)$')

        return bool(hostname_pattern.match(hostname))

    def _validate_port(self):
        """
        Validate that the port is within the valid range (1-65535).

        Raises:
            ValueError: If port is not in the valid range.
        """
        if not isinstance(self.port, int):
            raise ValueError(f"port must be an integer, got {type(self.port).__name__}")

        if self.port < 1 or self.port > 65535:
            raise ValueError(f"port must be in range 1-65535, got {self.port}")

    def _validate_qos(self):
        """
        Validate that the QoS level is one of the valid MQTT QoS values (0, 1, or 2).

        Raises:
            ValueError: If QoS is not 0, 1, or 2.
        """
        if not isinstance(self.qos, int):
            raise ValueError(f"qos must be an integer, got {type(self.qos).__name__}")

        valid_qos_levels = [0, 1, 2]
        if self.qos not in valid_qos_levels:
            raise ValueError(f"qos must be one of {valid_qos_levels}, got {self.qos}")

    def _parse_topic(self, topic_str: str):
        """
        TODO: add docs, implement parsing of topic string
        """
        return topic_str

    def _configure_tls(self, client):
        """
        Configure TLS settings on the MQTT client based on provided certificate options.
        """
        if self.require_tls:
            # Build tls_set arguments based on provided certificates
            tls_args = {k: v for k, v in self.tls_certs.items() if k != "disable_certs" and v}

            # Call tls_set with the appropriate parameters
            if tls_args:
                client.tls_set(**tls_args)
                logger.info(f"TLS configured with certificates: {list(tls_args.keys())}")
            else:
                # Basic TLS without custom certificates
                client.tls_set()
                logger.info("Basic TLS enabled")
        else:
            logger.info("TLS disabled")

    def initialOffset(self):
        return {"offset": 0}

    def latestOffset(self) -> dict:
        """
        Returns the current latest offset that the next microbatch will read to.
        """
        self.current += 1
        return {"offset": self.current}

    def partitions(self, start: dict, end: dict):
        """
        Plans the partitioning of the current microbatch defined by start and end offset. It
        needs to return a sequence of :class:`InputPartition` objects.
        """
        return [RangePartition(start["offset"], end["offset"])]

    def read(self, _):
        """
        Read MQTT messages from the broker.

        Returns:
            Iterator[list]: An iterator of lists containing the MQTT message data.
            The list contains the following elements:
            - received_time: The time the message was received.
            - topic: The topic of the message.
            - message: The payload of the message.
            - is_duplicate: Whether the message is a duplicate.
            - qos: The quality of service level of the message.
            - is_retained: Whether the message is retained.

        Raises:
            Exception: If the connection to the broker fails.
        """

        def _get_mqtt_client():
            return mqttClient.Client(
                mqttClient.CallbackAPIVersion.VERSION1, self.client_id, clean_session=self.clean_session
            )

        mqtt_client = _get_mqtt_client()

        # Configure TLS with certificates if provided
        self._configure_tls(mqtt_client)

        mqtt_client.username_pw_set(self.username, self.password)

        def on_connect(client, _userdata, _flags, rc):
            if rc == 0:
                client.subscribe(self.topic, qos=self.qos)
                logger.info(f"Connected to broker {self.broker_address} on port {self.port} with topic {self.topic}")
            else:
                logger.error(
                    f"Connection failed to broker {self.broker_address} on port {self.port} with topic {self.topic}"
                )

        def on_message(_client, _userdata, message):
            msg_data = [
                str(datetime.datetime.now()),
                message.topic,
                str(message.payload.decode("utf-8", "ignore")),
                message.dup,
                message.qos,
                message.retain,
            ]
            logger.info("Read message payload from broker")
            self.new_data.append(msg_data)

        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message

        try:
            mqtt_client.connect(self.broker_address, self.port, self.keep_alive)
        except Exception as e:
            connection_context = {
                "broker_address": self.broker_address,
                "port": self.port,
                "topic": self.topic,
                "client_id": self.client_id,
                "require_tls": self.require_tls,
                "keepalive": self.keep_alive,
                "qos": self.qos,
                "clean_session": self.clean_session,
                "conn_timeout": self.conn_timeout,
            }

            error_msg = f"Failed to connect to MQTT broker. Connection details: {connection_context}"
            logger.exception(error_msg)

            # Re-raise with enhanced context
            raise ConnectionError(error_msg) from e
        mqtt_client.loop_start()  # Use loop_start to run the loop in a separate thread

        time.sleep(self.conn_timeout)  # Wait for messages for the specified timeout

        mqtt_client.loop_stop()  # Stop the loop after the timeout
        mqtt_client.disconnect()

        return iter(self.new_data)


class MqttSimpleStreamWriter:
    # To be implemented
    def __init__(self, schema, options):
        pass
