
[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)
# Databricks Python Data Sources

Introduced in Spark 4.x, Python Data Source API allows you to create PySpark Data Sources leveraging long standing python libraries for handling unique file types or specialized interfaces with spark read, readStream, write and writeStream APIs.

| Data Source Name | Purpose |
| --- | --- |
| [mcap](mcap/README.md) | Read MCAP (ROS 2 bag) files |
| [mqtt](mqtt/README.md) | Stream data from MQTT brokers |
| [zipdcm](zipdcm/README.md) | Read DICOM files from Zip file archives |

## Installation

Install the base package:

```bash
pip install python-data-sources
```

Install with specific data source support:

```bash
# Install with MCAP support
pip install python-data-sources[mcap]

# Install with MQTT support
pip install python-data-sources[mqtt]

# Install with ZipDCM support
pip install python-data-sources[zipdcm]

# Install with all data sources
pip install python-data-sources[all]
```

## Quick Start

### MCAP Data Source

```python
from pyspark.sql import SparkSession
from python_data_sources.mcap import MCAPDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(MCAPDataSource)

df = spark.read.format("mcap") \
    .option("path", "/path/to/data.mcap") \
    .load()
```

### MQTT Streaming Data Source

```python
from pyspark.sql import SparkSession
from python_data_sources.mqtt import MqttDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(MqttDataSource)

df = spark.readStream.format("mqtt_pub_sub") \
    .option("broker_address", "mqtt.example.com") \
    .option("topic", "sensors/#") \
    .load()
```

### ZipDCM Data Source

```python
from pyspark.sql import SparkSession
from python_data_sources.zipdcm import ZipDCMDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(ZipDCMDataSource)

df = spark.read.format("zipdcm") \
    .load("/path/to/dicom_files.zip")
```

## Development

This project uses [Hatch](https://hatch.pypa.io/) for build and environment management.

### Setup

```bash
# Install hatch
pip install hatch

# Create development environment
hatch env create
```

### Running Tests

```bash
# Run tests for a specific submodule
hatch run test-mcap:test
hatch run test-mqtt:test
hatch run test-zipdcm:test

# Run tests with coverage
hatch run test-mcap:cov
hatch run test-mqtt:cov
hatch run test-zipdcm:cov
```

### Project Structure

```
python-data-sources/
├── pyproject.toml              # Unified build configuration
├── src/
│   └── python_data_sources/    # Main package
│       ├── mcap/               # MCAP data source
│       ├── mqtt/               # MQTT streaming data source
│       └── zipdcm/             # ZipDCM data source
├── tests/
│   ├── mcap/                   # MCAP tests
│   ├── mqtt/                   # MQTT tests
│   └── zipdcm/                 # ZipDCM tests
└── .github/workflows/
    └── test.yml                # CI/CD workflow
```

## Documentation

Refer to the [python-data-sources](https://databricks-industry-solutions.github.io/python-data-sources/) documentation for detailed information on how to use supplied python data sources, its features, and configuration options.

## Contributing

1. **git clone** this project locally
2. Install development dependencies: `pip install hatch`
3. Run tests to verify setup: `hatch run test-all:test`
4. Make your changes
5. Run tests for affected submodules
6. Contribute with pull requests (PRs), ensuring that you always have a second-party review from a capable teammate


## 📄 Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Datasource | Package    | Purpose                           | License     | Source                               |
| ---------- | ---------- | --------------------------------- | ----------- | ------------------------------------ |
| mcap       | mcap       | Python API for MCAP files         | MIT         | https://github.com/foxglove/mcap     |
| mcap       | protobuf   | Protocol Buffers support          | BSD-3       | https://github.com/protocolbuffers/protobuf |
| mqtt       | paho-mqtt  | MQTT client library               | EPL-2.0     | https://github.com/eclipse/paho.mqtt.python |
| zipdcm     | pydicom    | Python API for DICOM files        | MIT         | https://github.com/pydicom/pydicom   |
| zipdcm     | pylibjpeg  | Decoding / Encoding pixel formats | GPLv3 & MIT | https://github.com/pydicom/pylibjpeg |
