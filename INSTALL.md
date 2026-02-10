## Installation Guidelines

### Option 1: Install via pip

Install the package with the data sources you need:

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

### Option 2: Install in Databricks

You can install the package directly in a Databricks notebook:

```python
%pip install python-data-sources[all]
```

Or add it to your cluster's library configuration.

### Option 3: Deploy via Databricks Asset Bundles

1. Clone the project you'd like to run into your Databricks Workspace

<img width="1726" height="677" alt="Screenshot 2025-07-23 at 11 05 25 AM" src="https://github.com/user-attachments/assets/55b1729f-ad07-420e-a271-843266abfb71" />

2. Open the Asset Bundle Editor in the Databricks UI

<img width="1120" height="665" alt="Screenshot 2025-07-23 at 11 06 12 AM" src="https://github.com/user-attachments/assets/d1f91256-eb8f-4456-8d88-c0a37b1bd4c5" />

3. Click on "Deploy"

<img width="1523" height="902" alt="Screenshot 2025-07-23 at 11 09 37 AM" src="https://github.com/user-attachments/assets/9564cbdd-c5c5-4210-bf27-2b19e6efc85b" />

4. Navigate to the Deployments tab in the Asset Bundle UI (🚀 icon) and click "Run" on the job available. This will run the notebooks from this project sequentially.

<img width="1527" height="880" alt="Screenshot 2025-07-23 at 11 10 13 AM" src="https://github.com/user-attachments/assets/0f612882-7123-449b-8349-1835bc59523c" />

## Usage Examples

After installation, register and use the data sources:

### MCAP

```python
from pyspark.sql import SparkSession
from src.python_data_sources.mcap import MCAPDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(MCAPDataSource)

df = spark.read.format("mcap")
    .option("path", "/path/to/data.mcap")
    .option("numPartitions", "4")
    .load()
```

### MQTT

```python
from pyspark.sql import SparkSession
from python_data_sources.mqtt import MqttDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(MqttDataSource)

df = spark.readStream.format("mqtt_pub_sub") \
    .option("broker_address", "mqtt.example.com") \
    .option("topic", "sensors/#") \
    .option("username", "user") \
    .option("password", "pass") \
    .load()
```

### ZipDCM

```python
from pyspark.sql import SparkSession
from python_data_sources.zipdcm import ZipDCMDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(ZipDCMDataSource)

df = spark.read.format("zipdcm") \
    .option("numPartitions", "2") \
    .load("/path/to/dicom_files.zip")
```
