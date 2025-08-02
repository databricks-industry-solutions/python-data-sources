
![Zipped DiCOMS](../media/zip-dcm-blue.png)
## Introduction
Read DICOM file metadata from a zip file archive.

```python
from dbx.zip_dcm_ds import ZipDCMDataSource
spark.dataSource.register(ZipDCMDataSource)

# read DCMs with `numPartitions` parallelism.
df = spark.read.format("zipdcm").option('numPartitions',4).load("./resources")
df.display()
```
For more, see our [demo]($./demo) notebook.

## Install


Create a git folder in Databricks or Download
```bash
git clone https://github.com/databricks-industry-solutions/python-data-sources.git 
```

Create and activate python virtual environment
```bash
pyenv venv python-data-sources
pyenv activate python-data-sources
```

Install development dependencies

```bash
cd zipdcm
make dev
```

Run unit tests
```bash
make test
```
