
## Introduction
Read DICOM file metadata from a zip file archive.

```python
df = spark.read.format("zipdcm").load(...)
```


## Install


Download
```bash
git clone https://github.com/databricks-industry-solutions/python-data-sources.git 
```

Create and activate python virtual environment
```bash
pyenv venv python-data-sources, 
pyenv activate python-data-sources, 
```

Install development dependencies
```bash
make dev
```

Run unit tests
```bash
make test
```
