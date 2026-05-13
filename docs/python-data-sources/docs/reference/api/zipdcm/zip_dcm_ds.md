---
sidebar_label: zip_dcm_ds
title: python_data_sources.zipdcm.zip_dcm_ds
---

## ZipDCMDataSourceReader Objects

```python
class ZipDCMDataSourceReader(DataSourceReader)
```

Facilitate reading Zipfiles full of DCM (DICOM) files.

### partitions

```python
def partitions() -> Sequence[RangePartition]
```

Compute &#x27;splits&#x27; of the data to read
    self.paths is the list of files discovered and now need to be partitioned.

### read

```python
def read(
        partition: InputPartition
) -> Iterator[tuple] | Iterator["RecordBatch"]
```

Executor level method, performs read by Range Partition

## ZipDCMDataSource Objects

```python
class ZipDCMDataSource(DataSource)
```

A data source for batch query over zipped DICOM files the `ZipFile` and `PyDicom` libraries.

