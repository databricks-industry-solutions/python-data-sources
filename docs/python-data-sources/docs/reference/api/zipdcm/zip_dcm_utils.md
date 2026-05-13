---
sidebar_label: zip_dcm_utils
title: python_data_sources.zipdcm.zip_dcm_utils
---

### readzipdcm

```python
def readzipdcm(partition: RangePartition, paths: list,
               dicom_keys_filter: list[str]) -> Iterator[list[Any]]
```

Generator function to extract DICOM metadata from .dcm files within ZIP archives.

Iterates over a partitioned list of ZIP file paths, opens each ZIP file, and processes files
with a &#x27;.dcm&#x27; extension. For each DICOM file, reads the header metadata, removes specified
large keys from the metadata dictionary, computes a SHA-1 hash of the pixel data, and yields
the results as a list.

**Arguments**:

- `partition` _RangePartition_ - An object with &#x27;start&#x27; and &#x27;end&#x27; attributes specifying the range of paths to process.
- `paths` _list_ - List of ZIP file paths to process.
- `dicom_keys_filter` _list_ - List of metadata keys to remove from the extracted DICOM metadata.
  

**Yields**:

- `list` - A list containing:
  - rowid (int): Unique row identifier.
  - Either concatenation of:
  - zip_file_path (str): Path to the ZIP file.
  - &#x27;/&#x27;
  - name_in_zip (str): Name of the DICOM file within the ZIP archive.
  Or:
  - dcm_file_path (str): Path to the dcm file.
  - meta (dict): Filtered DICOM metadata dictionary with an added &#x27;pixel_hash&#x27; key.
  

**Notes**:

  - Assumes that the DICOM files can be read directly from the ZIP archive without extraction.
  - The &#x27;pixel_hash&#x27; is computed using SHA-1 on the pixel array of the DICOM file.
  - Logging is performed at various steps for debugging purposes.

