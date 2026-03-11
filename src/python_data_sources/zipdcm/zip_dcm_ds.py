import logging
from collections.abc import Iterator, Sequence

from pyarrow import RecordBatch
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from python_data_sources.zipdcm.zip_dcm_utils import path_handler, readzipdcm
from python_data_sources.common.range_partition import RangePartition
from python_data_sources.common.utils import get_range_partitions

logger = logging.getLogger(__name__)

DEFAULT_NUM_PARTITIONS = 2

# DICOM header keys to delete before saving the metadata
# raw pixel data - 0x7FE0, 0x0010
# overlay data - 6000 3000
# VOI LUT Sequence Attribute - 00283010
# LUT Data Attribute - 00283006
DEFAULT_DICOM_KEYS_FILTER = "60003000,7FE00010,00283010,00283006"


class ZipDCMDataSourceReader(DataSourceReader):
    """
    Facilitate reading Zipfiles full of DCM (DICOM) files.
    """

    def __init__(self, schema, options):
        logger.debug(f"ZipDCMDataSourceReader(schema: {schema}, options: {options})")
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.pathGlobFilter = self.options.get("pathGlobFilter", "*.zip")
        self.recursiveFileLookup = bool(self.options.get("recursiveFileLookup", "false"))
        self.numPartitions = int(self.options.get("numPartitions", DEFAULT_NUM_PARTITIONS))
        self.deep = False
        self.dicom_keys_filter = self.options.get("dicomKeysFilter", DEFAULT_DICOM_KEYS_FILTER).split(",")
        assert self.path is not None
        self.paths = path_handler(self.path, self.pathGlobFilter)

    def partitions(self) -> Sequence[RangePartition]:
        """
        Compute 'splits' of the data to read
            self.paths is the list of files discovered and now need to be partitioned.
        """
        logger.debug(f"ZipDCMDataSourceReader.partitions({self.numPartitions}, {self.path}, paths: {self.paths}): ")
        length = len(self.paths)
        return get_range_partitions(length, self.numPartitions)

    def read(self, partition: InputPartition) -> Iterator[tuple] | Iterator["RecordBatch"]:
        """
        Executor level method, performs read by Range Partition
        """
        logger.debug(f"ZipDCMDataSourceReader.read({partition},{self.path}, paths:{self.paths}):")

        assert self.path is not None, f"path: {self.path}"
        assert self.paths is not None, f"path: {self.path}"

        # Library imports must be within the method.
        return readzipdcm(partition, self.paths, self.dicom_keys_filter)


class ZipDCMDataSource(DataSource):
    """
    A data source for batch query over zipped DICOM files the `ZipFile` and `PyDicom` libraries.
    """

    @classmethod
    def name(cls):
        datasource_type = "zipdcm"
        logger.debug(f"ZipDCMDataSource.name({datasource_type}): ")
        return datasource_type

    def schema(self):
        schema = "rowid INT, path STRING, meta STRING"
        logger.debug(f"ZipDCMDataSource.schema({schema}): ")
        return schema

    def reader(self, schema: StructType):
        logger.debug(f"ZipDCMDataSource.reader({schema}, options={self.options}): ")
        return ZipDCMDataSourceReader(schema, self.options)
