import logging
from pathlib import Path

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

logger = logging.getLogger(__file__)

class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end


class ZipDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.numPartitions = int(self.options.get("numPartitions", 2))
        logger.debug(options)

    def partitions(self):
        return [RangePartition(0, 1000) for i in range(self.numPartitions)]

    def read(self, partition):
        # Library imports must be within the method.
        from zipfile import ZipFile

        logger.debug(partition)
        try:
            p = Path(self.path)
            if not p.exists():
                logger.warning(f"not exists {p}")
                return
            if p.is_dir():
                # a folder full of zips
                # beaware of .glob() at extreme
                for file in Path(self.path).glob("**/*.zip"):
                    logger.debug(f"{file}")
                    with ZipFile(file, "r") as zipFile:
                        for name in zipFile.namelist():
                            with zipFile.open(name, "r") as zipfile:
                                for line in zipfile:
                                    yield [f"{file}, {name} {line.decode('utf-8')}"]
            else:
                # single zip file
                with ZipFile(file, "r") as zipFile:
                    for name in zipFile.namelist():
                        with zipFile.open(name, "r") as zipfile:
                            for line in zipfile:
                                logger.debug(type(line))
                                yield [f"{file}, {name} {line.decode('utf-8')}"]
        except Exception as e:
            logger.error(e)


class ZipDataSource(DataSource):
    """
    An example data source for batch query using the `zipfile` library.
    """

    @classmethod
    def name(cls):
        return "zipcsv"

    def schema(self):
        logger.debug(f"schema->options:{self.options}")
        return "line string"

    def reader(self, schema: StructType):
        return ZipDataSourceReader(schema, self.options)