"""
Test suite for the ZipDCM data source.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from python_data_sources.zipdcm import ZipDCMDataSourceReader

logger = logging.getLogger(__name__)


def test_ZipDCMDataSourceReader(resources_dir: Path):
    """Test the ZipDCMDataSourceReader directly."""
    zip_file_path = str(resources_dir / "dcms")

    if not Path(zip_file_path).exists():
        pytest.skip(f"Test resources not found: {zip_file_path}")

    reader = ZipDCMDataSourceReader(
        schema="rowid INT, path STRING, meta STRING",
        options={"path": zip_file_path, "numPartitions": 32},
    )
    partitions = reader.partitions()
    logger.debug([_ for _ in partitions])

    for part in partitions:
        results = reader.read(part)
        logger.debug([_ for _ in results])


def test_wrongfile(spark: SparkSession, resources_dir: Path):
    """Test that wrong file path raises an exception."""
    from pyspark.errors import AnalysisException

    wrong_path = resources_dir / "wrongpath_that_does_not_exist.zip"
    with pytest.raises(AnalysisException):
        df = spark.read.option("numPartitions", "1").format("zipdcm").load(str(wrong_path))
        df.collect()


def test_wrongpath(spark: SparkSession, resources_dir: Path):
    """Test that wrong directory path raises an exception."""
    from pyspark.errors import AnalysisException

    wrong_path = resources_dir / "wrongpath_that_does_not_exist"
    with pytest.raises(AnalysisException):
        df = spark.read.option("numPartitions", "1").format("zipdcm").load(str(wrong_path))
        df.collect()


def test_dcm(spark: SparkSession, resources_dir: Path):
    """Test reading a single DCM file."""
    dcm_path = resources_dir / "dcms" / "y" / "1-1.dcm"

    if not dcm_path.exists():
        pytest.skip(f"Test file not found: {dcm_path}")

    df = spark.read.option("numPartitions", "1").format("zipdcm").load(str(dcm_path))
    result = df.collect()

    assert len(result) == 1
    assert "1-1.dcm" in result[0]["path"]
    logger.debug(f"test_dcm result: {result}")


def test_dcm_glob(spark: SparkSession, resources_dir: Path):
    """Test reading DCM files with glob pattern."""
    dcms_path = resources_dir / "dcms"

    if not dcms_path.exists():
        pytest.skip(f"Test directory not found: {dcms_path}")

    df = spark.read.option("numPartitions", "2").format("zipdcm").option("pathGlobFilter", "*.dcm").load(str(dcms_path))
    result = df.orderBy(df.path, ascending=False).collect()

    assert len(result) == 2
    logger.debug(f"test_dcm_glob result: {result}")


def test_single(spark: SparkSession, resources_dir: Path):
    """Test reading a single ZIP file."""
    zip_path = resources_dir / "dcms" / "3.5.574.1.3.9030958.6.376.2860280475000825621.zip"

    if not zip_path.exists():
        pytest.skip(f"Test file not found: {zip_path}")

    df = spark.read.option("numPartitions", "1").format("zipdcm").load(str(zip_path))
    result = df.collect()

    assert len(result) == 1
    logger.debug(f"test_single result: {result}")


def test_folder(spark: SparkSession, resources_dir: Path, tmp_path):
    """Test reading a folder of ZIP files."""
    dcms_path = resources_dir / "dcms"

    if not dcms_path.exists():
        pytest.skip(f"Test directory not found: {dcms_path}")

    df = spark.read.option("numPartitions", "2").format("zipdcm").load(str(dcms_path))
    df.limit(20).show()

    assert not df.isEmpty()
    save_path = tmp_path / "saves"
    df.write.format("csv").mode("overwrite").save(str(save_path))
    assert save_path.exists()


def test_rowid(spark: SparkSession, resources_dir: Path):
    """Test that rowid is generated correctly."""
    dcms_path = resources_dir / "dcms"

    if not dcms_path.exists():
        pytest.skip(f"Test directory not found: {dcms_path}")

    df = spark.read.option("numPartitions", "2").format("zipdcm").load(str(dcms_path))
    df.limit(20).show()

    df.createOrReplaceTempView("dicoms")
    distinct_count = spark.sql("select count(distinct rowid) from dicoms").collect()[0][0]
    assert distinct_count == 5
