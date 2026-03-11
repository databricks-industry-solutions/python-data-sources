"""
Test suite for the RangePartition class.
"""

from pyspark.sql.datasource import InputPartition

from python_data_sources.common.range_partition import RangePartition


def test_range_partition_is_input_partition():
    """RangePartition should be a subclass of InputPartition."""
    partition = RangePartition(0, 10)
    assert isinstance(partition, InputPartition)


def test_range_partition_stores_start_end():
    """RangePartition should store start and end attributes."""
    partition = RangePartition(5, 15)
    assert partition.start == 5
    assert partition.end == 15


def test_range_partition_repr():
    """RangePartition repr should show start and end."""
    partition = RangePartition(0, 10)
    assert repr(partition) == "RangePartition(0, 10)"


def test_range_partition_zero_range():
    """RangePartition should handle zero-length ranges."""
    partition = RangePartition(0, 0)
    assert partition.start == 0
    assert partition.end == 0
    assert repr(partition) == "RangePartition(0, 0)"
