"""
Test suite for the common utility functions.
"""


from python_data_sources.common.range_partition import RangePartition
from python_data_sources.common.utils import get_range_partitions


def test_get_range_partitions_basic():
    """Partitions should cover the full range without gaps or overlaps."""
    partitions = get_range_partitions(10, 2)
    assert len(partitions) == 2
    assert partitions[0].start == 0
    assert partitions[0].end == 5
    assert partitions[1].start == 5
    assert partitions[1].end == 10


def test_get_range_partitions_returns_range_partitions():
    """All returned partitions should be RangePartition instances."""
    partitions = get_range_partitions(10, 2)
    for p in partitions:
        assert isinstance(p, RangePartition)


def test_get_range_partitions_single_partition():
    """A single partition should cover the entire range."""
    partitions = get_range_partitions(5, 1)
    assert len(partitions) == 1
    assert partitions[0].start == 0
    assert partitions[0].end == 5


def test_get_range_partitions_more_partitions_than_items():
    """When numPartitions > length, each item gets its own partition."""
    partitions = get_range_partitions(3, 10)
    assert len(partitions) == 3
    for i, p in enumerate(partitions):
        assert p.start == i
        assert p.end == i + 1


def test_get_range_partitions_covers_full_range():
    """Partitions should cover every index from 0 to length."""
    length = 17
    partitions = get_range_partitions(length, 4)
    # Verify no gaps
    assert partitions[0].start == 0
    assert partitions[-1].end == length
    for i in range(1, len(partitions)):
        assert partitions[i].start == partitions[i - 1].end


def test_get_range_partitions_empty():
    """An empty list should produce no partitions."""
    partitions = get_range_partitions(0, 4)
    assert len(partitions) == 0


def test_get_range_partitions_one_item():
    """A single item should produce one partition."""
    partitions = get_range_partitions(1, 4)
    assert len(partitions) == 1
    assert partitions[0].start == 0
    assert partitions[0].end == 1
