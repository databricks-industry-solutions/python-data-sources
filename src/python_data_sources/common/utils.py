import logging
from python_data_sources.common.range_partition import RangePartition


logger = logging.getLogger(__name__)


def get_range_partitions(length: int, numPartitions: int) -> list[RangePartition]:
    partitions = []
    partition_size_max = max(1, int(length / numPartitions))
    start = 0

    while start < length:
        end = min(length, start + partition_size_max)
        partitions.append(RangePartition(start, end))
        start = start + partition_size_max

    logger.debug(f"#partitions {len(partitions)} {partitions}")
    return partitions
