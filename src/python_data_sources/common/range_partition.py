from pyspark.sql.datasource import InputPartition


class RangePartition(InputPartition):
    """
    This DataSource InputPartition class provides tracking of ranges within a list
    """

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __repr__(self):
        return f"RangePartition({self.start}, {self.end})"
