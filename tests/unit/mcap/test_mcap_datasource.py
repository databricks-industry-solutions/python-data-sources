"""
Test suite for the MCAP data source.
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def test_mcap_datasource(spark: SparkSession, test_mcap_file: Path):
    """Test the MCAP data source with a sample file."""

    if not test_mcap_file.exists():
        pytest.skip(f"Test file not found: {test_mcap_file}")

    # Read the MCAP file
    df = spark.read.format("mcap").option("path", str(test_mcap_file)).option("numPartitions", "2").load()

    # Verify schema
    assert "sequence" in df.columns
    assert "topic" in df.columns
    assert "schema" in df.columns
    assert "encoding" in df.columns
    assert "log_time" in df.columns
    assert "data" in df.columns

    # Count records
    count = df.count()
    assert count >= 0, "Should be able to count records"

    # Show unique topics
    topics = df.select("topic").distinct().collect()
    assert isinstance(topics, list)


def test_mcap_topic_filter(spark: SparkSession, test_mcap_file: Path):
    """Test the topicFilter option."""

    if not test_mcap_file.exists():
        pytest.skip(f"Test file not found: {test_mcap_file}")

    # Read all data
    df_all = spark.read.format("mcap").option("path", str(test_mcap_file)).load()

    # Get a topic to filter on
    topics = df_all.select("topic").distinct().collect()
    if len(topics) == 0:
        pytest.skip("No topics found in test file")

    test_topic = topics[0].topic

    # Read with filter using DataFrame filter
    df_filtered = df_all.filter(df_all.topic == test_topic)
    pose_count = df_filtered.count()

    # Read with topicFilter option
    df_topic_filter = (
        spark.read.format("mcap").option("path", str(test_mcap_file)).option("topicFilter", test_topic).load()
    )
    topic_filter_count = df_topic_filter.count()

    assert pose_count == topic_filter_count, "Topic filter counts should match"
