"""
Quick test script for the MCAP data source.
Run this to verify the data source is working correctly.
"""

import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))


def test_mcap_datasource():
    """Test the MCAP data source with a sample file."""

    print("=" * 80)
    print("Testing MCAP Spark Data Source")
    print("=" * 80)

    try:
        from pyspark.sql import SparkSession
        from mcap_datasource import MCAPDataSource

        print("\n✓ Imports successful")

        # Initialize Spark
        spark = SparkSession.builder.appName("MCAP Test").master("local[*]").getOrCreate()

        spark.dataSource.register(MCAPDataSource)

        print("✓ Spark session created")

        # Test file path
        test_file = str(Path(__file__).parent / "test.mcap")

        if not Path(test_file).exists():
            print(f"\n⚠ Test file not found: {test_file}")
            print("Update the test_file path to point to a valid MCAP file")
            return False

        print(f"✓ Test file found: {test_file}")

        # Read the MCAP file
        print("\nReading MCAP file...")
        df = spark.read.format("mcap").option("path", test_file).option("numPartitions", "2").load()

        print("✓ MCAP file loaded successfully")

        # Show schema
        print("\nSchema:")
        df.printSchema()

        # Count records
        count = df.count()
        print(f"\n✓ Total records: {count}")

        # Show unique topics
        topics = df.select("topic").distinct().collect()
        print(f"\n✓ Topics found: {[row.topic for row in topics]}")

        # Show sample data
        print("\nSample data (first 3 rows):")
        df.select("sequence", "topic", "schema", "encoding", "log_time").show(3, truncate=False)

        # Verify sequence ordering
        sequences = df.select("sequence").orderBy("sequence").limit(5).collect()
        print(f"\n✓ Sequence numbers: {[row.sequence for row in sequences]}")

        # Test filtering with DataFrame filter
        pose_count = df.filter(df.topic == "pose").count()
        print(f"\n✓ Pose messages (DataFrame filter): {pose_count}")

        # Test filtering with topicFilter option (more efficient)
        print("\nTesting topicFilter option...")
        pose_df = spark.read.format("mcap").option("path", test_file).option("topicFilter", "pose").load()
        pose_count_filtered = pose_df.count()
        print(f"✓ Pose messages (topicFilter): {pose_count_filtered}")

        # Verify both methods return same count
        assert pose_count == pose_count_filtered, "Topic filter counts don't match!"

        # Extract JSON field
        from pyspark.sql.functions import get_json_object, col

        pose_df = df.filter(col("topic") == "pose").limit(1)
        if pose_df.count() > 0:
            pose_with_pos = pose_df.select("topic", get_json_object(col("data"), "$.position.x").alias("pos_x"))
            print("\n✓ JSON extraction successful:")
            pose_with_pos.show(truncate=False)

        spark.stop()

        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        return True

    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("\nMake sure you have installed:")
        print("  pip install pyspark mcap mcap-protobuf-support protobuf")
        return False

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_mcap_datasource()
    sys.exit(0 if success else 1)
