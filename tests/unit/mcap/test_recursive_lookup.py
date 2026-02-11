"""
Unit test to verify recursive file lookup functionality.

This test verifies that the recursiveFileLookup option correctly
discovers MCAP files in subdirectories.
"""

import tempfile
from pathlib import Path

from python_data_sources.mcap.mcap_datasource import _path_handler


def test_non_recursive_lookup():
    """Test that non-recursive lookup only finds files in root directory."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create files in root and subdirectory
        (tmppath / "file1.mcap").touch()
        subdir = tmppath / "subdir"
        subdir.mkdir()
        (subdir / "file2.mcap").touch()

        files = _path_handler(str(tmppath), "*.mcap", recursive=False)

        assert len(files) == 1, f"Expected 1 file, found {len(files)}"
        assert "file1.mcap" in files[0], "Should find file1.mcap"


def test_recursive_lookup():
    """Test that recursive lookup finds files in subdirectories."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create directory structure
        (tmppath / "file1.mcap").touch()

        subdir1 = tmppath / "subdir1"
        subdir1.mkdir()
        (subdir1 / "file2.mcap").touch()

        subdir2 = subdir1 / "subdir2"
        subdir2.mkdir()
        (subdir2 / "file3.mcap").touch()

        other = tmppath / "other"
        other.mkdir()
        (other / "file4.mcap").touch()

        # Also create non-mcap files
        (tmppath / "readme.txt").touch()
        (subdir1 / "data.json").touch()

        files = _path_handler(str(tmppath), "*.mcap", recursive=True)

        assert len(files) == 4, f"Expected 4 files, found {len(files)}"

        file_names = [Path(f).name for f in files]
        assert "file1.mcap" in file_names
        assert "file2.mcap" in file_names
        assert "file3.mcap" in file_names
        assert "file4.mcap" in file_names

        # Ensure no non-mcap files
        for f in files:
            assert f.endswith(".mcap"), f"Non-mcap file found: {f}"


def test_single_file_path():
    """Test that single file path returns the file correctly."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.mcap"
        test_file.touch()

        files = _path_handler(str(test_file), "*.mcap", recursive=False)

        assert len(files) == 1, "Should return single file"
        assert "test.mcap" in files[0], "Should return correct file"


def test_subdirectory_recursive():
    """Test recursive lookup starting from a subdirectory."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        subdir1 = tmppath / "subdir1"
        subdir1.mkdir()
        (subdir1 / "file2.mcap").touch()

        subdir2 = subdir1 / "subdir2"
        subdir2.mkdir()
        (subdir2 / "file3.mcap").touch()

        files = _path_handler(str(subdir1), "*.mcap", recursive=True)

        assert len(files) == 2, f"Expected 2 files, found {len(files)}"
        file_names = [Path(f).name for f in files]
        assert "file2.mcap" in file_names
        assert "file3.mcap" in file_names
