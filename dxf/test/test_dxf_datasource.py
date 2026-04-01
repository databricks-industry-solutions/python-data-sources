"""
Unit tests for the DXF data source.

Tests verify entity extraction, path handling, layer filtering,
and partition logic without requiring PySpark.
"""

import json
import sys
import tempfile
import os
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


SAMPLE_DXF = str(Path(__file__).parent / "sample.dxf")


def test_path_handler_single_file():
    """Test _path_handler with a single file path."""
    from dxf_datasource import _path_handler

    paths = _path_handler(SAMPLE_DXF, "*.dxf")
    assert len(paths) == 1
    assert paths[0] == SAMPLE_DXF


def test_path_handler_directory():
    """Test _path_handler with a directory path."""
    from dxf_datasource import _path_handler

    test_dir = str(Path(__file__).parent)
    paths = _path_handler(test_dir, "*.dxf")
    assert len(paths) >= 1
    assert any("sample.dxf" in p for p in paths)


def test_path_handler_recursive():
    """Test _path_handler with recursive lookup."""
    from dxf_datasource import _path_handler

    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create nested DXF files
        (tmppath / "top.dxf").touch()
        subdir = tmppath / "sub"
        subdir.mkdir()
        (subdir / "nested.dxf").touch()
        (tmppath / "readme.txt").touch()

        # Non-recursive: only top level
        paths = _path_handler(str(tmppath), "*.dxf", recursive=False)
        assert len(paths) == 1
        assert "top.dxf" in paths[0]

        # Recursive: both levels
        paths = _path_handler(str(tmppath), "*.dxf", recursive=True)
        assert len(paths) == 2
        names = [Path(p).name for p in paths]
        assert "top.dxf" in names
        assert "nested.dxf" in names


def test_path_handler_empty():
    """Test _path_handler with empty directory."""
    from dxf_datasource import _path_handler

    with tempfile.TemporaryDirectory() as tmpdir:
        paths = _path_handler(tmpdir, "*.dxf")
        assert len(paths) == 0


def test_read_dxf_file():
    """Test reading entities from the sample DXF file."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    assert len(rows) == 7, f"Expected 7 entities, got {len(rows)}"

    # Each row should be a 5-tuple
    for row in rows:
        assert len(row) == 5
        file_path, entity_type, layer, handle, attrs_json = row
        assert file_path == SAMPLE_DXF
        assert isinstance(entity_type, str)
        assert isinstance(layer, str)
        assert isinstance(handle, str)
        # Verify attrs_json is valid JSON
        attrs = json.loads(attrs_json)
        assert isinstance(attrs, dict)


def test_read_dxf_entity_types():
    """Test that expected entity types are extracted."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    entity_types = [row[1] for row in rows]

    assert "LINE" in entity_types
    assert "CIRCLE" in entity_types
    assert "ARC" in entity_types
    assert "POINT" in entity_types
    assert "TEXT" in entity_types
    assert "LWPOLYLINE" in entity_types
    assert "ELLIPSE" in entity_types


def test_line_attributes():
    """Test LINE entity attributes are correctly extracted."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    line_rows = [r for r in rows if r[1] == "LINE"]
    assert len(line_rows) == 1

    attrs = json.loads(line_rows[0][4])
    assert attrs["start_x"] == 0.0
    assert attrs["start_y"] == 0.0
    assert attrs["end_x"] == 10.0
    assert attrs["end_y"] == 10.0


def test_circle_attributes():
    """Test CIRCLE entity attributes are correctly extracted."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    circle_rows = [r for r in rows if r[1] == "CIRCLE"]
    assert len(circle_rows) == 1

    attrs = json.loads(circle_rows[0][4])
    assert attrs["center_x"] == 5.0
    assert attrs["center_y"] == 5.0
    assert attrs["radius"] == 3.0


def test_text_attributes():
    """Test TEXT entity attributes are correctly extracted."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    text_rows = [r for r in rows if r[1] == "TEXT"]
    assert len(text_rows) == 1

    attrs = json.loads(text_rows[0][4])
    assert attrs["text"] == "Hello DXF"
    assert attrs["height"] == 0.5


def test_lwpolyline_attributes():
    """Test LWPOLYLINE entity attributes are correctly extracted."""
    from dxf_datasource import _read_dxf_file

    rows = list(_read_dxf_file(SAMPLE_DXF))
    poly_rows = [r for r in rows if r[1] == "LWPOLYLINE"]
    assert len(poly_rows) == 1

    attrs = json.loads(poly_rows[0][4])
    assert attrs["is_closed"] is True
    assert len(attrs["points"]) == 4


def test_layer_filter():
    """Test layer filtering."""
    from dxf_datasource import _read_dxf_file

    # Filter to geometry layer
    rows = list(_read_dxf_file(SAMPLE_DXF, layer_filter="geometry"))
    layers = {row[2] for row in rows}
    assert layers == {"geometry"}
    assert len(rows) == 5  # line, circle, arc, lwpolyline, ellipse

    # Filter to annotations layer
    rows = list(_read_dxf_file(SAMPLE_DXF, layer_filter="annotations"))
    assert len(rows) == 1
    assert rows[0][1] == "TEXT"

    # Wildcard should return all
    rows = list(_read_dxf_file(SAMPLE_DXF, layer_filter="*"))
    assert len(rows) == 7


def test_partition_logic():
    """Test RangePartition and partition splitting."""
    from dxf_datasource import RangePartition, _read_dxf_partition

    # Test single partition over all paths
    paths = [SAMPLE_DXF]
    partition = RangePartition(0, 1)
    rows = list(_read_dxf_partition(partition, paths))
    assert len(rows) == 7

    # Test empty partition
    partition = RangePartition(0, 0)
    rows = list(_read_dxf_partition(partition, paths))
    assert len(rows) == 0


def test_extract_entity_attributes_unknown():
    """Test that unknown entity types return empty attributes."""
    from dxf_datasource import _extract_entity_attributes

    class MockEntity:
        def dxftype(self):
            return "UNKNOWN_TYPE"

    entity = MockEntity()
    attrs = _extract_entity_attributes(entity)
    assert attrs == {}
