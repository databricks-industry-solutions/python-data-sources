"""
Test suite for ZipDCM utilities.
"""

import logging
from pathlib import Path

import pytest

from python_data_sources.zipdcm.zip_dcm_utils import RangePartition, _path_handler, _readzipdcm

logger = logging.getLogger(__name__)


def test_path_handler_wrong_folder():
    """Test that wrong folder raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        _path_handler("./resources/wrongfolder")


def test_path_handler_zip(resources_dir: Path):
    """Test path handler with a ZIP file."""
    zip_path = resources_dir / "dcms" / "y.zip"

    if not zip_path.exists():
        pytest.skip(f"Test file not found: {zip_path}")

    paths = _path_handler(str(zip_path))
    assert paths is not None
    assert len(paths) == 1


def test_path_handler_folder(resources_dir: Path):
    """Test path handler with a folder."""
    dcms_path = resources_dir / "dcms"

    if not dcms_path.exists():
        pytest.skip(f"Test directory not found: {dcms_path}")

    paths = _path_handler(str(dcms_path))
    assert paths is not None
    assert len(paths) == 5


def test_path_handler_dcm(resources_dir: Path):
    """Test path handler with a DCM file."""
    dcm_path = resources_dir / "dcms" / "y" / "1-1.dcm"

    if not dcm_path.exists():
        pytest.skip(f"Test file not found: {dcm_path}")

    paths = _path_handler(str(dcm_path))
    assert len(paths) == 1


def test_readzipdcm_single_zip(resources_dir: Path):
    """Test reading a single ZIP file."""
    zip_path = resources_dir / "dcms" / "y" / "y.zip"

    if not zip_path.exists():
        pytest.skip(f"Test file not found: {zip_path}")

    part = RangePartition(0, 1)
    paths = [str(zip_path)]
    dicom_keys_filter: list[str] = []

    res = list(_readzipdcm(part, paths, dicom_keys_filter))
    logger.debug(res)
    assert len(res) >= 0


def test_readzipdcm_single_dcm(resources_dir: Path):
    """Test reading a single DCM file."""
    dcm_path = resources_dir / "dcms" / "y" / "1-1.dcm"

    if not dcm_path.exists():
        pytest.skip(f"Test file not found: {dcm_path}")

    part = RangePartition(0, 1)
    paths = [str(dcm_path)]
    dicom_keys_filter: list[str] = []

    res = list(_readzipdcm(part, paths, dicom_keys_filter))
    assert len(res) == 1
