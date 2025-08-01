import logging

import pytest
from zip_dcm_utils import RangePartition, _path_handler

logger = logging.getLogger(__file__)


def test_path_handler_wrong_folder():
    with pytest.raises(FileNotFoundError):
        paths = _path_handler("./resources/wrongfolder")
        assert paths is not None
        assert len(paths) == 0


def test_path_handler_zip():
    paths = _path_handler("./resources/dcms/y.zip")
    assert paths is not None
    assert len(paths) == 1


def test_path_handler_folder():
    paths = _path_handler("./resources/dcms")
    assert paths is not None
    assert len(paths) == 5


def test_path_handler_dcm():
    paths = _path_handler("./resources/dcms/y/1-1.dcm")
    assert len(paths) == 1


def test_readzipdcm_single_zip():
    from zip_dcm_utils import _readzipdcm

    zip_file_path = "./resources/dcms/y/y.zip"
    part = RangePartition(0, 1)
    paths = [zip_file_path]
    dicom_keys_filter: list[str] = []
    res = _readzipdcm(part, paths, dicom_keys_filter)
    logger.debug(res)


def test_readzipdcm_single_dcm():
    from zip_dcm_utils import _readzipdcm

    zip_file_path = "./resources/dcms/y/1-1.dcm"
    part = RangePartition(0, 1)
    paths = [zip_file_path]
    dicom_keys_filter: list[str] = []
    res = _readzipdcm(part, paths, dicom_keys_filter)
    print(type([_ for _ in res]))


if __name__ == "__main__":
    test_readzipdcm_single_dcm()
