import logging
import hashlib
from zipfile import ZipFile
from collections.abc import Iterator
from io import BufferedReader
from pathlib import Path
from typing import IO, Any
from pydicom import dcmread

from python_data_sources.common.range_partition import RangePartition

logger = logging.getLogger(__name__)


def readzipdcm(partition: RangePartition, paths: list, dicom_keys_filter: list[str]) -> Iterator[list[Any]]:
    """
    Generator function to extract DICOM metadata from .dcm files within ZIP archives.

    Iterates over a partitioned list of ZIP file paths, opens each ZIP file, and processes files
    with a '.dcm' extension. For each DICOM file, reads the header metadata, removes specified
    large keys from the metadata dictionary, computes a SHA-1 hash of the pixel data, and yields
    the results as a list.

    Args:
        partition (RangePartition): An object with 'start' and 'end' attributes specifying the range of paths to process.
        paths (list): List of ZIP file paths to process.
        dicom_keys_filter (list): List of metadata keys to remove from the extracted DICOM metadata.

    Yields:
        list: A list containing:
            - rowid (int): Unique row identifier.
            - Either concatenation of:
                - zip_file_path (str): Path to the ZIP file.
                - '/'
                - name_in_zip (str): Name of the DICOM file within the ZIP archive.
            Or:
                - dcm_file_path (str): Path to the dcm file.
            - meta (dict): Filtered DICOM metadata dictionary with an added 'pixel_hash' key.

    Notes:
        - Assumes that the DICOM files can be read directly from the ZIP archive without extraction.
        - The 'pixel_hash' is computed using SHA-1 on the pixel array of the DICOM file.
        - Logging is performed at various steps for debugging purposes.
    """
    rowid = partition.start
    for path in paths[partition.start : partition.end]:
        try:
            logger.debug(f"processing path: {path}")
            if str(path).endswith(".dcm"):
                with open(path, "rb") as file:
                    rowid = rowid + 1
                    yield [rowid, path, _handle_dcm_fp(file, dicom_keys_filter)]
            else:
                with ZipFile(path, "r") as zipFile:
                    for name_in_zip in zipFile.namelist():
                        logger.debug(f" processing {path}::{name_in_zip}")
                        if name_in_zip.endswith(".dcm"):
                            with zipFile.open(name_in_zip, "r") as zip_fp:
                                rowid = rowid + 1
                                yield [
                                    rowid,
                                    f"{path}::{name_in_zip}",
                                    _handle_dcm_fp(zip_fp, dicom_keys_filter),
                                ]
        except Exception as e:
            logger.error(f"Processing {path} caused exception: {e}")
            raise RuntimeError(f"Processing {path} caused exception: {e}") from e


def _handle_dcm_fp(fp: BufferedReader | IO, dicom_keys_filter: list[str]):
    with dcmread(fp) as ds:
        meta = ds.to_json_dict()
        meta["hash"] = hashlib.sha1(fp.read()).hexdigest()
        if "7FE00010" in meta:  # will throw exception if no pixel data available
            meta["pixel_hash"] = hashlib.sha1(ds.PixelData).hexdigest()
        for key in dicom_keys_filter:
            if key in meta:
                del meta[key]
        logger.debug(f"meta: {meta}")
        return meta


def path_handler(path: str, pathGlobFilter="*.zip", recursiveFileLookup=True) -> list[Path]:
    #
    # In this implementation, we validate the path,
    # and get the list of the paths to scan.
    # TODO: Explore how to walk directory structures in parallel
    # TODO: Explore how to balance large skews in large archives vs. small. Current tests show 3-1 skew max v. median
    # TODO: Explore how to deal with large multi-frame DICOMs vs smaller single frame DICOMS (same amount of metadata)
    # TODO: Explore how to partition a single large Zip file
    #
    if path is None:
        raise ValueError("path parmeter is None")

    p = Path(path)
    if not p.exists():
        logger.error(f"not exists {path}")
        raise FileNotFoundError(f"{path}")  # TODO: Fix exception type

    # conflate either a direct zip file path or a dir into one case
    if p.is_dir():
        # a folder of zips
        # TODO: .glob() performance at extreme scales limits scale
        if recursiveFileLookup:
            paths = sorted(p.rglob(pathGlobFilter))
        else:
            paths = sorted(p.glob(pathGlobFilter))
    else:
        if not (str(p).lower().endswith(".dcm") or str(p).lower().endswith(".zip")):
            raise ValueError(f"File {path} does not have an allowed extension (dcm,zip,Zip)")
        paths = [Path(path)]

    length = len(paths)
    logger.debug(f"#zipfiles: {length}, paths:{paths}")
    return paths
