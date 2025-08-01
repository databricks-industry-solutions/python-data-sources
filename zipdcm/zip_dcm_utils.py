import logging
from io import BufferedReader
from pathlib import Path
from typing import IO, Any, Iterator, List, Union

from pyspark.sql.datasource import InputPartition

logger = logging.getLogger(__file__)


class RangePartition(InputPartition):
    """
    This DataSource InputPartition class provides tracking of ranges within a list
    """

    def __init__(self, start, end):
        self.start = start
        self.end = end


def _readzipdcm(
    partition: RangePartition, paths: list, dicom_keys_filter: list[str]
) -> Iterator[List[Any]]:
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

    import hashlib
    from zipfile import ZipFile

    from pydicom import dcmread

    def _handle_dcm_fp(fp: Union[BufferedReader, IO]):
        with dcmread(fp) as ds:
            meta = ds.to_json_dict()
            meta["hash"] = hashlib.sha1(fp.read()).hexdigest()
            if "7FE00010" in meta:  # will throw exception if no pixel data available
                meta["pixel_hash"] = hashlib.sha1(ds.PixelData).hexdigest()
            if meta is None:
                meta = ""
            for key in dicom_keys_filter:
                if key in meta:
                    del meta[key]
            logger.debug(f"meta: {meta}")
            return meta

    rowid = partition.start
    for path in paths[partition.start : partition.end]:
        try:
            logger.debug(f"processing path: {path}")
            if str(path).endswith(".dcm"):
                with open(path, "rb") as fp:
                    rowid = rowid + 1
                    yield [rowid, path, _handle_dcm_fp(fp)]
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
                                    _handle_dcm_fp(zip_fp),
                                ]
        except Exception as e:
            logger.error(f"Processing {path} caused exception: {e}")
            raise Exception(f"Processing {path} caused exception: {e}")


def _path_handler(path: str, pathGlobFilter="*.zip", recursiveFileLookup=True) -> list[Path]:
    #
    # In this implementation, we validate the path,
    # and get the list of the paths to scan.
    # TODO: Explore how to walk directory structures in parallel
    # TODO: Explore how to balance large skews in large archives vs. small. Current tests show 3-1 skew max v. median
    # TODO: Explore how to deal with large multi-frame DICOMs vs smaller single frame DICOMS (same amount of metadata)
    # TODO: Explore how to partition a single large Zip file
    #
    from pathlib import Path

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
        paths = sorted(Path(path).glob(f"**/{pathGlobFilter}"))
    else:
        if not (str(p).lower().endswith(".dcm") or str(p).lower().endswith(".zip")):
            raise ValueError(f"File {path} does not have an allowed extension (dcm,zip,Zip)")
        paths = [Path(path)]

    length = len(paths)
    logger.debug(f"#zipfiles: {length}, paths:{paths}")
    return paths
