import json
import logging
from pathlib import Path
from typing import Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

DEFAULT_numPartitions = 4
DEFAULT_pathGlobFilter = "*.dxf"


def _path_handler(path: str, glob_pattern: str, recursive: bool = False) -> list:
    """
    Discover files matching the glob pattern in the given path.

    Args:
        path: Path to search for files
        glob_pattern: Glob pattern to match files (e.g., "*.dxf")
        recursive: If True, recursively search subdirectories using rglob

    Returns:
        List of file paths matching the pattern
    """
    path_obj = Path(path)

    if path_obj.is_file():
        return [str(path_obj)]
    elif path_obj.is_dir():
        if recursive:
            files = sorted(path_obj.rglob(glob_pattern))
        else:
            files = sorted(path_obj.glob(glob_pattern))
        return [str(f) for f in files if f.is_file()]
    else:
        parent = path_obj.parent
        if parent.exists():
            files = sorted(parent.glob(path_obj.name))
            return [str(f) for f in files if f.is_file()]
    return []


class RangePartition(InputPartition):
    """
    Range partition for splitting file lists.
    """

    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end

    def __repr__(self):
        return f"RangePartition({self.start}, {self.end})"


def _extract_entity_attributes(entity) -> dict:
    """
    Extract common and type-specific attributes from a DXF entity.

    Args:
        entity: An ezdxf entity object

    Returns:
        Dictionary of extracted attributes
    """
    attrs = {}
    dxftype = entity.dxftype()

    if dxftype == "LINE":
        attrs["start_x"] = entity.dxf.start.x
        attrs["start_y"] = entity.dxf.start.y
        attrs["start_z"] = entity.dxf.start.z
        attrs["end_x"] = entity.dxf.end.x
        attrs["end_y"] = entity.dxf.end.y
        attrs["end_z"] = entity.dxf.end.z
    elif dxftype == "CIRCLE":
        attrs["center_x"] = entity.dxf.center.x
        attrs["center_y"] = entity.dxf.center.y
        attrs["center_z"] = entity.dxf.center.z
        attrs["radius"] = entity.dxf.radius
    elif dxftype == "ARC":
        attrs["center_x"] = entity.dxf.center.x
        attrs["center_y"] = entity.dxf.center.y
        attrs["center_z"] = entity.dxf.center.z
        attrs["radius"] = entity.dxf.radius
        attrs["start_angle"] = entity.dxf.start_angle
        attrs["end_angle"] = entity.dxf.end_angle
    elif dxftype == "POINT":
        attrs["location_x"] = entity.dxf.location.x
        attrs["location_y"] = entity.dxf.location.y
        attrs["location_z"] = entity.dxf.location.z
    elif dxftype in ("TEXT", "MTEXT"):
        if dxftype == "TEXT":
            attrs["text"] = entity.dxf.text
            attrs["insert_x"] = entity.dxf.insert.x
            attrs["insert_y"] = entity.dxf.insert.y
            attrs["insert_z"] = entity.dxf.insert.z
            attrs["height"] = entity.dxf.height
            attrs["rotation"] = entity.dxf.get("rotation", 0.0)
        else:
            attrs["text"] = entity.text
            attrs["insert_x"] = entity.dxf.insert.x
            attrs["insert_y"] = entity.dxf.insert.y
            attrs["insert_z"] = entity.dxf.insert.z
            attrs["height"] = entity.dxf.get("char_height", 1.0)
    elif dxftype == "LWPOLYLINE":
        points = list(entity.get_points(format="xyseb"))
        attrs["points"] = [
            {"x": p[0], "y": p[1], "start_width": p[2], "end_width": p[3], "bulge": p[4]}
            for p in points
        ]
        attrs["is_closed"] = entity.closed
    elif dxftype == "POLYLINE":
        vertices = []
        for v in entity.vertices:
            vertices.append({
                "x": v.dxf.location.x,
                "y": v.dxf.location.y,
                "z": v.dxf.location.z,
            })
        attrs["vertices"] = vertices
        attrs["is_closed"] = entity.is_closed
    elif dxftype == "SPLINE":
        ctrl_points = [{"x": p.x, "y": p.y, "z": p.z} for p in entity.control_points]
        attrs["control_points"] = ctrl_points
        attrs["degree"] = entity.dxf.degree
        attrs["is_closed"] = entity.closed
    elif dxftype == "ELLIPSE":
        attrs["center_x"] = entity.dxf.center.x
        attrs["center_y"] = entity.dxf.center.y
        attrs["center_z"] = entity.dxf.center.z
        attrs["major_axis_x"] = entity.dxf.major_axis.x
        attrs["major_axis_y"] = entity.dxf.major_axis.y
        attrs["major_axis_z"] = entity.dxf.major_axis.z
        attrs["ratio"] = entity.dxf.ratio
        attrs["start_param"] = entity.dxf.start_param
        attrs["end_param"] = entity.dxf.end_param
    elif dxftype == "INSERT":
        attrs["block_name"] = entity.dxf.name
        attrs["insert_x"] = entity.dxf.insert.x
        attrs["insert_y"] = entity.dxf.insert.y
        attrs["insert_z"] = entity.dxf.insert.z
        attrs["x_scale"] = entity.dxf.get("xscale", 1.0)
        attrs["y_scale"] = entity.dxf.get("yscale", 1.0)
        attrs["z_scale"] = entity.dxf.get("zscale", 1.0)
        attrs["rotation"] = entity.dxf.get("rotation", 0.0)
    elif dxftype == "DIMENSION":
        attrs["dimtype"] = entity.dxf.get("dimtype", 0)
        if hasattr(entity.dxf, "defpoint"):
            attrs["defpoint_x"] = entity.dxf.defpoint.x
            attrs["defpoint_y"] = entity.dxf.defpoint.y
            attrs["defpoint_z"] = entity.dxf.defpoint.z
    elif dxftype == "HATCH":
        attrs["pattern_name"] = entity.dxf.get("pattern_name", "")
        attrs["solid_fill"] = entity.dxf.get("solid_fill", 1)

    return attrs


def _read_dxf_file(file_path: str, layer_filter: str = None) -> Iterator[Tuple]:
    """
    Read a single DXF file and yield rows.

    Args:
        file_path: Path to the DXF file
        layer_filter: Optional layer name to filter entities. If None or "*", read all layers.

    Yields:
        Tuples of (file_path, entity_type, layer, handle, attributes_json)
    """
    import ezdxf

    logger.debug(f"Reading DXF file: {file_path}, layer_filter: {layer_filter}")

    if layer_filter == "*":
        layer_filter = None

    try:
        doc = ezdxf.readfile(file_path)
        msp = doc.modelspace()

        for entity in msp:
            dxftype = entity.dxftype()
            layer = entity.dxf.layer if hasattr(entity.dxf, "layer") else "0"
            handle = entity.dxf.handle if hasattr(entity.dxf, "handle") else ""

            if layer_filter and layer != layer_filter:
                continue

            try:
                attrs = _extract_entity_attributes(entity)
            except Exception as e:
                logger.warning(f"Error extracting attributes for {dxftype} entity: {e}")
                attrs = {"error": str(e)}

            attrs_json = json.dumps(attrs)

            yield (
                str(file_path),
                dxftype,
                layer,
                handle,
                attrs_json,
            )
    except Exception as e:
        logger.error(f"Error reading DXF file {file_path}: {e}")
        raise


def _read_dxf_partition(
    partition: RangePartition, paths: list, layer_filter: str = None
) -> Iterator[Tuple]:
    """
    Read DXF files for a given partition range.

    Args:
        partition: RangePartition with start and end indices
        paths: List of file paths to process
        layer_filter: Optional layer name to filter entities

    Yields:
        Tuples of (file_path, entity_type, layer, handle, attributes_json)
    """
    logger.debug(
        f"Processing partition: {partition}, paths subset: {paths[partition.start:partition.end]}, layer_filter: {layer_filter}"
    )

    for file_path in paths[partition.start : partition.end]:
        yield from _read_dxf_file(file_path, layer_filter=layer_filter)


class DXFDataSourceReader(DataSourceReader):
    """
    Facilitate reading AutoCAD DXF files.
    """

    def __init__(self, schema, options):
        logger.debug(f"DXFDataSourceReader(schema: {schema}, options: {options})")
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.pathGlobFilter = self.options.get("pathGlobFilter", DEFAULT_pathGlobFilter)
        self.recursiveFileLookup = bool(
            self.options.get("recursiveFileLookup", "false")
        )
        self.numPartitions = int(
            self.options.get("numPartitions", DEFAULT_numPartitions)
        )
        self.layerFilter = self.options.get("layerFilter", None)

        if self.layerFilter == "*":
            self.layerFilter = None

        assert self.path is not None, "path option is required"
        self.paths = _path_handler(
            self.path, self.pathGlobFilter, recursive=self.recursiveFileLookup
        )

        if not self.paths:
            logger.warning(
                f"No DXF files found at path: {self.path} with filter: {self.pathGlobFilter}"
            )

        if self.recursiveFileLookup:
            logger.info(
                f"Recursive file lookup enabled, found {len(self.paths)} files"
            )

        if self.layerFilter:
            logger.info(f"Layer filter enabled: {self.layerFilter}")

    def partitions(self) -> Sequence[RangePartition]:
        """
        Compute 'splits' of the data to read.

        Returns:
            List of RangePartition objects
        """
        logger.debug(
            f"DXFDataSourceReader.partitions({self.numPartitions}, {self.path}, paths: {self.paths})"
        )

        length = len(self.paths)
        if length == 0:
            return [RangePartition(0, 0)]

        partitions = []
        partition_size_max = int(max(1, length / self.numPartitions))
        start = 0

        while start < length:
            end = min(length, start + partition_size_max)
            partitions.append(RangePartition(start, end))
            start = start + partition_size_max

        logger.debug(f"#partitions {len(partitions)} {partitions}")
        return partitions

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """
        Executor level method, performs read by Range Partition.

        Args:
            partition: The partition to read

        Returns:
            Iterator of tuples (file_path, entity_type, layer, handle, attributes_json)
        """
        logger.debug(
            f"DXFDataSourceReader.read({partition}, {self.path}, paths: {self.paths}, layerFilter: {self.layerFilter})"
        )

        assert self.path is not None, f"path: {self.path}"
        assert self.paths is not None, f"paths: {self.paths}"

        # Library imports must be within the method for executor-level execution
        return _read_dxf_partition(
            partition, self.paths, layer_filter=self.layerFilter
        )


class DXFDataSource(DataSource):
    """
    A data source for batch query over AutoCAD DXF files using the ezdxf library.

    Usage:
        # Read all entities
        df = spark.read.format("dxf").option("path", "/path/to/dxf/files").load()

        # Filter by specific layer at read time
        df = spark.read.format("dxf") \\
            .option("path", "/path/to/dxf/files") \\
            .option("layerFilter", "walls") \\
            .load()

    Options:
        - path: Path to DXF file(s) or directory (required)
        - pathGlobFilter: Glob pattern for file matching (default: "*.dxf")
        - numPartitions: Number of partitions to split files across (default: 4)
        - recursiveFileLookup: Recursively search subdirectories (default: false)
        - layerFilter: Filter entities by layer name (optional). Use "*" or omit to read all layers.

    Schema:
        - file_path: STRING - Path to the source DXF file
        - entity_type: STRING - DXF entity type (LINE, CIRCLE, ARC, TEXT, etc.)
        - layer: STRING - The layer the entity belongs to
        - handle: STRING - Unique entity handle within the DXF file
        - attributes: STRING - JSON string containing entity-specific attributes
    """

    @classmethod
    def name(cls):
        datasource_type = "dxf"
        logger.debug(f"DXFDataSource.name({datasource_type})")
        return datasource_type

    def schema(self):
        schema = "file_path STRING, entity_type STRING, layer STRING, handle STRING, attributes STRING"
        logger.debug(f"DXFDataSource.schema({schema})")
        return schema

    def reader(self, schema: StructType):
        logger.debug(f"DXFDataSource.reader({schema}, options={self.options})")
        return DXFDataSourceReader(schema, self.options)
