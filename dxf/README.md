# DXF Data Source

A PySpark Python Data Source for reading AutoCAD DXF (Drawing Exchange Format) files using the [ezdxf](https://ezdxf.readthedocs.io/en/stable/) library.

DXF is an open exchange format for CAD systems. This data source extracts geometric entities from DXF files into a tabular format suitable for analysis in Spark.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| file_path | STRING | Path to the source DXF file |
| entity_type | STRING | DXF entity type (LINE, CIRCLE, ARC, TEXT, etc.) |
| layer | STRING | The layer the entity belongs to |
| handle | STRING | Unique entity handle within the DXF file |
| attributes | STRING | JSON string containing entity-specific attributes |

## Supported Entity Types

| Entity Type | Attributes |
|-------------|-----------|
| LINE | start_x/y/z, end_x/y/z |
| CIRCLE | center_x/y/z, radius |
| ARC | center_x/y/z, radius, start_angle, end_angle |
| POINT | location_x/y/z |
| TEXT | text, insert_x/y/z, height, rotation |
| MTEXT | text, insert_x/y/z, height |
| LWPOLYLINE | points (array), is_closed |
| POLYLINE | vertices (array), is_closed |
| SPLINE | control_points (array), degree, is_closed |
| ELLIPSE | center_x/y/z, major_axis_x/y/z, ratio, start_param, end_param |
| INSERT | block_name, insert_x/y/z, x/y/z_scale, rotation |
| DIMENSION | dimtype, defpoint_x/y/z |
| HATCH | pattern_name, solid_fill |

## Usage

```python
# Register the data source
from dxf_datasource import DXFDataSource
spark.dataSource.register(DXFDataSource)

# Read all entities from DXF files
df = spark.read.format("dxf").option("path", "/path/to/files").load()

# Filter by layer at read time
df = spark.read.format("dxf") \
    .option("path", "/path/to/files") \
    .option("layerFilter", "walls") \
    .load()

# Extract attributes from JSON
from pyspark.sql.functions import get_json_object, col

circles = df.filter(col("entity_type") == "CIRCLE") \
    .select(
        "layer",
        get_json_object(col("attributes"), "$.center_x").alias("cx"),
        get_json_object(col("attributes"), "$.center_y").alias("cy"),
        get_json_object(col("attributes"), "$.radius").alias("radius"),
    )
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| path | (required) | Path to DXF file(s) or directory |
| pathGlobFilter | `*.dxf` | Glob pattern for file matching |
| numPartitions | 4 | Number of partitions to split files across |
| recursiveFileLookup | false | Recursively search subdirectories |
| layerFilter | (all layers) | Filter entities by layer name |

## Dependencies

- [ezdxf](https://ezdxf.readthedocs.io/en/stable/) - Python library for reading/writing DXF files
