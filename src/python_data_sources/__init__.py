"""
Python Data Sources - A collection of custom PySpark data source connectors.

This package provides data sources for various formats:
- mcap: MCAP (ROS 2 bag) file reader
- mqtt: MQTT streaming data source
- zipdcm: Zipped DICOM file reader

Install optional dependencies for specific data sources:
    pip install python-data-sources[mcap]
    pip install python-data-sources[mqtt]
    pip install python-data-sources[zipdcm]
    pip install python-data-sources[all]
"""

__version__ = "0.1.0"

__all__ = [
    "__version__",
]
