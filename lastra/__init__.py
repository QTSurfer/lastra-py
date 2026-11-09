"""Lastra columnar time series format — Python implementation.

Public API::

    from lastra import LastraReader, RowGroupStats, Header, ColumnDescriptor
    from lastra.format import Codec, DataType, FLAG_HAS_FOOTER, ...

The wire format is documented in :doc:`FORMAT.md` and matches
lastra-java v0.8.0 and lastra-ts byte for byte.
"""

from importlib import metadata as _metadata

from .format import Codec, ColumnDescriptor, DataType, Header
from .reader import LastraReader, RowGroupStats
from .writer import DEFAULT_ROW_GROUP_SIZE, LastraWriter

try:
    __version__ = _metadata.version("lastra")
except _metadata.PackageNotFoundError:  # source tree that is not installed
    __version__ = "0.0.0+unknown"

__all__ = [
    "LastraReader",
    "LastraWriter",
    "RowGroupStats",
    "Header",
    "ColumnDescriptor",
    "Codec",
    "DataType",
    "DEFAULT_ROW_GROUP_SIZE",
    "__version__",
]
