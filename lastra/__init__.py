"""Lastra columnar time series format — Python implementation.

Public API::

    from lastra import LastraReader, RowGroupStats, Header, ColumnDescriptor
    from lastra.format import Codec, DataType, FLAG_HAS_FOOTER, ...

The wire format is documented in :doc:`FORMAT.md` and matches
lastra-java v0.8.0 and lastra-ts byte for byte.
"""

from .format import Codec, ColumnDescriptor, DataType, Header
from .reader import LastraReader, RowGroupStats
from .writer import DEFAULT_ROW_GROUP_SIZE, LastraWriter

__version__ = "0.8.1"

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
