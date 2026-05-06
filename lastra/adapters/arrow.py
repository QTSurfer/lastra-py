"""pyarrow adapter — load/store the series section as a :class:`pyarrow.Table`.

Type mapping
============

================================  ============================
arrow type                        Lastra DataType / Codec
================================  ============================
``int64`` (incl. ``timestamp``)   LONG / DELTA_VARINT
``float64``                       DOUBLE / ALP
``binary`` / ``string``           BINARY / VARLEN_ZSTD
================================  ============================

``timestamp`` columns are stored as their underlying int64. Loss of
unit metadata: on read they come back as plain ``int64``; reattach
the unit yourself if you need it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

from ..format import DataType
from ..reader import LastraReader
from ..writer import LastraWriter
from ._common import CodecOverride, resolve_codec


def _infer_data_type(field: pa.Field) -> DataType:
    t = field.type
    if pa.types.is_integer(t) or pa.types.is_timestamp(t) or pa.types.is_date(t) or pa.types.is_duration(t):
        return DataType.LONG
    if pa.types.is_floating(t):
        return DataType.DOUBLE
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_string(t) or pa.types.is_large_string(t):
        return DataType.BINARY
    raise TypeError(
        f"column {field.name!r}: arrow type {t} cannot map to a Lastra DataType. "
        f"Cast to int64 / float64 / binary first."
    )


def _column_to_array(column: pa.ChunkedArray, data_type: DataType) -> Any:
    if data_type is DataType.LONG:
        if pa.types.is_timestamp(column.type) or pa.types.is_date(column.type) or pa.types.is_duration(column.type):
            column = column.cast(pa.int64())
        return column.to_numpy(zero_copy_only=False).astype(np.int64, copy=False)
    if data_type is DataType.DOUBLE:
        return column.to_numpy(zero_copy_only=False).astype(np.float64, copy=False)

    out: list[bytes | None] = []
    for v in column.to_pylist():
        if v is None:
            out.append(None)
        elif isinstance(v, (bytes, bytearray, memoryview)):
            out.append(bytes(v))
        elif isinstance(v, str):
            out.append(v.encode("utf-8"))
        else:
            raise TypeError(
                f"column: unsupported binary value of type {type(v).__name__}"
            )
    return out


def to_arrow(reader: LastraReader) -> pa.Table:
    arrays: list[pa.Array] = []
    names: list[str] = []
    for col in reader.series_columns:
        names.append(col.name)
        if col.data_type is DataType.LONG:
            arrays.append(pa.array(reader.read_series_long(col.name), type=pa.int64()))
        elif col.data_type is DataType.DOUBLE:
            arrays.append(pa.array(reader.read_series_double(col.name), type=pa.float64()))
        else:
            arrays.append(pa.array(reader.read_series_binary(col.name), type=pa.binary()))
    return pa.Table.from_arrays(arrays, names=names)


def from_arrow(
    table: pa.Table,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> bytes:
    writer = LastraWriter() if row_group_size is None else LastraWriter(row_group_size)

    arrays: list[Any] = []
    for field in table.schema:
        data_type = _infer_data_type(field)
        codec = resolve_codec(field.name, data_type, codecs)
        writer.add_series_column(field.name, data_type, codec)
        arrays.append(_column_to_array(table.column(field.name), data_type))

    writer.write_series(table.num_rows, *arrays)
    return writer.to_bytes()


def read_arrow(path: str | Path) -> pa.Table:
    blob = Path(path).read_bytes()
    return to_arrow(LastraReader.from_bytes(blob))


def write_arrow(
    table: pa.Table,
    path: str | Path,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> int:
    blob = from_arrow(table, codecs=codecs, row_group_size=row_group_size)
    Path(path).write_bytes(blob)
    return len(blob)
