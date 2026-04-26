"""polars adapter — load/store the series section as a :class:`polars.DataFrame`.

Type mapping
============

================================  ============================
polars dtype                      Lastra DataType / Codec
================================  ============================
``Int64`` (incl. ``Datetime``)    LONG / DELTA_VARINT
``Float64``                       DOUBLE / ALP
``Utf8`` / ``Binary``             BINARY / VARLEN_ZSTD
================================  ============================

``Datetime`` columns are stored as int64 nanoseconds — polars'
internal physical representation. On read they come back as plain
``Int64``; convert with ``.cast(pl.Datetime("ns"))`` if you need.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from ..format import Codec, DataType
from ..reader import LastraReader
from ..writer import LastraWriter
from ._common import CodecOverride, resolve_codec


def _infer_data_type(series: pl.Series) -> DataType:
    dt = series.dtype
    if dt.is_integer() or isinstance(dt, (pl.Datetime, pl.Date, pl.Duration, pl.Time)):
        return DataType.LONG
    if dt.is_float():
        return DataType.DOUBLE
    if dt in (pl.Utf8, pl.Binary):
        return DataType.BINARY
    raise TypeError(
        f"column {series.name!r}: dtype {dt} cannot map to a Lastra DataType. "
        f"Cast to Int64 / Float64 / Binary first."
    )


def _series_to_array(series: pl.Series, data_type: DataType) -> Any:
    if data_type is DataType.LONG:
        return series.cast(pl.Int64).to_numpy(allow_copy=False).astype(np.int64, copy=False)
    if data_type is DataType.DOUBLE:
        return series.cast(pl.Float64).to_numpy(allow_copy=False).astype(np.float64, copy=False)

    out: list[bytes | None] = []
    for v in series.to_list():
        if v is None:
            out.append(None)
        elif isinstance(v, (bytes, bytearray, memoryview)):
            out.append(bytes(v))
        elif isinstance(v, str):
            out.append(v.encode("utf-8"))
        else:
            raise TypeError(
                f"column {series.name!r}: unsupported binary value of type "
                f"{type(v).__name__}"
            )
    return out


def to_polars(reader: LastraReader) -> pl.DataFrame:
    columns: dict[str, pl.Series] = {}
    for col in reader.series_columns:
        if col.data_type is DataType.LONG:
            columns[col.name] = pl.Series(col.name, reader.read_series_long(col.name))
        elif col.data_type is DataType.DOUBLE:
            columns[col.name] = pl.Series(col.name, reader.read_series_double(col.name))
        else:
            columns[col.name] = pl.Series(
                col.name, reader.read_series_binary(col.name), dtype=pl.Binary
            )
    return pl.DataFrame(columns)


def from_polars(
    df: pl.DataFrame,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> bytes:
    writer = LastraWriter() if row_group_size is None else LastraWriter(row_group_size)

    arrays: list[Any] = []
    for name in df.columns:
        series = df[name]
        data_type = _infer_data_type(series)
        codec = resolve_codec(name, data_type, codecs)
        writer.add_series_column(name, data_type, codec)
        arrays.append(_series_to_array(series, data_type))

    writer.write_series(len(df), *arrays)
    return writer.to_bytes()


def read_polars(path: str | Path) -> pl.DataFrame:
    blob = Path(path).read_bytes()
    return to_polars(LastraReader.from_bytes(blob))


def write_polars(
    df: pl.DataFrame,
    path: str | Path,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> int:
    blob = from_polars(df, codecs=codecs, row_group_size=row_group_size)
    Path(path).write_bytes(blob)
    return len(blob)
