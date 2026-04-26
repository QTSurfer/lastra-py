"""pandas adapter — load/store the series section as a :class:`pandas.DataFrame`.

Type mapping
============

================================  ============================
pandas dtype                      Lastra DataType / Codec
================================  ============================
``int64`` (incl. ``datetime64``)  LONG / DELTA_VARINT
``float64``                       DOUBLE / ALP
``object`` of ``bytes``/``str``   BINARY / VARLEN_ZSTD
================================  ============================

``datetime64`` columns are stored as their nanosecond ``int64`` view;
on read they come back as ``int64`` and the caller is responsible for
choosing the right time unit. (We don't second-guess the user — a
column called ``ts`` could be ms, µs, or ns; the format is
unit-agnostic.)
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from ..format import Codec, DataType
from ..reader import LastraReader
from ..writer import LastraWriter
from ._common import CodecOverride, resolve_codec


def _infer_data_type(series: pd.Series) -> DataType:
    if pd.api.types.is_datetime64_any_dtype(series):
        return DataType.LONG
    if pd.api.types.is_integer_dtype(series):
        return DataType.LONG
    if pd.api.types.is_float_dtype(series):
        return DataType.DOUBLE
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        return DataType.BINARY
    raise TypeError(
        f"column {series.name!r}: dtype {series.dtype} cannot map to a Lastra DataType. "
        f"Cast to int64 / float64 / bytes-object first."
    )


def _series_to_array(series: pd.Series, data_type: DataType):
    if data_type is DataType.LONG:
        if pd.api.types.is_datetime64_any_dtype(series):
            return series.view("int64").to_numpy(dtype=np.int64, copy=False)
        return series.to_numpy(dtype=np.int64, copy=False)
    if data_type is DataType.DOUBLE:
        return series.to_numpy(dtype=np.float64, copy=False)
    # BINARY
    out: list[bytes | None] = []
    for v in series:
        if v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NA:
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


def to_pandas(reader: LastraReader) -> pd.DataFrame:
    """Materialise the reader's series section as a :class:`pandas.DataFrame`."""
    data: dict[str, object] = {}
    for col in reader.series_columns:
        if col.data_type is DataType.LONG:
            data[col.name] = reader.read_series_long(col.name)
        elif col.data_type is DataType.DOUBLE:
            data[col.name] = reader.read_series_double(col.name)
        else:
            data[col.name] = reader.read_series_binary(col.name)
    return pd.DataFrame(data)


def from_pandas(
    df: pd.DataFrame,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> bytes:
    """Encode ``df`` as a series-only Lastra blob.

    ``codecs`` overrides the per-column codec; defaults are picked per
    :func:`lastra.adapters._common.default_codec`.
    """
    writer = LastraWriter() if row_group_size is None else LastraWriter(row_group_size)

    arrays: list[object] = []
    for name in df.columns:
        series = df[name]
        data_type = _infer_data_type(series)
        codec = resolve_codec(str(name), data_type, codecs)
        writer.add_series_column(str(name), data_type, codec)
        arrays.append(_series_to_array(series, data_type))

    writer.write_series(len(df), *arrays)
    return writer.to_bytes()


def read_pandas(path: str | Path) -> pd.DataFrame:
    """Read ``path`` and return its series section as a DataFrame."""
    blob = Path(path).read_bytes()
    return to_pandas(LastraReader.from_bytes(blob))


def write_pandas(
    df: pd.DataFrame,
    path: str | Path,
    *,
    codecs: CodecOverride | None = None,
    row_group_size: int | None = None,
) -> int:
    """Encode ``df`` and write it to ``path``. Returns the bytes written."""
    blob = from_pandas(df, codecs=codecs, row_group_size=row_group_size)
    Path(path).write_bytes(blob)
    return len(blob)
