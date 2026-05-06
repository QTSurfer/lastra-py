"""High-level :class:`LastraWriter` — encodes a complete ``.lastra`` blob.

Mirrors :class:`com.wualabs.qtsurfer.lastra.LastraWriter`. The default
flags (``FLAG_HAS_FOOTER | FLAG_HAS_CHECKSUMS``) match the Java reference
so files written here round-trip through the Java reader and vice versa.

Usage::

    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_series_column("close", DataType.DOUBLE, Codec.ALP)
    w.add_event_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_event_column("type", DataType.BINARY, Codec.VARLEN)
    w.write_series(n, ts, close)
    w.write_events(m, event_ts, event_type)
    blob = w.to_bytes()
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from typing import Any, BinaryIO, Mapping, Sequence

import numpy as np

from . import _descriptor as _desc
from . import format as _format
from .codecs import alp, delta_varint, gorilla, pongo, raw, varlen
from .format import (
    Codec,
    ColumnDescriptor,
    DataType,
    FLAG_HAS_CHECKSUMS,
    FLAG_HAS_EVENTS,
    FLAG_HAS_FOOTER,
    FLAG_HAS_ROW_GROUPS,
    FOOTER_MAGIC,
    Header,
    write_header,
)

DEFAULT_ROW_GROUP_SIZE: int = 4096


@dataclass
class _RowGroupData:
    row_count: int
    ts_min: int
    ts_max: int
    compressed_columns: list[bytes]
    crcs: list[int]


class LastraWriter:
    """Buffered writer. Add columns, push data, then finalise to bytes/stream."""

    def __init__(self, row_group_size: int = DEFAULT_ROW_GROUP_SIZE) -> None:
        self._series_columns: list[ColumnDescriptor] = []
        self._event_columns: list[ColumnDescriptor] = []

        self._row_group_size = row_group_size
        self._series_row_count = 0
        self._events_row_count = 0
        self._row_groups: list[_RowGroupData] = []

        # Event column buffers (compressed lazily on close).
        self._event_columns_data: list[Any] = []

    # ------------------------------------------------------------------
    # Column registration
    # ------------------------------------------------------------------

    def add_series_column(
        self,
        name: str,
        data_type: DataType,
        codec: Codec,
        metadata: Mapping[str, str] | None = None,
    ) -> "LastraWriter":
        self._series_columns.append(
            ColumnDescriptor(
                codec=codec, data_type=data_type, flags=0, name=name,
                metadata=dict(metadata) if metadata else {},
            )
        )
        return self

    def add_event_column(
        self,
        name: str,
        data_type: DataType,
        codec: Codec,
        metadata: Mapping[str, str] | None = None,
    ) -> "LastraWriter":
        self._event_columns.append(
            ColumnDescriptor(
                codec=codec, data_type=data_type, flags=0, name=name,
                metadata=dict(metadata) if metadata else {},
            )
        )
        return self

    def set_row_group_size(self, size: int) -> "LastraWriter":
        if size <= 0:
            raise ValueError(f"row group size must be positive, got {size}")
        self._row_group_size = size
        return self

    # ------------------------------------------------------------------
    # Data ingestion
    # ------------------------------------------------------------------

    def write_series(self, row_count: int, *column_data: Any) -> "LastraWriter":
        """Buffer + auto-partition series data into row groups.

        ``column_data`` order matches the registration order. The first
        ``LONG`` column is treated as the timestamp for row-group
        ``ts_min`` / ``ts_max`` stats — same convention as lastra-java.
        """
        if len(column_data) != len(self._series_columns):
            raise ValueError(
                f"writeSeries got {len(column_data)} arrays but {len(self._series_columns)} "
                f"series columns are registered"
            )
        # Accumulate across calls — each invocation appends rows to the same
        # series rather than replacing it. The footer's series_row_count must
        # reflect the total written across every write_series() call.
        self._series_row_count += row_count

        for start in range(0, row_count, self._row_group_size):
            end = min(start + self._row_group_size, row_count)
            rg_rows = end - start

            slices: list[Any] = []
            ts_min: int | None = None
            ts_max: int | None = None

            for i, col in enumerate(self._series_columns):
                src = column_data[i]
                if col.data_type is DataType.LONG:
                    chunk = _slice_long(src, start, end)
                    if ts_min is None and rg_rows > 0:
                        ts_min = int(chunk[0])
                        ts_max = int(chunk[-1])
                    slices.append(chunk)
                elif col.data_type is DataType.DOUBLE:
                    slices.append(_slice_double(src, start, end))
                elif col.data_type is DataType.BINARY:
                    slices.append(_slice_binary(src, start, end))
                else:
                    raise ValueError(f"unsupported data_type {col.data_type}")

            compressed = [
                _compress_column(self._series_columns[i], slices[i], rg_rows)
                for i in range(len(self._series_columns))
            ]
            crcs = [zlib.crc32(b) & 0xFFFFFFFF for b in compressed]
            self._row_groups.append(
                _RowGroupData(
                    row_count=rg_rows,
                    ts_min=ts_min if ts_min is not None else 0,
                    ts_max=ts_max if ts_max is not None else 0,
                    compressed_columns=compressed,
                    crcs=crcs,
                )
            )
        return self

    def write_events(self, row_count: int, *column_data: Any) -> "LastraWriter":
        if len(column_data) != len(self._event_columns):
            raise ValueError(
                f"writeEvents got {len(column_data)} arrays but {len(self._event_columns)} "
                f"event columns are registered"
            )
        self._events_row_count = row_count
        # Keep raw arrays — events compress as a single block on close().
        self._event_columns_data = list(column_data)
        return self

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def to_bytes(self) -> bytes:
        """Build and return the full ``.lastra`` blob."""
        has_events = bool(self._event_columns) and self._events_row_count > 0
        has_row_groups = len(self._row_groups) > 1

        flags = FLAG_HAS_FOOTER | FLAG_HAS_CHECKSUMS
        if has_events:
            flags |= FLAG_HAS_EVENTS
        if has_row_groups:
            flags |= FLAG_HAS_ROW_GROUPS

        body = bytearray()

        header = Header(
            version=_format.VERSION,
            flags=flags,
            series_row_count=self._series_row_count,
            series_col_count=len(self._series_columns),
            events_row_count=self._events_row_count if has_events else 0,
            events_col_count=len(self._event_columns) if has_events else 0,
        )
        body.extend(write_header(header))

        body.extend(_desc.write_descriptors(self._series_columns))
        if has_events:
            body.extend(_desc.write_descriptors(self._event_columns))

        data_start = len(body)

        rg_offsets: list[int] = []
        if has_row_groups:
            for rg in self._row_groups:
                rg_offsets.append(len(body) - data_start)
                for col_data in rg.compressed_columns:
                    body.extend(_int_le(len(col_data)))
                    body.extend(col_data)
        elif self._row_groups:
            for col_data in self._row_groups[0].compressed_columns:
                body.extend(_int_le(len(col_data)))
                body.extend(col_data)

        event_offsets: list[int] = []
        event_crcs: list[int] = []
        if has_events:
            for i, col in enumerate(self._event_columns):
                blob = _compress_column(col, self._event_columns_data[i], self._events_row_count)
                event_offsets.append(len(body) - data_start)
                body.extend(_int_le(len(blob)))
                body.extend(blob)
                event_crcs.append(zlib.crc32(blob) & 0xFFFFFFFF)

        footer_start = len(body)
        if has_row_groups:
            body.extend(_int_le(len(self._row_groups)))
            for i, rg in enumerate(self._row_groups):
                body.extend(_int_le(rg_offsets[i]))
                body.extend(_int_le(rg.row_count))
                body.extend(_long_le(rg.ts_min))
                body.extend(_long_le(rg.ts_max))
            for rg in self._row_groups:
                for crc in rg.crcs:
                    body.extend(_int_le(crc))
            for offset in event_offsets:
                body.extend(_int_le(offset))
            for crc in event_crcs:
                body.extend(_int_le(crc))
        elif self._row_groups:
            rg = self._row_groups[0]
            pos = 0
            for col_data in rg.compressed_columns:
                body.extend(_int_le(pos))
                pos += 4 + len(col_data)
            for offset in event_offsets:
                body.extend(_int_le(offset))
            for crc in rg.crcs:
                body.extend(_int_le(crc))
            for crc in event_crcs:
                body.extend(_int_le(crc))
        else:
            for offset in event_offsets:
                body.extend(_int_le(offset))
            for crc in event_crcs:
                body.extend(_int_le(crc))

        footer_size = len(body) - footer_start
        body.extend(_int_le(FOOTER_MAGIC))
        body.extend(_int_le(footer_size))

        return bytes(body)

    def write_to(self, stream: BinaryIO) -> int:
        """Write the blob to ``stream`` and return the number of bytes written."""
        blob = self.to_bytes()
        stream.write(blob)
        return len(blob)


# ----------------------------------------------------------------------
# Internals
# ----------------------------------------------------------------------


def _int_le(value: int) -> bytes:
    return (value & 0xFFFFFFFF).to_bytes(4, "little", signed=False)


def _long_le(value: int) -> bytes:
    return (value & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little", signed=False)


def _slice_long(src: Any, start: int, end: int) -> np.ndarray:
    arr = np.asarray(src, dtype=np.int64)
    return np.ascontiguousarray(arr[start:end])


def _slice_double(src: Any, start: int, end: int) -> np.ndarray:
    arr = np.asarray(src, dtype=np.float64)
    return np.ascontiguousarray(arr[start:end])


def _slice_binary(src: Any, start: int, end: int) -> list[bytes | None]:
    if isinstance(src, list):
        return list(src[start:end])
    return [src[i] for i in range(start, end)]


def _compress_column(col: ColumnDescriptor, data: Any, count: int) -> bytes:
    if col.data_type is DataType.LONG:
        arr = np.asarray(data, dtype=np.int64)[:count]
        if col.codec is Codec.DELTA_VARINT:
            return delta_varint.encode(arr)
        if col.codec is Codec.RAW:
            return raw.encode_long(arr)
        raise ValueError(f"codec {col.codec.name} is not valid for LONG columns")

    if col.data_type is DataType.DOUBLE:
        arr = np.asarray(data, dtype=np.float64)[:count]
        if col.codec is Codec.ALP:
            return alp.encode(arr)
        if col.codec is Codec.GORILLA:
            return gorilla.encode(arr)
        if col.codec is Codec.PONGO:
            return pongo.encode(arr)
        if col.codec is Codec.RAW:
            return raw.encode_double(arr)
        raise ValueError(f"codec {col.codec.name} is not valid for DOUBLE columns")

    if col.data_type is DataType.BINARY:
        values: Sequence[bytes | None] = data if isinstance(data, list) else list(data)
        values = list(values[:count])
        if col.codec is Codec.VARLEN:
            return varlen.encode(values, varlen.COMPRESSION_NONE)
        if col.codec is Codec.VARLEN_ZSTD:
            return varlen.encode(values, varlen.COMPRESSION_ZSTD)
        if col.codec is Codec.VARLEN_GZIP:
            return varlen.encode(values, varlen.COMPRESSION_GZIP)
        raise ValueError(f"codec {col.codec.name} is not valid for BINARY columns")

    raise ValueError(f"unsupported data_type {col.data_type}")
