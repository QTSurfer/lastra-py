"""High-level :class:`LastraReader` — parses a complete ``.lastra`` blob.

Mirrors :class:`com.wualabs.qtsurfer.lastra.LastraReader` but presents a
Pythonic API: NumPy arrays for numeric columns, ``list[bytes | None]``
for binary columns, and a ``RowGroupStats`` dataclass for partition
metadata.
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from io import BytesIO
from typing import BinaryIO

import numpy as np

from . import _descriptor as _desc
from . import format as _format
from .codecs import alp, delta_varint, gorilla, pongo, raw, varlen
from .format import Codec, ColumnDescriptor, DataType, FOOTER_MAGIC, Header, parse_header

_TRAILER_SIZE = 8  # FOOTER_MAGIC (4) + footerSize (4)


@dataclass(frozen=True)
class RowGroupStats:
    """Per-row-group metadata stored in the footer."""

    row_count: int
    byte_offset: int
    ts_min: int
    ts_max: int


class LastraReader:
    """Random-access reader for Lastra files.

    Build one with :meth:`from_bytes` (zero-copy, recommended) or
    :meth:`from_stream` (consumes an :class:`io.BufferedReader`-like
    source). Then call :meth:`read_series_long` / :meth:`read_series_double`
    / :meth:`read_series_binary` for whole-column reads, or the
    ``read_row_group_*`` variants for HTTP-Range-style partial reads.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._header: Header = parse_header(data[: _format.HEADER_SIZE])
        pos = _format.HEADER_SIZE

        self._series_columns, pos = _desc.read_descriptors(
            data, count=self._header.series_col_count, pos=pos
        )
        if self._header.has_events:
            self._event_columns, pos = _desc.read_descriptors(
                data, count=self._header.events_col_count, pos=pos
            )
        else:
            self._event_columns = []

        self._data_offset = pos

        # Default empty containers
        self._series_offsets: list[int] = []
        self._event_offsets: list[int] = []
        self._series_crcs: list[int] = []
        self._event_crcs: list[int] = []
        self._series_data_pos: list[int] = []
        self._series_data_len: list[int] = []
        self._event_data_pos: list[int] = []
        self._event_data_len: list[int] = []
        self._row_groups: list[RowGroupStats] = []
        self._rg_col_pos: list[list[int]] = []
        self._rg_col_len: list[list[int]] = []
        self._rg_col_crcs: list[list[int]] = []

        if self._header.has_footer:
            self._parse_footer_and_data_index()

    @classmethod
    def from_bytes(cls, data: bytes) -> "LastraReader":
        """Build a reader directly from an in-memory blob."""
        return cls(data)

    @classmethod
    def from_stream(cls, stream: BinaryIO) -> "LastraReader":
        """Read the whole stream into memory and parse it."""
        if hasattr(stream, "read"):
            return cls(stream.read())
        raise TypeError("expected a binary stream with a .read() method")

    @staticmethod
    def read_footer_size(trailer: bytes) -> int:
        """Return the footer size encoded in the last 8 bytes of a file.

        ``-1`` if the trailer doesn't contain a Lastra footer magic.
        Useful for HTTP-Range planning: fetch the last 8 bytes, then the
        footer body, then only the columns/row-groups you actually need.
        """
        if len(trailer) < _TRAILER_SIZE:
            return -1
        magic = int.from_bytes(trailer[-_TRAILER_SIZE:-4], "little", signed=False)
        if magic != FOOTER_MAGIC:
            return -1
        return int.from_bytes(trailer[-4:], "little", signed=False)

    # ------------------------------------------------------------------
    # Public introspection
    # ------------------------------------------------------------------

    @property
    def header(self) -> Header:
        return self._header

    @property
    def series_columns(self) -> list[ColumnDescriptor]:
        return list(self._series_columns)

    @property
    def event_columns(self) -> list[ColumnDescriptor]:
        return list(self._event_columns)

    @property
    def series_row_count(self) -> int:
        return self._header.series_row_count

    @property
    def events_row_count(self) -> int:
        return self._header.events_row_count

    @property
    def has_checksums(self) -> bool:
        return self._header.has_checksums

    @property
    def row_group_count(self) -> int:
        return len(self._row_groups) if self._row_groups else 1

    @property
    def row_group_stats(self) -> list[RowGroupStats]:
        """Per-row-group stats. Empty for files written without row groups."""
        return list(self._row_groups)

    def get_series_column(self, name: str) -> ColumnDescriptor:
        return self._series_columns[self._find_index(self._series_columns, name)]

    def get_event_column(self, name: str) -> ColumnDescriptor:
        return self._event_columns[self._find_index(self._event_columns, name)]

    # ------------------------------------------------------------------
    # Series readers (whole column across all row groups)
    # ------------------------------------------------------------------

    def read_series_long(self, name: str) -> np.ndarray:
        idx = self._find_index(self._series_columns, name)
        if self._row_groups:
            return self._concat_long_across_row_groups(idx, name)
        col = self._series_columns[idx]
        blob = self._extract(self._series_data_pos[idx], self._series_data_len[idx],
                             self._series_crcs, idx, name)
        return _decode_long(blob, self._header.series_row_count, col.codec)

    def read_series_double(self, name: str) -> np.ndarray:
        idx = self._find_index(self._series_columns, name)
        if self._row_groups:
            return self._concat_double_across_row_groups(idx, name)
        col = self._series_columns[idx]
        blob = self._extract(self._series_data_pos[idx], self._series_data_len[idx],
                             self._series_crcs, idx, name)
        return _decode_double(blob, self._header.series_row_count, col.codec)

    def read_series_binary(self, name: str) -> list[bytes | None]:
        idx = self._find_index(self._series_columns, name)
        if self._row_groups:
            return self._concat_binary_across_row_groups(idx, name)
        blob = self._extract(self._series_data_pos[idx], self._series_data_len[idx],
                             self._series_crcs, idx, name)
        return varlen.decode(blob, self._header.series_row_count)

    # ------------------------------------------------------------------
    # Row-group-scoped readers (range queries)
    # ------------------------------------------------------------------

    def read_row_group_long(self, rg_index: int, name: str) -> np.ndarray:
        idx = self._find_index(self._series_columns, name)
        col = self._series_columns[idx]
        blob = self._extract_row_group(rg_index, idx, name)
        return _decode_long(blob, self._row_groups[rg_index].row_count, col.codec)

    def read_row_group_double(self, rg_index: int, name: str) -> np.ndarray:
        idx = self._find_index(self._series_columns, name)
        col = self._series_columns[idx]
        blob = self._extract_row_group(rg_index, idx, name)
        return _decode_double(blob, self._row_groups[rg_index].row_count, col.codec)

    def read_row_group_binary(self, rg_index: int, name: str) -> list[bytes | None]:
        idx = self._find_index(self._series_columns, name)
        blob = self._extract_row_group(rg_index, idx, name)
        return varlen.decode(blob, self._row_groups[rg_index].row_count)

    # ------------------------------------------------------------------
    # Event readers
    # ------------------------------------------------------------------

    def read_event_long(self, name: str) -> np.ndarray:
        idx = self._find_index(self._event_columns, name)
        col = self._event_columns[idx]
        blob = self._extract(self._event_data_pos[idx], self._event_data_len[idx],
                             self._event_crcs, idx, name)
        return _decode_long(blob, self._header.events_row_count, col.codec)

    def read_event_double(self, name: str) -> np.ndarray:
        idx = self._find_index(self._event_columns, name)
        col = self._event_columns[idx]
        blob = self._extract(self._event_data_pos[idx], self._event_data_len[idx],
                             self._event_crcs, idx, name)
        return _decode_double(blob, self._header.events_row_count, col.codec)

    def read_event_binary(self, name: str) -> list[bytes | None]:
        idx = self._find_index(self._event_columns, name)
        blob = self._extract(self._event_data_pos[idx], self._event_data_len[idx],
                             self._event_crcs, idx, name)
        return varlen.decode(blob, self._header.events_row_count)

    # ==================================================================
    # Internals
    # ==================================================================

    def _parse_footer_and_data_index(self) -> None:
        """Walk the footer to fill column offsets, lengths and CRCs."""
        data = self._data
        if len(data) < _TRAILER_SIZE:
            raise ValueError("file too short to contain a Lastra trailer")
        trailer_magic = int.from_bytes(data[-_TRAILER_SIZE:-4], "little", signed=False)
        if trailer_magic != FOOTER_MAGIC:
            raise ValueError(
                f"missing footer trailer magic; got 0x{trailer_magic:08X}, "
                f"expected 0x{FOOTER_MAGIC:08X}"
            )
        footer_size = int.from_bytes(data[-4:], "little", signed=False)
        footer_pos = len(data) - _TRAILER_SIZE - footer_size

        if self._header.has_row_groups:
            self._parse_footer_with_row_groups(footer_pos)
        else:
            self._parse_footer_flat(footer_pos)

    def _parse_footer_flat(self, footer_pos: int) -> None:
        data = self._data
        series_count = self._header.series_col_count
        event_count = self._header.events_col_count
        has_crcs = self._header.has_checksums

        fp = footer_pos
        self._series_offsets = [
            int.from_bytes(data[fp + 4 * i : fp + 4 * (i + 1)], "little", signed=False)
            for i in range(series_count)
        ]
        fp += 4 * series_count

        self._event_offsets = [
            int.from_bytes(data[fp + 4 * i : fp + 4 * (i + 1)], "little", signed=False)
            for i in range(event_count)
        ]
        fp += 4 * event_count

        if has_crcs:
            self._series_crcs = [
                int.from_bytes(data[fp + 4 * i : fp + 4 * (i + 1)], "little", signed=False)
                for i in range(series_count)
            ]
            fp += 4 * series_count
            self._event_crcs = [
                int.from_bytes(data[fp + 4 * i : fp + 4 * (i + 1)], "little", signed=False)
                for i in range(event_count)
            ]
            fp += 4 * event_count

        # Index actual column positions by walking the data section's length prefixes.
        self._index_flat_data()

    def _parse_footer_with_row_groups(self, footer_pos: int) -> None:
        data = self._data
        series_count = self._header.series_col_count
        event_count = self._header.events_col_count
        has_crcs = self._header.has_checksums

        fp = footer_pos
        rg_count = int.from_bytes(data[fp : fp + 4], "little", signed=False)
        fp += 4

        for _ in range(rg_count):
            rg_offset = int.from_bytes(data[fp : fp + 4], "little", signed=False); fp += 4
            rg_rows = int.from_bytes(data[fp : fp + 4], "little", signed=False); fp += 4
            ts_min = int.from_bytes(data[fp : fp + 8], "little", signed=True); fp += 8
            ts_max = int.from_bytes(data[fp : fp + 8], "little", signed=True); fp += 8
            self._row_groups.append(
                RowGroupStats(row_count=rg_rows, byte_offset=rg_offset, ts_min=ts_min, ts_max=ts_max)
            )

        if has_crcs:
            for _ in range(rg_count):
                crcs = []
                for _ in range(series_count):
                    crcs.append(int.from_bytes(data[fp : fp + 4], "little", signed=False))
                    fp += 4
                self._rg_col_crcs.append(crcs)

        # Event offsets / CRCs sit after the per-RG stats.
        self._event_offsets = []
        for _ in range(event_count):
            self._event_offsets.append(int.from_bytes(data[fp : fp + 4], "little", signed=False))
            fp += 4
        if has_crcs:
            self._event_crcs = []
            for _ in range(event_count):
                self._event_crcs.append(int.from_bytes(data[fp : fp + 4], "little", signed=False))
                fp += 4

        # Walk row-group data to record per-RG / per-column positions and lengths.
        scan = self._data_offset
        for _ in range(rg_count):
            col_pos = []
            col_len = []
            for _ in range(series_count):
                length = int.from_bytes(data[scan : scan + 4], "little", signed=False)
                col_pos.append(scan + 4)
                col_len.append(length)
                scan += 4 + length
            self._rg_col_pos.append(col_pos)
            self._rg_col_len.append(col_len)

        # Events follow row groups in the data section.
        self._event_data_pos = []
        self._event_data_len = []
        for _ in range(event_count):
            length = int.from_bytes(data[scan : scan + 4], "little", signed=False)
            self._event_data_pos.append(scan + 4)
            self._event_data_len.append(length)
            scan += 4 + length

    def _index_flat_data(self) -> None:
        data = self._data
        pos = self._data_offset
        for _ in range(self._header.series_col_count):
            length = int.from_bytes(data[pos : pos + 4], "little", signed=False)
            self._series_data_pos.append(pos + 4)
            self._series_data_len.append(length)
            pos += 4 + length
        for _ in range(self._header.events_col_count):
            length = int.from_bytes(data[pos : pos + 4], "little", signed=False)
            self._event_data_pos.append(pos + 4)
            self._event_data_len.append(length)
            pos += 4 + length

    # --- Slice extraction with optional CRC verification --------------

    def _extract(self, pos: int, length: int, crcs: list[int], idx: int, name: str) -> bytes:
        blob = bytes(self._data[pos : pos + length])
        if self._header.has_checksums and idx < len(crcs):
            actual = zlib.crc32(blob)
            if actual != (crcs[idx] & 0xFFFFFFFF):
                raise ValueError(
                    f"CRC32 mismatch on column {name!r}: "
                    f"expected 0x{crcs[idx] & 0xFFFFFFFF:08X}, got 0x{actual:08X}"
                )
        return blob

    def _extract_row_group(self, rg_index: int, col_idx: int, name: str) -> bytes:
        pos = self._rg_col_pos[rg_index][col_idx]
        length = self._rg_col_len[rg_index][col_idx]
        blob = bytes(self._data[pos : pos + length])
        if self._header.has_checksums and rg_index < len(self._rg_col_crcs):
            expected = self._rg_col_crcs[rg_index][col_idx]
            actual = zlib.crc32(blob)
            if actual != (expected & 0xFFFFFFFF):
                raise ValueError(
                    f"CRC32 mismatch on RG {rg_index} column {name!r}: "
                    f"expected 0x{expected & 0xFFFFFFFF:08X}, got 0x{actual:08X}"
                )
        return blob

    # --- Cross-row-group concatenation --------------------------------

    def _concat_long_across_row_groups(self, col_idx: int, name: str) -> np.ndarray:
        out = np.empty(self._header.series_row_count, dtype=np.int64)
        offset = 0
        for rg in range(len(self._row_groups)):
            chunk = self.read_row_group_long(rg, name)
            out[offset : offset + chunk.size] = chunk
            offset += chunk.size
        return out

    def _concat_double_across_row_groups(self, col_idx: int, name: str) -> np.ndarray:
        out = np.empty(self._header.series_row_count, dtype=np.float64)
        offset = 0
        for rg in range(len(self._row_groups)):
            chunk = self.read_row_group_double(rg, name)
            out[offset : offset + chunk.size] = chunk
            offset += chunk.size
        return out

    def _concat_binary_across_row_groups(self, col_idx: int, name: str) -> list[bytes | None]:
        out: list[bytes | None] = []
        for rg in range(len(self._row_groups)):
            out.extend(self.read_row_group_binary(rg, name))
        return out

    # --- Helpers ------------------------------------------------------

    @staticmethod
    def _find_index(columns: list[ColumnDescriptor], name: str) -> int:
        for i, c in enumerate(columns):
            if c.name == name:
                return i
        raise KeyError(f"column not found: {name!r}")


# Module-level codec dispatchers — keeps :class:`LastraReader` short.

def _decode_long(blob: bytes, count: int, codec: Codec) -> np.ndarray:
    if codec is Codec.DELTA_VARINT:
        return delta_varint.decode(blob, count)
    if codec is Codec.RAW:
        return raw.decode_long(blob, count)
    raise ValueError(f"codec {codec.name} is not valid for LONG columns")


def _decode_double(blob: bytes, count: int, codec: Codec) -> np.ndarray:
    if codec is Codec.ALP:
        return alp.decode(blob, count)
    if codec is Codec.GORILLA:
        return gorilla.decode(blob, count)
    if codec is Codec.PONGO:
        return pongo.decode(blob, count)
    if codec is Codec.RAW:
        return raw.decode_double(blob, count)
    raise ValueError(f"codec {codec.name} is not valid for DOUBLE columns")
