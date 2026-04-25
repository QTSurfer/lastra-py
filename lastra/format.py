"""Wire-format constants and dataclasses.

Mirrors :class:`com.wualabs.qtsurfer.lastra.Lastra` exactly. All numeric
constants come straight from the Java reference and the file format is
documented in ``FORMAT.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum

# ASCII "LAST" / "LAS!" as little-endian uint32. Stored as the first / last
# four bytes of the file respectively.
MAGIC: int = 0x4C415354
FOOTER_MAGIC: int = 0x4C415321

VERSION: int = 1

EXTENSION: str = "lastra"


class DataType(IntEnum):
    LONG = 0
    DOUBLE = 1
    BINARY = 2


class Codec(IntEnum):
    RAW = 0
    DELTA_VARINT = 1
    ALP = 2
    VARLEN = 3
    VARLEN_ZSTD = 4
    VARLEN_GZIP = 5
    GORILLA = 6
    PONGO = 7


# Header flag bits. The flags byte at offset 5 holds the bitwise OR of these.
FLAG_HAS_EVENTS: int = 1 << 0
FLAG_HAS_FOOTER: int = 1 << 1
FLAG_HAS_CHECKSUMS: int = 1 << 2
FLAG_HAS_ROW_GROUPS: int = 1 << 3


HEADER_SIZE: int = 22  # bytes


@dataclass(frozen=True)
class Header:
    """Parsed 22-byte file header."""

    version: int
    flags: int
    series_row_count: int
    series_col_count: int
    events_row_count: int
    events_col_count: int

    @property
    def has_events(self) -> bool:
        return bool(self.flags & FLAG_HAS_EVENTS)

    @property
    def has_footer(self) -> bool:
        return bool(self.flags & FLAG_HAS_FOOTER)

    @property
    def has_checksums(self) -> bool:
        return bool(self.flags & FLAG_HAS_CHECKSUMS)

    @property
    def has_row_groups(self) -> bool:
        return bool(self.flags & FLAG_HAS_ROW_GROUPS)


@dataclass(frozen=True)
class ColumnDescriptor:
    """Per-column metadata read from the descriptor section.

    ``metadata`` is the optional gzip-compressed JSON map decoded into a
    plain ``dict[str, str]``. Empty when the column has no metadata.
    """

    codec: Codec
    data_type: DataType
    flags: int
    name: str
    metadata: dict[str, str] = field(default_factory=dict)


def parse_header(buf: bytes) -> Header:
    """Parse the first :data:`HEADER_SIZE` bytes of a Lastra file.

    Wire layout (little-endian throughout)::

        offset 0   4 bytes  MAGIC
        offset 4   2 bytes  version (uint16)
        offset 6   2 bytes  flags   (uint16)
        offset 8   4 bytes  seriesRowCount (uint32)
        offset 12  4 bytes  seriesColCount (uint32)
        offset 16  4 bytes  eventsRowCount (uint32)
        offset 20  2 bytes  eventsColCount (uint16)
    """
    if len(buf) < HEADER_SIZE:
        raise ValueError(
            f"truncated header: need {HEADER_SIZE} bytes, got {len(buf)}"
        )

    magic = int.from_bytes(buf[0:4], "little", signed=False)
    if magic != MAGIC:
        raise ValueError(
            f"not a Lastra file: magic 0x{magic:08X} != expected 0x{MAGIC:08X}"
        )

    version = int.from_bytes(buf[4:6], "little", signed=False)
    flags = int.from_bytes(buf[6:8], "little", signed=False)
    if version > VERSION:
        raise ValueError(f"unsupported Lastra version {version}; this build supports {VERSION}")

    series_row_count = int.from_bytes(buf[8:12], "little", signed=False)
    series_col_count = int.from_bytes(buf[12:16], "little", signed=False)
    events_row_count = int.from_bytes(buf[16:20], "little", signed=False)
    events_col_count = int.from_bytes(buf[20:22], "little", signed=False)

    return Header(
        version=version,
        flags=flags,
        series_row_count=series_row_count,
        series_col_count=series_col_count,
        events_row_count=events_row_count,
        events_col_count=events_col_count,
    )


def write_header(header: Header) -> bytes:
    """Serialise a :class:`Header` to its 22-byte little-endian wire form."""
    buf = bytearray(HEADER_SIZE)
    buf[0:4] = MAGIC.to_bytes(4, "little", signed=False)
    buf[4:6] = (header.version & 0xFFFF).to_bytes(2, "little", signed=False)
    buf[6:8] = (header.flags & 0xFFFF).to_bytes(2, "little", signed=False)
    buf[8:12] = (header.series_row_count & 0xFFFFFFFF).to_bytes(4, "little", signed=False)
    buf[12:16] = (header.series_col_count & 0xFFFFFFFF).to_bytes(4, "little", signed=False)
    buf[16:20] = (header.events_row_count & 0xFFFFFFFF).to_bytes(4, "little", signed=False)
    buf[20:22] = (header.events_col_count & 0xFFFF).to_bytes(2, "little", signed=False)
    return bytes(buf)
