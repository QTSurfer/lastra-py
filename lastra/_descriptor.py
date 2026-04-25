"""Column descriptor encode/decode.

The descriptor wire layout is documented in ``FORMAT.md``. Header values
match the lastra-java reference byte-for-byte, including the deliberately
minimal JSON metadata format (``{"k":"v",...}`` — no escapes, no nested
structures, no whitespace) that the Java reader's split-based parser
expects.
"""

from __future__ import annotations

from typing import Mapping

from .format import Codec, ColumnDescriptor, DataType

# Bit set in colFlags when the descriptor carries metadata bytes.
_COL_FLAG_HAS_METADATA = 0x02


class _Cursor:
    """Tiny helper for advancing through a byte buffer with bounds checking."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes, pos: int = 0) -> None:
        self.buf = buf
        self.pos = pos

    def read(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.buf):
            raise ValueError(
                f"truncated column descriptor: needed {n} bytes at pos {self.pos}, "
                f"only {len(self.buf) - self.pos} remaining"
            )
        chunk = self.buf[self.pos : end]
        self.pos = end
        return chunk

    def read_u8(self) -> int:
        return self.read(1)[0]

    def read_u16_le(self) -> int:
        return int.from_bytes(self.read(2), "little", signed=False)


def _serialise_metadata(meta: Mapping[str, str]) -> bytes:
    """Match the lastra-java ``mapToJson`` exactly — flat ``{"k":"v",...}``.

    No JSON escaping is applied (the Java side reciprocates with a naive
    split parser). Callers MUST sanitise their keys and values: no commas,
    no colons, no quote characters.
    """
    parts: list[str] = []
    for k, v in meta.items():
        if any(c in k for c in (",", ":", '"')):
            raise ValueError(f"metadata key contains a forbidden character: {k!r}")
        if any(c in v for c in (",", ":", '"')):
            raise ValueError(f"metadata value contains a forbidden character: {v!r}")
        parts.append(f'"{k}":"{v}"')
    return ("{" + ",".join(parts) + "}").encode("utf-8")


def _parse_metadata(blob: bytes) -> dict[str, str]:
    """Inverse of :func:`_serialise_metadata`. Mirrors lastra-java's
    minimal split-based parser."""
    text = blob.decode("utf-8").strip()
    if text.startswith("{"):
        text = text[1:]
    if text.endswith("}"):
        text = text[:-1]
    if not text:
        return {}
    out: dict[str, str] = {}
    for pair in text.split(","):
        k, _, v = pair.partition(":")
        out[k.strip().replace('"', "")] = v.strip().replace('"', "")
    return out


def write_descriptor(desc: ColumnDescriptor) -> bytes:
    """Serialise one descriptor to its wire bytes."""
    name_bytes = desc.name.encode("utf-8")
    if len(name_bytes) > 255:
        raise ValueError(
            f"column name {desc.name!r} encodes to {len(name_bytes)} bytes "
            f"but the descriptor only stores 1 byte for nameLen"
        )

    col_flags = _COL_FLAG_HAS_METADATA if desc.metadata else 0

    out = bytearray()
    out.append(int(desc.codec) & 0xFF)
    out.append(int(desc.data_type) & 0xFF)
    out.append(col_flags & 0xFF)
    out.append(len(name_bytes))
    out.extend(name_bytes)

    if col_flags & _COL_FLAG_HAS_METADATA:
        meta_bytes = _serialise_metadata(desc.metadata)
        if len(meta_bytes) > 0xFFFF:
            raise ValueError(
                f"metadata for column {desc.name!r} encodes to {len(meta_bytes)} "
                f"bytes; the descriptor only stores a uint16 metaLen"
            )
        out.extend(len(meta_bytes).to_bytes(2, "little", signed=False))
        out.extend(meta_bytes)

    return bytes(out)


def read_descriptor(buf: bytes, pos: int = 0) -> tuple[ColumnDescriptor, int]:
    """Parse one descriptor starting at ``pos``. Returns ``(descriptor, next_pos)``."""
    cursor = _Cursor(buf, pos)
    codec_id = cursor.read_u8()
    type_id = cursor.read_u8()
    col_flags = cursor.read_u8()
    name_len = cursor.read_u8()
    name = cursor.read(name_len).decode("utf-8")

    metadata: dict[str, str] = {}
    if col_flags & _COL_FLAG_HAS_METADATA:
        meta_len = cursor.read_u16_le()
        metadata = _parse_metadata(cursor.read(meta_len))

    desc = ColumnDescriptor(
        codec=Codec(codec_id),
        data_type=DataType(type_id),
        flags=col_flags,
        name=name,
        metadata=metadata,
    )
    return desc, cursor.pos


def write_descriptors(descriptors: list[ColumnDescriptor]) -> bytes:
    """Serialise a list of descriptors back-to-back."""
    return b"".join(write_descriptor(d) for d in descriptors)


def read_descriptors(buf: bytes, count: int, pos: int = 0) -> tuple[list[ColumnDescriptor], int]:
    """Parse exactly ``count`` descriptors starting at ``pos``."""
    out: list[ColumnDescriptor] = []
    for _ in range(count):
        desc, pos = read_descriptor(buf, pos)
        out.append(desc)
    return out, pos
