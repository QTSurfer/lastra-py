"""``VARLEN`` family — variable-length binary columns with optional ZSTD / gzip.

Wire format (matches ``com.wualabs.qtsurfer.lastra.codec.VarlenCodec``)::

    [1 byte]  compression marker (0=NONE, 1=ZSTD, 2=GZIP)
    NONE:
      [int32 LE] payloadLength
      [payloadLength bytes] payload
    ZSTD / GZIP:
      [int32 LE] uncompressedLength
      [int32 LE] compressedLength
      [compressedLength bytes] compressed payload

The decoded payload is a flat sequence of records::

    per row in [0, count):
      [int32 LE] length    # -1 = NULL row
      [length bytes] data  # absent if length == -1

NULL is preserved as Python ``None`` in the decoded list.
"""

from __future__ import annotations

import gzip
import io
from typing import Sequence

import zstandard as zstd

# Compression marker values — same as VarlenCodec.COMPRESSION_*.
COMPRESSION_NONE = 0
COMPRESSION_ZSTD = 1
COMPRESSION_GZIP = 2


def _build_payload(values: Sequence[bytes | None]) -> bytes:
    out = bytearray()
    for v in values:
        if v is None:
            out.extend((-1 & 0xFFFFFFFF).to_bytes(4, "little", signed=False))
        else:
            length = len(v)
            out.extend(length.to_bytes(4, "little", signed=True))
            out.extend(v)
    return bytes(out)


def _split_payload(payload: bytes, count: int) -> list[bytes | None]:
    out: list[bytes | None] = []
    pos = 0
    for _ in range(count):
        if pos + 4 > len(payload):
            raise ValueError(
                f"truncated VARLEN payload: length prefix missing at byte {pos}"
            )
        length = int.from_bytes(payload[pos : pos + 4], "little", signed=True)
        pos += 4
        if length < 0:
            out.append(None)
            continue
        end = pos + length
        if end > len(payload):
            raise ValueError(
                f"truncated VARLEN payload: row {len(out)} expects {length} bytes, "
                f"only {len(payload) - pos} remaining"
            )
        out.append(bytes(payload[pos:end]))
        pos = end
    return out


def encode(values: Sequence[bytes | None], compression: int = COMPRESSION_NONE) -> bytes:
    """Serialise ``values`` with the requested block compression marker.

    ``None`` entries become NULL on the wire (length = -1).
    """
    payload = _build_payload(values)
    out = bytearray()
    out.append(compression & 0xFF)

    if compression == COMPRESSION_NONE:
        out.extend(len(payload).to_bytes(4, "little", signed=True))
        out.extend(payload)
        return bytes(out)

    if compression == COMPRESSION_ZSTD:
        compressed = zstd.ZstdCompressor().compress(payload)
    elif compression == COMPRESSION_GZIP:
        # mtime=0 keeps gzip output deterministic for byte-equality tests.
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
            gz.write(payload)
        compressed = buf.getvalue()
    else:
        raise ValueError(f"unknown VARLEN compression marker: {compression}")

    out.extend(len(payload).to_bytes(4, "little", signed=True))
    out.extend(len(compressed).to_bytes(4, "little", signed=True))
    out.extend(compressed)
    return bytes(out)


def decode(data: bytes, count: int) -> list[bytes | None]:
    """Deserialise a VARLEN blob, returning a list of ``bytes | None`` (length ``count``)."""
    if count == 0:
        return []
    if not data:
        raise ValueError("truncated VARLEN blob: empty buffer")

    compression = data[0]
    pos = 1

    if compression == COMPRESSION_NONE:
        if pos + 4 > len(data):
            raise ValueError("truncated VARLEN-NONE blob: missing payload length")
        payload_len = int.from_bytes(data[pos : pos + 4], "little", signed=True)
        pos += 4
        if pos + payload_len > len(data):
            raise ValueError("truncated VARLEN-NONE blob: payload shorter than length")
        payload = data[pos : pos + payload_len]
    elif compression in (COMPRESSION_ZSTD, COMPRESSION_GZIP):
        if pos + 8 > len(data):
            raise ValueError("truncated VARLEN compressed blob: missing length pair")
        uncompressed_len = int.from_bytes(data[pos : pos + 4], "little", signed=True)
        compressed_len = int.from_bytes(data[pos + 4 : pos + 8], "little", signed=True)
        pos += 8
        if pos + compressed_len > len(data):
            raise ValueError("truncated VARLEN compressed blob: compressed body short")
        compressed = data[pos : pos + compressed_len]
        if compression == COMPRESSION_ZSTD:
            payload = zstd.ZstdDecompressor().decompress(compressed, max_output_size=uncompressed_len)
        else:
            payload = gzip.decompress(compressed)
        if len(payload) != uncompressed_len:
            raise ValueError(
                f"VARLEN payload size mismatch: expected {uncompressed_len}, "
                f"got {len(payload)}"
            )
    else:
        raise ValueError(f"unknown VARLEN compression marker: {compression}")

    return _split_payload(payload, count)
