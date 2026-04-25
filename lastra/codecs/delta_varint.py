"""``DELTA_VARINT`` codec — delta-of-delta + zigzag varint for ``LONG`` columns.

Wire format::

    [8 bytes] first value (little-endian, signed int64)
    [varint]  first delta (zigzag)
    [varint]  delta-of-delta[2] (zigzag)
    ...

Best for monotonically increasing or near-regular timestamps where the
second derivative is mostly zero — the typical case for sample-grid
sensors and exchange tick timestamps.

Byte-exact compatible with
:class:`com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec` (lastra-java).
"""

from __future__ import annotations

from typing import Sequence

import numpy as np

# 64-bit signed range to validate inputs come back unchanged after a round-trip.
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

_MASK64 = (1 << 64) - 1
_MASK7 = 0x7F
_HIGH_BIT = 0x80


def _zigzag_encode(value: int) -> int:
    """Map signed int64 → unsigned int64 such that small magnitudes give small bytes."""
    # Match Java's `(value << 1) ^ (value >> 63)` exactly using 64-bit math.
    v = value & _MASK64
    return ((v << 1) ^ (-(v >> 63) & _MASK64)) & _MASK64


def _zigzag_decode(encoded: int) -> int:
    """Inverse of :func:`_zigzag_encode`. Result is a signed Python int.

    The XOR with ``-(encoded & 1)`` already produces the correct signed
    Python value (Python ints carry sign at any precision), so no
    additional correction is needed.
    """
    return (encoded >> 1) ^ -(encoded & 1)


def _write_varint(buf: bytearray, value: int) -> None:
    """LEB128-style 7-bit-per-byte unsigned varint."""
    while (value & ~_MASK7) != 0:
        buf.append((value & _MASK7) | _HIGH_BIT)
        value >>= 7
    buf.append(value & _MASK7)


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Returns ``(value, new_pos)`` after reading one varint at ``data[pos:]``."""
    result = 0
    shift = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & _MASK7) << shift
        if (b & _HIGH_BIT) == 0:
            break
        shift += 7
    return result, pos


def encode(values: Sequence[int] | np.ndarray) -> bytes:
    """Encode ``values`` to the DELTA_VARINT wire format. Empty input → ``b""``."""
    n = len(values)
    if n == 0:
        return b""

    # Promote to a numpy int64 view for fast indexing without losing sign.
    arr = np.asarray(values, dtype=np.int64)

    out = bytearray()
    first = int(arr[0])
    if not (_INT64_MIN <= first <= _INT64_MAX):
        raise ValueError(f"value out of int64 range: {first}")
    out.extend(int(first & _MASK64).to_bytes(8, "little", signed=False))

    if n > 1:
        prev_delta = int(arr[1]) - int(arr[0])
        _write_varint(out, _zigzag_encode(prev_delta))
        for i in range(2, n):
            delta = int(arr[i]) - int(arr[i - 1])
            dod = delta - prev_delta
            _write_varint(out, _zigzag_encode(dod))
            prev_delta = delta

    return bytes(out)


def decode(data: bytes, count: int) -> np.ndarray:
    """Decode ``count`` int64 values from the DELTA_VARINT wire format."""
    if count == 0:
        return np.empty(0, dtype=np.int64)
    if len(data) < 8:
        raise ValueError(
            f"truncated DELTA_VARINT payload: need at least 8 bytes for the "
            f"first value, got {len(data)}"
        )

    out = np.empty(count, dtype=np.int64)
    first_unsigned = int.from_bytes(data[:8], "little", signed=False)
    # Reinterpret as signed int64.
    if first_unsigned & (1 << 63):
        first = first_unsigned - (1 << 64)
    else:
        first = first_unsigned
    out[0] = first

    pos = 8
    if count > 1:
        prev_delta_zz, pos = _read_varint(data, pos)
        prev_delta = _zigzag_decode(prev_delta_zz)
        out[1] = out[0] + prev_delta
        for i in range(2, count):
            dod_zz, pos = _read_varint(data, pos)
            dod = _zigzag_decode(dod_zz)
            delta = prev_delta + dod
            out[i] = out[i - 1] + delta
            prev_delta = delta

    return out
