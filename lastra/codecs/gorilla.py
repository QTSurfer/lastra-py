"""``GORILLA`` codec — Facebook Gorilla XOR compression for ``DOUBLE`` columns.

Reference: T. Pelkonen et al., *"Gorilla: A Fast, Scalable, In-Memory Time
Series Database"*, VLDB 2015. Lastra uses the value path only — timestamp
encoding is handled separately by :mod:`lastra.codecs.delta_varint`.

Algorithm (per row after the first)::

    xor = previous_value_bits ^ current_value_bits
    if xor == 0:
        emit 1 bit "0"                       # identical to previous
    else:
        emit 1 bit "1"
        if leading and trailing zero counts fit the previous window:
            emit 1 bit "0"
            emit `64 - prev_lead - prev_trail` significant bits
        else:
            emit 1 bit "1"
            emit 5 bits  → leading zero count (capped at 31)
            emit 6 bits  → significant bit count (0 means 64)
            emit `significant` bits

Wire format::

    [4 bytes LE]  count (must equal the row count requested by the reader)
    [bitstream]   first value as 64 raw bits, then the per-row codes above

Byte-exact compatible with
:class:`com.wualabs.qtsurfer.lastra.codec.GorillaCodec` (lastra-java).
"""

from __future__ import annotations

import struct
from typing import Sequence

import numpy as np

# Reuse Python ints throughout — Gorilla operates on 64-bit IEEE-754 raw bits
# stored as unsigned 64-bit integers.
_MASK64 = (1 << 64) - 1


def _double_to_bits(v: float) -> int:
    return int.from_bytes(struct.pack("<d", v), "little", signed=False)


def _bits_to_double(b: int) -> float:
    return struct.unpack("<d", (b & _MASK64).to_bytes(8, "little", signed=False))[0]


def _leading_zeros(x: int) -> int:
    """Number of leading zeros in a 64-bit value (0..64)."""
    if x == 0:
        return 64
    return 64 - x.bit_length()


def _trailing_zeros(x: int) -> int:
    """Number of trailing zeros in a 64-bit value (0..64)."""
    if x == 0:
        return 64
    # `(x & -x).bit_length() - 1` works for positive Python ints.
    return ((x & -x).bit_length()) - 1


class _BitWriter:
    """Minimal bit-level appender. Output bytes are MSB-first within each byte
    to match :class:`GorillaCodec.BitWriter` byte-for-byte."""

    __slots__ = ("buf", "_byte_pos", "_bit_pos")

    def __init__(self, initial_bytes: int) -> None:
        self.buf = bytearray(max(initial_bytes, 16))
        self._byte_pos = 0
        # `_bit_pos` is the bit index of the NEXT bit within the current byte,
        # counted from the MSB. 8 = fresh byte; 1 = last bit; 0 → roll to next.
        self._bit_pos = 8

    def write_raw_int(self, value: int) -> None:
        self._ensure(4)
        v = value & 0xFFFFFFFF
        self.buf[self._byte_pos] = v & 0xFF
        self.buf[self._byte_pos + 1] = (v >> 8) & 0xFF
        self.buf[self._byte_pos + 2] = (v >> 16) & 0xFF
        self.buf[self._byte_pos + 3] = (v >> 24) & 0xFF
        self._byte_pos += 4
        self._bit_pos = 8

    def write_bit(self, bit: int) -> None:
        self._ensure(1)
        if self._bit_pos == 8:
            self.buf[self._byte_pos] = 0
        if bit:
            self.buf[self._byte_pos] |= 1 << (self._bit_pos - 1)
        self._bit_pos -= 1
        if self._bit_pos == 0:
            self._byte_pos += 1
            self._bit_pos = 8

    def write_bits(self, value: int, num_bits: int) -> None:
        # Java emits MSB-first within `value`.
        for i in range(num_bits - 1, -1, -1):
            self.write_bit((value >> i) & 1)

    def to_bytes(self) -> bytes:
        used = self._byte_pos if self._bit_pos == 8 else self._byte_pos + 1
        return bytes(self.buf[:used])

    def _ensure(self, extra: int) -> None:
        needed = self._byte_pos + extra + 1
        if needed > len(self.buf):
            new_size = max(len(self.buf) * 2, needed)
            new_buf = bytearray(new_size)
            new_buf[: self._byte_pos + 1] = self.buf[: self._byte_pos + 1]
            self.buf = new_buf


class _BitReader:
    __slots__ = ("buf", "_byte_pos", "_bit_pos")

    def __init__(self, data: bytes) -> None:
        self.buf = data
        self._byte_pos = 0
        self._bit_pos = 8

    def read_raw_int(self) -> int:
        v = (
            self.buf[self._byte_pos]
            | (self.buf[self._byte_pos + 1] << 8)
            | (self.buf[self._byte_pos + 2] << 16)
            | (self.buf[self._byte_pos + 3] << 24)
        )
        self._byte_pos += 4
        self._bit_pos = 8
        return v

    def read_bit(self) -> int:
        bit = (self.buf[self._byte_pos] >> (self._bit_pos - 1)) & 1
        self._bit_pos -= 1
        if self._bit_pos == 0:
            self._byte_pos += 1
            self._bit_pos = 8
        return bit

    def read_bits(self, num_bits: int) -> int:
        result = 0
        for _ in range(num_bits):
            result = (result << 1) | self.read_bit()
        return result


def encode(values: Sequence[float] | np.ndarray) -> bytes:
    """Encode ``values`` into Gorilla's bitstream format. Empty input → ``b""``."""
    n = len(values)
    if n == 0:
        return b""
    arr = np.ascontiguousarray(np.asarray(values, dtype=np.float64))

    w = _BitWriter(initial_bytes=n * 2)
    w.write_raw_int(n)

    stored_val = _double_to_bits(float(arr[0]))
    w.write_bits(stored_val, 64)

    stored_lead = 1 << 31  # Integer.MAX_VALUE sentinel — first XOR is forced into "new window".
    stored_trail = 0

    for i in range(1, n):
        val = _double_to_bits(float(arr[i]))
        xor = (stored_val ^ val) & _MASK64

        if xor == 0:
            w.write_bit(0)
        else:
            lead = _leading_zeros(xor)
            trail = _trailing_zeros(xor)
            # Cap leading-zeros to 5-bit range, matches Java behaviour.
            if lead >= 32:
                lead = 31

            w.write_bit(1)
            if lead >= stored_lead and trail >= stored_trail:
                w.write_bit(0)
                significant = 64 - stored_lead - stored_trail
                w.write_bits((xor >> stored_trail) & _MASK64, significant)
            else:
                w.write_bit(1)
                w.write_bits(lead, 5)
                significant = 64 - lead - trail
                w.write_bits(significant, 6)
                w.write_bits((xor >> trail) & _MASK64, significant)
                stored_lead = lead
                stored_trail = trail

        stored_val = val

    return w.to_bytes()


def decode(data: bytes, count: int) -> np.ndarray:
    """Decode ``count`` doubles from a Gorilla bitstream blob."""
    if count == 0:
        return np.empty(0, dtype=np.float64)

    r = _BitReader(data)
    stored_count = r.read_raw_int()
    if stored_count != count:
        raise ValueError(
            f"Gorilla count mismatch: header says {stored_count}, expected {count}"
        )

    out = np.empty(count, dtype=np.float64)
    stored_val = r.read_bits(64)
    out[0] = _bits_to_double(stored_val)

    stored_lead = 1 << 31
    stored_trail = 0

    for i in range(1, count):
        if r.read_bit() == 0:
            out[i] = _bits_to_double(stored_val)
            continue

        if r.read_bit() == 1:
            stored_lead = r.read_bits(5)
            significant = r.read_bits(6)
            if significant == 0:
                significant = 64
            stored_trail = 64 - significant - stored_lead

        significant = 64 - stored_lead - stored_trail
        value = r.read_bits(significant)
        value = (value << stored_trail) & _MASK64
        stored_val = (stored_val ^ value) & _MASK64
        out[i] = _bits_to_double(stored_val)

    return out
