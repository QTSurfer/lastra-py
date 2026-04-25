"""``PONGO`` codec — decimal erasure + Gorilla XOR for ``DOUBLE`` columns.

Based on Shen et al. (CCDS 2025). Pongo zeros trailing mantissa bits that
are safely erasable for "decimal-native" doubles (prices, sensor readings)
before applying Gorilla XOR — typical compression ~18 bits per value on
2-decimal prices versus ~32-40 bits with plain Gorilla.

Per-row flag protocol::

    "0"        (1 bit)  → erased, same dp as previous row
    "10"       (2 bits) → not erased (special value or non-decimal-native)
    "11XXXXX"  (7 bits) → erased, new dp (5-bit value, 0–18)

Wire format::

    [4 bytes LE] count
    [bitstream] first value as raw 64 bits, then per-row (flag + Gorilla XOR)

Byte-exact compatible with
:class:`com.wualabs.qtsurfer.lastra.codec.PongoCodec` (lastra-java).
"""

from __future__ import annotations

import math
from typing import Sequence

import numpy as np

from . import _pongo_eraser as eraser
from .gorilla import _BitReader, _BitWriter, _bits_to_double, _double_to_bits

_MASK64 = (1 << 64) - 1
_MIN_ERASABLE_BITS = 5


def _leading_zeros_64(x: int) -> int:
    if x == 0:
        return 64
    return 64 - x.bit_length()


def _trailing_zeros_64(x: int) -> int:
    if x == 0:
        return 64
    return ((x & -x).bit_length()) - 1


def encode(values: Sequence[float] | np.ndarray) -> bytes:
    n = len(values)
    if n == 0:
        return b""
    arr = np.ascontiguousarray(np.asarray(values, dtype=np.float64))

    w = _BitWriter(initial_bytes=n * 2)
    w.write_raw_int(n)

    stored_val = _double_to_bits(float(arr[0]))
    w.write_bits(stored_val, 64)

    stored_lead = 1 << 31  # Java's Integer.MAX_VALUE sentinel
    stored_trail = 0
    last_dp = 1 << 31

    for i in range(1, n):
        value = float(arr[i])
        bits = _double_to_bits(value)

        bits_to_xor = bits
        erased = False
        dp = -1

        if value != 0.0 and not math.isinf(value) and not math.isnan(value):
            dp = eraser.detect_decimal_places(value)
            if dp >= 0:
                erasable = eraser.compute_erasable_bits(value, dp)
                if erasable >= _MIN_ERASABLE_BITS:
                    erased_bits = eraser.erase_bits(bits, erasable)
                    if eraser.restore(erased_bits, dp) == bits:
                        bits_to_xor = erased_bits
                        erased = True

        # Pongo flag header for this row
        if not erased:
            w.write_bit(1)
            w.write_bit(0)
        elif dp == last_dp:
            w.write_bit(0)
        else:
            w.write_bit(1)
            w.write_bit(1)
            w.write_bits(dp, 5)
            last_dp = dp

        # Gorilla XOR section
        xor = (stored_val ^ bits_to_xor) & _MASK64
        if xor == 0:
            w.write_bit(0)
        else:
            lead = _leading_zeros_64(xor)
            trail = _trailing_zeros_64(xor)
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

        stored_val = bits_to_xor

    return w.to_bytes()


def decode(data: bytes, count: int) -> np.ndarray:
    if count == 0:
        return np.empty(0, dtype=np.float64)

    r = _BitReader(data)
    stored_count = r.read_raw_int()
    if stored_count != count:
        raise ValueError(
            f"Pongo count mismatch: header says {stored_count}, expected {count}"
        )

    out = np.empty(count, dtype=np.float64)
    stored_val = r.read_bits(64)
    out[0] = _bits_to_double(stored_val)

    stored_lead = 1 << 31
    stored_trail = 0
    last_dp = 1 << 31

    for i in range(1, count):
        # Pongo flag
        flag = r.read_bit()
        if flag == 0:
            erased = True
            dp = last_dp
        else:
            second = r.read_bit()
            if second == 0:
                erased = False
                dp = last_dp
            else:
                erased = True
                dp = r.read_bits(5)
                last_dp = dp

        # Gorilla XOR
        if r.read_bit() == 0:
            pass  # same as previous stored_val
        else:
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

        if erased:
            restored_bits = eraser.restore(stored_val, dp)
            out[i] = _bits_to_double(restored_bits)
        else:
            out[i] = _bits_to_double(stored_val)

    return out
