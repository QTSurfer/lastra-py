"""Pongo decimal-aware bit eraser.

Based on Shen et al. (CCDS 2025). For ``decimal-native`` doubles (prices,
sensor readings) the trailing mantissa bits can be zeroed safely as long
as ``round(|v| * 10^dp) / 10^dp`` reproduces the exact same IEEE 754
bits — that condition is the ``detect_decimal_places`` contract.

This module is a Python port of
:class:`com.wualabs.qtsurfer.lastra.codec.PongoEraser`. Outputs match
byte-for-byte against ``PongoCodec``-generated Lastra files.
"""

from __future__ import annotations

import math
import struct

# Maximum decimal places we attempt to detect.
MAX_DECIMAL_PLACES = 18

_LOG2_10 = math.log(10) / math.log(2)

# Pre-computed powers of 10 for speed.
_POW10: tuple[int, ...] = tuple(10**i for i in range(MAX_DECIMAL_PLACES + 1))
_DPOW10: tuple[float, ...] = tuple(float(p) for p in _POW10)

_MASK64 = (1 << 64) - 1


def _bits(v: float) -> int:
    return int.from_bytes(struct.pack("<d", v), "little", signed=False)


def _from_bits(b: int) -> float:
    return struct.unpack("<d", (b & _MASK64).to_bytes(8, "little", signed=False))[0]


def _java_round(value: float) -> int:
    """Mimic Java's ``Math.round(double)``: ``floor(value + 0.5)``.

    Python's built-in ``round()`` uses banker's rounding, which doesn't
    match Java for half-even ties. Using ``floor(x + 0.5)`` gives the
    same integer result Java's ``Math.round`` produces.
    """
    return math.floor(value + 0.5)


def detect_decimal_places(value: float) -> int:
    """Return the number of decimal places needed to represent ``value`` exactly,
    or ``-1`` if no value in ``[0, MAX_DECIMAL_PLACES]`` reproduces the bits."""
    if value == 0.0 or math.isinf(value) or math.isnan(value):
        return -1
    abs_v = abs(value)
    original_bits = _bits(abs_v)
    for dp in range(MAX_DECIMAL_PLACES + 1):
        scaled = _java_round(abs_v * _DPOW10[dp])
        reconstructed = scaled / _DPOW10[dp]
        if _bits(reconstructed) == original_bits:
            return dp
    return -1


def compute_erasable_bits(value: float, dp: int) -> int:
    """How many trailing mantissa bits can be safely zeroed for the given dp."""
    if dp <= 0:
        return 0
    abs_v = abs(value)
    bits = _bits(abs_v)
    biased_exp = (bits >> 52) & 0x7FF
    exponent = biased_exp - 1023
    max_erasable = math.floor(52 - exponent - 1 - dp * _LOG2_10)
    return max(0, int(max_erasable))


def erase_bits(bits: int, erasable_bits: int) -> int:
    """Zero out ``erasable_bits`` lowest bits of a 64-bit raw double pattern."""
    if erasable_bits <= 0:
        return bits & _MASK64
    mask = (-1 << erasable_bits) & _MASK64
    return bits & mask


def restore(erased_bits: int, dp: int) -> int:
    """Inverse of :func:`erase_bits`. Recovers the original IEEE 754 bits
    using ``round(|v| * 10^dp) / 10^dp`` and re-applying the original sign."""
    if dp <= 0:
        return erased_bits & _MASK64
    erased = _from_bits(erased_bits)
    abs_v = abs(erased)
    scaled = _java_round(abs_v * _DPOW10[dp])
    restored = scaled / _DPOW10[dp]
    if erased < 0:
        restored = -restored
    return _bits(restored)
