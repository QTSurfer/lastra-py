"""``ALP`` codec — thin wrapper around the standalone :mod:`alp` package.

Lastra delegates the entire ALP wire format to ``alp-codec`` (the
``alp`` package) so the format spec for ALP lives there. Encode/decode
here exist purely so :mod:`lastra` exposes the codec under its standard
``encode(values) → bytes`` / ``decode(data, count) → np.ndarray`` shape.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np

import alp


def encode(values: Sequence[float] | np.ndarray) -> bytes:
    return alp.encode(values)


def decode(data: bytes, count: int) -> np.ndarray:
    out = alp.decode(data)
    if out.size != count:
        raise ValueError(
            f"ALP count mismatch: blob carries {out.size} values, expected {count}"
        )
    return out
