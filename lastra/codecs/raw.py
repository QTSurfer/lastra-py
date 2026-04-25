"""``RAW`` codec — uncompressed little-endian fallback for ``LONG`` and ``DOUBLE``.

Wire format: 8 bytes per value, little-endian. No header, no length prefix
— the column descriptor's data type and the row count from the header
together determine how to read the bytes.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np


def encode_long(values: Sequence[int] | np.ndarray) -> bytes:
    """Pack ``int64`` values to little-endian bytes."""
    arr = np.ascontiguousarray(np.asarray(values, dtype="<i8"))
    return arr.tobytes()


def decode_long(data: bytes, count: int) -> np.ndarray:
    """Unpack ``count`` little-endian ``int64`` values."""
    if count == 0:
        return np.empty(0, dtype=np.int64)
    expected = count * 8
    if len(data) < expected:
        raise ValueError(f"truncated RAW long payload: need {expected} bytes, got {len(data)}")
    return np.frombuffer(data, dtype="<i8", count=count).astype(np.int64, copy=True)


def encode_double(values: Sequence[float] | np.ndarray) -> bytes:
    """Pack ``float64`` values to little-endian bytes."""
    arr = np.ascontiguousarray(np.asarray(values, dtype="<f8"))
    return arr.tobytes()


def decode_double(data: bytes, count: int) -> np.ndarray:
    """Unpack ``count`` little-endian ``float64`` values."""
    if count == 0:
        return np.empty(0, dtype=np.float64)
    expected = count * 8
    if len(data) < expected:
        raise ValueError(f"truncated RAW double payload: need {expected} bytes, got {len(data)}")
    return np.frombuffer(data, dtype="<f8", count=count).astype(np.float64, copy=True)
