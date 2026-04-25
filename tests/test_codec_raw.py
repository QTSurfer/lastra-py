"""RAW codec round-trips for LONG and DOUBLE columns."""

from __future__ import annotations

import numpy as np
import pytest

from lastra.codecs import raw


def test_long_roundtrip() -> None:
    values = [1, -2, 3, -(2**62), (2**62)]
    blob = raw.encode_long(values)
    assert len(blob) == 8 * len(values)
    np.testing.assert_array_equal(raw.decode_long(blob, len(values)), values)


def test_double_roundtrip() -> None:
    values = [3.14, -2.71, 0.0, 1e308, -1e-308]
    blob = raw.encode_double(values)
    assert len(blob) == 8 * len(values)
    np.testing.assert_array_equal(raw.decode_double(blob, len(values)), values)


def test_empty_inputs_produce_empty_outputs() -> None:
    assert raw.encode_long([]) == b""
    assert raw.encode_double([]) == b""
    assert raw.decode_long(b"", 0).size == 0
    assert raw.decode_double(b"", 0).size == 0


def test_decoders_reject_truncated_buffers() -> None:
    with pytest.raises(ValueError, match="truncated"):
        raw.decode_long(b"\x00" * 7, 1)
    with pytest.raises(ValueError, match="truncated"):
        raw.decode_double(b"\x00" * 15, 2)
