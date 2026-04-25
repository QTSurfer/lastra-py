"""ALP codec wrapper tests — thin layer over the alp-codec package."""

from __future__ import annotations

import numpy as np
import pytest

from lastra.codecs.alp import decode, encode


def test_roundtrip() -> None:
    values = [65007.28, 65007.31, 65007.30, 65007.32, 65007.32]
    blob = encode(values)
    np.testing.assert_array_equal(decode(blob, len(values)), values)


def test_count_mismatch_rejected() -> None:
    blob = encode([1.0, 2.0, 3.0])
    with pytest.raises(ValueError, match="ALP count mismatch"):
        decode(blob, 5)


def test_empty_roundtrip() -> None:
    blob = encode([])
    decoded = decode(blob, 0)
    assert decoded.size == 0
