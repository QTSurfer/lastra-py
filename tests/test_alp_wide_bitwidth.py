"""Regression: ALP vectors whose frame-of-reference deltas need more than 56 bits.

``alp-codec`` 0.8.0-0.8.2 decoded such vectors wrongly and silently (the group fast
path of ``unpack`` read a fixed 8-byte window and lost the high bits). The fix shipped
in ``alp-codec`` 0.8.3, which is therefore the minimum ``lastra`` accepts.

The fixture is a 256-row ``.lastra`` file written by the reference Java encoder. Its
``close`` column holds prices that went through float32 (many decimal places, so ALP
needs wide bit widths). Reading it must return every value exactly.
"""

from __future__ import annotations

from importlib import metadata
from pathlib import Path

import numpy as np

import lastra

FIXTURES = Path(__file__).parent / "fixtures"


def test_wide_bitwidth_alp_column_reads_exactly() -> None:
    reader = lastra.LastraReader.from_bytes(
        (FIXTURES / "alp_wide_bitwidth_float32.lastra").read_bytes()
    )
    expected = np.fromfile(
        FIXTURES / "alp_wide_bitwidth_float32.expected.f32", dtype=np.float32
    ).astype(np.float64)
    close = reader.read_series_double("close")
    assert close.shape == expected.shape
    bad = np.flatnonzero(close != expected)
    assert bad.size == 0, (
        f"{bad.size} of {expected.size} values differ (alp-codec {metadata.version('alp-codec')}); "
        "alp-codec < 0.8.3 decodes wide bit widths wrongly"
    )


def test_installed_alp_codec_meets_the_declared_floor() -> None:
    installed = tuple(int(p) for p in metadata.version("alp-codec").split(".")[:3])
    assert installed >= (0, 8, 3)


def test_version_matches_installed_metadata() -> None:
    assert lastra.__version__ == metadata.version("lastra")
