"""Smoke test — verifies the package imports and exposes its version."""

import lastra


def test_version_present() -> None:
    assert isinstance(lastra.__version__, str)
    assert lastra.__version__.count(".") >= 1
