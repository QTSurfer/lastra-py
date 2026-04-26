"""Round-trip tests for the polars adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

from lastra import Codec, LastraReader

pl = pytest.importorskip("polars")

from lastra.adapters import polars as polars_adapter  # noqa: E402


def test_polars_roundtrip_mixed_columns(tmp_path: Path) -> None:
    df = pl.DataFrame({
        "ts": [1700000000000, 1700000001000, 1700000002000],
        "close": [100.0, 100.5, 101.0],
        "side": [b"buy", b"sell", None],
    })
    path = tmp_path / "df.lastra"
    polars_adapter.write_polars(df, path)
    out = polars_adapter.read_polars(path)

    assert out["ts"].to_list() == df["ts"].to_list()
    assert out["close"].to_list() == df["close"].to_list()
    assert out["side"].to_list() == df["side"].to_list()


def test_polars_string_columns_encode_as_utf8() -> None:
    df = pl.DataFrame({"sym": ["BTCUSDT", "ETHUSDT"]})
    blob = polars_adapter.from_polars(df)
    r = LastraReader.from_bytes(blob)
    assert r.read_series_binary("sym") == [b"BTCUSDT", b"ETHUSDT"]


def test_polars_codec_overrides_take_effect(tmp_path: Path) -> None:
    df = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
    path = tmp_path / "o.lastra"
    polars_adapter.write_polars(df, path, codecs={"a": Codec.PONGO})
    r = LastraReader.from_bytes(path.read_bytes())
    assert r.get_series_column("a").codec is Codec.PONGO
