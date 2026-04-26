"""Round-trip tests for the pandas adapter.

Lives in its own file so importing pandas doesn't drag polars +
pyarrow into the same Python process — combining the three trips
the harness's memory limit on this sandbox.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from lastra import Codec, LastraReader

pd = pytest.importorskip("pandas")

from lastra.adapters import pandas as pandas_adapter  # noqa: E402


def test_pandas_roundtrip_mixed_columns(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "ts": np.arange(1700000000000, 1700000000005000, 1000, dtype=np.int64),
        "close": [100.0, 100.5, 101.0, 100.75, 101.25],
        "side": [b"buy", b"sell", b"buy", None, b"sell"],
    })
    path = tmp_path / "df.lastra"
    pandas_adapter.write_pandas(df, path)
    out = pandas_adapter.read_pandas(path)

    np.testing.assert_array_equal(out["ts"].to_numpy(), df["ts"].to_numpy())
    np.testing.assert_array_equal(out["close"].to_numpy(), df["close"].to_numpy())
    assert list(out["side"]) == list(df["side"])


def test_pandas_codec_overrides_take_effect(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "ts": [1, 2, 3, 4],
        "value": [1.5, 2.5, 3.5, 4.5],
    })
    path = tmp_path / "override.lastra"
    pandas_adapter.write_pandas(df, path, codecs={"value": Codec.GORILLA, "ts": Codec.RAW})

    r = LastraReader.from_bytes(path.read_bytes())
    assert r.get_series_column("ts").codec is Codec.RAW
    assert r.get_series_column("value").codec is Codec.GORILLA


def test_pandas_string_columns_encode_as_utf8() -> None:
    df = pd.DataFrame({"sym": ["BTCUSDT", "ETHUSDT", "BNBUSDT"]})
    blob = pandas_adapter.from_pandas(df)
    r = LastraReader.from_bytes(blob)
    assert r.read_series_binary("sym") == [b"BTCUSDT", b"ETHUSDT", b"BNBUSDT"]


def test_pandas_datetime64_stored_as_int64() -> None:
    df = pd.DataFrame({
        "t": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    })
    blob = pandas_adapter.from_pandas(df)
    r = LastraReader.from_bytes(blob)
    out = r.read_series_long("t")
    # pandas stores ns since epoch
    assert out[0] == pd.Timestamp("2024-01-01").value


def test_pandas_unsupported_dtype_raises() -> None:
    df = pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])})
    with pytest.raises(TypeError, match="cannot map"):
        pandas_adapter.from_pandas(df)
