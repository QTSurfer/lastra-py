"""Round-trip tests for the pyarrow adapter."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from lastra import Codec, LastraReader

pa = pytest.importorskip("pyarrow")

from lastra.adapters import arrow as arrow_adapter  # noqa: E402


def test_arrow_roundtrip_mixed_columns(tmp_path: Path) -> None:
    table = pa.table({
        "ts": pa.array([1700000000000, 1700000001000, 1700000002000], type=pa.int64()),
        "close": pa.array([100.0, 100.5, 101.0], type=pa.float64()),
        "side": pa.array([b"buy", b"sell", None], type=pa.binary()),
    })
    path = tmp_path / "t.lastra"
    arrow_adapter.write_arrow(table, path)
    out = arrow_adapter.read_arrow(path)

    assert out["ts"].to_pylist() == table["ts"].to_pylist()
    assert out["close"].to_pylist() == table["close"].to_pylist()
    assert out["side"].to_pylist() == table["side"].to_pylist()


def test_arrow_timestamp_columns_stored_as_int64() -> None:
    ts_col = pa.array(
        [1700000000_000_000_000, 1700000001_000_000_000],
        type=pa.timestamp("ns"),
    )
    table = pa.table({"t": ts_col})
    blob = arrow_adapter.from_arrow(table)
    r = LastraReader.from_bytes(blob)
    out = r.read_series_long("t")
    np.testing.assert_array_equal(out, ts_col.cast(pa.int64()).to_numpy(zero_copy_only=False))


def test_arrow_string_columns_encode_as_utf8() -> None:
    table = pa.table({"sym": pa.array(["BTCUSDT", "ETHUSDT"], type=pa.string())})
    blob = arrow_adapter.from_arrow(table)
    r = LastraReader.from_bytes(blob)
    assert r.read_series_binary("sym") == [b"BTCUSDT", b"ETHUSDT"]


def test_arrow_codec_overrides_take_effect(tmp_path: Path) -> None:
    table = pa.table({"a": pa.array([1.0, 2.0, 3.0], type=pa.float64())})
    path = tmp_path / "o.lastra"
    arrow_adapter.write_arrow(table, path, codecs={"a": Codec.GORILLA})
    r = LastraReader.from_bytes(path.read_bytes())
    assert r.get_series_column("a").codec is Codec.GORILLA
