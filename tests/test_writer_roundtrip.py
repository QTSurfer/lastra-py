"""End-to-end tests for :class:`LastraWriter`.

The writer is verified two ways:

1. **Round-trip** — Python encodes, Python decodes; values must match
   exactly. Covers every codec / data-type combination.
2. **Cross-decoder** — Python encodes, the lastra-java fixture format
   is mirrored back via :class:`LastraReader` (since both readers/writers
   share the same wire format, the Python reader counts as an
   independent decoder for testing). The fixture-based comparisons in
   ``test_reader_compat.py`` already pin the wire layout to
   lastra-java, so a Python writer that survives the Python reader is
   wire-compatible with Java.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from lastra import Codec, DataType, LastraReader, LastraWriter
from lastra.format import parse_header

FIXTURE_DIR = Path(__file__).parent / "fixtures"


# --- Round-trip: every series codec --------------------------------------

def test_roundtrip_long_delta_varint() -> None:
    ts = np.arange(1700000000000, 1700000000000 + 5_000, 1000, dtype=np.int64)
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.write_series(len(ts), ts)
    blob = w.to_bytes()

    r = LastraReader.from_bytes(blob)
    assert r.series_row_count == len(ts)
    np.testing.assert_array_equal(r.read_series_long("ts"), ts)


def test_roundtrip_long_raw() -> None:
    ts = np.array([5, 0, -3, 999_999_999], dtype=np.int64)
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.RAW)
    w.write_series(len(ts), ts)
    r = LastraReader.from_bytes(w.to_bytes())
    np.testing.assert_array_equal(r.read_series_long("ts"), ts)


def test_roundtrip_double_alp_pongo_gorilla_raw() -> None:
    prices = np.array([65007.28, 65007.31, 65007.30, 65007.32, 65007.32])
    volumes = np.array([1.5, 2.5, 3.5, 4.5, 5.5])

    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_series_column("alp", DataType.DOUBLE, Codec.ALP)
    w.add_series_column("pongo", DataType.DOUBLE, Codec.PONGO)
    w.add_series_column("gorilla", DataType.DOUBLE, Codec.GORILLA)
    w.add_series_column("raw", DataType.DOUBLE, Codec.RAW)

    ts = np.arange(1700000000000, 1700000000000 + 5_000, 1000, dtype=np.int64)
    w.write_series(5, ts, prices, prices, prices, volumes)

    r = LastraReader.from_bytes(w.to_bytes())
    np.testing.assert_array_equal(r.read_series_double("alp"), prices)
    np.testing.assert_array_equal(r.read_series_double("pongo"), prices)
    np.testing.assert_array_equal(r.read_series_double("gorilla"), prices)
    np.testing.assert_array_equal(r.read_series_double("raw"), volumes)


def test_roundtrip_binary_varlen_family() -> None:
    rows: list[bytes | None] = [b"buy", b"sell", None, b"\x00\x01\x02"]
    for codec in (Codec.VARLEN, Codec.VARLEN_ZSTD, Codec.VARLEN_GZIP):
        w = LastraWriter()
        w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
        w.add_series_column("type", DataType.BINARY, codec)
        ts = np.array([1, 2, 3, 4], dtype=np.int64)
        w.write_series(4, ts, rows)

        r = LastraReader.from_bytes(w.to_bytes())
        assert r.read_series_binary("type") == rows, f"failed for {codec.name}"


# --- Header / flag wiring -------------------------------------------------

def test_default_flags_set_footer_and_checksums() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.write_series(2, np.array([1, 2], dtype=np.int64))
    blob = w.to_bytes()

    h = parse_header(blob[:22])
    assert h.has_footer
    assert h.has_checksums
    assert not h.has_events
    assert not h.has_row_groups


def test_events_flag_only_set_when_events_have_rows() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_event_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    # No write_events call → events_row_count stays 0 → no FLAG_HAS_EVENTS.
    w.write_series(1, np.array([1], dtype=np.int64))
    h = parse_header(w.to_bytes()[:22])
    assert not h.has_events


def test_row_group_flag_only_set_for_more_than_one_rg() -> None:
    w = LastraWriter(row_group_size=10)
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.write_series(5, np.arange(5, dtype=np.int64))
    h = parse_header(w.to_bytes()[:22])
    assert not h.has_row_groups  # only 1 RG fits in 10-row groups


def test_row_group_flag_set_when_multiple_groups() -> None:
    w = LastraWriter(row_group_size=2)
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.write_series(7, np.arange(7, dtype=np.int64))
    h = parse_header(w.to_bytes()[:22])
    assert h.has_row_groups


# --- Row groups: multi-RG roundtrip including stats ----------------------

def test_row_groups_multi_rg_full_and_partial_reads() -> None:
    w = LastraWriter(row_group_size=3)
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_series_column("close", DataType.DOUBLE, Codec.PONGO)

    ts = np.array([1700000000000 + i * 1000 for i in range(7)], dtype=np.int64)
    close = np.array([100.0 + i * 0.5 for i in range(7)])
    w.write_series(7, ts, close)

    r = LastraReader.from_bytes(w.to_bytes())
    assert r.row_group_count == 3
    rgs = r.row_group_stats
    assert [rg.row_count for rg in rgs] == [3, 3, 1]
    # ts_min / ts_max for each RG come from the first LONG column.
    assert rgs[0].ts_min == ts[0] and rgs[0].ts_max == ts[2]
    assert rgs[1].ts_min == ts[3] and rgs[1].ts_max == ts[5]
    assert rgs[2].ts_min == ts[6] and rgs[2].ts_max == ts[6]

    np.testing.assert_array_equal(r.read_series_long("ts"), ts)
    np.testing.assert_array_equal(r.read_series_double("close"), close)
    np.testing.assert_array_equal(
        r.read_row_group_long(2, "ts"), np.array([ts[6]])
    )


# --- Series + events + metadata ------------------------------------------

def test_series_events_and_metadata_roundtrip() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_series_column(
        "close",
        DataType.DOUBLE,
        Codec.ALP,
        metadata={"indicator": "close", "src": "binance"},
    )
    w.add_event_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_event_column("type", DataType.BINARY, Codec.VARLEN)

    ts = np.array([1700000000000, 1700000001000, 1700000002000], dtype=np.int64)
    close = np.array([100.10, 100.20, 100.30])
    event_ts = np.array([1700000000500, 1700000002500], dtype=np.int64)
    event_type = [b"buy", b"sell"]

    w.write_series(3, ts, close)
    w.write_events(2, event_ts, event_type)

    r = LastraReader.from_bytes(w.to_bytes())
    assert r.series_row_count == 3
    assert r.events_row_count == 2
    np.testing.assert_array_equal(r.read_series_long("ts"), ts)
    np.testing.assert_array_equal(r.read_series_double("close"), close)
    np.testing.assert_array_equal(r.read_event_long("ts"), event_ts)
    assert r.read_event_binary("type") == event_type

    close_col = r.get_series_column("close")
    # NB: dict order is implementation defined for the Java side;
    # we match by value, not by serialised JSON ordering.
    assert close_col.metadata == {"indicator": "close", "src": "binance"}


# --- CRC verification triggers on tampering -------------------------------

def test_corrupted_payload_triggers_crc_error() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.RAW)  # 8 bytes per value
    ts = np.arange(100, dtype=np.int64)
    w.write_series(100, ts)
    blob = bytearray(w.to_bytes())

    # Header (22) + descriptor (6 for "ts") + length-prefix (4) ≈ 32;
    # payload starts there and runs ~800 bytes. Byte 100 is firmly inside.
    blob[100] ^= 0xFF

    r = LastraReader.from_bytes(bytes(blob))
    with pytest.raises(ValueError, match="CRC32 mismatch"):
        r.read_series_long("ts")


# --- Argument validation -------------------------------------------------

def test_writeSeries_rejects_arity_mismatch() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.add_series_column("close", DataType.DOUBLE, Codec.ALP)
    with pytest.raises(ValueError, match="3 arrays but 2"):
        w.write_series(1, np.array([1], dtype=np.int64), np.array([1.0]),
                       np.array([2.0]))


def test_set_row_group_size_must_be_positive() -> None:
    w = LastraWriter()
    with pytest.raises(ValueError, match="positive"):
        w.set_row_group_size(0)


# --- Footer trailer is reachable via the Range-friendly helper ------------

def test_footer_size_helper_works_on_python_written_blob() -> None:
    w = LastraWriter()
    w.add_series_column("ts", DataType.LONG, Codec.DELTA_VARINT)
    w.write_series(3, np.array([1, 2, 3], dtype=np.int64))
    blob = w.to_bytes()
    assert LastraReader.read_footer_size(blob[-8:]) > 0


# --- Wire-equivalence: Python-written file decodes the same as the
#     equivalent Java-written fixture (semantically, not byte-for-byte
#     since metadata serialisation order isn't guaranteed by Java
#     HashMap; but for fixtures with no/single-key metadata, we can
#     check the layout matches roughly the same size class). ---

def test_python_writer_reproduces_events_fixture_semantics() -> None:
    java = LastraReader.from_bytes((FIXTURE_DIR / "lastra_with_events.lastra").read_bytes())
    ts = java.read_series_long("ts")
    close = java.read_series_double("close")
    event_ts = java.read_event_long("ts")
    event_type = java.read_event_binary("type")

    w = LastraWriter()
    for col in java.series_columns:
        w.add_series_column(col.name, col.data_type, col.codec, col.metadata or None)
    for col in java.event_columns:
        w.add_event_column(col.name, col.data_type, col.codec, col.metadata or None)
    w.write_series(java.series_row_count, ts, close)
    w.write_events(java.events_row_count, event_ts, event_type)

    py = LastraReader.from_bytes(w.to_bytes())
    np.testing.assert_array_equal(py.read_series_long("ts"), ts)
    np.testing.assert_array_equal(py.read_series_double("close"), close)
    np.testing.assert_array_equal(py.read_event_long("ts"), event_ts)
    assert py.read_event_binary("type") == event_type
    assert py.get_series_column("close").metadata == java.get_series_column("close").metadata


def test_python_writer_reproduces_row_groups_fixture_semantics() -> None:
    java = LastraReader.from_bytes((FIXTURE_DIR / "lastra_row_groups.lastra").read_bytes())
    ts = java.read_series_long("ts")
    close = java.read_series_double("close")

    w = LastraWriter(row_group_size=java.row_group_stats[0].row_count)
    for col in java.series_columns:
        w.add_series_column(col.name, col.data_type, col.codec, col.metadata or None)
    w.write_series(java.series_row_count, ts, close)

    py = LastraReader.from_bytes(w.to_bytes())
    assert py.row_group_count == java.row_group_count
    np.testing.assert_array_equal(py.read_series_long("ts"), ts)
    np.testing.assert_array_equal(py.read_series_double("close"), close)


def test_python_written_file_matches_java_fixture_decoder_output() -> None:
    """Reading the Java fixture and a Python re-encoding side by side
    must produce identical column data — proves the writer's encoding
    path is reader-compatible."""
    java_blob = (FIXTURE_DIR / "lastra_ticker.lastra").read_bytes()
    java = LastraReader.from_bytes(java_blob)

    ts = java.read_series_long("ts")
    opn = java.read_series_double("opn")
    hig = java.read_series_double("hig")
    low = java.read_series_double("low")
    cls = java.read_series_double("cls")
    vol = java.read_series_double("vol")

    w = LastraWriter()
    for col in java.series_columns:
        w.add_series_column(col.name, col.data_type, col.codec, col.metadata or None)
    w.write_series(java.series_row_count, ts, opn, hig, low, cls, vol)
    py = LastraReader.from_bytes(w.to_bytes())

    np.testing.assert_array_equal(py.read_series_long("ts"), ts)
    np.testing.assert_array_equal(py.read_series_double("opn"), opn)
    np.testing.assert_array_equal(py.read_series_double("hig"), hig)
    np.testing.assert_array_equal(py.read_series_double("low"), low)
    np.testing.assert_array_equal(py.read_series_double("cls"), cls)
    np.testing.assert_array_equal(py.read_series_double("vol"), vol)
