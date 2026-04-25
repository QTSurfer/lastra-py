"""Column descriptor encode/decode + JSON metadata round-trip."""

from __future__ import annotations

import pytest

from lastra._descriptor import (
    _parse_metadata,
    _serialise_metadata,
    read_descriptor,
    read_descriptors,
    write_descriptor,
    write_descriptors,
)
from lastra.format import Codec, ColumnDescriptor, DataType


def test_minimal_descriptor_roundtrip() -> None:
    d = ColumnDescriptor(codec=Codec.DELTA_VARINT, data_type=DataType.LONG, flags=0, name="ts")
    blob = write_descriptor(d)
    parsed, end = read_descriptor(blob)
    assert end == len(blob)
    assert parsed == d


def test_descriptor_with_metadata_roundtrip() -> None:
    d = ColumnDescriptor(
        codec=Codec.ALP,
        data_type=DataType.DOUBLE,
        flags=0x02,
        name="ema1",
        metadata={"indicator": "ema", "periods": "10"},
    )
    blob = write_descriptor(d)
    parsed, _ = read_descriptor(blob)
    assert parsed.codec is Codec.ALP
    assert parsed.data_type is DataType.DOUBLE
    assert parsed.name == "ema1"
    assert parsed.metadata == {"indicator": "ema", "periods": "10"}
    # has-metadata flag bit must be set on the wire
    assert parsed.flags & 0x02


def test_metadata_layout_known_bytes() -> None:
    """Spot-check the wire layout against a manually constructed blob."""
    d = ColumnDescriptor(
        codec=Codec.RAW,
        data_type=DataType.LONG,
        flags=0,  # writer overrides; the wire byte is what matters
        name="x",
    )
    blob = write_descriptor(d)
    # codec=0, dataType=0, colFlags=0, nameLen=1, name="x"
    assert blob == b"\x00\x00\x00\x01x"


def test_descriptor_without_meta_omits_metalen_bytes() -> None:
    d = ColumnDescriptor(codec=Codec.GORILLA, data_type=DataType.DOUBLE, flags=0, name="cpu")
    blob = write_descriptor(d)
    # 4 fixed bytes + 3 bytes of "cpu". No metaLen / meta tail.
    assert len(blob) == 4 + 3


def test_descriptor_with_meta_layout() -> None:
    d = ColumnDescriptor(
        codec=Codec.PONGO,
        data_type=DataType.DOUBLE,
        flags=0,
        name="t",
        metadata={"k": "v"},
    )
    blob = write_descriptor(d)
    # codec=7, type=1, colFlags=0x02, nameLen=1, name="t",
    # metaLen=7 (LE), meta='{"k":"v"}'
    expected_meta = b'{"k":"v"}'
    assert blob == bytes([7, 1, 0x02, 1]) + b"t" + len(expected_meta).to_bytes(2, "little") + expected_meta


def test_metadata_serialiser_rejects_forbidden_chars() -> None:
    with pytest.raises(ValueError, match="forbidden"):
        _serialise_metadata({"a:b": "v"})
    with pytest.raises(ValueError, match="forbidden"):
        _serialise_metadata({"k": '"oops"'})
    with pytest.raises(ValueError, match="forbidden"):
        _serialise_metadata({"a,b": "v"})


def test_metadata_parser_handles_empty_object() -> None:
    assert _parse_metadata(b"{}") == {}


def test_long_name_rejected() -> None:
    long_name = "x" * 256
    d = ColumnDescriptor(codec=Codec.RAW, data_type=DataType.LONG, flags=0, name=long_name)
    with pytest.raises(ValueError, match="nameLen"):
        write_descriptor(d)


def test_multiple_descriptors_back_to_back() -> None:
    descs = [
        ColumnDescriptor(codec=Codec.DELTA_VARINT, data_type=DataType.LONG, flags=0, name="ts"),
        ColumnDescriptor(codec=Codec.ALP, data_type=DataType.DOUBLE, flags=0, name="close"),
        ColumnDescriptor(
            codec=Codec.PONGO,
            data_type=DataType.DOUBLE,
            flags=0,
            name="ema",
            metadata={"periods": "20"},
        ),
    ]
    blob = write_descriptors(descs)
    parsed, end = read_descriptors(blob, count=3)
    assert end == len(blob)
    assert [p.name for p in parsed] == ["ts", "close", "ema"]
    assert parsed[2].metadata == {"periods": "20"}


def test_read_descriptors_truncated_buffer() -> None:
    # 4 fixed bytes for first descriptor but missing the name bytes
    bad = bytes([0, 0, 0, 5]) + b"abc"
    with pytest.raises(ValueError, match="truncated"):
        read_descriptors(bad, count=1)
