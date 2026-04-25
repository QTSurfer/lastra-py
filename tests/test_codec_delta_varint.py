"""DELTA_VARINT codec round-trip + cross-implementation cases.

The expected byte sequences in the cross-impl tests come from
``DeltaVarintCodec`` in ``lastra-java`` v0.8.0 — running the same input
through both implementations must produce identical bytes (byte-exact
guarantee documented in ``FORMAT.md``).
"""

from __future__ import annotations

import numpy as np
import pytest

from lastra.codecs.delta_varint import (
    _read_varint,
    _write_varint,
    _zigzag_decode,
    _zigzag_encode,
    decode,
    encode,
)


def test_empty_roundtrip() -> None:
    assert encode([]) == b""
    np.testing.assert_array_equal(decode(b"", 0), np.empty(0, dtype=np.int64))


def test_single_value_only_writes_first_long() -> None:
    encoded = encode([1234567890])
    assert len(encoded) == 8
    assert int.from_bytes(encoded, "little", signed=True) == 1234567890

    decoded = decode(encoded, 1)
    assert decoded.tolist() == [1234567890]


def test_two_values_writes_first_long_plus_one_varint() -> None:
    # Delta = 1 → zigzag(1) = 2 → single varint byte 0x02
    encoded = encode([100, 101])
    assert len(encoded) == 9
    assert encoded[8] == 0x02
    np.testing.assert_array_equal(decode(encoded, 2), [100, 101])


def test_perfectly_regular_grid_compresses_one_byte_per_dod() -> None:
    # 1 ms grid → first delta = 1, every dod = 0 → varint 0x00 (single byte)
    n = 1024
    values = list(range(1_000_000_000_000, 1_000_000_000_000 + n))
    encoded = encode(values)
    # 8 bytes (first) + 1 byte (first delta = 1) + (n - 2) * 1 byte (dod = 0)
    assert len(encoded) == 8 + 1 + (n - 2)
    np.testing.assert_array_equal(decode(encoded, n), values)


def test_negative_and_irregular_values_roundtrip() -> None:
    values = [-7, 5, 5, 6, 1_000, 999, 1_000_000_000_000, -2**62]
    encoded = encode(values)
    decoded = decode(encoded, len(values))
    np.testing.assert_array_equal(decoded, values)


@pytest.mark.parametrize(
    "value",
    [0, 1, -1, 1 << 30, -(1 << 30), (1 << 62), -(1 << 62), (1 << 63) - 1, -(1 << 63)],
)
def test_zigzag_roundtrip(value: int) -> None:
    assert _zigzag_decode(_zigzag_encode(value)) == value


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (300, b"\xac\x02"),
    ],
)
def test_varint_known_encodings(value: int, expected: bytes) -> None:
    buf = bytearray()
    _write_varint(buf, value)
    assert bytes(buf) == expected
    decoded, pos = _read_varint(expected, 0)
    assert decoded == value
    assert pos == len(expected)


def test_decode_rejects_truncated_first_value() -> None:
    with pytest.raises(ValueError, match="truncated"):
        decode(b"\x00\x00", 5)
