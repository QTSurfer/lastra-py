"""Cross-implementation byte-equality tests against ``lastra-java`` v0.8.0.

The expected hex strings come from running
:class:`com.wualabs.qtsurfer.lastra.codec.DeltaVarintCodec` on the same
inputs. If a future change here breaks byte-equality, these tests will
catch it before any file written by Python becomes unreadable by Java
(or vice versa).

Generation snippet (see ``tests/fixtures/GenFixture.java``):

    long[] case = {100, 101};
    byte[] out = DeltaVarintCodec.encode(case, case.length);
    // hex: 640000000000000002
"""

from __future__ import annotations

import numpy as np

from lastra.codecs.delta_varint import decode, encode

# Map of human label → (input ints, hex output emitted by lastra-java 0.8.0).
JAVA_FIXTURES: dict[str, tuple[list[int], str]] = {
    "two_values": ([100, 101], "640000000000000002"),
    "regular_grid_10": (
        [1_000_000_000_000 + i for i in range(10)],
        "0010a5d4e8000000020000000000000000",
    ),
    "irregular_with_negatives": (
        [-7, 5, 5, 6, 1_000, 999, 1_000_000_000_000, -(1 << 62)],
        "f9ffffffffffffff181702c20fc50fb4b0a8ca9a3ab1f0d094b5f480808001",
    ),
    "single_value": ([1234567890], "d202964900000000"),
}


def test_python_encode_matches_java_byte_for_byte() -> None:
    for label, (values, expected_hex) in JAVA_FIXTURES.items():
        produced = encode(values).hex()
        assert produced == expected_hex, (
            f"{label}: python produced {produced}, java produced {expected_hex}"
        )


def test_python_decode_of_java_bytes_matches_input() -> None:
    for label, (values, expected_hex) in JAVA_FIXTURES.items():
        decoded = decode(bytes.fromhex(expected_hex), len(values))
        np.testing.assert_array_equal(
            decoded,
            values,
            err_msg=f"{label}: decode mismatch",
        )
