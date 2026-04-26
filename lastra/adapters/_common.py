"""Shared helpers for the dataframe adapters.

Defines codec defaults and the ``CodecOverride`` type used by every
adapter for per-column codec selection.
"""

from __future__ import annotations

from typing import Mapping

from ..format import Codec, DataType

CodecOverride = Mapping[str, Codec]


def default_codec(data_type: DataType) -> Codec:
    """Pick a sensible codec for a given :class:`DataType`.

    ``LONG``  → DELTA_VARINT (handles timestamps and monotonic ids well)
    ``DOUBLE`` → ALP (best general-purpose float codec, falls back gracefully)
    ``BINARY`` → VARLEN_ZSTD (short strings still compress; safe default)
    """
    if data_type is DataType.LONG:
        return Codec.DELTA_VARINT
    if data_type is DataType.DOUBLE:
        return Codec.ALP
    if data_type is DataType.BINARY:
        return Codec.VARLEN_ZSTD
    raise ValueError(f"unsupported data_type: {data_type}")


def resolve_codec(
    name: str,
    data_type: DataType,
    overrides: CodecOverride | None,
) -> Codec:
    """Look up the codec for ``name`` in ``overrides`` or fall back to default."""
    if overrides and name in overrides:
        return overrides[name]
    return default_codec(data_type)
