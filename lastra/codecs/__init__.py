"""Per-column codecs.

Each codec module exposes ``encode(values) -> bytes`` and ``decode(data, count)
-> values`` matching the byte-exact wire format documented in ``FORMAT.md``.
"""
