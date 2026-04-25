"""Header parser + writer round-trips."""

from __future__ import annotations

import pytest

from lastra.format import (
    FLAG_HAS_CHECKSUMS,
    FLAG_HAS_EVENTS,
    FLAG_HAS_FOOTER,
    FLAG_HAS_ROW_GROUPS,
    HEADER_SIZE,
    MAGIC,
    Header,
    parse_header,
    write_header,
)


def test_header_roundtrip_minimal() -> None:
    h = Header(
        version=1,
        flags=0,
        series_row_count=0,
        series_col_count=0,
        events_row_count=0,
        events_col_count=0,
    )
    raw = write_header(h)
    assert len(raw) == HEADER_SIZE
    assert int.from_bytes(raw[0:4], "little") == MAGIC
    assert parse_header(raw) == h


def test_header_roundtrip_full() -> None:
    h = Header(
        version=1,
        flags=FLAG_HAS_EVENTS | FLAG_HAS_FOOTER | FLAG_HAS_CHECKSUMS | FLAG_HAS_ROW_GROUPS,
        series_row_count=86_400,
        series_col_count=11,
        events_row_count=42,
        events_col_count=3,
    )
    raw = write_header(h)
    parsed = parse_header(raw)
    assert parsed == h
    assert parsed.has_events
    assert parsed.has_footer
    assert parsed.has_checksums
    assert parsed.has_row_groups


def test_header_rejects_bad_magic() -> None:
    raw = bytearray(HEADER_SIZE)
    raw[0:4] = (0xDEADBEEF).to_bytes(4, "little", signed=False)
    with pytest.raises(ValueError, match="not a Lastra file"):
        parse_header(bytes(raw))


def test_header_rejects_truncated_input() -> None:
    with pytest.raises(ValueError, match="truncated header"):
        parse_header(b"LAST")
