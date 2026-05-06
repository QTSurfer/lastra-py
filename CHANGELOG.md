# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.3] - 2026-05-06

### 🔴 Fixed

- `LastraWriter.write_series()` now accumulates `series_row_count` across calls
  instead of overwriting it on every invocation. Pre-0.8.3, when `write_series`
  was called more than once on the same writer (streaming-append usage), the
  footer recorded only the last call's count, and full-series reads via
  `LastraReader.read_series_long` / `read_series_double` raised
  `ValueError: could not broadcast input array from shape (X,) into shape (Y,)`
  because the result buffer was sized from that under-count while per-row-group
  slices fed it the full data. Single-call usage is unaffected. Per-row-group
  reads (`read_row_group_long` / `row_group_stats`) were already correct — only
  the aggregate `series_row_count` was wrong. Mirrors the `lastra-java` 0.8.2
  fix for the same bug.

### 🧪 Tests

- New `test_multiple_write_series_calls_accumulate` covers the streaming-append
  pattern: 4 × `write_series(100, ts, v)` → 4 row groups, validates the
  accumulated `series_row_count`, per-RG stats, full-series concatenation, and
  per-RG selective reads. Mirrors `lastra-java`'s `testMultipleWriteSeriesCallsAccumulate`
  and `lastra-ts`'s `Multiple writeSeries() calls` suite.

### 📝 Docs

- `FORMAT.md`: dropped pre-creation framing now that the package exists.

## [0.8.2] - 2026-04-26

### 📝 Docs

- README status reads version-agnostic (defers to the PyPI badge for the
  current version).

## [0.8.1] - 2026-04-26

### 📝 Docs

- README uses absolute URLs and drops pre-release language.

## [0.8.0] - 2026-04-26

### ✨ Added

- Initial public release. Python reader and writer for the Lastra columnar
  time-series format, bit-exact compatible with `lastra-java` and `lastra-ts`.
- `LastraReader` with full support for series and event sections, row groups
  with per-RG stats, footer-trailer-driven Range-friendly reads, and CRC32
  per-column verification.
- `LastraWriter` with auto-partitioning into row groups, codec selection per
  column, and metadata propagation.
- Codecs: `RAW`, `DELTA_VARINT`, `ALP` (via `alp-codec`), `GORILLA`, `PONGO`,
  and the `VARLEN` family (`VARLEN`, `VARLEN_ZSTD`, `VARLEN_GZIP`) for binary
  columns.
- Adapters for `pandas`, `polars`, and `pyarrow` (optional extras).
- GitHub Actions release workflow that publishes to PyPI on tag push.

[Unreleased]: https://github.com/QTSurfer/lastra-py/compare/0.8.3...HEAD
[0.8.3]: https://github.com/QTSurfer/lastra-py/compare/0.8.2...0.8.3
[0.8.2]: https://github.com/QTSurfer/lastra-py/compare/0.8.1...0.8.2
[0.8.1]: https://github.com/QTSurfer/lastra-py/compare/0.8.0...0.8.1
[0.8.0]: https://github.com/QTSurfer/lastra-py/releases/tag/0.8.0
