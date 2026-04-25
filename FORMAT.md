# Lastra File Format Specification

Version 1 — extracted from `lastra-java` v0.8.0 reference implementation.
This document is the canonical wire-format reference for any Lastra reader
or writer (Java, TypeScript, Python). It will live in `lastra-py` once the
repo is created.

## Constants

| Constant | Value | Notes |
|----------|-------|-------|
| `MAGIC` | `0x4C415354` | ASCII `"LAST"` (LE bytes: `54 53 41 4C`) |
| `FOOTER_MAGIC` | `0x4C415321` | ASCII `"LAS!"` (LE bytes: `21 53 41 4C`) |
| `VERSION` | `1` | Bump on incompatible changes |
| File extension | `.lastra` | |

All multi-byte integers and floats are **little-endian**.

## Data types

| Name | ID | Description |
|------|----|-------------|
| `LONG` | 0 | Signed 64-bit integer (typical use: nanosecond/millisecond timestamps) |
| `DOUBLE` | 1 | IEEE 754 binary64 |
| `BINARY` | 2 | Length-prefixed byte buffer (variable per row) |

## Codecs

| Name | ID | DataType | Notes |
|------|----|----------|-------|
| `RAW` | 0 | LONG / DOUBLE | Uncompressed fallback |
| `DELTA_VARINT` | 1 | LONG | Delta-of-delta + zigzag varint. ~1 byte / value for regular intervals. |
| `ALP` | 2 | DOUBLE | Decimal scaling + FOR + bit-packing. ~3-4 bits / value for 2-dp prices. |
| `VARLEN` | 3 | BINARY | Plain variable-length encoding for short strings. |
| `VARLEN_ZSTD` | 4 | BINARY | Variable-length + ZSTD block compression. JSON/bulk binary. |
| `VARLEN_GZIP` | 5 | BINARY | Variable-length + gzip. Browser-decompressable metadata/small text. |
| `GORILLA` | 6 | DOUBLE | Facebook Gorilla XOR. Volatile metrics (CPU, latency). |
| `PONGO` | 7 | DOUBLE | Decimal erasure + Gorilla XOR. Decimal doubles (prices, sensors). |

## Header flags

| Flag | Bit | Description |
|------|-----|-------------|
| `FLAG_HAS_EVENTS` | 0 (`0x01`) | File contains an events section |
| `FLAG_HAS_FOOTER` | 1 (`0x02`) | Footer with column offsets is present |
| `FLAG_HAS_CHECKSUMS` | 2 (`0x04`) | Per-column CRC32 checksums in footer |
| `FLAG_HAS_ROW_GROUPS` | 3 (`0x08`) | Multiple row groups with per-group statistics |

## File layout

```
+-------------------+--------------------+--------------+--------------+
| HEADER (22 bytes) | COLUMN DESCRIPTORS | SERIES DATA  | EVENTS DATA  |
| (LE)              | (series, then      | (optionally  | (when        |
|                   |  events)           |  in row      |  HAS_EVENTS) |
|                   |                    |  groups)     |              |
+-------------------+--------------------+--------------+--------------+
| FOOTER (column offsets + CRCs) — when HAS_FOOTER                    |
+----------------------------------------------------------------------+
```

### Header (22 bytes, LE)

| Offset | Size | Field |
|-------:|-----:|-------|
| 0  | 4 | `MAGIC` (`0x4C415354`) |
| 4  | 2 | `version` (uint16; currently 1) |
| 6  | 2 | `flags` (uint16 bitmask of `FLAG_*`) |
| 8  | 4 | `seriesRowCount` (uint32) — rows in the series section |
| 12 | 4 | `seriesColCount` (uint32) — number of series columns |
| 16 | 4 | `eventsRowCount` (uint32) |
| 20 | 2 | `eventsColCount` (uint16) |

### Column descriptors

For each series column (in declared order), then for each event column,
the descriptor is:

| Field | Size | Notes |
|-------|------|-------|
| `codec` | 1 byte | Codec ID |
| `dataType` | 1 byte | DataType ID |
| `colFlags` | 1 byte | `0x02` set when metadata follows; other bits reserved |
| `nameLen` | 1 byte (uint8) | Length of the UTF-8 name (max 255) |
| `name` | `nameLen` bytes | UTF-8 column name |
| `metaLen` | 2 bytes (uint16, LE) | Present only when `colFlags & 0x02`. Length of the JSON blob. |
| `meta` | `metaLen` bytes | Present only when `colFlags & 0x02`. UTF-8 JSON object `{"k":"v",...}` (plain text, no compression). |

When `colFlags & 0x02 == 0` the descriptor ends after `name` — no
`metaLen`/`meta` bytes are written. The reference Java JSON parser is
deliberately minimal (`split(",")` + `split(":")`), so metadata keys and
values must not contain `,`, `:`, or unescaped quotes — typically used
for short tags like `{"unit":"celsius","sensor":"dht22"}`.

### Series data

Each column's compressed bytes are framed by a 4-byte LE length prefix:

```
[uint32 length] [length bytes of compressed column data]
```

When `FLAG_HAS_ROW_GROUPS` is NOT set: the series section contains exactly
`seriesColCount` framed columns in declared order. The codec state is
contiguous over the whole `seriesRowCount` rows.

When `FLAG_HAS_ROW_GROUPS` IS set: the section contains `rgCount` row
groups, each with `seriesColCount` framed columns. Each row group's
codecs are independent — readers can decode any single row group without
the others.

### Events data

When `FLAG_HAS_EVENTS` is set, the events section follows the series
section. It is always laid out as `eventsColCount` framed columns with no
row groups. All event columns share a single `eventsRowCount`; columns
shorter than the section length must be padded by the writer (zero/empty).

### Footer

Present when `FLAG_HAS_FOOTER` is set. Two layouts depending on whether
row groups are in use:

#### Without row groups

| Field | Size | Notes |
|-------|------|-------|
| `seriesOffsets` | `seriesColCount * 8` bytes | uint64 per column: byte offset of the framed data within the file |
| `seriesCrc32` | `seriesColCount * 4` bytes | when `FLAG_HAS_CHECKSUMS` |
| `eventOffsets` | `eventsColCount * 8` bytes | when `FLAG_HAS_EVENTS` |
| `eventCrc32` | `eventsColCount * 4` bytes | when both `FLAG_HAS_EVENTS` and `FLAG_HAS_CHECKSUMS` |
| `FOOTER_MAGIC` | 4 bytes | `0x4C415321` |
| `footerSize` | 4 bytes (uint32 LE) | Size of the footer body, excluding this trailer |

The trailing `FOOTER_MAGIC` + `footerSize` pair forms an 8-byte trailer
that lets HTTP-Range readers fetch the last 8 bytes, locate the footer,
and pull row-group statistics with one extra request before retrieving
any data.

#### With row groups

| Field | Size | Notes |
|-------|------|-------|
| `rgCount` | 4 bytes (uint32) | Number of row groups |
| Per row group: `byteOffset` (8) + `rowCount` (4) + `tsMin` (8) + `tsMax` (8) | `rgCount * 28` bytes | tsMin/tsMax are LONG values from the first declared timestamp column |
| Per row group: per-column CRC32 | `rgCount * seriesColCount * 4` bytes | when `FLAG_HAS_CHECKSUMS` |
| `eventOffsets` | `eventsColCount * 8` bytes | when `FLAG_HAS_EVENTS` |
| `eventCrc32` | `eventsColCount * 4` bytes | when `HAS_EVENTS` && `HAS_CHECKSUMS` |
| `FOOTER_MAGIC` | 4 bytes | `0x4C415321` |
| `footerSize` | 4 bytes (uint32 LE) | same trailer hint as the no-row-groups variant |

Row groups enable two things readers should support:

1. **Range queries**: skip any row group whose `[tsMin, tsMax]` does not
   overlap the query window — useful for HTTP range requests against
   remote files.
2. **Independent decoding**: each row group resets codec state, so a
   reader can decode RG `n` without touching RGs `0..n-1`.

CRC32 uses the standard IEEE 802.3 polynomial (`crc32` from Python's
`zlib`, `java.util.zip.CRC32`). It is computed over the framed compressed
bytes of the column (the bytes that follow the 4-byte length prefix).

## Codec details

### `DELTA_VARINT` (LONG)

Two-level delta with zigzag varint output. Best for monotonically
increasing or near-regular timestamps.

```
out = [
  varint(zigzag(values[0])),                 # absolute first value
  varint(zigzag(values[1] - values[0])),     # first delta
  for i in [2, n):
    varint(zigzag((v[i] - v[i-1]) - (v[i-1] - v[i-2])))   # delta-of-delta
]
```

`zigzag(x) = (x << 1) ^ (x >> 63)` (uint64 result). `varint` is the
LEB128-style 7-bit-per-byte unsigned encoding used by Protobuf.

### `ALP` (DOUBLE)

Adaptive Lossless floating-Point. Reference: ACM SIGMOD 2024
[doi:10.1145/3626717](https://dl.acm.org/doi/10.1145/3626717). Java
implementation in `qtsurfer/alp-java` (Apache 2.0).

Pipeline per block (default block size 1024):

1. Find `(e, f)` such that for every value `v`: `v * 10^e * 2^-f` is
   exactly representable as a small int64.
2. Encode the integers via FOR (Frame Of Reference) + bit-packing.
3. Outliers (values that don't fit the chosen `(e, f)`) are stored
   verbatim with a position list.

### `GORILLA` (DOUBLE)

Facebook Gorilla XOR encoding (VLDB 2015). XOR each value with the
previous; emit leading zeros + significant bits. Java reference in
`com.wualabs.qtsurfer.lastra.codec.GorillaCodec`.

### `PONGO` (DOUBLE)

Decimal erasure followed by Gorilla XOR. The eraser zeroes out the
mantissa noise that comes from `value * 100 -> integer -> double`
round-trips, dramatically improving Gorilla's XOR efficiency on prices.
Java reference in `PongoCodec` + `PongoEraser`.

### `VARLEN`, `VARLEN_ZSTD`, `VARLEN_GZIP` (BINARY)

The VARLEN family shares one byte-level layout. Each column blob starts
with a 1-byte compression marker, followed by an optional uncompressed
length, then the payload (compressed when the marker selects ZSTD/GZIP).

```
[1 byte]  compression marker (0=NONE, 1=ZSTD, 2=GZIP)
NONE:
  [int32 LE] payloadLength
  [payload bytes]
ZSTD / GZIP:
  [int32 LE] uncompressedLength
  [int32 LE] compressedLength
  [compressed bytes]
```

The decoded payload is a flat sequence of per-row records:

```
per row i in [0, count):
  [int32 LE] length         # -1 means NULL
  [length bytes] data       # absent when length == -1
```

`VARLEN_GZIP` exists specifically so browsers can decompress with
`DecompressionStream("gzip")` without polyfills — used for metadata and
small text payloads served over HTTP.

### `RAW` (LONG / DOUBLE)

Plain little-endian 8 bytes per value. Fallback when the data shape
defeats the specialised codecs.

## Byte-exact round trip

Any compliant reader/writer pair MUST produce byte-identical output for
the same input given the same configuration (codec choices, row group
size, metadata). The `lastra-java` test suite includes a corpus of
fixtures used to validate `lastra-ts` and `lastra-py` against this
guarantee.

## Reference implementations

| Language | Repo | Status |
|----------|------|--------|
| Java | [QTSurfer/lastra-java](https://github.com/QTSurfer/lastra-java) | Reference (v0.8.0) |
| TypeScript | [QTSurfer/lastra-ts](https://github.com/QTSurfer/lastra-ts) | Reader feature-complete |
| Python | [QTSurfer/lastra-py](https://github.com/QTSurfer/lastra-py) | Planned |
