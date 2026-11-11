# Change Log

## [Unreleased]

## [0.1.0]

First release.

- Ordered key/value buckets with ACID transactions (`begin`, `begin-read`,
  `commit`, `rollback`).
- Point reads and writes, bulk reads and writes, range scans, projected scans.
- Document envelopes: a byte-offset header plus independently compressed field
  blobs, so a single-field read does not pay for the whole document.
- `version` var.

Fixed before the first release:

- Compaction could discard a write that committed while it ran. The copy reads
  a snapshot and the file is then swapped in wholesale, so the writer slot is
  now held for the whole compaction.
- A plain value that happens to start with the envelope marker was read back as
  a malformed envelope. Detection now requires the header to parse, and a value
  whose header does not parse is returned as a plain document.
- Envelope headers store the field count and each key length as `uint16`;
  values that did not fit were silently truncated, dropping fields on read.
  They are now rejected before the write.
- Envelopes written under the earlier `DRS1` marker still decode.
