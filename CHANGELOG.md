# Change Log

## [Unreleased]

## [0.1.2]

- A transaction whose first operation was abandoned while it waited for the
  writer slot no longer orphans itself. The transaction begins lazily, so the
  client's rollback could arrive before the pod had registered it; the pod
  then registered it anyway and nobody ever finished it, taking bbolt's single
  writer slot for good. `finish` on an unknown id now leaves a tombstone that
  the pending begin checks.
- `tx` holds `closeMu` across the begin, like `begin`. A compaction holds it
  for its whole run and closes the database, so a transaction that began under
  it could be closed out from under the caller.
- `finish` looks the id up and leaves its tombstone under one lock. In two
  critical sections a begin could register in between and never be finished -
  the same orphan, in a narrower window.
- A tombstone is only left when a begin is actually pending. Before, every
  transaction that never touched the pod - `(transact! d identity)`, an empty
  `upsert-many!`, an `f` that throws before its first operation - left one
  behind forever, and the set was cleared wholesale at 4096, which could drop
  the one that mattered.

## [0.1.1]

- A transaction that only reads commits as a rollback instead of a commit, so
  it skips the fsync. `finish` now checks the transaction was begun writable
  as well as dirty.
- `get-path` answers `["nodoc" ""]` for an absent document, distinct from
  `["missing" ""]` for a document that has no such field. The adapter needs the
  difference: a missing field still has a document to fall back to.

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
- `count` reported the last COMMITTED state inside a write transaction: it used
  `Bucket.Stats().KeyN`, which walks the on-disk pages, and pages are only
  written at commit. It returned 0 for a bucket created in the same
  transaction, and a stale number after any put or delete. The count is now
  kept in the bucket's `Sequence` field, maintained by every write that can
  change the key set - so it is correct inside a transaction and O(1), where a
  cursor walk would have been correct but O(n).
