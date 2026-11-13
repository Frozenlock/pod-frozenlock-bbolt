# pod-frozenlock-bbolt

A [babashka](https://babashka.org) pod exposing
[bbolt](https://github.com/etcd-io/bbolt) — an embedded B+tree key/value
store — as a generic ordered key/value store with ACID transactions.

The pod is deliberately dumb. It stores opaque strings in buckets, keeps keys
ordered, and gives you transactions. It has **no query language**: filtering,
sorting and projection are the caller's business, built on top of `scan`.

## Usage

```clojure
(require '[babashka.pods :as pods])
(pods/load-pod 'frozenlock/bbolt "0.1.0")

(require '[pod.frozenlock.bbolt :as b])
```

Or declare it in `bb.edn` and just `require` the namespace:

```clojure
{:pods {frozenlock/bbolt {:version "0.1.0"}}}
```

## Namespace

`pod.frozenlock.bbolt`

## Vars

Every var takes the transaction id as its first argument (except `open`,
`close`, `begin`, `begin-read` and `compact`, which take the handle). `begin`
and `begin-read` return the transaction id.

| var | args | returns |
|---|---|---|
| `version` | — | the pod's version string |
| `open` | `path`, `durable?` | handle |
| `close` | `handle` | `nil` |
| `begin` | `handle` | tx id |
| `begin-read` | `handle` | tx id |
| `commit` | `tx` | `nil` |
| `rollback` | `tx` | `nil` |
| `get` | `tx`, `bucket`, `key` | `[kind payload]` or `nil` |
| `get-many` | `tx`, `bucket`, `keys` | `[[key [kind payload]] ...]` |
| `get-path` | `tx`, `bucket`, `key`, `field-key` | `[kind payload]` |
| `get-paths` | `tx`, `bucket`, `key`, `field-keys` | `[kind payload]` or a projected map |
| `raw-size` | `tx`, `bucket`, `key` | stored byte length |
| `put` | `tx`, `bucket`, `key`, `value` | `nil` |
| `put-many` | `tx`, `bucket`, `pairs` | `nil` |
| `put-field` | `tx`, `bucket`, `key`, `field-key`, `blob` | `nil` |
| `put-doc` | `tx`, `bucket`, `key`, `keys`, `offsets`, `body` | `nil` |
| `put-doc-many` | `tx`, `bucket`, `[[key keys offsets body] ...]` | `nil` |
| `delete` | `tx`, `bucket`, `key` | `nil` |
| `delete-many` | `tx`, `bucket`, `keys` | `nil` |
| `scan` | `tx`, `bucket`, `from`, `from-excl?`, `to`, `to-excl?`, `limit` | `[[key [kind payload]] ...]` |
| `scan-paths` | `tx`, `bucket`, `from`, `from-excl?`, `to`, `to-excl?`, `limit`, `paths`, `project-plain?` | `[[key [kind payload]] ...]` or projected maps |
| `buckets` | `tx` | bucket names |
| `create-bucket` | `tx`, `bucket` | `nil` |
| `delete-bucket` | `tx`, `bucket` | `nil` |
| `count` | `tx`, `bucket` | integer |
| `stats` | `tx` or `handle` | map |
| `compact` | `handle` | `nil` |

Keys and values are strings. Keys are ordered bytewise, so a caller that wants
a particular order encodes it into the key. `nil` `from`/`to` means unbounded;
`limit <= 0` means no limit.

`open` takes a `durable?` flag, default `true`. `false` sets bbolt's `NoSync`:
commits no longer fsync, so a power loss can lose recent commits while the
file stays consistent.

`put-doc` is the envelope writer: `keys` are the field keys, `offsets` one
`[offset length]` pair per key into `body`, and the pod compresses each slice
and assembles the envelope. Passing `nil` for `keys` stores `body` verbatim.

### Return shapes

A read returns a `[kind payload]` pair, because a stored value is either a
plain transit document or an envelope whose fields are stored separately:

| kind | payload | meaning |
|---|---|---|
| `"doc"` | the stored string | a plain (small) document, whole |
| `"fields"` | `[[field-key blob] ...]` | an envelope, one entry per stored field |
| `"field"` | one blob | `get-path` found the requested field |
| `"missing"` | `""` | `get-path` found no such field |
| `"nodoc"` | `""` | `get-path` found no such document |

`get` and `get-many` return `"doc"` or `"fields"`; `get-path` returns
`"field"`, `"doc"`, `"missing"` or `"nodoc"`. `"missing"` and `"nodoc"` are
kept apart because they call for different fallbacks: a missing field still
has a document to read, a missing document does not.

Each blob is a complete transit value on its own — the writer disables
transit's cache, so a blob never refers to another field's encoding.

`get-paths` and `scan-paths` project instead: they return the same
`[kind payload]` shape for an envelope, but a plain document comes back as a
projected map (or `["doc" blob]` when `project-plain?` is false).

## Compaction

`compact` rewrites the database into a fresh file and swaps it in, reclaiming
the pages freed by deletes — bbolt reuses freed pages but never shrinks its
file. It is a full rewrite, so it costs time and disk proportional to the
database, not to what was freed:

| database | compaction |
|---|---|
| 1 MB | ~26 ms |
| 100 MB | ~0.5 s |
| 1 GB | ~5.6 s |
| 10 GB | ~56 s |

Roughly 180 MB/s, and it needs **2× the database size free** while it runs.

The writer slot is held for the whole operation, so writes block for its
duration; reads are unaffected. That is fine for the sizes above, but on a
database of tens of GB, schedule compaction rather than triggering it from a
write path.

## Transactions

`begin` takes bbolt's single writer slot and blocks until it is free; `begin-read`
takes a read-only snapshot and never blocks. A transaction must be committed or
rolled back — an abandoned writer transaction blocks every later writer.

## Document envelopes

A value is either plain transit JSON (small documents) or an **envelope**: a
byte-offset header followed by independently compressed field blobs.

```
envelope := "PBB1" [u32be headerLen] [header] [blobs...]
header   := [u16 count] count * {[u16 keyLen] [key] [u32 off] [u32 len] [u32 origLen] [u8 flags]}
```

`off` is relative to the start of the blob area; `key` is the transit-encoded
map key. Envelopes written under the earlier `DRS1` marker still decode.

A value is only treated as an envelope when its header parses. `put` stores
opaque strings, so a plain value that happens to start with `PBB1` is returned
unchanged rather than being misread. Each blob is compressed when that helps: **zstd** at
`SpeedBetterCompression` first, falling back to **flate** when zstd stores the
input raw, which it does for incompressible data while flate still finds
structure in a low-entropy alphabet such as base64 or hex.

The envelope is what makes multi-MB documents cheap to read:

| operation | 5 MB document |
|---|---|
| `get` (whole document) | ~43 ms |
| `get-path` (one field) | ~0.6 ms |
| `get-paths` (projection) | ~0.7 ms |
| `put-field` (update one field) | ~2.5 ms |
| `scan-paths` (projected scan) | ~1.5 ms for 20 MB |

## Build and test

```sh
go build -o pod-frozenlock-bbolt .
script/test
```

`script/test` builds the binary, runs the Go tests (which call the internals
directly), then runs `test/script.clj`, which loads the built binary as a pod
and exercises it over bencode. The second half is what catches a var declared
in `describe` but missing from the dispatch switch, or the reverse.

## Versioning

The pod follows [semver](https://semver.org). The version appears in three
places, which must be bumped together:

- the `version` constant in `main.go`, reported by the `version` var
- the git tag (`v0.1.0`)
- the pod-registry manifest

A client that depends on a specific var set should check `version` at load time
rather than assume the pod on `PATH` matches.

## License

Copyright © 2026 frozenlock

Distributed under the Eclipse Public License 2.0 (see `LICENSE`).
