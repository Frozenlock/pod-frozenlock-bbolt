// pod-frozenlock-bbolt: a babashka pod exposing bbolt (an embedded B+tree
// key/value store) as a generic ordered key/value store with transactions.
//
// The pod is deliberately dumb: it stores opaque strings in buckets, keeps
// keys ordered, and gives you ACID transactions. It has no query language -
// filtering, sorting and projection are the caller's business, built on top
// of `scan`.
//
// Protocol: bencode messages on stdin/stdout, JSON payloads.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/bencode"
	bolt "go.etcd.io/bbolt"
)

const ns = "pod.frozenlock.bbolt"

// version is the pod's own version, reported by the `version` var so a client
// can check what it is talking to at run time. It is kept in step with the git
// tag and the pod-registry manifest by the release script.
const version = "0.1.1"

// Bulk-load page fill. bbolt splits a page when it is half full by default,
// which for a sequential load means every page is written twice and the file
// ends up twice the size. 0.9 keeps a little slack for the inserts that follow
// without paying for the splits.
const (
	bulkFillPercent = 0.9
	bulkFillMin     = 100
)

var vars = []string{
	"version",
	"open", "close",
	"begin", "begin-read", "commit", "rollback",
	"get", "get-many", "get-path", "get-paths", "raw-size", "put-field", "put", "put-doc", "put-doc-many", "put-many", "delete", "delete-many",
	"scan", "scan-paths", "buckets", "create-bucket", "delete-bucket", "count", "stats", "compact",
}

// txRec keeps the owning DB with the transaction so `close` can roll back the
// transactions of the DB it is about to close (bbolt's Close waits for them).
type txRec struct {
	tx       *bolt.Tx
	db       *bolt.DB
	writable bool // began as a write transaction (or upgraded to one)
	dirty    bool // any write happened; a clean tx commits as a rollback
}

// dbRef is one open file, shared by every handle that names it. bbolt
// file-locks: a second Open on a live path blocks forever, so handles to the
// same path must share one *bolt.DB.
type dbRef struct {
	db   *bolt.DB
	refs int
}

type pod struct {
	mu sync.Mutex
	// closeMu is held for writing while a *bolt.DB is closed or swapped, and
	// for reading across `begin`. Without it a transaction can be created
	// between the closer's rollback sweep and its Close, which then waits
	// forever for a transaction it never saw. Lock order: closeMu, then mu.
	closeMu sync.RWMutex
	dbs     map[string]*bolt.DB
	paths   map[string]*dbRef
	openMu  map[string]*sync.Mutex
	txs     map[string]*txRec
	seq     int64
}

func newPod() *pod {
	return &pod{
		dbs:    map[string]*bolt.DB{},
		paths:  map[string]*dbRef{},
		openMu: map[string]*sync.Mutex{},
		txs:    map[string]*txRec{},
	}
}

func (p *pod) nextID(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, atomic.AddInt64(&p.seq, 1))
}

// --- message plumbing -------------------------------------------------------

// bufio.Writer is not safe for concurrent use, and every invoke replies from
// its own goroutine, so replies must be serialized.
var writeMu sync.Mutex

func writeMsg(w *bufio.Writer, m map[string]interface{}) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	if err := bencode.NewEncoder(w).Encode(m); err != nil {
		return err
	}
	return w.Flush()
}

// rawJSON is a reply payload that is already JSON. `scan` builds its result
// straight into a byte slice instead of a []interface{} tree, which for a
// 100k-document scan saves ~100k inner slices, ~100k string copies and the
// json.Marshal pass over the whole thing.
type rawJSON []byte

func reply(w *bufio.Writer, id string, value interface{}) {
	if r, ok := value.(rawJSON); ok {
		replyRaw(w, id, r)
		return
	}
	payload, err := json.Marshal(value)
	if err != nil {
		replyErr(w, id, err.Error())
		return
	}
	writeMsg(w, map[string]interface{}{
		"id":     id,
		"value":  string(payload),
		"status": []interface{}{"done"},
	})
}

// replyRaw writes the bencode reply around an already-encoded JSON payload.
// It skips the `string(payload)` copy that writeMsg would make, which matters
// when the payload is tens of megabytes. Keys are written in sorted order, as
// bencode requires.
func replyRaw(w *bufio.Writer, id string, payload []byte) {
	writeMu.Lock()
	defer writeMu.Unlock()
	w.WriteByte('d')
	writeBencodeString(w, "id")
	writeBencodeString(w, id)
	writeBencodeString(w, "status")
	w.WriteString("l4:donee")
	writeBencodeString(w, "value")
	writeBencodeBytes(w, payload)
	w.WriteByte('e')
	w.Flush()
}

func writeBencodeString(w *bufio.Writer, s string) {
	w.WriteString(strconv.Itoa(len(s)))
	w.WriteByte(':')
	w.WriteString(s)
}

func writeBencodeBytes(w *bufio.Writer, b []byte) {
	w.WriteString(strconv.Itoa(len(b)))
	w.WriteByte(':')
	w.Write(b)
}

func replyErr(w *bufio.Writer, id string, msg string) {
	writeMsg(w, map[string]interface{}{
		"id":         id,
		"ex-message": msg,
		"ex-data":    "{}",
		"status":     []interface{}{"done", "error"},
	})
}

// --- argument helpers -------------------------------------------------------

func str(args []interface{}, i int) string {
	if i >= len(args) || args[i] == nil {
		return ""
	}
	s, _ := args[i].(string)
	return s
}

func optStr(args []interface{}, i int) []byte {
	if i >= len(args) || args[i] == nil {
		return nil
	}
	s, ok := args[i].(string)
	if !ok {
		return nil
	}
	return []byte(s)
}

func num(args []interface{}, i int) int {
	if i >= len(args) || args[i] == nil {
		return 0
	}
	f, _ := args[i].(float64)
	return int(f)
}

func flag(args []interface{}, i int) bool {
	return i < len(args) && args[i] == true
}

// --- key counting -----------------------------------------------------------
//
// A bucket's `Sequence` field holds its number of keys. bbolt has no O(1)
// count of its own: `Bucket.Stats().KeyN` walks the on-disk PAGES, so inside a
// write transaction - where the changes are still in the in-memory node tree -
// it reports the last committed state. A cursor walk is correct but O(n).
//
// The sequence is transactional metadata in the bucket header: it is rolled
// back with the transaction, works for inline buckets, and `bolt.Compact`
// copies it. Nothing else in this pod uses it, so it is reserved for the count.
//
// Every write that can change the KEY SET must go through putKey/delKey.
// `put-field` rewrites one field inside an existing envelope and cannot.

// hasKey reports whether k is present. A cursor Seek, not Get: an empty value
// is a valid value, and Get cannot tell it from a missing key.
func hasKey(b *bolt.Bucket, k []byte) bool {
	ck, _ := b.Cursor().Seek(k)
	return ck != nil && bytes.Equal(ck, k)
}

// putKey writes k/v and bumps the count when k is new.
func putKey(b *bolt.Bucket, k, v []byte) error {
	existed := hasKey(b, k)
	if err := b.Put(k, v); err != nil {
		return err
	}
	if existed {
		return nil
	}
	return b.SetSequence(b.Sequence() + 1)
}

// delKey removes k and drops the count when k was there.
func delKey(b *bolt.Bucket, k []byte) error {
	if !hasKey(b, k) {
		return nil
	}
	if err := b.Delete(k); err != nil {
		return err
	}
	if s := b.Sequence(); s > 0 {
		return b.SetSequence(s - 1)
	}
	return nil
}

// --- handles ----------------------------------------------------------------

func (p *pod) db(handle string) (*bolt.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	db, ok := p.dbs[handle]
	if !ok {
		return nil, fmt.Errorf("no such handle: %s", handle)
	}
	return db, nil
}

// tx returns the transaction `id` names, beginning one on first use.
//
// The id is "<handle>:<n>", so a transaction costs no round-trip until its
// first operation - and that operation runs INSIDE the transaction, so a read
// is never outside the snapshot it will later write to. Beginning eagerly
// instead would cost an extra round-trip; beginning lazily on the client side
// would leave the first read outside the transaction and open a lost-update
// window against concurrent writers.
//
// It is always a WRITE transaction, even when the transaction only reads: a
// read transaction cannot be upgraded in place, and dropping its snapshot to
// begin a write one would let two read-modify-write transactions read the same
// value and lose an update. Reads that are not in a transaction go through
// `withTx`, which uses `db.View` and so never takes the writer slot.
func (p *pod) tx(id string) (*bolt.Tx, error) {
	p.mu.Lock()
	rec, ok := p.txs[id]
	p.mu.Unlock()
	if ok {
		return rec.tx, nil
	}

	i := strings.LastIndex(id, ":")
	if i < 0 {
		return nil, fmt.Errorf("no such transaction: %s", id)
	}
	db, err := p.db(id[:i])
	if err != nil {
		return nil, err
	}
	// Blocks until bbolt's single writer slot is free, so it must not hold
	// p.mu - the message loop and other operations need it.
	tx, db, err := p.beginOn(id[:i], db, true)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	// Another operation may have begun it while we waited for the slot.
	if rec, ok := p.txs[id]; ok {
		p.mu.Unlock()
		_ = tx.Rollback()
		return rec.tx, nil
	}
	p.txs[id] = &txRec{tx: tx, db: db, writable: true}
	p.mu.Unlock()
	return tx, nil
}

// rollbackFor rolls back every open transaction belonging to `db`. Caller
// holds p.mu.
func (p *pod) rollbackFor(db *bolt.DB) {
	for id, rec := range p.txs {
		if rec.db == db {
			_ = rec.tx.Rollback()
			delete(p.txs, id)
		}
	}
}

// --- operations -------------------------------------------------------------

// open returns a handle for `path`. Handles to the same path share one
// *bolt.DB, refcounted: a second bolt.Open on a live path blocks forever on
// bbolt's file lock. The first open's sync setting wins.
func (p *pod) open(args []interface{}) (interface{}, error) {
	path := str(args, 0)
	durable := !(len(args) > 1 && args[1] == false)

	// Claim the path so two concurrent opens don't both call bolt.Open.
	p.mu.Lock()
	lk, ok := p.openMu[path]
	if !ok {
		lk = &sync.Mutex{}
		p.openMu[path] = lk
	}
	p.mu.Unlock()

	lk.Lock()
	defer lk.Unlock()

	p.mu.Lock()
	if ref, ok := p.paths[path]; ok {
		ref.refs++
		h := p.nextID("db")
		p.dbs[h] = ref.db
		p.mu.Unlock()
		return h, nil
	}
	p.mu.Unlock()

	// A file held by another process still blocks; time out so the caller
	// gets an error rather than a hang.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	// NoSync trades durability for speed: commits no longer fsync, so a power
	// loss can lose recent commits (the file stays consistent).
	db.NoSync = !durable

	p.mu.Lock()
	p.paths[path] = &dbRef{db: db, refs: 1}
	h := p.nextID("db")
	p.dbs[h] = db
	p.mu.Unlock()
	return h, nil
}

func (p *pod) close(args []interface{}) (interface{}, error) {
	h := str(args, 0)
	// No begin may be in flight while we sweep and close.
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	p.mu.Lock()
	db, ok := p.dbs[h]
	if !ok {
		p.mu.Unlock()
		return nil, fmt.Errorf("no such handle: %s", h)
	}
	delete(p.dbs, h)

	// Drop one reference to the file; only the last handle closes it.
	last := true
	for path, ref := range p.paths {
		if ref.db != db {
			continue
		}
		ref.refs--
		if last = ref.refs <= 0; last {
			delete(p.paths, path)
			// Close waits for open transactions; roll them back first or it
			// deadlocks (their commits can no longer be read).
			p.rollbackFor(db)
		}
		break
	}
	p.mu.Unlock()
	if !last {
		return nil, nil
	}
	return nil, db.Close()
}

// beginOn begins a transaction on `db`, retrying once against the file that is
// current now when the first attempt fails.
//
// A compaction holds bbolt's writer slot and then swaps the file out from under
// any Begin that was waiting for it, so that Begin can land on a closed
// database. Re-resolving the handle and trying again is enough: the compaction
// has finished by the time the slot is free.
func (p *pod) beginOn(handle string, db *bolt.DB, writable bool) (*bolt.Tx, *bolt.DB, error) {
	tx, err := db.Begin(writable)
	if err != nil {
		if cur, cerr := p.db(handle); cerr == nil && cur != db {
			db = cur
			tx, err = db.Begin(writable)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return tx, db, nil
}

func (p *pod) begin(args []interface{}, writable bool) (interface{}, error) {
	handle := str(args, 0)
	// Held across the whole begin: the transaction must be registered before a
	// concurrent close/compact sweeps for open transactions, or Close blocks on
	// one it cannot see.
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	db, err := p.db(handle)
	if err != nil {
		return nil, err
	}
	// A write tx blocks until the single writer slot is free. Runs in its own
	// goroutine so the message loop keeps reading (a commit from another
	// thread must be able to arrive while this one waits). A read tx never
	// blocks and never fsyncs.
	tx, db, err := p.beginOn(handle, db, writable)
	if err != nil {
		return nil, err
	}
	id := p.nextID("tx")
	p.mu.Lock()
	p.txs[id] = &txRec{tx: tx, db: db, writable: true}
	p.mu.Unlock()
	return id, nil
}

func (p *pod) finish(args []interface{}, commit bool) (interface{}, error) {
	id := str(args, 0)
	p.mu.Lock()
	rec, ok := p.txs[id]
	delete(p.txs, id)
	p.mu.Unlock()
	if !ok {
		// A transaction that ran no operation never began; nothing to finish.
		return nil, nil
	}
	tx := rec.tx
	// A transaction that wrote nothing commits as a rollback: bbolt's Commit
	// fsyncs, and there is nothing to make durable. Deciding here (rather than
	// in the client) means a write method that forgets to flag itself can't
	// silently lose data.
	if commit && rec.writable && rec.dirty && tx.Writable() {
		return nil, tx.Commit()
	}
	return nil, tx.Rollback()
}

// withTx runs fn against the transaction `id` names, or - when `id` is a
// database handle - against a short read transaction.
//
// The handle form is for reads outside any transaction: it costs one
// round-trip and never takes bbolt's writer slot. A transaction id begins the
// transaction on first use (see tx).
func (p *pod) withTx(id string, fn func(*bolt.Tx) (interface{}, error)) (interface{}, error) {
	if tx, err := p.tx(id); err == nil {
		return fn(tx)
	}
	db, err := p.db(id)
	if err != nil {
		return nil, err
	}
	var out interface{}
	err = db.View(func(tx *bolt.Tx) error {
		var e error
		out, e = fn(tx)
		return e
	})
	return out, err
}

func (p *pod) get(args []interface{}) (interface{}, error) {
	bucket, key := str(args, 1), str(args, 2)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil, nil
		}
		v := b.Get([]byte(key))
		if v == nil {
			return nil, nil
		}
		// Build the JSON directly: for a multi-MB document the []interface{}
		// tree valuePayload builds is one slice and one string copy per field,
		// and json.Marshal then walks it again.
		buf, err := appendValueJSON(nil, v)
		if err != nil {
			return nil, err
		}
		return rawJSON(buf), nil
	})
}

// getMany fetches several keys in one round-trip, in the order asked for.
// Missing keys are skipped. A filtered fetch that projects to the `where`
// fields needs the full documents of the survivors; without this that is one
// RPC per match.
//
//	args: tx bucket keys
func (p *pod) getMany(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	keys, ok := args[2].([]interface{})
	if !ok {
		return nil, fmt.Errorf("get-many expects a list of keys")
	}
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return []interface{}{}, nil
		}
		buf := make([]byte, 0, 4096)
		buf = append(buf, '[')
		n := 0
		for _, k := range keys {
			ks, _ := k.(string)
			v := b.Get([]byte(ks))
			if v == nil {
				continue
			}
			if n > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '[')
			buf = appendJSONString(buf, []byte(ks))
			buf = append(buf, ',')
			var err error
			if buf, err = appendValueJSON(buf, v); err != nil {
				return nil, err
			}
			buf = append(buf, ']')
			n++
		}
		buf = append(buf, ']')
		return rawJSON(buf), nil
	})
}

// putDoc stores a document as an envelope: the client sends the concatenated
// field encodings plus their offsets, and the pod compresses each field and
// writes the offset header. One round-trip, and the stored value is smaller
// than the JSON it came from.
//
//	args: tx bucket key keys offsets body
func (p *pod) putDoc(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b, err := tx.CreateBucketIfNotExists([]byte(str(args, 1)))
	if err != nil {
		return nil, err
	}
	body := []byte(str(args, 5))
	if len(args) < 6 {
		return nil, fmt.Errorf("put-doc expects 6 arguments")
	}
	if args[3] == nil {
		// Small document: a plain transit value is smaller than an envelope.
		return nil, putKey(b, []byte(str(args, 2)), body)
	}
	keys, ok := args[3].([]interface{})
	if !ok {
		return nil, fmt.Errorf("put-doc expects a list of keys")
	}
	offs, ok := args[4].([]interface{})
	if !ok || len(offs) != len(keys) {
		return nil, fmt.Errorf("put-doc expects one offset pair per key")
	}

	fields := make([]envField, 0, len(keys))
	var out bytes.Buffer
	for i, k := range keys {
		ks, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("put-doc expects string keys")
		}
		pair, ok := offs[i].([]interface{})
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("put-doc expects [offset length] pairs")
		}
		off, _ := pair[0].(float64)
		ln, _ := pair[1].(float64)
		if off < 0 || ln < 0 || int(off+ln) > len(body) {
			return nil, fmt.Errorf("put-doc field %q out of range", ks)
		}
		blob := body[int(off):int(off+ln)]
		orig := len(blob)
		c, flags, ok := compressField(blob)
		if ok {
			blob = c
		}
		fields = append(fields, envField{
			key:     ks,
			off:     uint32(out.Len()),
			length:  uint32(len(blob)),
			origLen: uint32(orig),
			flags:   flags,
		})
		out.Write(blob)
	}
	env, err := buildEnvelope(fields, out.Bytes())
	if err != nil {
		return nil, err
	}
	return nil, putKey(b, []byte(str(args, 2)), env)
}

// putDocMany is putDoc for a batch, in one round-trip.
//
//	args: tx bucket [[key keys offsets body] ...]
func (p *pod) putDocMany(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b, err := tx.CreateBucketIfNotExists([]byte(str(args, 1)))
	if err != nil {
		return nil, err
	}
	docs, ok := args[2].([]interface{})
	if !ok {
		return nil, fmt.Errorf("put-doc-many expects a list of documents")
	}
	// A bulk load should fill pages densely. bbolt's default 0.5 splits every
	// page half-full, which doubles the file and the number of splits; its own
	// compaction uses 1.0 for the same reason. Only for a real batch - a
	// handful of documents gains nothing and would leave the pages dense for
	// the random inserts that follow.
	//
	// Not restored afterwards: bbolt spills pages at COMMIT, so resetting the
	// field before returning would put the default back before the split ever
	// happens. `tx.Bucket` hands out a fresh handle per call, so this only
	// affects this batch.
	if len(docs) >= bulkFillMin {
		b.FillPercent = bulkFillPercent
	}
	for _, d := range docs {
		rec, ok := d.([]interface{})
		if !ok || len(rec) != 4 {
			return nil, fmt.Errorf("put-doc-many expects [key keys offsets body]")
		}
		key, _ := rec[0].(string)
		body := []byte(str(rec, 3))
		if rec[1] == nil {
			// Small document: a plain transit value is smaller than an envelope.
			if err := putKey(b, []byte(key), body); err != nil {
				return nil, err
			}
			continue
		}
		keys, _ := rec[1].([]interface{})
		offs, _ := rec[2].([]interface{})
		if len(keys) != len(offs) {
			return nil, fmt.Errorf("put-doc-many expects one offset pair per key")
		}
		fields := make([]envField, 0, len(keys))
		var out bytes.Buffer
		for i, k := range keys {
			ks, _ := k.(string)
			pair, _ := offs[i].([]interface{})
			if len(pair) != 2 {
				return nil, fmt.Errorf("put-doc-many expects [offset length] pairs")
			}
			off, _ := pair[0].(float64)
			ln, _ := pair[1].(float64)
			if off < 0 || ln < 0 || int(off+ln) > len(body) {
				return nil, fmt.Errorf("put-doc-many field %q out of range", ks)
			}
			blob := body[int(off):int(off+ln)]
			orig := len(blob)
			c, flags, ok := compressField(blob)
			if ok {
				blob = c
			}
			fields = append(fields, envField{
				key:     ks,
				off:     uint32(out.Len()),
				length:  uint32(len(blob)),
				origLen: uint32(orig),
				flags:   flags,
			})
			out.Write(blob)
		}
		env, err := buildEnvelope(fields, out.Bytes())
		if err != nil {
			return nil, err
		}
		if err := putKey(b, []byte(key), env); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// rawSize returns the stored byte length of a value, so tests can assert that
// compression actually shrank it.
func (p *pod) rawSize(args []interface{}) (interface{}, error) {
	tx, err := p.tx(str(args, 0))
	if err != nil {
		return nil, err
	}
	b := tx.Bucket([]byte(str(args, 1)))
	if b == nil {
		return 0, nil
	}
	v := b.Get([]byte(str(args, 2)))
	if v == nil {
		return 0, nil
	}
	return len(v), nil
}

// putField replaces one field of an envelope in place. The other blobs are
// copied as-is (already compressed), so updating a small field of a multi-MB
// document costs one small read and one small compression.
//
//	args: tx bucket key field-key blob
func (p *pod) putField(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b := tx.Bucket([]byte(str(args, 1)))
	if b == nil {
		return nil, fmt.Errorf("no such bucket")
	}
	key := str(args, 2)
	v := b.Get([]byte(key))
	if v == nil {
		return nil, fmt.Errorf("no such document: %s", key)
	}
	fields, body, ok := envelopeOf(v)
	if !ok {
		return nil, fmt.Errorf("document is not an envelope: %s", key)
	}
	want := str(args, 3)
	blob := []byte(str(args, 4))
	orig := len(blob)
	c, flags, ok := compressField(blob)
	if ok {
		blob = c
	}

	var out bytes.Buffer
	found := false
	for i := range fields {
		f := &fields[i]
		src := body[f.off : f.off+f.length]
		if f.key == want {
			src = blob
			f.length = uint32(len(blob))
			f.origLen = uint32(orig)
			f.flags = flags
			found = true
		}
		f.off = uint32(out.Len())
		out.Write(src)
	}
	if !found {
		return nil, fmt.Errorf("no such field: %s", want)
	}
	env, err := buildEnvelope(fields, out.Bytes())
	if err != nil {
		return nil, err
	}
	// The key already exists (the field was found in it), so the count is
	// unchanged; putKey keeps every write on the same path.
	return nil, putKey(b, []byte(key), env)
}

// getPath returns one field of a document without touching the rest.
//
//	args: tx bucket key field-key
//
// Returns [kind blob] where kind is "field" (envelope hit), "doc" (the value
// is not an envelope, so the whole document came back), "missing" (the
// document exists but has no such field) or "nodoc" (no such document).
func (p *pod) getPath(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		return getPathIn(tx, bucket, args)
	})
}

func getPathIn(tx *bolt.Tx, bucket string, args []interface{}) (interface{}, error) {
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		return []interface{}{"nodoc", ""}, nil
	}
	v := b.Get([]byte(str(args, 2)))
	if v == nil {
		return []interface{}{"nodoc", ""}, nil
	}
	fields, body, ok := envelopeOf(v)
	if !ok {
		return []interface{}{"doc", string(v)}, nil
	}
	want := str(args, 3)
	for _, f := range fields {
		if f.key != want {
			continue
		}
		blob, err := fieldBytes(f, body)
		if err != nil {
			return nil, err
		}
		return []interface{}{"field", string(blob)}, nil
	}
	return []interface{}{"missing", ""}, nil
}

// getPaths is getPath for several fields in one round-trip, so a projection
// costs one RPC instead of one per path.
//
//	args: tx bucket key field-keys
func (p *pod) getPaths(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		return getPathsIn(tx, bucket, args)
	})
}

func getPathsIn(tx *bolt.Tx, bucket string, args []interface{}) (interface{}, error) {
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	v := b.Get([]byte(str(args, 2)))
	if v == nil {
		return nil, nil
	}
	wants, ok := args[3].([]interface{})
	if !ok {
		return nil, fmt.Errorf("get-paths expects a list of keys")
	}
	keys := make([]string, len(wants))
	for i, w := range wants {
		keys[i], _ = w.(string)
	}
	// No projection asked for: send the value whole.
	if len(keys) == 0 {
		return valuePayload(v)
	}
	buf, err := appendProjectedJSON(nil, v, keys, projectKeys(keys), true)
	if err != nil {
		return nil, err
	}
	return rawJSON(buf), nil
}

func (p *pod) markDirty(id string) {
	p.mu.Lock()
	if rec, ok := p.txs[id]; ok {
		rec.dirty = true
	}
	p.mu.Unlock()
}

func (p *pod) put(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b, err := tx.CreateBucketIfNotExists([]byte(str(args, 1)))
	if err != nil {
		return nil, err
	}
	return nil, putKey(b, []byte(str(args, 2)), []byte(str(args, 3)))
}

// putMany writes [[key value] ...] in one round-trip. Bulk loads otherwise pay
// one RPC per document.
func (p *pod) putMany(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b, err := tx.CreateBucketIfNotExists([]byte(str(args, 1)))
	if err != nil {
		return nil, err
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("put-many expects a list of [key value] pairs")
	}
	pairs, ok := args[2].([]interface{})
	if !ok {
		return nil, fmt.Errorf("put-many expects a list of [key value] pairs")
	}
	for _, pr := range pairs {
		kv, ok := pr.([]interface{})
		if !ok || len(kv) != 2 {
			return nil, fmt.Errorf("put-many expects [key value] pairs")
		}
		k, ok1 := kv[0].(string)
		v, ok2 := kv[1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("put-many expects string keys and values")
		}
		if err := putKey(b, []byte(k), []byte(v)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// deleteMany removes a list of keys in one round-trip.
func (p *pod) deleteMany(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b := tx.Bucket([]byte(str(args, 1)))
	if b == nil {
		return nil, nil
	}
	if len(args) < 3 {
		return nil, fmt.Errorf("delete-many expects a list of keys")
	}
	keys, ok := args[2].([]interface{})
	if !ok {
		return nil, fmt.Errorf("delete-many expects a list of keys")
	}
	for _, k := range keys {
		ks, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("delete-many expects string keys")
		}
		if err := delKey(b, []byte(ks)); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (p *pod) del(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	b := tx.Bucket([]byte(str(args, 1)))
	if b == nil {
		return nil, nil
	}
	return nil, delKey(b, []byte(str(args, 2)))
}

// --- direct JSON encoding ---------------------------------------------------
//
// The scan results are the only replies big enough for the intermediate
// []interface{} tree to matter: one inner slice and one string copy per
// document, then json.Marshal over the whole thing. These helpers append the
// same JSON straight into a byte slice.

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string literal. Bytes >= 0x80 are
// passed through: the payloads are transit JSON, which is valid UTF-8, and
// JSON allows raw UTF-8 in strings.
func appendJSONString(dst, s []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// appendValueJSON appends the JSON for one stored value, matching what
// json.Marshal(valuePayload(v)) would produce:
//
//	["doc","<transit json>"]        - a plain value
//	["fields",[["key","blob"],...]] - an envelope
func appendValueJSON(dst, v []byte) ([]byte, error) {
	fields, body, ok := envelopeOf(v)
	if !ok {
		dst = append(dst, `["doc",`...)
		dst = appendJSONString(dst, v)
		return append(dst, ']'), nil
	}
	dst = append(dst, `["fields",[`...)
	for i, f := range fields {
		if i > 0 {
			dst = append(dst, ',')
		}
		blob, err := fieldBytes(f, body)
		if err != nil {
			return nil, err
		}
		dst = append(dst, '[')
		dst = appendJSONString(dst, []byte(f.key))
		dst = append(dst, ',')
		dst = appendJSONString(dst, blob)
		dst = append(dst, ']')
	}
	return append(dst, ']', ']'), nil
}

// skipJSONSpace advances past JSON whitespace.
func skipJSONSpace(v []byte, i int) int {
	for i < len(v) {
		switch v[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// skipJSONValue advances past one JSON value starting at i and returns the
// index just past it. Strings are skipped with escape awareness; objects and
// arrays by depth, so a nested `}` or `,` cannot end the value early.
func skipJSONValue(v []byte, i int) (int, bool) {
	n := len(v)
	if i >= n {
		return i, false
	}
	switch v[i] {
	case '"':
		i++
		for i < n {
			switch v[i] {
			case '\\':
				i += 2
			case '"':
				return i + 1, true
			default:
				i++
			}
		}
		return i, false
	case '{', '[':
		depth := 0
		for i < n {
			switch v[i] {
			case '"':
				i++
				for i < n {
					switch v[i] {
					case '\\':
						i += 2
					case '"':
						goto closed
					default:
						i++
					}
				}
				return i, false
			closed:
				i++
			case '{', '[':
				depth++
				i++
			case '}', ']':
				depth--
				i++
				if depth == 0 {
					return i, true
				}
			default:
				i++
			}
		}
		return i, false
	default:
		start := i
		for i < n {
			switch v[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i, i > start
			default:
				i++
			}
		}
		return i, i > start
	}
}

// scanJSONObject walks the top-level members of a JSON object, calling fn with
// each key and its raw value text. It allocates nothing: the key and value are
// slices into v. Returns false when v is not a well-formed object.
func scanJSONObject(v []byte, fn func(key, val []byte)) bool {
	i := skipJSONSpace(v, 0)
	if i >= len(v) || v[i] != '{' {
		return false
	}
	i = skipJSONSpace(v, i+1)
	if i < len(v) && v[i] == '}' {
		return true
	}
	for {
		i = skipJSONSpace(v, i)
		if i >= len(v) || v[i] != '"' {
			return false
		}
		keyStart := i + 1
		i++
		for i < len(v) {
			switch v[i] {
			case '\\':
				i += 2
			case '"':
				goto keyDone
			default:
				i++
			}
		}
		return false
	keyDone:
		key := v[keyStart:i]
		i = skipJSONSpace(v, i+1)
		if i >= len(v) || v[i] != ':' {
			return false
		}
		i = skipJSONSpace(v, i+1)
		valStart := i
		var ok bool
		if i, ok = skipJSONValue(v, i); !ok {
			return false
		}
		fn(key, v[valStart:i])
		i = skipJSONSpace(v, i)
		if i >= len(v) {
			return false
		}
		switch v[i] {
		case ',':
			i++
		case '}':
			return true
		default:
			return false
		}
	}
}

// projectKeys precomputes the byte form of the requested keys. Doing it once
// per scan instead of once per document removes two allocations per document.
func projectKeys(wants []string) [][]byte {
	out := make([][]byte, len(wants))
	for i, w := range wants {
		out[i] = []byte(w)
	}
	return out
}

// projectPlainJSON reduces a plain transit-JSON document to the requested
// keys at the JSON level. Small documents are stored as one transit value, so
// without this a projection ships the whole document and the client decodes
// every field just to keep one.
//
// The scan is hand-rolled rather than json.Unmarshal into a
// map[string]json.RawMessage: that costs ~3us and 27 allocations per document,
// which on a 100k-document scan is more than the client saves by decoding one
// field instead of seven. This is ~0.3us and allocates only the output.
//
// Each value is copied out as its raw JSON text, which is a complete transit
// encoding on its own: the writer disables transit's cache, so no value
// carries a `"^N"` reference into a neighbouring field.
func projectPlainJSON(dst, v []byte, wantB [][]byte) ([]byte, bool) {
	if len(v) == 0 || v[0] != '{' {
		return dst, false
	}
	dst = append(dst, `["fields",[`...)
	first := true
	ok := scanJSONObject(v, func(key, val []byte) {
		for _, w := range wantB {
			if !bytes.Equal(key, w) {
				continue
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = append(dst, '[')
			dst = appendJSONString(dst, key)
			dst = append(dst, ',')
			dst = appendJSONString(dst, val)
			dst = append(dst, ']')
			break
		}
	})
	if !ok {
		return dst, false
	}
	return append(dst, ']', ']'), true
}

// appendProjectedJSON is appendValueJSON reduced to the requested field keys,
// in the order they were asked for. A missing key is skipped, as projectValue
// does.
func appendProjectedJSON(dst, v []byte, wants []string, wantB [][]byte, projectPlain bool) ([]byte, error) {
	fields, body, isEnv := envelopeOf(v)
	if !isEnv {
		if projectPlain {
			if out, ok := projectPlainJSON(dst, v, wantB); ok {
				return out, nil
			}
		}
		dst = append(dst, `["doc",`...)
		dst = appendJSONString(dst, v)
		return append(dst, ']'), nil
	}
	dst = append(dst, `["fields",[`...)
	first := true
	for _, w := range wants {
		for _, f := range fields {
			if f.key != w {
				continue
			}
			blob, err := fieldBytes(f, body)
			if err != nil {
				return nil, err
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = append(dst, '[')
			dst = appendJSONString(dst, []byte(f.key))
			dst = append(dst, ',')
			dst = appendJSONString(dst, blob)
			dst = append(dst, ']')
			break
		}
	}
	return append(dst, ']', ']'), nil
}

// scan returns [[key value] ...] in key order.
//
//	args: tx bucket from from-exclusive? to to-exclusive? limit
//
// nil from/to means unbounded; limit <= 0 means no limit.
func (p *pod) scan(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		return scanIn(tx, bucket, args)
	})
}

func scanIn(tx *bolt.Tx, bucket string, args []interface{}) (interface{}, error) {
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		return []interface{}{}, nil
	}
	from, to := optStr(args, 2), optStr(args, 4)
	fromExcl, toExcl := flag(args, 3), flag(args, 5)
	limit := num(args, 6)

	// Bounds are compared as bytes: `string(k)` per iteration would allocate
	// once per document just to throw it away.
	buf := make([]byte, 0, 4096)
	buf = append(buf, '[')
	n := 0
	c := b.Cursor()
	var k, v []byte
	if from != nil {
		k, v = c.Seek(from)
		if fromExcl && k != nil && bytes.Equal(k, from) {
			k, v = c.Next()
		}
	} else {
		k, v = c.First()
	}
	for ; k != nil; k, v = c.Next() {
		if to != nil {
			cmp := bytes.Compare(k, to)
			if cmp > 0 || (toExcl && cmp == 0) {
				break
			}
		}
		if n > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '[')
		buf = appendJSONString(buf, k)
		buf = append(buf, ',')
		var err error
		if buf, err = appendValueJSON(buf, v); err != nil {
			return nil, err
		}
		buf = append(buf, ']')
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	buf = append(buf, ']')
	return rawJSON(buf), nil
}

// scanPaths is scan with a projection: each document comes back as only its
// requested fields, so scanning multi-MB documents doesn't transfer fields
// nobody asked for.
//
//	args: tx bucket from from-exclusive? to to-exclusive? limit paths
func (p *pod) scanPaths(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		return scanPathsIn(tx, bucket, args)
	})
}

func scanPathsIn(tx *bolt.Tx, bucket string, args []interface{}) (interface{}, error) {
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		return []interface{}{}, nil
	}
	from, to := optStr(args, 2), optStr(args, 4)
	fromExcl, toExcl := flag(args, 3), flag(args, 5)
	limit := num(args, 6)
	if len(args) < 8 {
		return nil, fmt.Errorf("scan-paths expects 8 arguments")
	}
	wants, ok := args[7].([]interface{})
	if !ok {
		return nil, fmt.Errorf("scan-paths expects a list of paths")
	}
	keys := make([]string, len(wants))
	for i, w := range wants {
		keys[i], _ = w.(string)
	}
	wantB := projectKeys(keys)
	// A plain document is small; projecting it only pays when the caller asked
	// for specific fields. For a bare `where` the client wants whole documents
	// anyway, and returning them whole lets it skip the second round-trip.
	projectPlain := true
	if len(args) > 8 {
		projectPlain = flag(args, 8)
	}

	buf := make([]byte, 0, 4096)
	buf = append(buf, '[')
	n := 0
	c := b.Cursor()
	var k, v []byte
	if from != nil {
		k, v = c.Seek(from)
		if fromExcl && k != nil && bytes.Equal(k, from) {
			k, v = c.Next()
		}
	} else {
		k, v = c.First()
	}
	for ; k != nil; k, v = c.Next() {
		if to != nil {
			cmp := bytes.Compare(k, to)
			if cmp > 0 || (toExcl && cmp == 0) {
				break
			}
		}
		if n > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '[')
		buf = appendJSONString(buf, k)
		buf = append(buf, ',')
		var err error
		if buf, err = appendProjectedJSON(buf, v, keys, wantB, projectPlain); err != nil {
			return nil, err
		}
		buf = append(buf, ']')
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	buf = append(buf, ']')
	return rawJSON(buf), nil
}

func (p *pod) buckets(args []interface{}) (interface{}, error) {
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		out := []string{}
		err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			out = append(out, string(name))
			return nil
		})
		return out, err
	})
}

func (p *pod) createBucket(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	// Without this a transaction that only creates a bucket commits as a
	// rollback and the bucket is lost.
	p.markDirty(id)
	_, err = tx.CreateBucketIfNotExists([]byte(str(args, 1)))
	return nil, err
}

func (p *pod) deleteBucket(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	tx, err := p.tx(id)
	if err != nil {
		return nil, err
	}
	p.markDirty(id)
	err = tx.DeleteBucket([]byte(str(args, 1)))
	if err == bolt.ErrBucketNotFound {
		return nil, nil
	}
	return nil, err
}

func (p *pod) count(args []interface{}) (interface{}, error) {
	bucket := str(args, 1)
	return p.withTx(str(args, 0), func(tx *bolt.Tx) (interface{}, error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return 0, nil
		}
		// The bucket's sequence field is its key count, maintained by
		// putKey/delKey. `Stats().KeyN` cannot be used: it walks the on-disk
		// pages, so inside a write transaction - where the changes are still
		// in the in-memory node tree - it reports the last committed state.
		// A cursor walk is correct but O(n).
		return b.Sequence(), nil
	})
}

// stats reports how much of the file is actually in use, as opposed to how
// large bbolt has grown it. bbolt doubles the file when it runs out of room,
// so the file size alone says little about the data.
//
// The argument is a transaction id or a database handle; a handle is what you
// have after a commit, when the transaction is already gone.
func (p *pod) stats(args []interface{}) (interface{}, error) {
	id := str(args, 0)
	var db *bolt.DB
	var tx *bolt.Tx
	if t, err := p.tx(id); err == nil {
		tx, db = t, t.DB()
	} else if d, err := p.db(id); err == nil {
		db = d
	} else {
		return nil, fmt.Errorf("no such transaction or handle: %s", id)
	}

	s := db.Stats()
	var used int64
	if tx != nil {
		used = tx.Size()
	} else if err := db.View(func(t *bolt.Tx) error { used = t.Size(); return nil }); err != nil {
		return nil, err
	}
	// How much of the file compaction would reclaim. Three things count:
	// pages on the freelist, pages still pending release, and pages the file
	// grew into but never allocated (bbolt doubles the file, so the tail past
	// the allocation high-water mark is dead weight). Live pages are what is
	// allocated minus what is free; everything else is reclaimable.
	pageSize := int64(os.Getpagesize())
	total := fileSize(db.Path()) / pageSize
	allocated := used / pageSize
	free := int64(s.FreePageN + s.PendingPageN)
	live := allocated - free
	if live < 0 {
		live = 0
	}
	ratio := 0.0
	if total > 0 {
		ratio = float64(total-live) / float64(total)
	}
	return map[string]interface{}{
		"tx-size":       used,
		"free-pages":    s.FreePageN,
		"pending-pages": s.PendingPageN,
		"free-alloc":    s.FreeAlloc,
		"free-ratio":    ratio,
		"file-size":     fileSize(db.Path()),
	}, nil
}

// fileSize is the on-disk size, or 0 if the file is gone.
func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// compact rewrites the database into a fresh file and swaps it in, reclaiming
// the pages freed by deletes. bbolt never shrinks its file on its own: freed
// pages are reused, but the high-water mark stays. Compaction is a full
// rewrite, so it is expensive - callers decide when it is worth it.
//
// Every open transaction on the database is rolled back, since the file is
// replaced underneath them.
func (p *pod) compact(args []interface{}) (interface{}, error) {
	handle := str(args, 0)

	// Held for the whole compaction: no transaction may begin between the
	// rollback sweep and Close, or Close waits for one it never saw.
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	p.mu.Lock()
	db, ok := p.dbs[handle]
	if !ok {
		p.mu.Unlock()
		return nil, fmt.Errorf("no such handle: %s", handle)
	}
	path := db.Path()
	p.mu.Unlock()

	// Hold bbolt's single writer slot for the whole compaction. The copy reads
	// a snapshot, and the file is then swapped in wholesale - so any write that
	// committed between the snapshot and the swap would be silently discarded.
	// A write transaction blocks every other writer for the duration, which is
	// exactly the exclusion needed; readers are unaffected (bbolt is MVCC).
	//
	// Rolled back explicitly before the swap below; the defer is the error-path
	// net (a second Rollback on a closed tx is a harmless ErrTxClosed).
	guard, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = guard.Rollback() }()

	tmp := path + ".compact"
	_ = os.Remove(tmp)

	dst, err := bolt.Open(tmp, 0600, nil)
	if err != nil {
		return nil, err
	}
	if err := bolt.Compact(dst, db, 0); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}

	before := fileSize(path)

	p.mu.Lock()
	defer p.mu.Unlock()

	ref, ok := p.paths[path]
	if !ok || ref.db != db {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("database changed during compaction: %s", path)
	}
	p.rollbackFor(db)
	// Release the writer slot before Close, which waits for every transaction.
	_ = guard.Rollback()
	noSync := db.NoSync
	if err := db.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	ndb, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	ndb.NoSync = noSync
	ref.db = ndb
	for h, d := range p.dbs {
		if d == db {
			p.dbs[h] = ndb
		}
	}
	return map[string]interface{}{
		"before": before,
		"after":  fileSize(path),
	}, nil
}

// --- dispatch ---------------------------------------------------------------

func (p *pod) invoke(name string, args []interface{}) (interface{}, error) {
	switch name {
	case "version":
		return version, nil
	case "open":
		return p.open(args)
	case "close":
		return p.close(args)
	case "begin":
		return p.begin(args, true)
	case "begin-read":
		return p.begin(args, false)
	case "commit":
		return p.finish(args, true)
	case "rollback":
		return p.finish(args, false)
	case "get":
		return p.get(args)
	case "get-many":
		return p.getMany(args)
	case "get-path":
		return p.getPath(args)
	case "raw-size":
		return p.rawSize(args)
	case "get-paths":
		return p.getPaths(args)
	case "put-doc":
		return p.putDoc(args)
	case "put-field":
		return p.putField(args)
	case "put-doc-many":
		return p.putDocMany(args)
	case "put":
		return p.put(args)
	case "put-many":
		return p.putMany(args)
	case "delete":
		return p.del(args)
	case "delete-many":
		return p.deleteMany(args)
	case "scan":
		return p.scan(args)
	case "scan-paths":
		return p.scanPaths(args)
	case "buckets":
		return p.buckets(args)
	case "create-bucket":
		return p.createBucket(args)
	case "delete-bucket":
		return p.deleteBucket(args)
	case "count":
		return p.count(args)
	case "stats":
		return p.stats(args)
	case "compact":
		return p.compact(args)
	}
	return nil, fmt.Errorf("unknown var: %s", name)
}

func describe() map[string]interface{} {
	vs := make([]interface{}, 0, len(vars))
	for _, v := range vars {
		vs = append(vs, map[string]interface{}{"name": v})
	}
	return map[string]interface{}{
		"format": "json",
		"namespaces": []interface{}{
			map[string]interface{}{"name": ns, "vars": vs},
		},
		"ops": map[string]interface{}{"shutdown": map[string]interface{}{}},
	}
}

func main() {
	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	p := newPod()

	for {
		var msg map[string]interface{}
		if err := bencode.NewDecoder(in).Decode(&msg); err != nil {
			return // EOF: the client went away
		}

		op, _ := msg["op"].(string)
		switch op {
		case "describe":
			writeMsg(out, describe())

		case "shutdown":
			// Roll back open transactions, then exit. Closing the DBs here
			// would deadlock: Close waits for bbolt's rwlock, an open tx holds
			// it, and that tx's commit can no longer be read. bbolt fsyncs on
			// commit, so committed state is already durable.
			p.mu.Lock()
			for id, rec := range p.txs {
				_ = rec.tx.Rollback()
				delete(p.txs, id)
			}
			p.mu.Unlock()
			return

		case "invoke":
			id, _ := msg["id"].(string)
			v, _ := msg["var"].(string)
			argsStr, _ := msg["args"].(string)
			var args []interface{}
			if argsStr != "" {
				if err := json.Unmarshal([]byte(argsStr), &args); err != nil {
					replyErr(out, id, "bad args: "+err.Error())
					continue
				}
			}
			// One goroutine per invoke: `begin` blocks on bbolt's single
			// writer slot, and the loop must keep reading so another
			// transaction's commit can arrive.
			go func(id, v string, args []interface{}) {
				// A panic in one invoke would otherwise take down the process
				// and every store it holds; report it as an error instead.
				defer func() {
					if r := recover(); r != nil {
						replyErr(out, id, fmt.Sprintf("panic: %v", r))
					}
				}()
				name := v
				if i := len(ns) + 1; len(v) > i {
					name = v[i:]
				}
				val, err := p.invoke(name, args)
				if err != nil {
					replyErr(out, id, err.Error())
					return
				}
				reply(out, id, val)
			}(id, v, args)
		}
	}
}
