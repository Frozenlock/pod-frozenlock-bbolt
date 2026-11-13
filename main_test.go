package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// marshalRef encodes v the way the pod used to: json.Marshal over the
// []interface{} tree that valuePayload builds. HTML escaping is off so the
// comparison is against the same escaping rules appendJSONString uses.
func marshalRef(t *testing.T, v interface{}) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		t.Fatalf("marshal ref: %v", err)
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func mustPayload(t *testing.T, v []byte) interface{} {
	t.Helper()
	p, err := valuePayload(v)
	if err != nil {
		t.Fatalf("valuePayload: %v", err)
	}
	return p
}

// envelopeFor builds a stored value from field key -> raw transit JSON.
func envelopeFor(t *testing.T, fields [][2]string) []byte {
	t.Helper()
	var body []byte
	envFields := make([]envField, 0, len(fields))
	for _, kv := range fields {
		blob, flags, ok := compressField([]byte(kv[1]))
		if !ok {
			blob, flags = []byte(kv[1]), 0
		}
		envFields = append(envFields, envField{
			key:     kv[0],
			off:     uint32(len(body)),
			length:  uint32(len(blob)),
			origLen: uint32(len(kv[1])),
			flags:   flags,
		})
		body = append(body, blob...)
	}
	env, err := buildEnvelope(envFields, body)
	if err != nil {
		panic(err)
	}
	return env
}

func TestAppendValueJSONMatchesMarshal(t *testing.T) {
	plain := [][]byte{
		[]byte(`{"~:id":"u1","~:name":"Person 1"}`),
		[]byte(`plain`),
		[]byte(``),
		[]byte("quotes \" and \\ backslash"),
		[]byte("newline\ntab\tcr\rbackspace\bformfeed\f"),
		[]byte("control \x00\x01\x1f"),
		[]byte("unicode héllo 世界 🎉"),
		[]byte("html <b>&</b>"),
	}
	for _, v := range plain {
		want := marshalRef(t, mustPayload(t, v))
		got, err := appendValueJSON(nil, v)
		if err != nil {
			t.Fatalf("appendValueJSON(%q): %v", v, err)
		}
		if string(got) != want {
			t.Errorf("plain %q:\n got %s\nwant %s", v, got, want)
		}
	}

	envs := [][][2]string{
		{{"~:id", `"u1"`}, {"~:name", `"Person 1"`}},
		{{"~:body", `"` + string(bytes.Repeat([]byte("abcdefgh"), 100)) + `"`}},
		{{"~:a", `1`}, {"~:b", `"two"`}, {"~:c", `[1 2 3]`}},
		{{"~:weird\"key", `"v"`}},
	}
	for _, fs := range envs {
		v := envelopeFor(t, fs)
		want := marshalRef(t, mustPayload(t, v))
		got, err := appendValueJSON(nil, v)
		if err != nil {
			t.Fatalf("appendValueJSON(env): %v", err)
		}
		if string(got) != want {
			t.Errorf("envelope %v:\n got %s\nwant %s", fs, got, want)
		}
	}
}

func TestAppendProjectedJSONMatchesMarshal(t *testing.T) {
	v := envelopeFor(t, [][2]string{
		{"~:id", `"u1"`},
		{"~:name", `"Person 1"`},
		{"~:body", `"` + string(bytes.Repeat([]byte("xyz"), 200)) + `"`},
	})
	// Order follows `wants`; a missing key is skipped.
	for _, wants := range [][]string{
		{"~:id"},
		{"~:name", "~:id"},
		{"~:body", "~:missing", "~:id"},
		{"~:missing"},
		{},
	} {
		// Reference: the old projectValue, rebuilt here.
		fields, body, err := parseEnvelope(v)
		if err != nil {
			t.Fatal(err)
		}
		byKey := map[string]envField{}
		for _, f := range fields {
			byKey[f.key] = f
		}
		out := make([]interface{}, 0, len(wants))
		for _, w := range wants {
			f, ok := byKey[w]
			if !ok {
				continue
			}
			blob, err := fieldBytes(f, body)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, []interface{}{w, string(blob)})
		}
		want := marshalRef(t, []interface{}{"fields", out})

		envB, plainB := projectKeys(wants)
		got, err := appendProjectedJSON(nil, v, wants, envB, plainB, true)
		if err != nil {
			t.Fatalf("appendProjectedJSON(%v): %v", wants, err)
		}
		if string(got) != want {
			t.Errorf("wants %v:\n got %s\nwant %s", wants, got, want)
		}
	}
}

// A plain (non-envelope) value is projected at the JSON level too: small
// documents are stored as one transit value, and shipping the whole thing to
// return one field is what made `:only` useless for them.
func TestAppendProjectedJSONPlainValue(t *testing.T) {
	// A transit :json map is ["^ ",k,v,...]; the adapter sends keys as
	// (encode k), so a keyword arrives tagged.
	v := []byte(`["^ ","~:id","u1","~:name","Person 1","~:n",7]`)
	wants := []string{`["~#'","~:id"]`, `["~#'","~:n"]`, `["~#'","~:missing"]`}
	envB, plainB := projectKeys(wants)
	got, err := appendProjectedJSON(nil, v, wants, envB, plainB, true)
	if err != nil {
		t.Fatal(err)
	}
	want := `["fields",[["[\"~#'\",\"~:id\"]","\"u1\""],["[\"~#'\",\"~:n\"]","7"]]]`
	if string(got) != want {
		t.Errorf("got %s want %s", got, want)
	}
	// Each projected blob must be a complete transit value on its own.
	var parsed []interface{}
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	fields := parsed[1].([]interface{})
	for _, f := range fields {
		pair := f.([]interface{})
		var back interface{}
		if err := json.Unmarshal([]byte(pair[1].(string)), &back); err != nil {
			t.Errorf("blob %q is not standalone JSON: %v", pair[1], err)
		}
	}
}

// A value that is not a JSON object cannot be projected; it must come back
// whole rather than being dropped.
func TestAppendProjectedJSONNonObject(t *testing.T) {
	for _, v := range []string{`"just a string"`, `[1,2,3]`, `not json at all`} {
		envB, plainB := projectKeys([]string{`["~#'","~:id"]`})
		got, err := appendProjectedJSON(nil, []byte(v), []string{`["~#'","~:id"]`}, envB, plainB, true)
		if err != nil {
			t.Fatal(err)
		}
		want := marshalRef(t, mustPayload(t, []byte(v)))
		if string(got) != want {
			t.Errorf("%q: got %s want %s", v, got, want)
		}
	}
}

func TestAppendJSONStringRoundTrip(t *testing.T) {
	for _, s := range []string{
		"", "abc", `"`, `\`, "\n", "\x00", "héllo 世界", "<&>",
		string(bytes.Repeat([]byte("a"), 1000)),
	} {
		got := appendJSONString(nil, []byte(s))
		var back string
		if err := json.Unmarshal(got, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", got, err)
		}
		if back != s {
			t.Errorf("round trip: got %q want %q", back, s)
		}
	}
}

func BenchmarkProjectPlainJSON(b *testing.B) {
	v := []byte(`["^ ","~:id","u500","~:name","Person 500","~:email","p500@x.com","~:age",38,"~:tags",["a","b","c"],"~:address",["^ ","~:street","500 Main St","~:city","Springfield","~:zip","12345"]]`)
	_, plain := projectKeys([]string{`["~#'","~:id"]`})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := projectPlainJSON(nil, v, []string{`["~#'","~:id"]`}, plain); !ok {
			b.Fatal("not projected")
		}
	}
}

func BenchmarkValuePayloadPlain(b *testing.B) {
	v := []byte(`["^ ","~:id","u500","~:name","Person 500","~:email","p500@x.com","~:age",38,"~:tags",["a","b","c"],"~:address",["^ ","~:street","500 Main St","~:city","Springfield","~:zip","12345"]]`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := appendValueJSON(nil, v); err != nil {
			b.Fatal(err)
		}
	}
}

// The hand-rolled scanner must agree with the document on every shape a
// transit document can take.
//
// A transit :json map is the ARRAY ["^ ",k,v,...] - not an object. The adapter
// sends keys as `(encode k)`, so a keyword arrives as ["~#'","~:id"] while the
// key INSIDE the map is "~:id". Both forms matter: the first is what the
// caller asks for, the second is what the document holds.
func TestProjectPlainJSONMatchesUnmarshal(t *testing.T) {
	docs := []struct {
		transit string
		want    map[string]string // caller's key form -> the raw JSON text
	}{
		{`["^ "]`, map[string]string{}},
		{`["^ ","~:id","u1"]`, map[string]string{`["~#'","~:id"]`: `"u1"`}},
		{`["^ ","~:id","u1","~:n",7]`, map[string]string{`["~#'","~:id"]`: `"u1"`, `["~#'","~:n"]`: `7`}},
		{`["^ ","~:id","u1","~:tags",["a","b"],"~:address",["^ ","~:city","S","~:zip","1"]]`,
			map[string]string{
				`["~#'","~:id"]`:      `"u1"`,
				`["~#'","~:tags"]`:    `["a","b"]`,
				`["~#'","~:address"]`: `["^ ","~:city","S","~:zip","1"]`,
			}},
		{`["^ ","~:s","has \"quotes\" and \\ backslash"]`,
			map[string]string{`["~#'","~:s"]`: `"has \"quotes\" and \\ backslash"`}},
		{`["^ ","~:s","brace } and comma , inside"]`,
			map[string]string{`["~#'","~:s"]`: `"brace } and comma , inside"`}},
		{`["^ ","~:s","[not an array]"]`, map[string]string{`["~#'","~:s"]`: `"[not an array]"`}},
		{`["^ ","~:n",-1.5e10,"~:b",true,"~:z",null]`,
			map[string]string{`["~#'","~:n"]`: `-1.5e10`, `["~#'","~:b"]`: `true`, `["~#'","~:z"]`: `null`}},
		{`["^ ","~:empty",["^ "],"~:earr",[]]`,
			map[string]string{`["~#'","~:empty"]`: `["^ "]`, `["~#'","~:earr"]`: `[]`}},
		{`["^ ","~:deep",["^ ","a",["^ ","b",["^ ","c",[1,["^ ","d","}"]]]]]]`,
			map[string]string{`["~#'","~:deep"]`: `["^ ","a",["^ ","b",["^ ","c",[1,["^ ","d","}"]]]]]`}},
		{`["^ ","~:unicode","héllo 世界 🎉"]`,
			map[string]string{`["~#'","~:unicode"]`: `"héllo 世界 🎉"`}},
		// A string key: the adapter sends the JSON string, the map holds the
		// bare text.
		{`["^ ","id","u1","~:n",7]`, map[string]string{`"id"`: `"u1"`, `["~#'","~:n"]`: `7`}},
	}
	wants := []string{
		`["~#'","~:id"]`, `["~#'","~:n"]`, `["~#'","~:s"]`, `["~#'","~:tags"]`,
		`["~#'","~:address"]`, `["~#'","~:empty"]`, `["~#'","~:earr"]`,
		`["~#'","~:unicode"]`, `["~#'","~:b"]`, `["~#'","~:z"]`,
		`["~#'","~:deep"]`, `["~#'","~:missing"]`, `"id"`,
	}
	_, plain := projectKeys(wants)
	wanted := map[string]bool{}
	for _, w := range wants {
		wanted[w] = true
	}
	for _, d := range docs {
		got, ok := projectPlainJSON(nil, []byte(d.transit), wants, plain)
		if !ok {
			t.Fatalf("%s: not projected", d.transit)
		}
		var parsed []interface{}
		if err := json.Unmarshal(got, &parsed); err != nil {
			t.Fatalf("%s: output not JSON: %v (%s)", d.transit, err, got)
		}
		seen := map[string]string{}
		for _, f := range parsed[1].([]interface{}) {
			pair := f.([]interface{})
			seen[pair[0].(string)] = pair[1].(string)
		}
		// Exactly the wanted keys the document has, with their exact text.
		expect := map[string]string{}
		for k, v := range d.want {
			if wanted[k] {
				expect[k] = v
			}
		}
		if !reflect.DeepEqual(seen, expect) {
			t.Errorf("%s:\n got %v\nwant %v", d.transit, seen, expect)
		}
	}
}

func TestProjectPlainJSONRejectsNonObjects(t *testing.T) {
	for _, v := range []string{``, `[]`, `"s"`, `7`, `null`, `[`, `["^ "`, `["^ ","a"]`, `["^ ","a",]`} {
		_, plain := projectKeys([]string{"a"})
		if _, ok := projectPlainJSON(nil, []byte(v), []string{"a"}, plain); ok {
			t.Errorf("%q: expected rejection", v)
		}
	}
}

// --- envelope detection and header bounds ----------------------------------

// A value that merely starts with the magic is a plain document, not an
// envelope: `put` stores opaque strings, so a caller may store "PBB1..." and
// must get it back unchanged.
func TestPlainValueStartingWithMagicIsNotAnEnvelope(t *testing.T) {
	for _, v := range [][]byte{
		[]byte("PBB1abcd"),
		[]byte("PBB1"),
		[]byte("PBB1\x00\x00\x00\x00"),
		[]byte("DRS1abcd"),
	} {
		fields, _, ok := envelopeOf(v)
		if ok {
			t.Errorf("envelopeOf(%q) = ok, want plain", v)
		}
		if fields != nil {
			t.Errorf("envelopeOf(%q) returned fields for a plain value", v)
		}
		got, err := valuePayload(v)
		if err != nil {
			t.Fatalf("valuePayload(%q): %v", v, err)
		}
		want := []interface{}{"doc", string(v)}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("valuePayload(%q) = %v, want %v", v, got, want)
		}
	}
}

// Envelopes written before the marker rename still decode.
func TestLegacyMagicStillDecodes(t *testing.T) {
	fields := []envField{{key: "~:name", off: 0, length: 5, origLen: 5, flags: 0}}
	body := []byte("hello")

	env, err := buildEnvelope(fields, body)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if !bytes.Equal(env[:4], envMagic) {
		t.Fatalf("new envelopes must use the current magic, got %q", env[:4])
	}

	legacy := append([]byte("DRS1"), env[4:]...)
	got, err := valuePayload(legacy)
	if err != nil {
		t.Fatalf("valuePayload(legacy): %v", err)
	}
	want := []interface{}{"fields", []interface{}{[]interface{}{"~:name", "hello"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("legacy envelope = %v, want %v", got, want)
	}
}

// The header stores the field count and each key length as uint16. Narrowing
// them silently would write a header that parses to fewer fields than were
// given, losing data on read.
func TestBuildEnvelopeRejectsUnrepresentableHeaders(t *testing.T) {
	t.Run("too many fields", func(t *testing.T) {
		fields := make([]envField, 0x10000)
		for i := range fields {
			fields[i] = envField{key: "k", off: 0, length: 1, origLen: 1}
		}
		if _, err := buildEnvelope(fields, []byte("x")); err == nil {
			t.Fatal("buildEnvelope accepted 65536 fields")
		}
	})

	t.Run("field key too long", func(t *testing.T) {
		fields := []envField{{key: string(bytes.Repeat([]byte("k"), 0x10000)), off: 0, length: 1, origLen: 1}}
		if _, err := buildEnvelope(fields, []byte("x")); err == nil {
			t.Fatal("buildEnvelope accepted a 65536-byte key")
		}
	})

	t.Run("at the limit is fine", func(t *testing.T) {
		fields := make([]envField, 0xFFFF)
		for i := range fields {
			fields[i] = envField{key: "k", off: 0, length: 1, origLen: 1}
		}
		env, err := buildEnvelope(fields, []byte("x"))
		if err != nil {
			t.Fatalf("buildEnvelope at the limit: %v", err)
		}
		got, _, ok := envelopeOf(env)
		if !ok {
			t.Fatal("envelope at the limit did not parse")
		}
		if len(got) != 0xFFFF {
			t.Fatalf("round-tripped %d fields, want %d", len(got), 0xFFFF)
		}
	})
}

// A write that commits while a compaction is running must survive it: the copy
// reads a snapshot and the file is then swapped in wholesale, so without
// exclusion the write would be discarded.
func TestCompactionKeepsConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/compact.db"

	p := newPod()
	h, err := p.open([]interface{}{path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _, _ = p.close([]interface{}{h}) }()

	// Enough data that the copy takes long enough to race against.
	tx, err := p.begin([]interface{}{h}, true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	blob := string(bytes.Repeat([]byte("x"), 200))
	for i := 0; i < 20000; i++ {
		if _, err := p.put([]interface{}{tx, "bulk", fmt.Sprintf("k%06d", i), blob}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := p.finish([]interface{}{tx}, true); err != nil {
		t.Fatalf("commit: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.compact([]interface{}{h})
		done <- err
	}()

	time.Sleep(5 * time.Millisecond) // let the copy start

	tx, err = p.begin([]interface{}{h}, true)
	if err != nil {
		t.Fatalf("begin during compaction: %v", err)
	}
	if _, err := p.put([]interface{}{tx, "kv", "during", "kept"}); err != nil {
		t.Fatalf("put during compaction: %v", err)
	}
	if _, err := p.finish([]interface{}{tx}, true); err != nil {
		t.Fatalf("commit during compaction: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("compact: %v", err)
	}

	tx, err = p.begin([]interface{}{h}, false)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer func() { _, _ = p.finish([]interface{}{tx}, false) }()

	got, err := p.get([]interface{}{tx, "kv", "during"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("a write committed during compaction was lost")
	}
}

func TestFinishBeforeBeginLeavesNoTransaction(t *testing.T) {
	dir := t.TempDir()
	p := newPod()
	h, err := p.open([]interface{}{dir + "/tombstone.db"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _, _ = p.close([]interface{}{h}) }()

	// Hold the writer slot so the lazy begin below blocks in the pod.
	blocker, err := p.begin([]interface{}{h}, true)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}

	id := fmt.Sprintf("%s:1", h)
	done := make(chan error, 1)
	go func() {
		_, err := p.tx(id)
		done <- err
	}()

	// Wait until the begin is counted as pending.
	pending := false
	for i := 0; i < 2000; i++ {
		p.mu.Lock()
		n := p.begins[id]
		p.mu.Unlock()
		if n > 0 {
			pending = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !pending {
		t.Fatal("the begin never became pending")
	}

	// The client gives up while its first operation waits.
	if _, err := p.finish([]interface{}{id}, false); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Free the slot: the begin must not register a transaction nobody
	// finishes, which would take bbolt's single writer slot for good.
	if _, err := p.finish([]interface{}{blocker}, true); err != nil {
		t.Fatalf("commit blocker: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("tx on a finished-before-begin id should fail")
	}
	p.mu.Lock()
	_, registered := p.txs[id]
	p.mu.Unlock()
	if registered {
		t.Fatal("the transaction was registered after being finished")
	}

	// And the writer slot is free: a real transaction still works.
	tx, err := p.begin([]interface{}{h}, true)
	if err != nil {
		t.Fatalf("begin after the tombstone: %v", err)
	}
	if _, err := p.put([]interface{}{tx, "b", "k", "v"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := p.finish([]interface{}{tx}, true); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
