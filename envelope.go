package main

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// A document value is either plain transit JSON (small documents) or an
// envelope: a header of byte offsets followed by independently compressed
// field blobs.
//
// The envelope is what makes multi-MB documents cheap to read. `get-path`
// returns one field without decompressing or transferring the rest, and each
// field is compressed on its own so a partial read only pays for the bytes it
// asked for.
//
//	envelope := "PBB1" [u32be headerLen] [header] [blobs...]
//	header   := [u16 count] count * {[u16 keyLen] [key] [u32 off] [u32 len] [u32 origLen] [u8 flags]}
//
// `off` is relative to the start of the blob area. `key` is the transit-encoded
// map key (e.g. `"~:name"`), so the client can look a field up by encoding the
// path element the same way.
var envMagic = []byte("PBB1")

// legacyMagics are markers written by earlier versions of this pod. They are
// still decoded so a database written before the rename keeps working; new
// values always use envMagic.
var legacyMagics = [][]byte{[]byte("DRS1")}

const (
	// One flag per codec, so a value written by either can be read back.
	envFlagZstd  byte = 1
	envFlagFlate byte = 2
	// compressMin is the smallest blob worth compressing: below this the
	// codec's header costs more than it saves.
	compressMin = 256
)

// zstd beats flate on every axis for the transit JSON we store. SpeedBetter
// over SpeedDefault: ~19% smaller and ~38% faster to decompress (there is less
// to read), at ~1.7x slower compression - the right trade for documents that
// are written once and read many times. The encoders are safe for concurrent
// use, so one pair serves every request.
var (
	zstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	zstdDec, _ = zstd.NewReader(nil)
)

type envField struct {
	key     string
	off     uint32
	length  uint32
	origLen uint32
	flags   byte
}

// hasMagic reports whether v starts with an envelope marker, current or
// legacy. It is only a prefix test - see envelopeOf for the real question.
func hasMagic(v []byte) bool {
	if len(v) < 8 {
		return false
	}
	if bytes.Equal(v[:4], envMagic) {
		return true
	}
	for _, m := range legacyMagics {
		if bytes.Equal(v[:4], m) {
			return true
		}
	}
	return false
}

// envelopeOf parses v as an envelope, reporting ok=false when it is not one.
//
// A value that merely STARTS with the magic is not an envelope: `put` stores
// opaque strings, so a caller may legitimately store "PBB1..." as a plain
// value. Detection therefore requires the header to parse, and a value whose
// header does not parse falls back to being a plain document rather than
// failing the read.
func envelopeOf(v []byte) (fields []envField, body []byte, ok bool) {
	if !hasMagic(v) {
		return nil, nil, false
	}
	f, b, err := parseEnvelope(v)
	if err != nil {
		return nil, nil, false
	}
	return f, b, true
}

func parseEnvelope(v []byte) ([]envField, []byte, error) {
	if !hasMagic(v) {
		return nil, nil, fmt.Errorf("not an envelope")
	}
	hl := int(binary.BigEndian.Uint32(v[4:8]))
	if hl < 2 || 8+hl > len(v) {
		return nil, nil, fmt.Errorf("bad envelope header length: %d", hl)
	}
	hdr, body := v[8:8+hl], v[8+hl:]
	n := int(binary.BigEndian.Uint16(hdr[0:2]))
	fields := make([]envField, 0, n)
	p := 2
	for i := 0; i < n; i++ {
		if p+2 > len(hdr) {
			return nil, nil, fmt.Errorf("truncated envelope header")
		}
		kl := int(binary.BigEndian.Uint16(hdr[p : p+2]))
		p += 2
		if p+kl+13 > len(hdr) {
			return nil, nil, fmt.Errorf("truncated envelope header")
		}
		f := envField{
			key:     string(hdr[p : p+kl]),
			off:     binary.BigEndian.Uint32(hdr[p+kl : p+kl+4]),
			length:  binary.BigEndian.Uint32(hdr[p+kl+4 : p+kl+8]),
			origLen: binary.BigEndian.Uint32(hdr[p+kl+8 : p+kl+12]),
			flags:   hdr[p+kl+12],
		}
		p += kl + 13
		if int(f.off)+int(f.length) > len(body) {
			return nil, nil, fmt.Errorf("envelope field %q out of range", f.key)
		}
		fields = append(fields, f)
	}
	return fields, body, nil
}

// buildEnvelope assembles the envelope. The header stores the field count and
// each key length as uint16, so both are bounds-checked here: narrowing them
// silently would produce a header that parses to fewer fields than were
// written, losing data on read.
func buildEnvelope(fields []envField, body []byte) ([]byte, error) {
	if len(fields) > 0xFFFF {
		return nil, fmt.Errorf("too many fields: %d (max %d)", len(fields), 0xFFFF)
	}
	hl := 2
	for _, f := range fields {
		if len(f.key) > 0xFFFF {
			return nil, fmt.Errorf("field key too long: %d bytes (max %d)", len(f.key), 0xFFFF)
		}
		hl += 2 + len(f.key) + 13
	}
	out := make([]byte, 8+hl+len(body))
	copy(out, envMagic)
	binary.BigEndian.PutUint32(out[4:8], uint32(hl))
	binary.BigEndian.PutUint16(out[8:10], uint16(len(fields)))
	p := 10
	for _, f := range fields {
		binary.BigEndian.PutUint16(out[p:p+2], uint16(len(f.key)))
		p += 2
		copy(out[p:], f.key)
		p += len(f.key)
		binary.BigEndian.PutUint32(out[p:p+4], f.off)
		binary.BigEndian.PutUint32(out[p+4:p+8], f.length)
		binary.BigEndian.PutUint32(out[p+8:p+12], f.origLen)
		out[p+12] = f.flags
		p += 13
	}
	copy(out[8+hl:], body)
	return out, nil
}

// compressField returns the compressed bytes, the codec flag, and whether
// compressing helped.
//
// zstd first: it is smaller and much faster to decompress on the transit JSON
// we store. But zstd stores incompressible input raw, while flate still finds
// structure in a low-entropy alphabet (base64, hex), so flate is tried before
// giving up.
func compressField(b []byte) ([]byte, byte, bool) {
	if len(b) < compressMin {
		return b, 0, false
	}
	if out := zstdEnc.EncodeAll(b, nil); len(out) < len(b) {
		return out, envFlagZstd, true
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return b, 0, false
	}
	if _, err := w.Write(b); err != nil {
		return b, 0, false
	}
	if err := w.Close(); err != nil {
		return b, 0, false
	}
	if buf.Len() < len(b) {
		return buf.Bytes(), envFlagFlate, true
	}
	return b, 0, false
}

func decompressField(b []byte, origLen int, flags byte) ([]byte, error) {
	var out []byte
	switch {
	case flags&envFlagZstd != 0:
		var err error
		if out, err = zstdDec.DecodeAll(b, make([]byte, 0, origLen)); err != nil {
			return nil, err
		}
	case flags&envFlagFlate != 0:
		r := flate.NewReader(bytes.NewReader(b))
		defer r.Close()
		out = make([]byte, origLen)
		if _, err := io.ReadFull(r, out); err != nil {
			return nil, err
		}
	default:
		return b, nil
	}
	if len(out) != origLen {
		return nil, fmt.Errorf("decompressed %d bytes, expected %d", len(out), origLen)
	}
	return out, nil
}

func fieldBytes(f envField, body []byte) ([]byte, error) {
	blob := body[f.off : f.off+f.length]
	return decompressField(blob, int(f.origLen), f.flags)
}

// valuePayload renders a stored value for the client. Envelopes come back as
// their per-field blobs rather than reassembled into one transit value: each
// blob was encoded by its own writer, so its `"^N"` cache references are only
// valid when that blob is decoded on its own.
//
//	["doc", "<transit json>"]        - a plain value
//	["fields", [[key blob] ...]]     - an envelope
func valuePayload(v []byte) (interface{}, error) {
	fields, body, ok := envelopeOf(v)
	if !ok {
		return []interface{}{"doc", string(v)}, nil
	}
	out := make([]interface{}, 0, len(fields))
	for _, f := range fields {
		blob, err := fieldBytes(f, body)
		if err != nil {
			return nil, err
		}
		out = append(out, []interface{}{f.key, string(blob)})
	}
	return []interface{}{"fields", out}, nil
}
