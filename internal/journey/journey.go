// Package journey derives journey IDs, so that hops which never pass headers
// to each other still agree on which message they saw.
//
// A journey ID has the shape of a W3C trace ID (32 lowercase hex characters),
// so it can be used directly as the trace ID of OpenTelemetry spans.
package journey

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ID derives the journey ID of one reading from its tenant, device and the
// device's own sequence number. Any hop that knows these three values computes
// the same ID.
func ID(tenant, device string, seq uint64) string {
	return BootID(tenant, device, "", seq)
}

// BootID is ID for devices that report a boot counter or boot ID: readings
// from different boots never share a journey ID, even when the sequence
// number starts again. An empty boot gives the same ID as ID.
func BootID(tenant, device, boot string, seq uint64) string {
	h := sha256.New()
	writeField(h, "hopwake/journey/v1")
	writeField(h, tenant)
	writeField(h, device)
	if boot != "" {
		writeField(h, "boot:"+boot)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	h.Write(b[:])
	sum := h.Sum(nil)
	id := sum[:16]
	if allZero(id) { // an all-zero trace ID is invalid; practically unreachable
		id[15] = 1
	}
	return hex.EncodeToString(id)
}

// writeField writes a length-prefixed string, so that ("ab","c") and ("a","bc") differ.
func writeField(h interface{ Write([]byte) (int, error) }, s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	h.Write(n[:])
	h.Write([]byte(s))
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// FromTraceparent returns the trace ID of a W3C traceparent header value.
func FromTraceparent(v string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", false
	}
	if parts[0] == "ff" {
		return "", false
	}
	id := strings.ToLower(parts[1])
	b, err := hex.DecodeString(id)
	if err != nil || allZero(b) {
		return "", false
	}
	return id, true
}

// Rule says where a hop finds the device and the sequence number of a message.
//
// TopicPattern is an MQTT-style pattern in which one level is {device}, for
// example "plant/+/{device}/telemetry"; leave it empty when the device is in
// the payload. DeviceField and SeqField are top-level JSON field names.
type Rule struct {
	Tenant       string
	TopicPattern string
	DeviceField  string
	SeqField     string
	BootField    string // optional: a boot counter or boot ID in the payload
}

// Key is what a rule extracts from one message.
type Key struct {
	Tenant string
	Device string
	Boot   string // empty when the source has no boot field
	Seq    uint64
}

// ID returns the journey ID for the key.
func (k Key) ID() string { return BootID(k.Tenant, k.Device, k.Boot, k.Seq) }

// ErrNoMatch means the message does not carry what the rule looks for.
var ErrNoMatch = errors.New("journey: message does not match the rule")

// Extract applies the rule to one message.
func (r Rule) Extract(topic string, payload []byte) (Key, error) {
	k := Key{Tenant: r.Tenant}
	if r.TopicPattern != "" {
		d, ok := deviceFromTopic(r.TopicPattern, topic)
		if !ok {
			return Key{}, ErrNoMatch
		}
		k.Device = d
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(payload, &body); err != nil {
		return Key{}, fmt.Errorf("%w: payload is not a JSON object", ErrNoMatch)
	}
	if r.DeviceField != "" {
		raw, ok := body[r.DeviceField]
		if !ok {
			return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.DeviceField)
		}
		var d string
		if err := json.Unmarshal(raw, &d); err != nil || d == "" {
			return Key{}, fmt.Errorf("%w: %q is not a string", ErrNoMatch, r.DeviceField)
		}
		k.Device = d
	}
	if k.Device == "" {
		return Key{}, fmt.Errorf("%w: no device", ErrNoMatch)
	}
	raw, ok := body[r.SeqField]
	if !ok {
		return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.SeqField)
	}
	seq, err := strconv.ParseUint(strings.Trim(string(raw), `"`), 10, 64)
	if err != nil {
		return Key{}, fmt.Errorf("%w: %q is not a sequence number", ErrNoMatch, r.SeqField)
	}
	k.Seq = seq
	if r.BootField != "" {
		raw, ok := body[r.BootField]
		if !ok {
			return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.BootField)
		}
		k.Boot = strings.Trim(string(raw), `"`)
		if k.Boot == "" {
			return Key{}, fmt.Errorf("%w: %q is empty", ErrNoMatch, r.BootField)
		}
	}
	return k, nil
}

func deviceFromTopic(pattern, topic string) (string, bool) {
	p := strings.Split(pattern, "/")
	t := strings.Split(topic, "/")
	if len(p) != len(t) {
		return "", false
	}
	device := ""
	for i := range p {
		switch p[i] {
		case "+":
		case "{device}":
			device = t[i]
		default:
			if p[i] != t[i] {
				return "", false
			}
		}
	}
	return device, device != ""
}
