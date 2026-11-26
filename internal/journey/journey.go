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
	"time"
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

// TimeID derives the journey ID of a reading that has no sequence number,
// from its device, its message type and the time the device gave it. One
// device sends several message types with the same time, so the type is part
// of the ID. A device that resends a reading with the same time gets the same
// ID, which is what a resend is.
func TimeID(tenant, device, messageType, at string) string {
	h := sha256.New()
	writeField(h, "hopwake/journey/time/v1")
	writeField(h, tenant)
	writeField(h, device)
	writeField(h, "type:"+messageType)
	writeField(h, "time:"+at)
	sum := h.Sum(nil)
	id := sum[:16]
	if allZero(id) {
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
//
// Field names may be dotted paths into the payload, with [n] for an array
// element ("meterValue[0].timestamp"). Where the payload does not carry them,
// DeviceHeader, SeqFrom and BootFrom name a Kafka header or MQTT 5 user
// property, and DeviceFromKey takes the device from the Kafka record key.
// TimeField replaces the sequence number with the message's own timestamp;
// it may list several fields, separated by commas, tried in order.
type Rule struct {
	Tenant        string
	TopicPattern  string
	DeviceField   string
	SeqField      string
	BootField     string // optional: a boot counter or boot ID in the payload
	DeviceHeader  string
	DeviceFromKey bool
	SeqFrom       string
	BootFrom      string
	TimeField     string
	TimePrecision time.Duration // rounds the time down; zero keeps it
}

// Meta is what a message carries besides its payload.
type Meta struct {
	Header func(name string) string // a Kafka header or MQTT 5 user property; nil when there are none
	Key    []byte                   // the Kafka record key
	Type   string                   // the message's type, when the source names one
}

func (m Meta) header(name string) string {
	if m.Header == nil {
		return ""
	}
	return m.Header(name)
}

// Key is what a rule extracts from one message.
type Key struct {
	Tenant string
	Device string
	Boot   string // empty when the source has no boot field
	Seq    uint64
	Type   string // the message type, when the source names one
	Time   string // the message's own time, RFC 3339 in UTC, when there is no sequence number
}

// ID returns the journey ID for the key.
func (k Key) ID() string {
	if k.Time != "" {
		return TimeID(k.Tenant, k.Device, k.Type, k.Time)
	}
	return BootID(k.Tenant, k.Device, k.Boot, k.Seq)
}

// ErrNoMatch means the message does not carry what the rule looks for.
var ErrNoMatch = errors.New("journey: message does not match the rule")

// Extract applies the rule to one JSON message.
func (r Rule) Extract(topic string, payload []byte) (Key, error) {
	return r.ExtractMeta(topic, payload, Meta{})
}

// ExtractMeta applies the rule to one message: its topic, its payload as
// JSON (Protobuf payloads are decoded first) and what it carries besides.
func (r Rule) ExtractMeta(topic string, payload []byte, m Meta) (Key, error) {
	k := Key{Tenant: r.Tenant, Type: m.Type}
	if r.TopicPattern != "" {
		d, ok := deviceFromTopic(r.TopicPattern, topic)
		if !ok {
			return Key{}, ErrNoMatch
		}
		k.Device = d
	}
	var body any
	if r.DeviceField != "" || r.SeqField != "" || r.BootField != "" || r.TimeField != "" {
		dec := json.NewDecoder(strings.NewReader(string(payload)))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			return Key{}, fmt.Errorf("%w: payload is not JSON", ErrNoMatch)
		}
		if _, ok := body.(map[string]any); !ok {
			return Key{}, fmt.Errorf("%w: payload is not a JSON object", ErrNoMatch)
		}
	}
	switch {
	case r.DeviceField != "":
		// A device ID may be text or a whole number, such as a numeric
		// system ID in a Protobuf record.
		v, ok := Lookup(body, r.DeviceField)
		d := ""
		if n, isNum := v.(json.Number); isNum {
			if _, err := n.Int64(); err == nil {
				d = n.String()
			}
		} else {
			d, _ = v.(string)
		}
		if !ok || d == "" {
			return Key{}, fmt.Errorf("%w: %q is not a string or a whole number", ErrNoMatch, r.DeviceField)
		}
		k.Device = d
	case r.DeviceHeader != "":
		k.Device = m.header(r.DeviceHeader)
	case r.DeviceFromKey:
		k.Device = string(m.Key)
	}
	if k.Device == "" {
		return Key{}, fmt.Errorf("%w: no device", ErrNoMatch)
	}
	switch {
	case r.SeqField != "" || r.SeqFrom != "":
		raw := m.header(r.SeqFrom)
		name := r.SeqFrom
		if r.SeqField != "" {
			v, ok := Lookup(body, r.SeqField)
			if !ok {
				return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.SeqField)
			}
			raw, name = scalar(v), r.SeqField
		}
		seq, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return Key{}, fmt.Errorf("%w: %q is not a sequence number", ErrNoMatch, name)
		}
		k.Seq = seq
	case r.TimeField != "":
		// One source can carry several message types, each naming its time
		// differently: the first of the listed fields that is present counts.
		var v any
		ok := false
		for _, f := range strings.Split(r.TimeField, ",") {
			if v, ok = Lookup(body, strings.TrimSpace(f)); ok {
				break
			}
		}
		if !ok {
			return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.TimeField)
		}
		t, err := timeOf(v)
		if err != nil {
			return Key{}, fmt.Errorf("%w: %q is not a time", ErrNoMatch, r.TimeField)
		}
		k.Time = FormatTime(t, r.TimePrecision)
	default:
		return Key{}, fmt.Errorf("%w: no sequence number or time", ErrNoMatch)
	}
	switch {
	case r.BootField != "":
		v, ok := Lookup(body, r.BootField)
		if !ok {
			return Key{}, fmt.Errorf("%w: no %q field", ErrNoMatch, r.BootField)
		}
		k.Boot = scalar(v)
		if k.Boot == "" {
			return Key{}, fmt.Errorf("%w: %q is empty", ErrNoMatch, r.BootField)
		}
	case r.BootFrom != "":
		if k.Boot = m.header(r.BootFrom); k.Boot == "" {
			return Key{}, fmt.Errorf("%w: no %q", ErrNoMatch, r.BootFrom)
		}
	}
	return k, nil
}

// Device returns the device of a message whose payload could not be read,
// from the topic, a header or the record key.
func (r Rule) Device(topic string, m Meta) (string, error) {
	d := ""
	switch {
	case r.TopicPattern != "":
		d, _ = deviceFromTopic(r.TopicPattern, topic)
	case r.DeviceHeader != "":
		d = m.header(r.DeviceHeader)
	case r.DeviceFromKey:
		d = string(m.Key)
	}
	if d == "" {
		return "", ErrNoMatch
	}
	return d, nil
}

// FormatTime writes a reading's time as journey IDs use it: UTC, RFC 3339,
// rounded down to the precision (zero keeps it as given).
func FormatTime(t time.Time, precision time.Duration) string {
	if precision > 0 {
		t = t.Truncate(precision)
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Lookup finds a value in decoded JSON by a dotted path, with [n] for an
// array element: "basic.measurement_time", "meterValue[0].timestamp".
func Lookup(doc any, path string) (any, bool) {
	cur := doc
	for _, part := range strings.Split(path, ".") {
		name, idx := part, -1
		if i := strings.IndexByte(part, '['); i >= 0 && strings.HasSuffix(part, "]") {
			n, err := strconv.Atoi(part[i+1 : len(part)-1])
			if err != nil || n < 0 {
				return nil, false
			}
			name, idx = part[:i], n
		}
		if name != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			if cur, ok = m[name]; !ok {
				return nil, false
			}
		}
		if idx >= 0 {
			a, ok := cur.([]any)
			if !ok || idx >= len(a) {
				return nil, false
			}
			cur = a[idx]
		}
	}
	return cur, true
}

// scalar renders a JSON scalar as text: strings as they are, numbers as written.
func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}

// timeOf reads a time written as RFC 3339 text or as Unix seconds or
// milliseconds.
func timeOf(v any) (time.Time, error) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, nil
		}
		if n, err := strconv.ParseFloat(x, 64); err == nil {
			return unix(n), nil
		}
		return time.Time{}, errors.New("not a time")
	case json.Number:
		n, err := x.Float64()
		if err != nil {
			return time.Time{}, err
		}
		return unix(n), nil
	}
	return time.Time{}, errors.New("not a time")
}

func unix(n float64) time.Time {
	if n > 1e11 { // milliseconds
		return time.UnixMilli(int64(n))
	}
	sec := int64(n)
	return time.Unix(sec, int64((n-float64(sec))*1e9))
}

func deviceFromTopic(pattern, topic string) (string, bool) {
	p := strings.Split(pattern, "/")
	t := strings.Split(topic, "/")
	if n := len(p); n > 0 && p[n-1] == "#" {
		// "#" matches the rest of the topic, including nothing.
		if len(t) < n-1 {
			return "", false
		}
		p, t = p[:n-1], t[:n-1]
	}
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
