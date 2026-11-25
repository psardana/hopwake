// Package shape reduces a payload to its shape: the field paths and their
// types, never the values. Two readings with the same fields and types have
// the same fingerprint, so a change in fingerprint at a hop means the payload
// changed structure there.
package shape

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// MaxKeys is how many keys an object may have before it is treated as a map
// with variable keys (per-sensor IDs as keys, for example) and collapsed.
const MaxKeys = 64

// Field is one leaf of a payload: its dotted path and its type.
type Field struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

// Shape is a payload's sorted fields and their fingerprint.
type Shape struct {
	Fields      []Field `json:"fields"`
	Fingerprint string  `json:"fingerprint"`
}

// Of returns the shape of a payload. JSON is walked field by field; anything
// else has the single field "$" of type "binary".
func Of(payload []byte) Shape {
	var v any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil || dec.More() {
		return build([]Field{{Path: "$", Type: "binary"}})
	}
	var fields []Field
	walk("", v, &fields)
	return build(fields)
}

func walk(path string, v any, out *[]Field) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			*out = append(*out, Field{Path: orRoot(path), Type: "object"})
			return
		}
		if len(x) > MaxKeys {
			*out = append(*out, Field{Path: orRoot(path), Type: "map"})
			return
		}
		for k, child := range x {
			walk(join(path, k), child, out)
		}
	case []any:
		if len(x) == 0 {
			*out = append(*out, Field{Path: orRoot(path), Type: "array"})
			return
		}
		// The first element stands for all: arrays of readings share a shape.
		walk(orRoot(path)+"[]", x[0], out)
	case json.Number:
		*out = append(*out, Field{Path: orRoot(path), Type: "number"})
	case string:
		*out = append(*out, Field{Path: orRoot(path), Type: "string"})
	case bool:
		*out = append(*out, Field{Path: orRoot(path), Type: "bool"})
	case nil:
		*out = append(*out, Field{Path: orRoot(path), Type: "null"})
	}
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func orRoot(path string) string {
	if path == "" {
		return "$"
	}
	return path
}

// New builds a shape from fields found some other way, for example from a
// Protobuf message's descriptor.
func New(fields []Field) Shape { return build(fields) }

func build(fields []Field) Shape {
	sort.Slice(fields, func(i, j int) bool { return fields[i].Path < fields[j].Path })
	s := Shape{Fields: fields}
	sum := sha256.Sum256([]byte(s.String()))
	s.Fingerprint = hex.EncodeToString(sum[:8])
	return s
}

// String lists the fields as "path:type", separated by spaces, in path order.
func (s Shape) String() string {
	parts := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		parts[i] = f.Path + ":" + f.Type
	}
	return strings.Join(parts, " ")
}

// Parse reads the String form back.
func Parse(s string) Shape {
	var fields []Field
	for _, part := range strings.Fields(s) {
		i := strings.LastIndex(part, ":")
		if i <= 0 {
			continue
		}
		fields = append(fields, Field{Path: part[:i], Type: part[i+1:]})
	}
	return build(fields)
}

// Change is how one shape differs from another.
type Change struct {
	Added   []Field `json:"added,omitempty"`
	Removed []Field `json:"removed,omitempty"`
	Retyped []Field `json:"retyped,omitempty"` // with their new type
}

// Empty reports whether nothing changed.
func (c Change) Empty() bool { return len(c.Added)+len(c.Removed)+len(c.Retyped) == 0 }

// Diff says what changed from before to after.
func Diff(before, after Shape) Change {
	old := map[string]string{}
	for _, f := range before.Fields {
		old[f.Path] = f.Type
	}
	seen := map[string]bool{}
	var c Change
	for _, f := range after.Fields {
		seen[f.Path] = true
		t, ok := old[f.Path]
		switch {
		case !ok:
			c.Added = append(c.Added, f)
		case t != f.Type:
			c.Retyped = append(c.Retyped, f)
		}
	}
	for _, f := range before.Fields {
		if !seen[f.Path] {
			c.Removed = append(c.Removed, f)
		}
	}
	return c
}
