// Package protodec decodes Protobuf payloads whose message type is named
// outside the payload, for example in a Kafka header or an MQTT 5 user
// property, using .proto files the team already keeps.
//
// Decoded messages are handed on as JSON (field names as in the .proto), so
// everything downstream of a tap works the same for JSON and Protobuf
// sources. Shapes come from the message descriptor, which knows the real
// field types.
package protodec

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/psardana/hopwake/internal/shape"
)

// Decoder holds the message types of a directory of .proto files.
type Decoder struct {
	types   map[string]protoreflect.MessageDescriptor
	Skipped map[string]string // .proto files that did not compile, and why
}

// ErrUnknownType means no compiled .proto file defines the named message.
var ErrUnknownType = errors.New("unknown message type")

// Load compiles every .proto file under dir. Imports resolve against dir and
// the well-known google/protobuf types. A file that does not compile is left
// out and listed in Skipped, so one broken file does not hide the others.
func Load(dir string) (*Decoder, error) {
	var names []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".proto") {
			rel, _ := filepath.Rel(dir, p)
			names = append(names, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("protodec: no .proto files under %s", dir)
	}
	sort.Strings(names)
	d := &Decoder{types: map[string]protoreflect.MessageDescriptor{}, Skipped: map[string]string{}}
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{ImportPaths: []string{dir}}),
	}
	ctx := context.Background()
	if files, err := compiler.Compile(ctx, names...); err == nil {
		for _, f := range files {
			d.add(f.Messages())
		}
		return d, nil
	}
	// Something failed: compile file by file and keep what works.
	for _, n := range names {
		files, err := compiler.Compile(ctx, n)
		if err != nil {
			d.Skipped[n] = firstLine(err.Error())
			continue
		}
		for _, f := range files {
			d.add(f.Messages())
		}
	}
	if len(d.types) == 0 {
		return nil, fmt.Errorf("protodec: no .proto file under %s compiled", dir)
	}
	return d, nil
}

func (d *Decoder) add(ms protoreflect.MessageDescriptors) {
	for i := 0; i < ms.Len(); i++ {
		m := ms.Get(i)
		d.types[string(m.FullName())] = m
		d.add(m.Messages())
	}
}

// Types returns the full names of every message type, sorted.
func (d *Decoder) Types() []string {
	out := make([]string, 0, len(d.types))
	for n := range d.types {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Result is one decoded message.
type Result struct {
	JSON  []byte      // the message as JSON, field names as in the .proto
	Shape shape.Shape // from the descriptor: real field types
}

var toJSON = protojson.MarshalOptions{UseProtoNames: true}

// Decode decodes payload as the named message type. Errors are short
// reasons, such as "decode BasicMeasurement: required field measurement_time
// not set".
func (d *Decoder) Decode(typeName string, payload []byte) (Result, error) {
	md, ok := d.types[typeName]
	if !ok {
		return Result{}, fmt.Errorf("%w %s", ErrUnknownType, typeName)
	}
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return Result{}, fmt.Errorf("decode %s: %s", md.Name(), reason(err, md))
	}
	js, err := toJSON.Marshal(msg)
	if err != nil {
		return Result{}, fmt.Errorf("decode %s: %v", md.Name(), err)
	}
	var fields []shape.Field
	walk("", msg, &fields)
	if len(fields) == 0 {
		fields = []shape.Field{{Path: "$", Type: "object"}}
	}
	return Result{JSON: js, Shape: shape.New(fields)}, nil
}

// Encode builds the named message from JSON and serialises it. Used by
// simulators and tests. Required fields may be missing, so a simulator can
// send what a buggy device would; Decode still rejects such a message.
func (d *Decoder) Encode(typeName string, jsonDoc []byte) ([]byte, error) {
	md, ok := d.types[typeName]
	if !ok {
		return nil, fmt.Errorf("%w %s", ErrUnknownType, typeName)
	}
	msg := dynamicpb.NewMessage(md)
	if err := (protojson.UnmarshalOptions{AllowPartial: true}).Unmarshal(jsonDoc, msg); err != nil {
		return nil, err
	}
	return proto.MarshalOptions{AllowPartial: true}.Marshal(msg)
}

// reason shortens a protobuf error to what went wrong.
func reason(err error, md protoreflect.MessageDescriptor) string {
	s := strings.TrimPrefix(err.Error(), "proto: ")
	s = strings.TrimPrefix(s, "proto: ")
	if strings.HasPrefix(s, "required field ") {
		// "required field ems.x.BasicMeasurement.measurement_time not set"
		name := strings.TrimSuffix(strings.TrimPrefix(s, "required field "), " not set")
		name = name[strings.LastIndex(name, ".")+1:]
		return "required field " + name + " not set"
	}
	if strings.Contains(s, "cannot parse invalid wire-format data") {
		return "not a valid " + string(md.Name()) + " message"
	}
	return s
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// walk lists the set fields of a message as shape fields with descriptor types.
func walk(prefix string, m protoreflect.Message, out *[]shape.Field) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		path := string(fd.Name())
		if prefix != "" {
			path = prefix + "." + path
		}
		switch {
		case fd.IsMap():
			*out = append(*out, shape.Field{Path: path, Type: "map"})
		case fd.IsList():
			l := v.List()
			if l.Len() == 0 {
				*out = append(*out, shape.Field{Path: path, Type: "array"})
				break
			}
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				if t := wellKnown(fd.Message()); t != "" {
					*out = append(*out, shape.Field{Path: path + "[]", Type: t})
				} else {
					walk(path+"[]", l.Get(0).Message(), out)
				}
			} else {
				*out = append(*out, shape.Field{Path: path + "[]", Type: kind(fd)})
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			if t := wellKnown(fd.Message()); t != "" {
				*out = append(*out, shape.Field{Path: path, Type: t})
			} else {
				walk(path, v.Message(), out)
			}
		default:
			*out = append(*out, shape.Field{Path: path, Type: kind(fd)})
		}
		return true
	})
}

func wellKnown(md protoreflect.MessageDescriptor) string {
	switch md.FullName() {
	case "google.protobuf.Timestamp":
		return "timestamp"
	case "google.protobuf.Duration":
		return "duration"
	}
	return ""
}

func kind(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.EnumKind:
		return "enum"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return "number"
	default:
		return "int"
	}
}
