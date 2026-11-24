// Package schema checks payloads against schemas a team has declared, so a
// hop can say what was wrong with a reading and not just that it changed.
//
// Only JSON Schema files are supported so far; registries for Avro and
// Protobuf come later.
package schema

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Validator checks payloads against one JSON Schema.
type Validator struct {
	path   string
	schema *jsonschema.Schema
}

// Load compiles the JSON Schema file at path.
func Load(path string) (*Validator, error) {
	c := jsonschema.NewCompiler()
	s, err := c.Compile(path)
	if err != nil {
		return nil, fmt.Errorf("schema %s: %w", path, err)
	}
	return &Validator{path: path, schema: s}, nil
}

var printer = message.NewPrinter(language.English)

// Check validates a payload. The error, if any, is one short reason such as
// "pressure_kpa: got string, want number", naming the first field at fault.
func (v *Validator) Check(payload []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return errors.New("not JSON")
	}
	err = v.schema.Validate(doc)
	if err == nil {
		return nil
	}
	var ve *jsonschema.ValidationError
	if !errors.As(err, &ve) {
		return err
	}
	leaf := ve
	for len(leaf.Causes) > 0 {
		leaf = leaf.Causes[0]
	}
	msg := leaf.ErrorKind.LocalizedString(printer)
	if where := strings.Join(leaf.InstanceLocation, "."); where != "" {
		return fmt.Errorf("%s: %s", where, msg)
	}
	return errors.New(msg)
}
