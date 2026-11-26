// Package typemap maps message types to Kafka topics by their longest
// package prefix, the way bridges and routers that route by message type
// are often configured:
//
//	consumer-mapping:
//	  plant:
//	    readings: [plant.readings.in]
//	    logs:
//	      Entry: [plant.logs.in, plant.audit.in]
//
// "plant.readings.Basic" goes to plant.readings.in; "plant.logs.Entry" to
// both logs and audit topics.
package typemap

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Map holds type prefixes and their topics.
type Map struct{ prefixes map[string][]string }

// Load reads a mapping file; key names the top-level key holding the
// mapping, for example "consumer-mapping".
func Load(path, key string) (*Map, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b, key)
}

// Parse reads a mapping from YAML.
func Parse(b []byte, key string) (*Map, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	// key may be dotted: "bridge.consumer-mapping".
	var node any = doc
	for _, k := range strings.Split(key, ".") {
		m, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("typemap: no %q mapping", key)
		}
		node = m[k]
	}
	root, ok := node.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("typemap: no %q mapping", key)
	}
	m := &Map{prefixes: map[string][]string{}}
	if err := m.walk("", root); err != nil {
		return nil, err
	}
	if len(m.prefixes) == 0 {
		return nil, fmt.Errorf("typemap: %q maps nothing", key)
	}
	return m, nil
}

func (m *Map) walk(prefix string, node map[string]any) error {
	for k, v := range node {
		p := k
		if prefix != "" {
			p = prefix + "." + k
		}
		switch x := v.(type) {
		case map[string]any:
			if err := m.walk(p, x); err != nil {
				return err
			}
		case []any:
			for _, t := range x {
				s, ok := t.(string)
				if !ok || s == "" {
					return fmt.Errorf("typemap: %s: topics must be names", p)
				}
				m.prefixes[p] = append(m.prefixes[p], s)
			}
		default:
			return fmt.Errorf("typemap: %s: want a list of topics or more levels", p)
		}
	}
	return nil
}

// Topics returns the topics for a message type: those of its longest mapped
// prefix, or nothing when no prefix is mapped.
func (m *Map) Topics(messageType string) []string {
	parts := strings.Split(messageType, ".")
	for n := len(parts); n > 0; n-- {
		if t, ok := m.prefixes[strings.Join(parts[:n], ".")]; ok {
			return t
		}
	}
	return nil
}

// All returns every topic the map can send to, sorted.
func (m *Map) All() []string {
	seen := map[string]bool{}
	for _, ts := range m.prefixes {
		for _, t := range ts {
			seen[t] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
