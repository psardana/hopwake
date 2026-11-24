// Package capture prepares payloads of watched devices for storage: only
// JSON, only up to a size, and with configured fields redacted before the
// payload leaves the tap.
package capture

import (
	"encoding/json"
	"strings"
)

// MaxBytes is the largest payload captured; larger ones are cut short.
const MaxBytes = 16 << 10

// Redacted replaces the value of each redacted field.
const Redacted = "‹redacted›"

// Prepare returns the payload as it may be stored: compact JSON with the
// fields at the given dotted paths replaced by Redacted. ok is false for
// payloads that are not JSON objects, which are not captured.
func Prepare(payload []byte, redact []string) (string, bool) {
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return "", false
	}
	for _, p := range redact {
		redactPath(doc, strings.Split(p, "."))
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", false
	}
	if len(b) > MaxBytes {
		return string(b[:MaxBytes]) + "…", true
	}
	return string(b), true
}

func redactPath(m map[string]any, path []string) {
	v, ok := m[path[0]]
	if !ok {
		return
	}
	if len(path) == 1 {
		m[path[0]] = Redacted
		return
	}
	switch x := v.(type) {
	case map[string]any:
		redactPath(x, path[1:])
	case []any:
		for _, e := range x {
			if em, ok := e.(map[string]any); ok {
				redactPath(em, path[1:])
			}
		}
	}
}
