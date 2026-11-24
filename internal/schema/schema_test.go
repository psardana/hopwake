package schema

import (
	"os"
	"path/filepath"
	"testing"
)

const raw = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["device_id", "seq", "temp_c", "pressure_kpa"],
  "properties": {
    "device_id": {"type": "string"},
    "seq": {"type": "integer", "minimum": 0},
    "temp_c": {"type": "number", "minimum": -60, "maximum": 150},
    "pressure_kpa": {"type": "number", "x-unit": "kPa"}
  }
}`

func TestCheck(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raw.json")
	if err := os.WriteFile(p, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		`{"device_id":"d","seq":1,"temp_c":21.5,"pressure_kpa":101.2}`:      "",
		`{"device_id":"d","seq":1,"temp_c":21.5,"pressure_kpa":"101.2"}`:    "pressure_kpa: got string, want number",
		`{"device_id":"d","seq":1,"temperature":21.5,"pressure_kpa":101.2}`: "missing property 'temp_c'",
		`{"device_id":"d","seq":1,"temp_c":400,"pressure_kpa":101.2}`:       "temp_c: maximum: got 400, want 150",
		`seq=1`: "not JSON",
	}
	for payload, want := range cases {
		got := ""
		if err := v.Check([]byte(payload)); err != nil {
			got = err.Error()
		}
		if got != want {
			t.Errorf("%s:\n got %q\nwant %q", payload, got, want)
		}
	}
}

// The reference pipeline's schemas accept what its services send.
func TestReferenceSchemas(t *testing.T) {
	for file, payload := range map[string]string{
		"raw.json":        `{"device_id":"dev-00042","site":"site-02","seq":3,"ts":"2026-11-18T06:38:18Z","temp_c":20.5,"pressure_kpa":101.3,"vibration_mm_s":1.2}`,
		"normalized.json": `{"schema":"telemetry/v2","device":"dev-00042","site":"site-02","seq":3,"observed_at":"2026-11-18T06:38:18Z","temperature_k":293.65,"pressure_pa":101300,"vibration_mm_s":1.2}`,
	} {
		v, err := Load(filepath.Join("..", "..", "reference", "schemas", file))
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Check([]byte(payload)); err != nil {
			t.Errorf("%s: %v", file, err)
		}
	}
}
