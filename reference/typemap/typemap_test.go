package typemap

import (
	"strings"
	"testing"
)

func TestLongestPrefix(t *testing.T) {
	m, err := Parse([]byte(`
consumer-mapping:
  plant:
    - plant.all.in
  plant.readings: [ignored-key-with-dot]
  x:
    readings: [plant.readings.in]
    logs:
      Entry: [plant.logs.in, plant.audit.in]
`), "consumer-mapping")
	if err != nil {
		t.Fatal(err)
	}
	for typ, want := range map[string]string{
		"x.readings.Basic": "plant.readings.in",
		"x.logs.Entry":     "plant.logs.in,plant.audit.in",
		"x.logs.Other":     "",
		"plant.status.Up":  "plant.all.in",
		"nothing.Here":     "",
	} {
		if got := strings.Join(m.Topics(typ), ","); got != want {
			t.Errorf("%s: got %q, want %q", typ, got, want)
		}
	}
}
