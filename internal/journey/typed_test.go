package journey

import (
	"errors"
	"strings"
	"testing"
)

const typedRules = `
tenant: t
sources:
  - name: devices
    kind: mqtt
    match: fleet/+/up/#
    device_from_topic: fleet/{device}/up/#
    format: protobuf
    proto: {path: protos, type_from: MessageType}
    time_field: measured_at
  - name: all messages
    kind: kafka
    match: all.in
    device_header: Serial
    format: protobuf
    proto: {path: protos, type_from: MessageType}
    time_field: measured_at
  - {name: readings, kind: kafka, match: readings.in, device_from_key: true, format: protobuf, proto: {path: protos, type_from: MessageType}, time_field: measured_at}
  - {name: logs, kind: kafka, match: logs.in, device_from_key: true, format: protobuf, proto: {path: protos, type_from: MessageType}, time_field: measured_at}
routes:
  - name: readings
    types: [plant.readings]
    steps:
      - fleet/+/up/#
      - {hop: kafka:all.in, via: bridge}
      - {hop: kafka:readings.in, via: router}
  - name: logs
    types: [plant.logs, plant.readings.Debug]
    steps:
      - fleet/+/up/#
      - {hop: kafka:logs.in, via: bridge}
`

func TestTypedRoutes(t *testing.T) {
	_, err := ParseConfig([]byte(typedRules))
	if err == nil || !strings.Contains(err.Error(), "not a source") {
		t.Fatalf("a step without kind: was accepted: %v", err)
	}
	c, err := ParseConfig([]byte(strings.ReplaceAll(typedRules, "      - fleet/+/up/#", "      - mqtt:fleet/+/up/#")))
	if err != nil {
		t.Fatal(err)
	}
	names := func(rs []TypedRoute) string {
		var n []string
		for _, r := range rs {
			n = append(n, r.Name)
		}
		return strings.Join(n, ",")
	}
	for typ, want := range map[string]string{
		"plant.readings.Basic":  "readings",
		"plant.readings.Debug":  "readings,logs", // fan-out: both routes cover it
		"plant.readingsX.Basic": "",
		"plant.logs.Entry":      "logs",
		"":                      "readings,logs",
	} {
		if got := names(c.RoutesFor(typ)); got != want {
			t.Errorf("%q: got %q, want %q", typ, got, want)
		}
	}
	if c.Via("kafka:readings.in") != "router" || c.Via("kafka:logs.in") != "bridge" {
		t.Fatal("via across typed routes")
	}
	if got := strings.Join(c.Hops(), " "); got != "mqtt:fleet/+/up/# kafka:all.in kafka:readings.in" {
		t.Fatalf("primary route %q", got)
	}
	bad := strings.Replace(strings.ReplaceAll(typedRules, "      - fleet/+/up/#", "      - mqtt:fleet/+/up/#"), "    types: [plant.logs, plant.readings.Debug]\n", "", 1)
	if _, err := ParseConfig([]byte(bad)); err == nil {
		t.Fatal("a typed route without types was accepted")
	}
}

func TestProtobufSourceChecks(t *testing.T) {
	base := "tenant: t\nsources:\n  - {name: a, kind: kafka, match: a, device_from_key: true, %s}\n"
	for _, bad := range []string{
		"seq_field: seq, time_field: t",                     // two ways to number readings
		"format: protobuf, time_field: t",                   // no proto
		"format: protobuf, proto: {path: p}, time_field: t", // no type
		"proto: {path: p, type: x.Y}, time_field: t",        // proto without format
		"format: xml, time_field: t",                        // unknown format
	} {
		if _, err := ParseConfig([]byte(strings.Replace(base, "%s", bad, 1))); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	if _, err := ParseConfig([]byte(strings.Replace(base, "%s", "format: protobuf, proto: {path: p, type: x.Y}, time_field: t", 1))); err != nil {
		t.Fatal(err)
	}
}

func TestExtractFromHeadersKeyAndTime(t *testing.T) {
	h := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	r := Rule{Tenant: "t", DeviceHeader: "Serial", TimeField: "basic.measured_at"}
	payload := []byte(`{"basic":{"measured_at":"2026-11-18T10:00:00.5+01:00"}}`)
	a, err := r.ExtractMeta("", payload, Meta{Header: h(map[string]string{"Serial": "S1"}), Type: "plant.readings.Basic"})
	if err != nil || a.Device != "S1" || a.Time != "2026-11-18T09:00:00.5Z" {
		t.Fatalf("%+v %v", a, err)
	}
	b, _ := r.ExtractMeta("", payload, Meta{Header: h(map[string]string{"Serial": "S1"}), Type: "plant.readings.Temp"})
	if a.ID() == b.ID() {
		t.Fatal("two message types at the same time share an ID")
	}
	k := Rule{Tenant: "t", DeviceFromKey: true, TimeField: "measured_at"}
	c, err := k.ExtractMeta("", []byte(`{"measured_at": 1795000000}`), Meta{Key: []byte("S1"), Type: "plant.readings.Basic"})
	if err != nil || c.Time != "2026-11-18T11:06:40Z" {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := k.ExtractMeta("", []byte(`{}`), Meta{Key: []byte("S1")}); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("no time: %v", err)
	}
	s := Rule{Tenant: "t", TopicPattern: "fleet/{device}/up/#", SeqFrom: "Seq"}
	d, err := s.ExtractMeta("fleet/S2/up/prio", nil, Meta{Header: h(map[string]string{"Seq": "42"})})
	if err != nil || d.Device != "S2" || d.Seq != 42 || d.ID() != ID("t", "S2", 42) {
		t.Fatalf("%+v %v", d, err)
	}
	if _, ok := deviceFromTopic("fleet/{device}/up/#", "fleet/S2/up"); !ok {
		t.Fatal("# must match nothing as well")
	}
}

func TestLookup(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"b": []any{map[string]any{"c": "x"}}}}
	if v, ok := Lookup(doc, "a.b[0].c"); !ok || v != "x" {
		t.Fatalf("%v %v", v, ok)
	}
	for _, p := range []string{"a.b[1].c", "a.x", "a.b.c", "a.b[-1]"} {
		if _, ok := Lookup(doc, p); ok {
			t.Errorf("%s found", p)
		}
	}
}

func TestTimeFieldCandidates(t *testing.T) {
	r := Rule{Tenant: "t", DeviceFromKey: true, TimeField: "measurement_time, timestamp"}
	k, err := r.ExtractMeta("", []byte(`{"timestamp":"2026-11-18T10:00:00Z"}`), Meta{Key: []byte("S1"), Type: "x.Status"})
	if err != nil || k.Time != "2026-11-18T10:00:00Z" {
		t.Fatalf("%+v %v", k, err)
	}
}

func TestNumericDeviceField(t *testing.T) {
	r := Rule{Tenant: "t", DeviceField: "systemId", TimeField: "time"}
	k, err := r.ExtractMeta("records", []byte(`{"systemId":100030,"time":"1795000000"}`), Meta{Type: "m.Reading"})
	if err != nil || k.Device != "100030" {
		t.Fatalf("got %+v, %v; want device 100030", k, err)
	}
	if _, err := r.ExtractMeta("records", []byte(`{"systemId":1.5,"time":"1795000000"}`), Meta{Type: "m.Reading"}); err == nil {
		t.Fatal("a fractional device ID was accepted")
	}
}
