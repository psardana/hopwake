package journey

import (
	"fmt"
	"strings"
	"testing"
)

const referenceRules = `
tenant: reference
sources:
  - name: raw readings
    kind: mqtt
    match: plant/+/+/telemetry
    device_from_topic: plant/+/{device}/telemetry
    seq_field: seq
  - name: normalised readings
    kind: kafka
    match: telemetry.normalized
    device_field: device
    seq_field: seq
`

func TestConfigGivesTheSameIDAtBothHops(t *testing.T) {
	c, err := ParseConfig([]byte(referenceRules))
	if err != nil {
		t.Fatal(err)
	}
	up, ok := c.Rule("mqtt", "plant/site-02/dev-00042/telemetry")
	if !ok {
		t.Fatal("no rule for the MQTT topic")
	}
	down, ok := c.Rule("kafka", "telemetry.normalized")
	if !ok {
		t.Fatal("no rule for the Kafka topic")
	}
	a, err := up.Extract("plant/site-02/dev-00042/telemetry", []byte(`{"device_id":"dev-00042","seq":7}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := down.Extract("telemetry.normalized", []byte(`{"device":"dev-00042","seq":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() != b.ID() || a.Tenant != "reference" {
		t.Fatalf("got %+v and %+v", a, b)
	}
}

func TestReferenceRulesFileLoads(t *testing.T) {
	c, err := LoadConfig("../../reference/journey-rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{"telemetry.raw", "telemetry.normalized"} {
		if _, ok := c.Rule("kafka", topic); !ok {
			t.Errorf("no rule for %s", topic)
		}
	}
	if len(c.Route) != 5 || len(c.RequiredHops()) != 4 || c.Route[0].Hop != "mqtt:plant/+/+/telemetry" {
		t.Errorf("route = %v", c.Route)
	}
}

func TestNoRuleForOtherTopics(t *testing.T) {
	c, _ := ParseConfig([]byte(referenceRules))
	for _, tc := range []struct{ kind, topic string }{
		{"mqtt", "plant/site-02/dev-00042/status"},
		{"mqtt", "plant/site-02/telemetry"},
		{"kafka", "telemetry.raw"},
		{"amqp", "telemetry.normalized"},
	} {
		if _, ok := c.Rule(tc.kind, tc.topic); ok {
			t.Errorf("unexpected rule for %s %s", tc.kind, tc.topic)
		}
	}
}

func TestTopicMatches(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"plant/+/+/telemetry", "plant/a/b/telemetry", true},
		{"plant/#", "plant/a/b/telemetry", true},
		{"plant/+", "plant/a/b", false},
		{"plant/a", "plant/a", true},
		{"plant/a/b", "plant/a", false},
	}
	for _, c := range cases {
		if got := topicMatches(c.filter, c.topic); got != c.want {
			t.Errorf("%s vs %s: got %v", c.filter, c.topic, got)
		}
	}
}

func TestConfigRejects(t *testing.T) {
	cases := map[string]string{
		"no tenant":         "sources:\n  - {kind: kafka, match: t, device_field: d, seq_field: s}\n",
		"no sources":        "tenant: x\n",
		"bad kind":          "tenant: x\nsources:\n  - {kind: amqp, match: t, device_field: d, seq_field: s}\n",
		"no match":          "tenant: x\nsources:\n  - {kind: kafka, device_field: d, seq_field: s}\n",
		"both device ways":  "tenant: x\nsources:\n  - {kind: mqtt, match: a/+, device_from_topic: 'a/{device}', device_field: d, seq_field: s}\n",
		"no device way":     "tenant: x\nsources:\n  - {kind: kafka, match: t, seq_field: s}\n",
		"topic on kafka":    "tenant: x\nsources:\n  - {kind: kafka, match: t, device_from_topic: 'a/{device}', seq_field: s}\n",
		"no device in path": "tenant: x\nsources:\n  - {kind: mqtt, match: a/+, device_from_topic: a/+, seq_field: s}\n",
		"no seq":            "tenant: x\nsources:\n  - {kind: kafka, match: t, device_field: d}\n",
		"unknown key":       "tenant: x\nsources:\n  - {kind: kafka, match: t, device_field: d, seq_field: s, colour: red}\n",
		"route unknown hop": "tenant: x\nsources:\n  - {kind: kafka, match: t, device_field: d, seq_field: s}\nroute: [kafka:other]\n",
		"route twice":       "tenant: x\nsources:\n  - {kind: kafka, match: t, device_field: d, seq_field: s}\nroute: [kafka:t, kafka:t]\n",
	}
	for name, yml := range cases {
		if _, err := ParseConfig([]byte(yml)); err == nil || !strings.Contains(err.Error(), "journey rules") {
			t.Errorf("%s: got %v, want an error", name, err)
		}
	}
}

func TestRouteStepsWithVia(t *testing.T) {
	c, err := ParseConfig([]byte(`
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
  - {name: b, kind: kafka, match: b, device_field: device, seq_field: seq}
  - {name: c, kind: kafka, match: c, device_field: device, seq_field: seq}
route:
  - mqtt:a/+
  - hop: kafka:b
    via: bridge
  - hop: kafka:c
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.Hops(), " "); got != "mqtt:a/+ kafka:b kafka:c" {
		t.Fatalf("hops %q", got)
	}
	if c.Via("kafka:b") != "bridge" || c.Via("kafka:c") != "" || c.Via("mqtt:a/+") != "" {
		t.Fatalf("via: %+v", c.Route)
	}
	_, err = ParseConfig([]byte(`
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
route:
  - {hop: "mqtt:a/+", via: nobody}
`))
	if err == nil || !strings.Contains(err.Error(), "cannot have via or mapping") {
		t.Fatalf("via on the first hop: %v", err)
	}
}

func TestDeadLetterSource(t *testing.T) {
	base := `
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
  - {name: b, kind: kafka, match: b, device_field: device, seq_field: seq}
  - {name: dlq, kind: kafka, match: b.dlq, device_field: device, seq_field: seq, role: dead-letter, rejects: %s, reason_header: why}
route: [mqtt:a/+, kafka:b]
`
	c, err := ParseConfig([]byte(fmt.Sprintf(base, "kafka:b")))
	if err != nil {
		t.Fatal(err)
	}
	if d := c.DeadLetters(); len(d) != 1 || d[0].HopName() != "kafka:b.dlq" || !d[0].DeadLetter() {
		t.Fatalf("%+v", d)
	}
	for _, bad := range []string{"mqtt:a/+", "kafka:nowhere", `""`} {
		if _, err := ParseConfig([]byte(fmt.Sprintf(base, bad))); err == nil {
			t.Errorf("rejects %s was accepted", bad)
		}
	}
}

func TestAggregateSource(t *testing.T) {
	ok := `
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
  - {name: b, kind: kafka, match: b, device_field: device, seq_field: seq}
  - {name: m, kind: kafka, match: minute, role: aggregate, window_header: hopwake-window}
route: [mqtt:a/+, kafka:b]
`
	c, err := ParseConfig([]byte(ok))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Sources[2].Aggregate() {
		t.Fatal("not an aggregate")
	}
	for _, bad := range []string{
		strings.Replace(ok, "window_header: hopwake-window", "seq_field: seq", 1),
		strings.Replace(ok, "role: aggregate, ", "", 1),
		strings.Replace(ok, "[mqtt:a/+, kafka:b]", "[mqtt:a/+, kafka:b, kafka:minute]", 1),
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("accepted:\n%s", bad)
		}
	}
}

func TestHookSourcesAndOptionalSteps(t *testing.T) {
	ok := `
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
  - {name: stored, kind: hook, match: db.stored}
  - {name: served, kind: hook, match: api.served}
route:
  - mqtt:a/+
  - {hop: hook:db.stored, via: recorder}
  - {hop: hook:api.served, via: api, optional: true}
`
	c, err := ParseConfig([]byte(ok))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.RequiredHops(), " "); got != "mqtt:a/+ hook:db.stored" {
		t.Fatalf("required %q", got)
	}
	if _, err := ParseConfig([]byte(strings.Replace(ok, "{name: stored, kind: hook, match: db.stored}", "{name: stored, kind: hook, match: db.stored, seq_field: seq}", 1))); err == nil {
		t.Error("hook source with seq_field accepted")
	}
	swapped := strings.Replace(strings.Replace(ok, "{hop: hook:db.stored, via: recorder}", "X", 1), "{hop: hook:api.served, via: api, optional: true}", "{hop: hook:db.stored, via: recorder}", 1)
	swapped = strings.Replace(swapped, "X", "{hop: hook:api.served, via: api, optional: true}", 1)
	if _, err := ParseConfig([]byte(swapped)); err == nil {
		t.Error("required hop after an optional one accepted")
	}
}
