package journey

import (
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
	if len(c.Route) != 3 || c.Route[0] != "mqtt:plant/+/+/telemetry" {
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
