package journey

import (
	"errors"
	"regexp"
	"testing"
)

var traceID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestIDIsStableAndTraceShaped(t *testing.T) {
	a := ID("acme", "dev-00042", 1683)
	if a != ID("acme", "dev-00042", 1683) {
		t.Fatal("same inputs gave different IDs")
	}
	if !traceID.MatchString(a) {
		t.Fatalf("%q is not a 32-character hex trace ID", a)
	}
	for _, other := range []string{
		ID("acme", "dev-00042", 1684),
		ID("acme", "dev-00043", 1683),
		ID("other", "dev-00042", 1683),
		ID("acme", "dev-0004", 21683),
	} {
		if other == a {
			t.Fatalf("different inputs gave the same ID %s", a)
		}
	}
}

func TestFieldBoundariesMatter(t *testing.T) {
	if ID("ab", "c", 1) == ID("a", "bc", 1) {
		t.Fatal("tenant and device boundaries are ambiguous")
	}
}

// Two hops see the same reading in different shapes and agree on its ID.
func TestTwoHopsAgreeWithoutHeaders(t *testing.T) {
	mqttHop := Rule{Tenant: "acme", TopicPattern: "plant/+/{device}/telemetry", SeqField: "seq"}
	kafkaHop := Rule{Tenant: "acme", DeviceField: "device", SeqField: "seq"}

	raw := []byte(`{"device_id":"dev-00042","site":"site-02","seq":1683,"temp_c":21.5}`)
	normalised := []byte(`{"schema":"telemetry/v2","device":"dev-00042","site":"site-02","seq":1683,"temperature_k":294.65}`)

	a, err := mqttHop.Extract("plant/site-02/dev-00042/telemetry", raw)
	if err != nil {
		t.Fatal(err)
	}
	b, err := kafkaHop.Extract("telemetry.normalized", normalised)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() != b.ID() {
		t.Fatalf("hops disagree: %s vs %s", a.ID(), b.ID())
	}
}

func TestExtractRejects(t *testing.T) {
	r := Rule{TopicPattern: "plant/+/{device}/telemetry", SeqField: "seq"}
	cases := map[string]struct {
		topic   string
		payload string
	}{
		"wrong topic":    {"plant/site-02/dev-1/status", `{"seq":1}`},
		"not json":       {"plant/site-02/dev-1/telemetry", `seq=1`},
		"no seq":         {"plant/site-02/dev-1/telemetry", `{"n":1}`},
		"seq not number": {"plant/site-02/dev-1/telemetry", `{"seq":"abc"}`},
		"negative seq":   {"plant/site-02/dev-1/telemetry", `{"seq":-4}`},
	}
	for name, c := range cases {
		if _, err := r.Extract(c.topic, []byte(c.payload)); !errors.Is(err, ErrNoMatch) {
			t.Errorf("%s: got %v, want ErrNoMatch", name, err)
		}
	}
}

func TestSeqAsString(t *testing.T) {
	r := Rule{DeviceField: "device", SeqField: "seq"}
	k, err := r.Extract("", []byte(`{"device":"d","seq":"17"}`))
	if err != nil || k.Seq != 17 {
		t.Fatalf("got %+v, %v", k, err)
	}
}

func TestFromTraceparent(t *testing.T) {
	id, ok := FromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if !ok || id != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("got %q %v", id, ok)
	}
	for _, bad := range []string{
		"",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e47-00f067aa0ba902b7-01",
		"00-zzf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		if _, ok := FromTraceparent(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestBootSeparatesReadingsWithTheSameSeq(t *testing.T) {
	if ID("t", "d", 1) != BootID("t", "d", "", 1) {
		t.Fatal("an empty boot must give the same ID as before")
	}
	if BootID("t", "d", "7", 1) == BootID("t", "d", "8", 1) || BootID("t", "d", "7", 1) == ID("t", "d", 1) {
		t.Fatal("different boots must give different IDs")
	}
	r := Rule{DeviceField: "device", SeqField: "seq", BootField: "boot"}
	a, err := r.Extract("", []byte(`{"device":"d","seq":1,"boot":7}`))
	if err != nil || a.Boot != "7" {
		t.Fatalf("%+v %v", a, err)
	}
	b, _ := r.Extract("", []byte(`{"device":"d","seq":1,"boot":"8"}`))
	if a.ID() == b.ID() {
		t.Fatal("boots 7 and 8 share an ID")
	}
	if _, err := r.Extract("", []byte(`{"device":"d","seq":1}`)); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("missing boot field: %v", err)
	}
}
