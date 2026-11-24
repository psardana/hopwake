package verdict

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

var route = []string{"mqtt:plant/+/+/telemetry", "kafka:telemetry.raw", "kafka:telemetry.normalized"}
var now = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)

// journeys builds newest-first journeys, one per second ending 20 s ago,
// each reaching hops[0..reach[i]].
func journeys(reach ...int) []Journey {
	var out []Journey
	for i, r := range reach {
		first := now.Add(-20*time.Second - time.Duration(i)*time.Second)
		j := Journey{ID: "j", Seq: uint64(100 - i), First: first}
		for h := 0; h <= r; h++ {
			j.Hops = append(j.Hops, HopTime{Hop: route[h], At: first.Add(time.Duration(h) * 10 * time.Millisecond)})
		}
		out = append(out, j)
	}
	return out
}

func TestHealthyDevice(t *testing.T) {
	js := journeys(2, 2, 2)
	v := Decide("dev-00044", route, js, now, 15*time.Second, nil)
	if v.State != OK || v.Median != 20*time.Millisecond || !strings.Contains(v.Sentence, "reach every hop") {
		t.Fatalf("%+v", v)
	}
	for _, j := range js {
		if j.Reached != 2 {
			t.Fatalf("the caller's journeys must carry how far they got: %d", j.Reached)
		}
	}
}

func TestStopsAfterTheFirstHop(t *testing.T) {
	v := Decide("dev-00042", route, journeys(0, 0, 0, 2, 2), now, 15*time.Second, nil)
	if v.State != Stopped || v.StoppedAfter != route[0] || v.NotReached != route[1] || v.Missing != 3 {
		t.Fatalf("%+v", v)
	}
	if v.LastComplete == nil || !v.LastComplete.Equal(now.Add(-23*time.Second)) {
		t.Fatalf("last complete %v", v.LastComplete)
	}
	if !strings.Contains(v.Sentence, "stop after mqtt:plant/+/+/telemetry. 3 readings") {
		t.Fatal(v.Sentence)
	}
}

func TestStopsBeforeTheLastHop(t *testing.T) {
	v := Decide("dev-00043", route, journeys(1, 1), now, 15*time.Second, nil)
	if v.State != Stopped || v.StoppedAfter != "kafka:telemetry.raw" || v.NotReached != "kafka:telemetry.normalized" {
		t.Fatalf("%+v", v)
	}
}

func TestRecentJourneysAreNotJudged(t *testing.T) {
	js := journeys(0)
	js[0].First = now.Add(-2 * time.Second)
	v := Decide("dev-00001", route, js, now, 15*time.Second, nil)
	if v.State != OK || !strings.Contains(v.Sentence, "on their way") {
		t.Fatalf("%+v", v)
	}
}

func TestSilentAndUnknown(t *testing.T) {
	seen := &HopTime{Hop: route[0], At: now.Add(-5 * time.Minute)}
	if v := Decide("dev-00042", route, nil, now, 15*time.Second, seen); v.State != Silent || !strings.Contains(v.Sentence, "09:55:00") {
		t.Fatalf("%+v", v)
	}
	if v := Decide("dev-09999", route, nil, now, 15*time.Second, nil); v.State != Unknown {
		t.Fatalf("%+v", v)
	}
}

func TestQuick(t *testing.T) {
	g, s := 15*time.Second, 60*time.Second
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	cases := []struct {
		name  string
		last  map[string]time.Time
		state State
		after string
	}{
		{"healthy", map[string]time.Time{route[0]: ago(time.Second), route[1]: ago(time.Second), route[2]: ago(time.Second)}, OK, ""},
		{"stops after mqtt", map[string]time.Time{route[0]: ago(time.Second), route[1]: ago(3 * time.Minute), route[2]: ago(3 * time.Minute)}, Stopped, route[0]},
		{"stops before last", map[string]time.Time{route[0]: ago(time.Second), route[1]: ago(time.Second), route[2]: ago(2 * time.Minute)}, Stopped, route[1]},
		{"just stopped, within grace", map[string]time.Time{route[0]: ago(time.Second), route[1]: ago(10 * time.Second), route[2]: ago(10 * time.Second)}, OK, ""},
		{"silent", map[string]time.Time{route[0]: ago(5 * time.Minute), route[1]: ago(5 * time.Minute), route[2]: ago(5 * time.Minute)}, Silent, ""},
		{"never seen", map[string]time.Time{}, Unknown, ""},
	}
	for _, c := range cases {
		state, after := Quick(route, c.last, now, g, s)
		if state != c.state || after != c.after {
			t.Errorf("%s: got %s %q, want %s %q", c.name, state, after, c.state, c.after)
		}
	}
}

func TestHopsOutsideTheRouteAreIgnored(t *testing.T) {
	js := journeys(0)
	js[0].Hops = []HopTime{{Hop: "kafka:telemetry.dlq", At: js[0].First}}
	v := Decide("dev-00043", route, js, now, 15*time.Second, nil)
	if v.State != Stopped || v.StoppedAfter != "" {
		t.Fatalf("%+v", v)
	}
}

func TestAssembleSplitsReadingsFromDifferentBoots(t *testing.T) {
	t0 := now.Add(-10 * time.Minute)
	ev := []Event{
		// seq 1 before the reboot, at two hops
		{JourneyID: "a", Seq: 1, Hop: route[0], At: t0},
		{JourneyID: "a", Seq: 1, Hop: route[1], At: t0.Add(20 * time.Millisecond)},
		// seq 1 again, five minutes later, after a reboot: only at the first hop
		{JourneyID: "a", Seq: 1, Hop: route[0], At: t0.Add(5 * time.Minute)},
		{JourneyID: "b", Seq: 2, Hop: route[0], At: t0.Add(time.Second)},
	}
	js := Assemble(ev, time.Minute)
	if len(js) != 3 {
		t.Fatalf("got %d journeys: %+v", len(js), js)
	}
	if !js[0].First.Equal(t0.Add(5*time.Minute)) || len(js[0].Hops) != 1 {
		t.Fatalf("newest first, and the rebooted reading stands alone: %+v", js[0])
	}
	if js[2].ID != "a" || len(js[2].Hops) != 2 {
		t.Fatalf("the original reading keeps both hops: %+v", js[2])
	}
}

func TestStopAtNamedService(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	route := []string{"mqtt", "raw", "normalized"}
	var js []Journey
	for i := 0; i < 5; i++ {
		at := now.Add(-time.Duration(30+i) * time.Second)
		js = append(js, Journey{ID: string(rune('a' + i)), First: at, Hops: []HopTime{{"mqtt", at}}})
	}
	v := Decide("d", route, js, now, 15*time.Second, nil)
	v.At("d", "bridge")
	if v.StoppedAt != "bridge" || !strings.Contains(v.Sentence, "stop at bridge") || !strings.Contains(v.Sentence, "never arrived at raw") {
		t.Fatalf("%+v", v)
	}
	ok := Verdict{State: OK, Sentence: "fine"}
	ok.At("d", "bridge")
	if ok.StoppedAt != "" || ok.Sentence != "fine" {
		t.Fatalf("At changed a healthy verdict: %+v", ok)
	}
}

func TestRejectionsNameTheReason(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	route := []string{"mqtt", "raw", "normalized"}
	var ev []Event
	for i := 0; i < 6; i++ {
		at := now.Add(-time.Duration(30+i) * time.Second)
		id := fmt.Sprintf("j%d", i)
		ev = append(ev, Event{JourneyID: id, Hop: "mqtt", At: at}, Event{JourneyID: id, Hop: "raw", At: at.Add(time.Millisecond)})
		if i < 4 {
			ev = append(ev, Event{JourneyID: id, Hop: "dlq", At: at.Add(2 * time.Millisecond), Op: "drop", Reason: "no field temp_c"})
		}
	}
	js := Assemble(ev, time.Minute)
	v := Decide("d", route, js, now, 15*time.Second, nil)
	if v.State != Stopped || v.Missing != 6 || v.Rejected != 4 || v.Reason != "no field temp_c" || v.NotReached != "normalized" {
		t.Fatalf("%+v", v)
	}
	v.At("d", "normaliser")
	want := "the normaliser rejected 4 readings since"
	if !strings.Contains(v.Sentence, want) || !strings.Contains(v.Sentence, ": no field temp_c. 2 more never arrived at normalized.") {
		t.Fatalf("sentence %q", v.Sentence)
	}
}
