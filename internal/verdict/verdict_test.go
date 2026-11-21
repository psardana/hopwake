package verdict

import (
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
	v := Decide("dev-00044", route, journeys(2, 2, 2), now, 15*time.Second, nil)
	if v.State != OK || v.Median != 20*time.Millisecond || !strings.Contains(v.Sentence, "reach every hop") {
		t.Fatalf("%+v", v)
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

func TestHopsOutsideTheRouteAreIgnored(t *testing.T) {
	js := journeys(0)
	js[0].Hops = []HopTime{{Hop: "kafka:telemetry.dlq", At: js[0].First}}
	v := Decide("dev-00043", route, js, now, 15*time.Second, nil)
	if v.State != Stopped || v.StoppedAfter != "" {
		t.Fatalf("%+v", v)
	}
}
