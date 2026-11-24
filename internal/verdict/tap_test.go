package verdict

import (
	"strings"
	"testing"
	"time"
)

func beats(start time.Time, n int, every time.Duration, change func(i int, b *Beat)) []Beat {
	var out []Beat
	for i := 0; i < n; i++ {
		b := Beat{At: start.Add(time.Duration(i+1) * every), Connected: true, Every: every}
		if change != nil {
			change(i, &b)
		}
		out = append(out, b)
	}
	return out
}

func TestBlindness(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	every := 5 * time.Second
	now := t0.Add(60 * time.Second)

	if b := Blindness("h", beats(t0, 12, every, nil), t0, now, 7*time.Second); b != nil {
		t.Fatalf("healthy tap reported blind: %+v", b)
	}
	if b := Blindness("h", nil, t0, now, 7*time.Second); b != nil {
		t.Fatal("a tap that never reports must not count as blind")
	}
	b := Blindness("h", beats(t0, 12, every, func(i int, x *Beat) {
		if i == 5 || i == 6 {
			x.Missed = 10
		}
	}), t0, now, 7*time.Second)
	if b == nil || b.Missed != 20 || b.Away || !b.From.Equal(t0.Add(25*time.Second)) || !b.To.Equal(t0.Add(35*time.Second)) {
		t.Fatalf("dropped messages: %+v", b)
	}
	// Two stretches: only the later one counts.
	b = Blindness("h", beats(t0, 12, every, func(i int, x *Beat) {
		if i == 1 || i == 9 {
			x.Missed = 4
		}
	}), t0, now, 7*time.Second)
	if b == nil || b.Missed != 4 || !b.From.Equal(t0.Add(45*time.Second)) {
		t.Fatalf("two stretches: %+v", b)
	}
	// Reports stop at 30 s: the tap is gone.
	b = Blindness("h", beats(t0, 6, every, nil), t0, now, 7*time.Second)
	if b == nil || !b.Away || !b.To.Equal(now) {
		t.Fatalf("silent tap: %+v", b)
	}
	b = Blindness("h", beats(t0, 12, every, func(i int, x *Beat) {
		if i == 10 {
			x.Delay = 40 * time.Second
		}
	}), t0, now, 7*time.Second)
	if b == nil || b.Behind != 40*time.Second || !strings.Contains(b.Describe(), "fell 40s behind") {
		t.Fatalf("lagging tap: %+v", b)
	}
}

func TestHoldOnlyWhileTheTapExplainsTheStop(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	since := now.Add(-30 * time.Second)
	stopped := func() Verdict { return Verdict{State: Stopped, Since: &since, Sentence: "stopped"} }

	v := stopped()
	v.Hold("d", &Blind{Hop: "raw", From: now.Add(-40 * time.Second), To: now, Away: true}, now, 15*time.Second)
	if v.State != Unsure || !strings.Contains(v.Sentence, "the tap was not watching raw") {
		t.Fatalf("%+v", v)
	}
	v = stopped()
	v.Hold("d", &Blind{Hop: "raw", From: now.Add(-10 * time.Minute), To: now.Add(-9 * time.Minute), Missed: 3}, now, 15*time.Second)
	if v.State != Stopped {
		t.Fatalf("an old blind stretch held a current stop: %+v", v)
	}
	early := now.Add(-5 * time.Minute)
	v = Verdict{State: Stopped, Since: &early, Sentence: "stopped"}
	v.Hold("d", &Blind{Hop: "raw", From: now.Add(-40 * time.Second), To: now, Away: true}, now, 15*time.Second)
	if v.State != Stopped {
		t.Fatalf("a stop that began before the tap went blind was held: %+v", v)
	}
	ok := Verdict{State: OK}
	ok.Hold("d", &Blind{Hop: "raw", From: now, To: now, Away: true}, now, 15*time.Second)
	if ok.State != OK {
		t.Fatal("a healthy verdict was held")
	}
}
