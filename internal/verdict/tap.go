package verdict

import (
	"fmt"
	"time"
)

// Unsure means the verdict depends on a hop the tap did not fully watch.
const Unsure State = "unsure"

// Beat is one tap health report for a hop: it covers the Every before At.
type Beat struct {
	At        time.Time
	Connected bool
	Missed    int
	Delay     time.Duration
	Every     time.Duration
}

// Blind is a stretch of time in which the tap did not fully watch a hop.
type Blind struct {
	Hop    string        `json:"hop"`
	From   time.Time     `json:"from"`
	To     time.Time     `json:"to"`
	Missed int           `json:"missed,omitempty"`    // messages the tap knows it dropped
	Behind time.Duration `json:"behind_ns,omitempty"` // the furthest the tap fell behind
	Away   bool          `json:"away,omitempty"`      // the tap was disconnected or not reporting
}

// Blindness finds when, between from and now, the tap was not fully watching
// a hop: reports that say it was disconnected, dropped messages or fell more
// than behind behind, and gaps where no report came at all. Only the latest
// stretch is kept: an earlier one that ended cannot explain what is due now.
// Beats must be in time order. It returns nil when the tap watched the whole time, or when it
// never reported, as older taps did not.
func Blindness(hop string, beats []Beat, from, now time.Time, behind time.Duration) *Blind {
	if len(beats) == 0 {
		return nil
	}
	var b *Blind
	mark := func(f, t time.Time) bool {
		if t.Before(from) {
			return false
		}
		if b == nil || f.Sub(b.To) > 10*time.Second {
			b = &Blind{Hop: hop, From: f, To: t}
			return true
		}
		if f.Before(b.From) {
			b.From = f
		}
		if t.After(b.To) {
			b.To = t
		}
		return true
	}
	for i, x := range beats {
		every := x.Every
		if every <= 0 {
			every = 5 * time.Second
		}
		if i > 0 && x.At.Sub(beats[i-1].At) > 2*every {
			if mark(beats[i-1].At, x.At.Add(-every)) {
				b.Away = true
			}
		}
		bad := !x.Connected || x.Missed > 0 || x.Delay > behind
		if !bad {
			continue
		}
		if !mark(x.At.Add(-every), x.At) {
			continue
		}
		b.Missed += x.Missed
		if x.Delay > b.Behind {
			b.Behind = x.Delay
		}
		if !x.Connected {
			b.Away = true
		}
	}
	last := beats[len(beats)-1]
	every := last.Every
	if every <= 0 {
		every = 5 * time.Second
	}
	if now.Sub(last.At) > 2*every {
		if mark(last.At, now) {
			b.Away = true
		}
	}
	return b
}

// Hold turns a verdict that rests on hop into Unsure when the tap was blind
// there recently enough to explain it: the newest settled readings fall
// inside the blind stretch. A stop or silence that began before the tap went
// blind stands. grace is the time readings get to arrive.
func (v *Verdict) Hold(device string, b *Blind, now time.Time, grace time.Duration) {
	if b == nil || (v.State != Stopped && v.State != Silent) {
		return
	}
	if !b.Recent(now, grace) || (v.Since != nil && b.Predates(*v.Since, grace)) {
		return
	}
	v.Wait(device, b)
}

// Wait makes the verdict Unsure because of what the tap missed.
func (v *Verdict) Wait(device string, b *Blind) {
	v.State = Unsure
	v.WaitingOn = b.Hop
	v.Sentence = fmt.Sprintf("%s: %s; no verdict until it catches up.", device, b.Describe())
}

// Recent reports whether the blind stretch could explain readings that are
// only now due: it lasted until less than grace (plus a report interval) ago.
func (b *Blind) Recent(now time.Time, grace time.Duration) bool {
	return b != nil && !b.To.Before(now.Add(-grace-10*time.Second))
}

// Predates reports whether t lies clearly before the blind stretch, so what
// happened at t was seen by a working tap.
func (b *Blind) Predates(t time.Time, grace time.Duration) bool {
	return t.Before(b.From.Add(-grace))
}

// Describe says in a few words what the tap missed.
func (b *Blind) Describe() string {
	at := b.From.Format("15:04:05 MST")
	switch {
	case b.Away:
		return fmt.Sprintf("the tap was not watching %s from %s", b.Hop, at)
	case b.Missed > 0:
		return fmt.Sprintf("the tap missed %d readings at %s since %s", b.Missed, b.Hop, at)
	default:
		return fmt.Sprintf("the tap fell %s behind at %s", b.Behind.Round(time.Second), b.Hop)
	}
}
