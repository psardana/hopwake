// Package verdict decides, from the hops a device's recent readings reached,
// whether its data gets through and, if not, where it stops.
package verdict

import (
	"fmt"
	"sort"
	"time"
)

// State is the one-word answer.
type State string

// The possible answers.
const (
	OK      State = "ok"      // recent readings reach every hop of the route
	Stopped State = "stopped" // recent readings stop part-way
	Silent  State = "silent"  // no readings at any hop in the window
	Unknown State = "unknown" // never seen
)

// HopTime is one hop a journey reached, and when.
type HopTime struct {
	Hop string    `json:"hop"`
	At  time.Time `json:"at"`
}

// Journey is what was seen of one reading.
type Journey struct {
	ID      string    `json:"journey_id"`
	Seq     uint64    `json:"seq"`
	First   time.Time `json:"first"`
	Hops    []HopTime `json:"hops"`
	Reached int       `json:"reached"` // index in the route of the furthest hop seen, -1 if none
}

// Verdict is the answer for one device.
type Verdict struct {
	State        State         `json:"state"`
	Sentence     string        `json:"sentence"`
	StoppedAfter string        `json:"stopped_after,omitempty"`
	NotReached   string        `json:"not_reached,omitempty"`
	Since        *time.Time    `json:"since,omitempty"`
	Missing      int           `json:"missing,omitempty"`
	LastComplete *time.Time    `json:"last_complete,omitempty"`
	Median       time.Duration `json:"median_ns,omitempty"`
}

// Decide works out the verdict. Journeys must be newest first. Journeys that
// started less than grace ago are still under way and are not judged.
// lastSeen is the device's most recent hop event at any time, if any.
func Decide(device string, route []string, journeys []Journey, now time.Time, grace time.Duration, lastSeen *HopTime) Verdict {
	index := map[string]int{}
	for i, h := range route {
		index[h] = i
	}
	// Reached is set on the caller's journeys too, so they can show it.
	var settled []Journey
	for k := range journeys {
		j := &journeys[k]
		j.Reached = -1
		for _, h := range j.Hops {
			if i, ok := index[h.Hop]; ok && i > j.Reached {
				j.Reached = i
			}
		}
		if now.Sub(j.First) >= grace {
			settled = append(settled, *j)
		}
	}
	last := len(route) - 1

	if len(settled) == 0 {
		if len(journeys) > 0 {
			return Verdict{State: OK, Sentence: fmt.Sprintf("%s: readings are arriving; the newest are still on their way.", device)}
		}
		if lastSeen == nil {
			return Verdict{State: Unknown, Sentence: fmt.Sprintf("%s: never seen at any hop.", device)}
		}
		at := lastSeen.At
		return Verdict{State: Silent, Since: &at,
			Sentence: fmt.Sprintf("%s: no readings at any hop since %s; last seen at %s.", device, at.Format("15:04:05 MST"), lastSeen.Hop)}
	}

	v := Verdict{}
	var lats []time.Duration
	for _, j := range settled {
		if j.Reached == last {
			if v.LastComplete == nil {
				t := j.First
				v.LastComplete = &t
			}
			if len(j.Hops) > 1 {
				lats = append(lats, span(j.Hops))
			}
		}
	}
	if len(lats) > 0 {
		sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
		v.Median = lats[len(lats)/2]
	}

	newest := settled[0]
	if newest.Reached == last {
		v.State = OK
		v.Sentence = fmt.Sprintf("%s: readings reach every hop.", device)
		if v.Median > 0 {
			v.Sentence = fmt.Sprintf("%s: readings reach every hop, %s from first to last.", device, roundDur(v.Median))
		}
		return v
	}

	// The newest settled journeys stop at the same place: count them.
	stop := newest.Reached
	run := 0
	since := newest.First
	for _, j := range settled {
		if j.Reached != stop {
			break
		}
		run++
		since = j.First
	}
	v.State = Stopped
	v.Missing = run
	v.Since = &since
	if stop < 0 {
		v.Sentence = fmt.Sprintf("%s: readings seen, but at no hop of the route.", device)
		return v
	}
	v.StoppedAfter = route[stop]
	v.NotReached = route[stop+1]
	v.Sentence = fmt.Sprintf("%s: readings stop after %s. %d readings since %s did not reach %s.",
		device, route[stop], run, since.Format("15:04:05 MST"), route[stop+1])
	return v
}

// Quick judges a device from only the last time each hop saw it, which is
// cheap enough to run for a whole fleet. A hop counts as reached when it saw
// the device within grace of the first hop's latest sighting. silentAfter is
// how long without any reading at the first hop makes a device silent.
func Quick(route []string, last map[string]time.Time, now time.Time, grace, silentAfter time.Duration) (State, string) {
	if len(route) == 0 {
		return Unknown, ""
	}
	first, ok := last[route[0]]
	if !ok || first.IsZero() {
		if len(last) == 0 {
			return Unknown, ""
		}
		return Stopped, "" // seen downstream, never at the first hop
	}
	if now.Sub(first) > silentAfter {
		return Silent, ""
	}
	reference := first.Add(-grace)
	reached := 0
	for i := len(route) - 1; i > 0; i-- {
		if t, ok := last[route[i]]; ok && !t.Before(reference) {
			reached = i
			break
		}
	}
	if reached == len(route)-1 {
		return OK, ""
	}
	return Stopped, route[reached]
}

func span(h []HopTime) time.Duration {
	lo, hi := h[0].At, h[0].At
	for _, x := range h[1:] {
		if x.At.Before(lo) {
			lo = x.At
		}
		if x.At.After(hi) {
			hi = x.At
		}
	}
	return hi.Sub(lo)
}

func roundDur(d time.Duration) time.Duration {
	if d < time.Second {
		return d.Round(time.Millisecond)
	}
	return d.Round(100 * time.Millisecond)
}
