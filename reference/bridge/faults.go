package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

// faults injects failures into the bridge so that missing, late and repeated
// data can be reproduced. Every injected fault is logged as one JSON line, which
// is the ground truth to check a tracing tool against.
type faults struct {
	devices   map[string]bool // empty means every device
	drop      float64
	duplicate float64
	delayRate float64
	delay     time.Duration
	from      time.Time // faults apply from this time on
}

func faultsFromEnv() faults {
	f := faults{
		devices:   map[string]bool{},
		drop:      envFloat("FAULT_DROP"),
		duplicate: envFloat("FAULT_DUPLICATE"),
		delayRate: envFloat("FAULT_DELAY_RATE"),
	}
	if d, err := time.ParseDuration(os.Getenv("FAULT_DELAY")); err == nil {
		f.delay = d
	}
	// FAULT_AFTER holds faults back for a while after start, so a demo can
	// show a device that worked and then stopped.
	f.from = time.Now()
	if d, err := time.ParseDuration(os.Getenv("FAULT_AFTER")); err == nil {
		f.from = f.from.Add(d)
	}
	for _, d := range strings.Split(os.Getenv("FAULT_DEVICES"), ",") {
		if d = strings.TrimSpace(d); d != "" {
			f.devices[d] = true
		}
	}
	if f.active() {
		log.Printf("faults on: drop=%.3f duplicate=%.3f delay=%s at %.3f, devices=%d (0 means all)",
			f.drop, f.duplicate, f.delay, f.delayRate, len(f.devices))
	}
	return f
}

func (f faults) active() bool {
	return f.drop > 0 || f.duplicate > 0 || (f.delayRate > 0 && f.delay > 0)
}

func (f faults) applies(device string) bool {
	return f.active() && !time.Now().Before(f.from) && (len(f.devices) == 0 || f.devices[device])
}

// decide returns what to do with one message: drop it, send it this many times,
// and after what delay.
func (f faults) decide(device string, payload []byte) (drop bool, copies int, delay time.Duration) {
	copies = 1
	if !f.applies(device) {
		return false, copies, 0
	}
	switch {
	case rand.Float64() < f.drop:
		logFault("drop", device, payload, nil)
		return true, 0, 0
	case rand.Float64() < f.duplicate:
		copies = 2
		logFault("duplicate", device, payload, nil)
	}
	if f.delay > 0 && rand.Float64() < f.delayRate {
		delay = f.delay
		logFault("delay", device, payload, map[string]any{"delay_ms": f.delay.Milliseconds()})
	}
	return false, copies, delay
}

func logFault(kind, device string, payload []byte, extra map[string]any) {
	var body struct {
		Seq int64 `json:"seq"`
	}
	_ = json.Unmarshal(payload, &body)
	line := map[string]any{"fault": kind, "hop": "bridge", "device": device, "seq": body.Seq, "at": time.Now().UTC().Format(time.RFC3339Nano)}
	for k, v := range extra {
		line[k] = v
	}
	b, _ := json.Marshal(line)
	log.Printf("%s", b)
}

func envFloat(key string) float64 {
	v, err := strconv.ParseFloat(os.Getenv(key), 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}
