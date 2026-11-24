package main

import "testing"

func TestParseWindow(t *testing.T) {
	w, err := parseWindow(`{"v":1,"op":"median","key":"site-02","start":"2026-11-18T10:00:00Z","end":"2026-11-18T10:01:00Z","input_hop":"kafka:telemetry.normalized","inputs":3,"device_count":2,"devices":["a","b"]}`)
	if err != nil || w.Key != "site-02" || w.Inputs != 3 || len(w.Devices) != 2 || w.End.Minute() != 1 {
		t.Fatalf("%+v %v", w, err)
	}
	for _, bad := range []string{"", "{}", `{"v":2,"key":"k","end":"2026-11-18T10:01:00Z","input_hop":"h"}`, "nope"} {
		if _, err := parseWindow(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
