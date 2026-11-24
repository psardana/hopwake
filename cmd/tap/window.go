package main

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/psardana/hopwake/internal/hop"
)

// parseWindow reads the window record an aggregating job puts in a header:
//
//	{"v":1,"op":"median","key":"site-02","start":"...","end":"...",
//	 "input_hop":"kafka:telemetry.normalized","inputs":312,
//	 "device_count":12,"devices":["dev-00002", ...]}
//
// with "device_digest" in place of "devices" above 256 devices.
func parseWindow(raw string) (hop.Window, error) {
	if raw == "" {
		return hop.Window{}, errors.New("no window record")
	}
	var x struct {
		V           int       `json:"v"`
		Op          string    `json:"op"`
		Key         string    `json:"key"`
		Start       time.Time `json:"start"`
		End         time.Time `json:"end"`
		InputHop    string    `json:"input_hop"`
		Inputs      int       `json:"inputs"`
		DeviceCount int       `json:"device_count"`
		Devices     []string  `json:"devices"`
		Digest      string    `json:"device_digest"`
	}
	if err := json.Unmarshal([]byte(raw), &x); err != nil {
		return hop.Window{}, err
	}
	if x.V != 1 || x.Key == "" || x.End.IsZero() || x.InputHop == "" {
		return hop.Window{}, errors.New("window record lacks version 1, key, end or input hop")
	}
	return hop.Window{Op: x.Op, Key: x.Key, Start: x.Start, End: x.End, InputHop: x.InputHop, Inputs: x.Inputs,
		Devices: x.Devices, Digest: x.Digest, DeviceCount: x.DeviceCount}, nil
}
