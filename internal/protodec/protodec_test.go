package protodec

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sensor = `syntax = "proto2";
package plant.readings;
import "google/protobuf/timestamp.proto";
message Reading {
  optional double temp_c = 1;
  optional uint32 state = 2;
  required google.protobuf.Timestamp measured_at = 3;
  repeated Cell cells = 4;
}
message Cell { optional float volts = 1; }
`

func load(t *testing.T, files map[string]string) *Decoder {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	d, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRoundTripAndShape(t *testing.T) {
	d := load(t, map[string]string{"sensor.proto": sensor})
	bin, err := d.Encode("plant.readings.Reading", []byte(`{"temp_c": 21.5, "state": 3, "measured_at": "2026-11-18T10:00:00Z", "cells": [{"volts": 3.3}]}`))
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Decode("plant.readings.Reading", bin)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(r.JSON), `"measured_at":"2026-11-18T10:00:00Z"`) {
		t.Fatalf("json %s", r.JSON)
	}
	if got := r.Shape.String(); got != "cells[].volts:number measured_at:timestamp state:int temp_c:number" {
		t.Fatalf("shape %q", got)
	}
}

func TestReasons(t *testing.T) {
	d := load(t, map[string]string{"sensor.proto": sensor})
	noTime, err := d.Encode("plant.readings.Reading", []byte(`{"temp_c": 1}`)) // a buggy device leaves out the time
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Decode("plant.readings.Reading", noTime); err == nil || !strings.Contains(err.Error(), "required field measured_at not set") {
		t.Fatalf("missing required: %v", err)
	}
	if _, err := d.Decode("plant.readings.Reading", []byte{0xff, 0xff, 0xff}); err == nil || !strings.Contains(err.Error(), "not a valid Reading message") {
		t.Fatalf("garbage: %v", err)
	}
	if _, err := d.Decode("plant.readings.Nope", nil); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("unknown: %v", err)
	}
}

func TestBrokenFileIsSkipped(t *testing.T) {
	d := load(t, map[string]string{"sensor.proto": sensor, "broken.proto": "syntax = \"proto2\"; message {"})
	if _, ok := d.Skipped["broken.proto"]; !ok {
		t.Fatalf("broken file not reported: %+v", d.Skipped)
	}
	if len(d.Types()) != 2 {
		t.Fatalf("types %v", d.Types())
	}
}
