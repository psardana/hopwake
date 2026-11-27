//go:build cgo

package cmark

import (
	"testing"
	"time"

	"github.com/psardana/hopwake/internal/devmark"
)

func TestQueueThroughTheTapReader(t *testing.T) {
	q := New(2)
	q.MarkTime("ems.sent", false, "ems.measurements.Measurement", 1763460000750, 1763460000900)
	q.MarkTime("ems.sent", true, "ems.measurements.Measurement", 1763460005750, 0)
	if dropped := q.MarkTime("ems.sent", false, "ems.measurements.Measurement", 1763460010750, 0); !dropped {
		t.Fatal("a full queue must drop its oldest mark")
	}
	msg, n := q.Encode(1024)
	if n != 2 {
		t.Fatalf("%d marks in %s", n, msg)
	}
	m, err := devmark.Parse(msg)
	if err != nil {
		t.Fatal(err)
	}
	if m.Dropped != 1 || m.Marks[0].Op != "store" {
		t.Fatalf("%s", msg)
	}
	e, err := m.Marks[1].Event("fleet", "EMS0000042", "device:ems.sent", time.Second, time.UnixMilli(1763460011000))
	if err != nil || e.MessageType != "ems.measurements.Measurement" {
		t.Fatalf("%+v %v", e, err)
	}
	q.Commit(n)
	if q.Len() != 0 {
		t.Fatal("not committed")
	}
}
