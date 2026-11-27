package devmark

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/psardana/hopwake/internal/journey"
)

// TestDeviceMarksFromC checks the C device hook end to end on the tap's side:
// messages the C library writes are read here into the journey IDs a tap
// derives for the readings themselves.
func TestDeviceMarksFromC(t *testing.T) {
	dir, _ := filepath.Abs("../../pkg/hook/c")
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("no make")
	}
	if out, err := exec.Command("make", "-s", "-C", dir, "build/emit").CombinedOutput(); err != nil {
		t.Skipf("cannot build the C hook here: %v\n%s", err, out)
	}
	// A queue of 3 for 4 marks: the oldest is dropped, and the message says so.
	out, err := exec.Command(filepath.Join(dir, "build/emit"), "3",
		"time", "ems.sent", "send", "ems.measurements.Measurement", "1763460000250", "1763460000300",
		"time", "ems.sent", "send", "ems.measurements.Measurement", "1763460000750", "1763460000900",
		"seq", "meter.sent", "store", "18446744073709551615", "7", "0",
		"time", "ems.sent", "send", "", "1763460002000", "0",
	).Output()
	if err != nil {
		t.Fatal(err)
	}
	var msgs []Message
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		m, err := Parse(sc.Bytes())
		if err != nil {
			t.Fatalf("%v: %s", err, sc.Text())
		}
		msgs = append(msgs, m)
	}
	if len(msgs) != 1 || len(msgs[0].Marks) != 3 || msgs[0].Dropped != 1 {
		t.Fatalf("got %s", out)
	}
	recv := time.UnixMilli(1763460003000)
	want := []string{
		journey.TimeID("fleet", "EMS0000042", "ems.measurements.Measurement", "2025-11-18T10:00:00Z"),
		journey.BootID("fleet", "EMS0000042", "7", 18446744073709551615),
		journey.TimeID("fleet", "EMS0000042", "", "2025-11-18T10:00:02Z"),
	}
	for i, m := range msgs[0].Marks {
		e, err := m.Event("fleet", "EMS0000042", "device:"+m.Hop, time.Second, recv)
		if err != nil {
			t.Fatal(err)
		}
		if e.JourneyID != want[i] {
			t.Errorf("mark %d: journey %s, want %s", i, e.JourneyID, want[i])
		}
	}
	_ = os.Remove(filepath.Join(dir, "build/emit"))
}
