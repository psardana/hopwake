package devmark

import (
	"testing"
	"time"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

func TestTimeAndSeqMarks(t *testing.T) {
	recv := time.UnixMilli(1763460001000)
	msg, err := Parse([]byte(`{"v":1,"marks":[
		{"hop":"ems.sent","op":"send","type":"ems.measurements.Measurement","time":1763460000750,"at":1763460000900},
		{"hop":"ems.sent","op":"store","seq":18446744073709551615,"boot":"7","at":1},
		{"hop":"ems.sent","op":"send"},
		{"hop":"ems.sent","op":"jump","seq":1}],"dropped":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Dropped != 3 || len(msg.Marks) != 4 {
		t.Fatalf("%+v", msg)
	}
	e, err := msg.Marks[0].Event("fleet", "EMS0000042", "device:ems.sent", time.Second, recv)
	if err != nil {
		t.Fatal(err)
	}
	want := journey.TimeID("fleet", "EMS0000042", "ems.measurements.Measurement", "2025-11-18T10:00:00Z")
	if e.JourneyID != want || e.Op != hop.OpSend || !e.At.Equal(time.UnixMilli(1763460000900)) || e.MessageType != "ems.measurements.Measurement" {
		t.Fatalf("%+v, want journey %s", e, want)
	}
	e, err = msg.Marks[1].Event("fleet", "EMS0000042", "device:ems.sent", time.Second, recv)
	if err != nil {
		t.Fatal(err)
	}
	if e.JourneyID != journey.BootID("fleet", "EMS0000042", "7", 18446744073709551615) || e.Op != hop.OpStore || !e.At.Equal(recv) {
		t.Fatalf("%+v: an implausible device clock must give way to the time received", e)
	}
	if _, err := msg.Marks[2].Event("fleet", "d", "h", 0, recv); err == nil {
		t.Fatal("a mark with neither seq nor time was accepted")
	}
	if _, err := msg.Marks[3].Event("fleet", "d", "h", 0, recv); err == nil {
		t.Fatal("an unknown op was accepted")
	}
	if _, err := Parse([]byte(`{"v":2,"marks":[]}`)); err == nil {
		t.Fatal("version 2 was accepted")
	}
}
