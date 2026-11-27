// Package devmark reads device marks: small messages a device publishes on
// its own MQTT topic to say it sent or kept a reading. The device has no
// journey IDs to compute; the tap derives them, exactly as for the reading
// itself.
//
// A message holds one or more marks, so a device that was offline can send
// what it kept in one go:
//
//	{"v":1,"marks":[{"hop":"ems.sent","op":"send","type":"ems.measurements.Measurement","time":1763460000750,"at":1763460000900}],"dropped":0}
//
// time is the reading's own time and at when the device did it, both in
// Unix milliseconds; a reading with a sequence number gives seq (and boot)
// instead of time. dropped counts marks the device had to throw away since
// its last message.
package devmark

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

// Mark is one mark in a message.
type Mark struct {
	Hop  string      `json:"hop"` // the device source's match, e.g. "ems.sent"
	Op   string      `json:"op"`  // "send" or "store"
	Type string      `json:"type,omitempty"`
	Time *int64      `json:"time,omitempty"` // the reading's own time, Unix ms
	Seq  json.Number `json:"seq,omitempty"`
	Boot string      `json:"boot,omitempty"`
	At   int64       `json:"at,omitempty"` // when the device did it, Unix ms
}

// Message is one device mark message.
type Message struct {
	V       int    `json:"v"`
	Marks   []Mark `json:"marks"`
	Dropped uint64 `json:"dropped,omitempty"`
}

// ErrBadMark means a mark cannot be read.
var ErrBadMark = errors.New("devmark: bad mark")

// Parse reads one message.
func Parse(payload []byte) (Message, error) {
	var m Message
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrBadMark, err)
	}
	if m.V != 1 {
		return Message{}, fmt.Errorf("%w: version %d", ErrBadMark, m.V)
	}
	return m, nil
}

// Op returns the mark's op.
func (k Mark) op() (hop.Op, error) {
	switch k.Op {
	case "send":
		return hop.OpSend, nil
	case "store":
		return hop.OpStore, nil
	}
	return "", fmt.Errorf("%w: op %q", ErrBadMark, k.Op)
}

// Event turns a mark from device into a hop event at hopName. received is
// when the tap got the message; the device's own clock is used for the
// event's time only when it is plausible.
func (k Mark) Event(tenant, device, hopName string, precision time.Duration, received time.Time) (hop.Event, error) {
	op, err := k.op()
	if err != nil {
		return hop.Event{}, err
	}
	key := journey.Key{Tenant: tenant, Device: device, Type: k.Type}
	switch {
	case k.Seq != "":
		n, err := strconv.ParseUint(k.Seq.String(), 10, 64)
		if err != nil {
			return hop.Event{}, fmt.Errorf("%w: seq %q", ErrBadMark, k.Seq)
		}
		key.Seq, key.Boot, key.Type = n, k.Boot, ""
	case k.Time != nil:
		key.Time = journey.FormatTime(time.UnixMilli(*k.Time), precision)
	default:
		return hop.Event{}, fmt.Errorf("%w: neither seq nor time", ErrBadMark)
	}
	at := received
	if k.At != 0 {
		if t := time.UnixMilli(k.At); !t.Before(received.Add(-24*time.Hour)) && !t.After(received.Add(5*time.Second)) {
			at = t
		}
	}
	return hop.Event{
		JourneyID:   key.ID(),
		Tenant:      tenant,
		Device:      device,
		Seq:         key.Seq,
		Hop:         hopName,
		System:      "device",
		Op:          op,
		Correlation: hop.Derived,
		At:          at,
		MessageType: key.Type,
	}, nil
}
