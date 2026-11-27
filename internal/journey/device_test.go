package journey

import (
	"strings"
	"testing"
)

const deviceRules = `
tenant: fleet
time_precision: 1s
sources:
  - name: device marks
    kind: device
    match: ems.sent
    topic: fleet/+/from/hopwake
    device_from_topic: fleet/{device}/from/hopwake
  - name: readings
    kind: mqtt
    match: fleet/+/from/ems/#
    device_from_topic: fleet/{device}/from/ems
    seq_field: seq
  - name: stored
    kind: hook
    match: lvc.stored
  - name: served
    kind: hook
    match: api.served
route:
  - hop: device:ems.sent
    optional: true
  - hop: mqtt:fleet/+/from/ems/#
    via: device link
  - hop: hook:lvc.stored
  - hop: hook:api.served
    optional: true
`

func TestDeviceSourcesAndLeadingOptionalHops(t *testing.T) {
	c, err := ParseConfig([]byte(deviceRules))
	if err != nil {
		t.Fatal(err)
	}
	srcs, dev := c.DeviceSources("fleet/EMS0000042/from/hopwake")
	if len(srcs) != 1 || dev != "EMS0000042" || srcs[0].HopName() != "device:ems.sent" {
		t.Fatalf("got %v %q", srcs, dev)
	}
	if srcs, _ := c.DeviceSources("fleet/EMS0000042/from/ems"); len(srcs) != 0 {
		t.Fatal("a reading topic is not a mark topic")
	}
	rt := c.AllRoutes()[0]
	none := rt.JudgedHops(func(string) bool { return false })
	if strings.Join(none, " ") != "mqtt:fleet/+/from/ems/# hook:lvc.stored" {
		t.Fatalf("a device without marks: %v", none)
	}
	marks := rt.JudgedHops(func(h string) bool { return h == "device:ems.sent" })
	if strings.Join(marks, " ") != "device:ems.sent mqtt:fleet/+/from/ems/# hook:lvc.stored" {
		t.Fatalf("a device with marks: %v", marks)
	}
}

func TestRouteNeedsARequiredHopBetweenOptionalOnes(t *testing.T) {
	for _, route := range []string{
		"route:\n  - hop: device:ems.sent\n    optional: true\n",
		"route:\n  - hop: device:ems.sent\n  - hop: hook:lvc.stored\n    optional: true\n  - hop: hook:api.served\n",
	} {
		head := deviceRules[:strings.Index(deviceRules, "route:")]
		if _, err := ParseConfig([]byte(head + route)); err == nil {
			t.Fatalf("accepted %q", route)
		}
	}
}

func TestDeviceSourceChecks(t *testing.T) {
	bad := `
tenant: t
sources:
  - name: marks
    kind: device
    match: ems.sent
    topic: fleet/+/from/hopwake
    device_from_topic: other/{device}
route: [device:ems.sent]
`
	if _, err := ParseConfig([]byte(bad)); err == nil || !strings.Contains(err.Error(), "does not fit") {
		t.Fatalf("got %v", err)
	}
}
