package capture

import "testing"

func TestPrepare(t *testing.T) {
	got, ok := Prepare([]byte(`{"device_id":"d","owner":{"email":"a@b.c","site":"x"},"readings":[{"gps":"1,2"},{"gps":"3,4"}],"seq":1}`),
		[]string{"owner.email", "readings.gps", "not.there"})
	want := `{"device_id":"d","owner":{"email":"‹redacted›","site":"x"},"readings":[{"gps":"‹redacted›"},{"gps":"‹redacted›"}],"seq":1}`
	if !ok || got != want {
		t.Fatalf("got %s", got)
	}
	if _, ok := Prepare([]byte{0x01, 0x02}, nil); ok {
		t.Fatal("binary payload captured")
	}
}
