package shape

import (
	"fmt"
	"strings"
	"testing"
)

func TestShapeOfAReading(t *testing.T) {
	s := Of([]byte(`{"device_id":"dev-00043","seq":81,"temp_c":21.5,"ok":true,"note":null,"location":{"lat":52.1,"lon":4.3}}`))
	want := "device_id:string location.lat:number location.lon:number note:null ok:bool seq:number temp_c:number"
	if s.String() != want {
		t.Fatalf("got  %s\nwant %s", s, want)
	}
	if len(s.Fingerprint) != 16 {
		t.Fatalf("fingerprint %q", s.Fingerprint)
	}
}

func TestValuesAndOrderDoNotMatter(t *testing.T) {
	a := Of([]byte(`{"seq":1,"temp_c":21,"device_id":"a"}`))
	b := Of([]byte(`{"device_id":"b","temp_c":21.75,"seq":99999}`))
	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("%s vs %s", a, b)
	}
}

func TestARenamedFieldIsANewShape(t *testing.T) {
	before := Of([]byte(`{"device_id":"dev-00043","seq":80,"temp_c":21.5}`))
	after := Of([]byte(`{"device_id":"dev-00043","seq":81,"temperature":21.5}`))
	if before.Fingerprint == after.Fingerprint {
		t.Fatal("a rename must change the fingerprint")
	}
	c := Diff(before, after)
	if len(c.Added) != 1 || c.Added[0].Path != "temperature" || len(c.Removed) != 1 || c.Removed[0].Path != "temp_c" || len(c.Retyped) != 0 {
		t.Fatalf("%+v", c)
	}
}

func TestRetypedField(t *testing.T) {
	c := Diff(Of([]byte(`{"p":101.3}`)), Of([]byte(`{"p":"101.3"}`)))
	if len(c.Retyped) != 1 || c.Retyped[0].Type != "string" || c.Empty() {
		t.Fatalf("%+v", c)
	}
	if !Diff(Of([]byte(`{"p":1}`)), Of([]byte(`{"p":2}`))).Empty() {
		t.Fatal("same shape must give an empty change")
	}
}

func TestArraysUseTheirFirstElement(t *testing.T) {
	a := Of([]byte(`{"readings":[{"v":1,"t":"x"},{"v":2,"t":"y"}]}`))
	b := Of([]byte(`{"readings":[{"v":5,"t":"z"}]}`))
	if a.String() != "readings[].t:string readings[].v:number" || a.Fingerprint != b.Fingerprint {
		t.Fatalf("%s / %s", a, b)
	}
	if Of([]byte(`{"readings":[]}`)).String() != "readings:array" {
		t.Fatal("empty array")
	}
}

func TestLargeMapsCollapse(t *testing.T) {
	var parts []string
	for i := 0; i < MaxKeys+1; i++ {
		parts = append(parts, fmt.Sprintf(`"sensor-%d":%d`, i, i))
	}
	a := Of([]byte(`{"seq":1,"sensors":{` + strings.Join(parts, ",") + `}}`))
	if a.String() != "sensors:map seq:number" {
		t.Fatalf("got %s", a)
	}
}

func TestNonJSONIsBinary(t *testing.T) {
	for _, p := range []string{"\x01\x02\x03", `{"a":1} {"b":2}`, `{"a":`} {
		if Of([]byte(p)).String() != "$:binary" {
			t.Errorf("%q: got %s", p, Of([]byte(p)))
		}
	}
	if Of([]byte(`42`)).String() != "$:number" {
		t.Error("a bare number is a JSON value")
	}
}

func BenchmarkOfReferenceReading(b *testing.B) {
	p := []byte(`{"device_id":"dev-00038","site":"site-02","seq":1,"ts":"2026-11-18T06:38:18.434825Z","temp_c":24.16,"pressure_kpa":101.47,"vibration_mm_s":0.999}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Of(p)
	}
}

func TestParseRoundTrip(t *testing.T) {
	s := Of([]byte(`{"device_id":"d","seq":1,"loc":{"lat":1}}`))
	if p := Parse(s.String()); p.Fingerprint != s.Fingerprint || p.String() != s.String() {
		t.Fatalf("%s vs %s", p, s)
	}
}
