package mapping

import (
	"fmt"
	"math"
	"testing"
)

// pairs builds readings the way the reference normaliser maps them.
func pairs(n int, kelvin float64) []Pair {
	var out []Pair
	for i := 0; i < n; i++ {
		c := math.Round((18+float64(i%40)*0.37)*100) / 100
		kpa := math.Round((100+float64(i%13)*0.21)*100) / 100
		in := Values{"device_id": fmt.Sprintf("dev-%05d", i%7), "site": "site-02", "seq": float64(i), "ts": fmt.Sprintf("t%d", i),
			"temp_c": c, "pressure_kpa": kpa, "vibration_mm_s": 1.5}
		out = append(out, Pair{Label: in["device_id"].(string), In: in, Out: Values{"schema": "telemetry/v2", "device": in["device_id"], "site": "site-02", "seq": float64(i),
			"observed_at": in["ts"], "temperature_k": math.Round((c+kelvin)*100) / 100, "pressure_pa": math.Round(kpa * 1000), "vibration_mm_s": 1.5}})
	}
	return out
}

func TestProposeFindsTheNormaliser(t *testing.T) {
	p := Propose(pairs(60, 273.15), 20, 0.95)
	got := map[string]string{}
	for _, r := range p.Relations {
		got[r.Out] = r.Expr
	}
	want := map[string]string{"device": "device_id", "seq": "seq", "observed_at": "ts", "site": "site",
		"temperature_k": "temp_c + 273.15", "pressure_pa": "pressure_kpa * 1000", "vibration_mm_s": "vibration_mm_s"}
	for f, e := range want {
		if got[f] != e {
			t.Errorf("%s: got %q, want %q", f, got[f], e)
		}
	}
	if len(p.New) != 1 || p.New[0] != "schema" {
		t.Errorf("new: %v", p.New)
	}
	if len(p.Dropped) != 0 {
		t.Errorf("dropped: %v", p.Dropped)
	}
	if few := Propose(pairs(5, 273.15), 20, 0.95); len(few.Relations) != 0 {
		t.Errorf("proposed from 5 pairs: %+v", few.Relations)
	}
}

func TestCheckFindsAWrongConversion(t *testing.T) {
	m := map[string]Expr{}
	for f, s := range map[string]string{"temperature_k": "temp_c + 273.15", "pressure_pa": "pressure_kpa * 1000", "device": "device_id"} {
		e, err := ParseExpr(s)
		if err != nil {
			t.Fatal(err)
		}
		m[f] = e
	}
	for _, p := range pairs(20, 273.15) {
		if mm := Check(m, p); len(mm) != 0 {
			t.Fatalf("good pair flagged: %+v", mm)
		}
	}
	bad := pairs(1, 272.65)[0]
	mm := Check(m, bad)
	if len(mm) != 1 || mm[0].Field != "temperature_k" || !Close(mm[0].Want, bad.In["temp_c"].(float64)+273.15) {
		t.Fatalf("%+v", mm)
	}
}

func TestParseExpr(t *testing.T) {
	for _, s := range []string{"x", "a.b + 1", "t - 273.15", "p * 1000", "p / 1e3"} {
		if _, err := ParseExpr(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	for _, s := range []string{"", "x + y", "x / 0", "x ** 2"} {
		if _, err := ParseExpr(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestPartialRelationNamesTheOddDevice(t *testing.T) {
	ps := pairs(60, 273.15)
	for i := range ps {
		if ps[i].Label == "dev-00003" {
			ps[i].Out["temperature_k"] = ps[i].Out["temperature_k"].(float64) - 0.5
		}
	}
	p := Propose(ps, 20, 0.95)
	for _, r := range p.Relations {
		if r.Out != "temperature_k" {
			continue
		}
		if !r.Partial || r.Expr != "temp_c + 273.15" || len(r.Outliers) != 1 || r.Outliers[0] != "dev-00003" {
			t.Fatalf("%+v", r)
		}
		return
	}
	t.Fatal("no relation for temperature_k")
}
