// Package mapping works out how a service maps fields from one hop to the
// next, from the same readings seen at both, and checks values against a
// mapping a person confirmed.
//
// A mapping names, per output field, an input field and an optional
// constant step: "device_id", "temp_c + 273.15", "pressure_kpa * 1000".
package mapping

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Values are a payload's leaf values by dotted path.
type Values map[string]any

// Flatten parses a JSON object into its leaf values.
func Flatten(payload []byte) (Values, error) {
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return nil, err
	}
	out := Values{}
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		if m, ok := v.(map[string]any); ok && len(m) > 0 {
			for k, x := range m {
				p := k
				if prefix != "" {
					p = prefix + "." + k
				}
				walk(p, x)
			}
			return
		}
		out[prefix] = v
	}
	walk("", doc)
	return out, nil
}

// Pair is one reading seen before and after a service. Label says whose
// reading it is, e.g. the device, so readings that do not fit can be named.
type Pair struct {
	In, Out Values
	Label   string
}

// Expr is one confirmed output field: In, then Op and K if a constant step.
type Expr struct {
	In string
	Op byte // 0 for a plain copy, or one of + - * /
	K  float64
}

var exprRe = regexp.MustCompile(`^\s*([A-Za-z0-9_.\[\]-]+)\s*(?:([-+*/])\s*(-?[0-9]+(?:\.[0-9]+)?(?:e-?[0-9]+)?))?\s*$`)

// ParseExpr reads one mapping expression.
func ParseExpr(s string) (Expr, error) {
	m := exprRe.FindStringSubmatch(s)
	if m == nil {
		return Expr{}, fmt.Errorf("mapping %q: want a field, optionally followed by + - * or / and a number", s)
	}
	e := Expr{In: m[1]}
	if m[2] != "" {
		e.Op = m[2][0]
		e.K, _ = strconv.ParseFloat(m[3], 64)
		if e.Op == '/' && e.K == 0 {
			return Expr{}, fmt.Errorf("mapping %q divides by zero", s)
		}
	}
	return e, nil
}

func (e Expr) String() string {
	if e.Op == 0 {
		return e.In
	}
	return fmt.Sprintf("%s %c %s", e.In, e.Op, strconv.FormatFloat(e.K, 'f', -1, 64))
}

// Apply computes the expected output from an input value.
func (e Expr) Apply(in any) (any, bool) {
	if e.Op == 0 {
		return in, true
	}
	x, ok := number(in)
	if !ok {
		return nil, false
	}
	switch e.Op {
	case '+':
		return x + e.K, true
	case '-':
		return x - e.K, true
	case '*':
		return x * e.K, true
	default:
		return x / e.K, true
	}
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// Close reports whether got matches want, allowing for rounding to two
// decimal places.
func Close(want, got any) bool {
	w, ok1 := number(want)
	g, ok2 := number(got)
	if ok1 && ok2 {
		return math.Abs(w-g) <= math.Max(0.006, 1e-6*math.Abs(w))
	}
	return fmt.Sprint(want) == fmt.Sprint(got)
}

// Mismatch is one output value that differs from what the mapping expects.
type Mismatch struct {
	Field string `json:"field"`
	Expr  string `json:"expr"`
	In    any    `json:"in"`
	Want  any    `json:"want"`
	Got   any    `json:"got"`
}

// Check compares one pair with a confirmed mapping (output field → expression).
// Fields missing on either side are left to shapes and schemas.
func Check(m map[string]Expr, p Pair) []Mismatch {
	var out []Mismatch
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		e := m[f]
		in, ok := p.In[e.In]
		got, ok2 := p.Out[f]
		if !ok || !ok2 {
			continue
		}
		want, ok := e.Apply(in)
		if !ok || Close(want, got) {
			continue
		}
		out = append(out, Mismatch{Field: f, Expr: e.String(), In: in, Want: round(want), Got: got})
	}
	return out
}

func round(v any) any {
	if x, ok := v.(float64); ok {
		return math.Round(x*1e6) / 1e6
	}
	return v
}

// Relation is one proposed output field and how it follows from the input.
type Relation struct {
	Out   string  `json:"out"`
	Expr  string  `json:"expr"`
	Kind  string  `json:"kind"`  // equal, offset or scale
	Agree float64 `json:"agree"` // share of pairs that fit
	// Partial is set when the relation holds for most pairs but not enough
	// to propose it; Outliers names whose pairs do not fit (up to ten).
	Partial  bool     `json:"partial,omitempty"`
	Outliers []string `json:"outliers,omitempty"`
}

// Proposal is what Propose found.
type Proposal struct {
	Pairs     int        `json:"pairs"`
	Relations []Relation `json:"relations"`
	Dropped   []string   `json:"dropped"` // input fields no output follows from
	New       []string   `json:"new"`     // output fields that follow from no input
}

// Propose looks, across pairs, for an input field that each output field
// follows from: equal values, a constant difference or a constant ratio,
// holding for at least share of the pairs (0.95 is a sensible bar). Too few
// pairs give no relations.
func Propose(pairs []Pair, minPairs int, share float64) Proposal {
	p := Proposal{Pairs: len(pairs), Relations: []Relation{}, Dropped: []string{}, New: []string{}}
	if len(pairs) == 0 {
		return p
	}
	ins, outs := keys(pairs, true), keys(pairs, false)
	used := map[string]bool{}
	for _, o := range outs {
		var best *Relation
		bestIn := ""
		if len(pairs) >= minPairs {
			for _, in := range ins {
				r, ok := relate(pairs, in, o, share)
				if !ok {
					continue
				}
				// Prefer equal over offset over scale, then the same name, then a better fit.
				if best == nil || rank(r, in, o) > rank(*best, bestIn, o) {
					rr := r
					best, bestIn = &rr, in
				}
			}
		}
		if best == nil {
			p.New = append(p.New, o)
			continue
		}
		used[bestIn] = true
		p.Relations = append(p.Relations, *best)
	}
	for _, in := range ins {
		if !used[in] {
			p.Dropped = append(p.Dropped, in)
		}
	}
	return p
}

func rank(r Relation, in, out string) float64 {
	k := map[string]float64{"equal": 3, "offset": 2, "scale": 1}[r.Kind] * 10
	if !r.Partial {
		k += 100
	}
	if in == out {
		k += 5
	}
	return k + r.Agree
}

// relate tests whether out follows from in across the pairs. A relation
// that fits at least half the pairs but less than share comes back Partial.
func relate(pairs []Pair, in, out string, share float64) (Relation, bool) {
	type obs struct {
		a, b  any
		x, y  float64
		num   bool
		label string
	}
	var os []obs
	for _, p := range pairs {
		a, ok1 := p.In[in]
		b, ok2 := p.Out[out]
		if !ok1 || !ok2 {
			continue
		}
		o := obs{a: a, b: b, label: p.Label}
		x, okx := number(a)
		y, oky := number(b)
		o.x, o.y, o.num = x, y, okx && oky
		os = append(os, o)
	}
	n := len(os)
	if n == 0 || n < len(pairs)/2 {
		return Relation{}, false
	}
	type candidate struct {
		kind, expr string
		fits       func(o obs) bool
	}
	var cands []candidate
	cands = append(cands, candidate{"equal", in, func(o obs) bool { return fmt.Sprint(o.a) == fmt.Sprint(o.b) }})
	if varies(pairs, in) {
		var diffs, ratios []float64
		for _, o := range os {
			if o.num {
				diffs = append(diffs, o.y-o.x)
				if o.x != 0 {
					ratios = append(ratios, o.y/o.x)
				}
			}
		}
		if len(diffs) > 0 {
			if k := nice(median(diffs)); k != 0 {
				op, kk := '+', k
				if kk < 0 {
					op, kk = '-', -kk
				}
				cands = append(cands, candidate{"offset", fmt.Sprintf("%s %c %s", in, op, strconv.FormatFloat(kk, 'f', -1, 64)),
					func(o obs) bool { return o.num && math.Abs(o.y-o.x-k) <= 0.011 }})
			}
		}
		if len(ratios) > 0 {
			if k := nice(median(ratios)); k != 1 && k != 0 {
				cands = append(cands, candidate{"scale", fmt.Sprintf("%s * %s", in, strconv.FormatFloat(k, 'f', -1, 64)),
					func(o obs) bool { return o.num && math.Abs(o.y-o.x*k) <= math.Max(0.006, 1e-4*math.Abs(o.x*k)) }})
			}
		}
	}
	var best *Relation
	for _, c := range cands {
		fit := 0
		outliers := map[string]bool{}
		for _, o := range os {
			if c.fits(o) {
				fit++
			} else if o.label != "" {
				outliers[o.label] = true
			}
		}
		agree := float64(fit) / float64(n)
		if agree < 0.5 {
			continue
		}
		r := Relation{Out: out, Expr: c.expr, Kind: c.kind, Agree: agree, Partial: agree < share}
		if r.Partial {
			for l := range outliers {
				r.Outliers = append(r.Outliers, l)
			}
			sort.Strings(r.Outliers)
			if len(r.Outliers) > 10 {
				r.Outliers = r.Outliers[:10]
			}
		}
		if best == nil || (!r.Partial && best.Partial) || (r.Partial == best.Partial && r.Agree > best.Agree+1e-9) {
			rr := r
			best = &rr
		}
	}
	if best == nil {
		return Relation{}, false
	}
	return *best, true
}

// varies reports whether an input field takes more than one value, so that
// a constant difference means something.
func varies(pairs []Pair, in string) bool {
	first := ""
	for i, p := range pairs {
		s := fmt.Sprint(p.In[in])
		if i == 0 {
			first = s
		} else if s != first {
			return true
		}
	}
	return false
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

// nice rounds a constant to the fewest decimals that keep it equal within
// floating-point noise, e.g. 273.15000000000003 to 273.15.
func nice(k float64) float64 {
	for d := 0; d <= 6; d++ {
		p := math.Pow(10, float64(d))
		if r := math.Round(k*p) / p; math.Abs(r-k) <= 1e-6*math.Max(1, math.Abs(k)) {
			return r
		}
	}
	return k
}

func keys(pairs []Pair, in bool) []string {
	seen := map[string]bool{}
	for _, p := range pairs {
		v := p.Out
		if in {
			v = p.In
		}
		for k := range v {
			seen[k] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// YAML renders relations as the mapping block of a route step.
func YAML(rels []Relation) string {
	var b strings.Builder
	b.WriteString("mapping:\n")
	for _, r := range rels {
		if r.Partial {
			fmt.Fprintf(&b, "  # %s: %s  (holds for %.0f%% of readings; not for %s)\n", r.Out, r.Expr, r.Agree*100, strings.Join(r.Outliers, ", "))
			continue
		}
		fmt.Fprintf(&b, "  %s: %s\n", r.Out, r.Expr)
	}
	return b.String()
}
