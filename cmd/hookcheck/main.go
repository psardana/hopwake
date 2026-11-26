// Command hookcheck checks a hook, in any language, on the wire.
//
// It stands in for Hopwake: it accepts OTLP/HTTP spans (protobuf or JSON)
// and serves the alias list. It runs the hook's conformance program with the
// environment a hook reads, waits for it to finish, and then compares what
// arrived with what the conformance script in docs/hooks.md must produce:
//
//	hookcheck -- java -jar pkg/hook/java/conformance/target/conformance.jar
//
// It exits non-zero when anything is missing, extra or different.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/psardana/hopwake/internal/journey"
)

// span is what hookcheck keeps of one received span.
type span struct {
	TraceID string
	Name    string
	Kind    int
	Start   uint64
	End     uint64
	Service string
	Attrs   map[string]string // every value as text
}

// expected is one mark the conformance script must produce.
type expected struct {
	what  string
	attrs map[string]string
}

const (
	tenant  = "conformance"
	hopName = "hook:conformance.check"
	msgType = "ems.measurements.Measurement"
)

// script returns the marks the conformance script produces, in order.
func script() []expected {
	at := time.Date(2026, 11, 18, 10, 0, 0, 750_000_000, time.UTC)
	t := journey.FormatTime(at, time.Second)
	timeID := journey.TimeID(tenant, "EMS0000042", msgType, t)
	common := func(id, device, op, seq string) map[string]string {
		return map[string]string{
			"hopwake.schema":      "hop/v1",
			"hopwake.tenant":      tenant,
			"hopwake.device.id":   device,
			"hopwake.seq":         seq,
			"hopwake.journey.id":  id,
			"hopwake.hop":         hopName,
			"hopwake.op":          op,
			"hopwake.correlation": "derived",
			"messaging.system":    "hook",
		}
	}
	with := func(m map[string]string, kv ...string) map[string]string {
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	return []expected{
		{"1 mark store dev-00042 seq 1683", common(journey.ID(tenant, "dev-00042", 1683), "dev-00042", "store", "1683")},
		{"2 mark store dev-00042 boot 7 seq 12", common(journey.BootID(tenant, "dev-00042", "7", 12), "dev-00042", "store", "12")},
		{"3 mark serve dev-00042 seq 1683 (sent once, though marked twice)", common(journey.ID(tenant, "dev-00042", 1683), "dev-00042", "serve", "1683")},
		{"5 mark store EMS0000042 by type and time", with(common(timeID, "EMS0000042", "store", "0"), "hopwake.message.type", msgType)},
		{"6 teach systemId 100042 = EMS0000042", with(common(timeID, "EMS0000042", "store", "0"), "hopwake.message.type", msgType, "hopwake.device.alias", "systemId:100042")},
		{"7 mark serve by alias systemId 100042", with(common(timeID, "EMS0000042", "serve", "0"), "hopwake.message.type", msgType, "hopwake.device.alias", "systemId:100042")},
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to accept spans and serve aliases on")
	wait := flag.Duration("wait", 4*time.Second, "how long to wait for spans after the program exits")
	timeout := flag.Duration("timeout", 90*time.Second, "how long the program may run")
	service := flag.String("service", "", "the service.name the program must use; empty accepts any")
	flag.Parse()
	if flag.NArg() == 0 {
		log.Fatal("usage: hookcheck [flags] -- command [args...]")
	}

	var mu sync.Mutex
	var got []span
	var decodeErrs []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		spans, err := decode(r.Header.Get("Content-Type"), body)
		mu.Lock()
		if err != nil {
			decodeErrs = append(decodeErrs, err.Error())
		}
		got = append(got, spans...)
		mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		if strings.Contains(r.Header.Get("Content-Type"), "json") {
			_, _ = w.Write([]byte("{}"))
		}
	})
	mux.HandleFunc("GET /api/aliases", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"kind":"systemId","value":"100042","device":"EMS0000042"}]`))
	})
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	base := "http://" + ln.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	args := flag.Args()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(),
		"HOPWAKE_OTLP_ENDPOINT="+base,
		"HOPWAKE_API="+base,
		"HOPWAKE_TIME_PRECISION=1s",
		"HOPWAKE_TENANT="+tenant,
		"HOPWAKE_HOP="+hopName,
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	started := time.Now()
	runErr := cmd.Run()
	// The program may exit before its last batch is on the wire.
	deadline := time.Now().Add(*wait)
	want := script()
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= len(want) {
			time.Sleep(500 * time.Millisecond) // a late duplicate would still show
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("hookcheck: %s\n", strings.Join(args, " "))
	if runErr != nil {
		fmt.Printf("  the program failed: %v\n%s", runErr, indent(out.String()))
	}
	fails := 0
	if runErr != nil {
		fails++
	}
	for _, e := range decodeErrs {
		fmt.Printf("  FAIL could not read a request: %s\n", e)
		fails++
	}
	used := make([]bool, len(got))
	for _, w := range want {
		best, why := -1, "no span with this journey ID, hop and op"
		for i, s := range got {
			if used[i] || s.Attrs["hopwake.journey.id"] != w.attrs["hopwake.journey.id"] || s.Attrs["hopwake.op"] != w.attrs["hopwake.op"] ||
				s.Attrs["hopwake.device.alias"] != w.attrs["hopwake.device.alias"] {
				continue
			}
			best, why = i, check(s, w, started, *service)
			break
		}
		if best >= 0 {
			used[best] = true
		}
		if why == "" {
			fmt.Printf("  pass %s\n", w.what)
		} else {
			fmt.Printf("  FAIL %s: %s\n", w.what, why)
			fails++
		}
	}
	for i, s := range got {
		if !used[i] {
			fmt.Printf("  FAIL extra span: op %s, device %s, journey %s, alias %q\n", s.Attrs["hopwake.op"], s.Attrs["hopwake.device.id"], s.Attrs["hopwake.journey.id"], s.Attrs["hopwake.device.alias"])
			fails++
		}
	}
	if fails > 0 {
		fmt.Printf("  %d problem(s)\n", fails)
		if runErr == nil && out.Len() > 0 {
			fmt.Print(indent(out.String()))
		}
		os.Exit(1)
	}
	fmt.Printf("  all %d marks as the contract says\n", len(want))
}

// check compares one span with the mark it should be.
func check(s span, w expected, started time.Time, service string) string {
	var problems []string
	if s.TraceID != w.attrs["hopwake.journey.id"] {
		problems = append(problems, fmt.Sprintf("trace ID %s is not the journey ID", s.TraceID))
	}
	if s.Name != "hop "+hopName {
		problems = append(problems, fmt.Sprintf("span name %q, want %q", s.Name, "hop "+hopName))
	}
	if s.Kind != 5 {
		problems = append(problems, fmt.Sprintf("span kind %d, want 5 (consumer)", s.Kind))
	}
	if s.Start != s.End {
		problems = append(problems, "start and end differ")
	}
	now := uint64(time.Now().UnixNano())
	if s.Start < uint64(started.Add(-time.Minute).UnixNano()) || s.Start > now+uint64(time.Minute) {
		problems = append(problems, "the span's time is not when it was marked")
	}
	if s.Service == "" || (service != "" && s.Service != service) {
		problems = append(problems, fmt.Sprintf("service.name %q", s.Service))
	}
	keys := make([]string, 0, len(w.attrs))
	for k := range w.attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if g, ok := s.Attrs[k]; !ok {
			problems = append(problems, "no "+k)
		} else if g != w.attrs[k] {
			problems = append(problems, fmt.Sprintf("%s = %q, want %q", k, g, w.attrs[k]))
		}
	}
	for _, k := range []string{"hopwake.message.type", "hopwake.device.alias"} {
		if _, want := w.attrs[k]; !want && s.Attrs[k] != "" {
			problems = append(problems, k+" set where it should not be")
		}
	}
	return strings.Join(problems, "; ")
}

func indent(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("    | " + l + "\n")
	}
	return b.String()
}

// decode reads an OTLP/HTTP export request, protobuf or JSON.
func decode(contentType string, body []byte) ([]span, error) {
	if strings.Contains(contentType, "json") {
		return decodeJSON(body)
	}
	var req coltrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("protobuf: %w", err)
	}
	var out []span
	for _, rs := range req.ResourceSpans {
		svc := ""
		for _, a := range rs.GetResource().GetAttributes() {
			if a.Key == "service.name" {
				svc = a.GetValue().GetStringValue()
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				s := span{TraceID: hex.EncodeToString(sp.TraceId), Name: sp.Name, Kind: int(sp.Kind),
					Start: sp.StartTimeUnixNano, End: sp.EndTimeUnixNano, Service: svc, Attrs: map[string]string{}}
				for _, a := range sp.Attributes {
					s.Attrs[a.Key] = valueText(a.Value)
				}
				out = append(out, s)
			}
		}
	}
	return out, nil
}

func valueText(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	}
	return ""
}

// OTLP/JSON: trace IDs in hex, 64-bit integers as strings or numbers.
type jsonValue struct {
	StringValue *string         `json:"stringValue"`
	IntValue    json.RawMessage `json:"intValue"`
	BoolValue   *bool           `json:"boolValue"`
	DoubleValue *float64        `json:"doubleValue"`
}

type jsonAttr struct {
	Key   string    `json:"key"`
	Value jsonValue `json:"value"`
}

type jsonRequest struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []jsonAttr `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []struct {
				TraceID    string          `json:"traceId"`
				SpanID     string          `json:"spanId"`
				Name       string          `json:"name"`
				Kind       int             `json:"kind"`
				Start      json.RawMessage `json:"startTimeUnixNano"`
				End        json.RawMessage `json:"endTimeUnixNano"`
				Attributes []jsonAttr      `json:"attributes"`
			} `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

func decodeJSON(body []byte) ([]span, error) {
	var req jsonRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}
	var out []span
	for _, rs := range req.ResourceSpans {
		svc := ""
		for _, a := range rs.Resource.Attributes {
			if a.Key == "service.name" && a.Value.StringValue != nil {
				svc = *a.Value.StringValue
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				if len(sp.TraceID) != 32 || strings.ToLower(sp.TraceID) != sp.TraceID {
					return out, fmt.Errorf("json: trace ID %q is not 32 lowercase hex characters", sp.TraceID)
				}
				if len(sp.SpanID) != 16 {
					return out, fmt.Errorf("json: span ID %q is not 16 hex characters", sp.SpanID)
				}
				start, err1 := jsonUint(sp.Start)
				end, err2 := jsonUint(sp.End)
				if err1 != nil || err2 != nil {
					return out, fmt.Errorf("json: span times must be integers")
				}
				s := span{TraceID: sp.TraceID, Name: sp.Name, Kind: sp.Kind, Start: start, End: end, Service: svc, Attrs: map[string]string{}}
				for _, a := range sp.Attributes {
					v := a.Value
					switch {
					case v.StringValue != nil:
						s.Attrs[a.Key] = *v.StringValue
					case v.IntValue != nil:
						n, err := jsonUint(v.IntValue)
						if err != nil {
							return out, fmt.Errorf("json: %s: %v", a.Key, err)
						}
						s.Attrs[a.Key] = strconv.FormatUint(n, 10)
					case v.BoolValue != nil:
						s.Attrs[a.Key] = strconv.FormatBool(*v.BoolValue)
					case v.DoubleValue != nil:
						s.Attrs[a.Key] = strconv.FormatFloat(*v.DoubleValue, 'g', -1, 64)
					}
				}
				out = append(out, s)
			}
		}
	}
	return out, nil
}

// jsonUint reads a 64-bit integer written as a JSON string or number.
func jsonUint(raw json.RawMessage) (uint64, error) {
	s := strings.Trim(string(raw), `"`)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		if i, err2 := strconv.ParseInt(s, 10, 64); err2 == nil {
			return uint64(i), nil
		}
		return 0, fmt.Errorf("%q is not an integer", s)
	}
	return n, nil
}
