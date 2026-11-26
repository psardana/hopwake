package journey

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/psardana/hopwake/internal/mapping"
)

// Config says, per source, where a hop finds the device and the sequence
// number of a message. It is read from YAML:
//
//	tenant: reference
//	sources:
//	  - name: raw readings
//	    kind: mqtt
//	    match: plant/+/+/telemetry
//	    device_from_topic: plant/+/{device}/telemetry
//	    seq_field: seq
//	  - name: normalised readings
//	    kind: kafka
//	    match: telemetry.normalized
//	    device_field: device
//	    seq_field: seq
//	route:
//	  - hop: mqtt:plant/+/+/telemetry
//	  - hop: kafka:telemetry.normalized
//	    via: normaliser
//
// Route lists the hops a reading should pass, in order, by HopName. A step
// may name the service that carries readings to it from the step before
// (via), so a stop between two hops can be reported as a stop at that
// service. A step may also be written as just the hop name.
type Config struct {
	Tenant  string   `yaml:"tenant"`
	Sources []Source `yaml:"sources"`
	Route   []Step   `yaml:"route"`
	// Routes are routes chosen by message type, for sources that name each
	// message's type (see Source.Proto). A message follows every route one of
	// whose type prefixes matches its type; Route, if set, is the route for
	// messages no typed route covers.
	Routes []TypedRoute `yaml:"routes"`
	// TimePrecision rounds a reading's own time down before it becomes part
	// of a journey ID, so hops that keep the time less precisely (a store
	// that keeps whole seconds) still agree. Zero keeps it as given.
	TimePrecision time.Duration `yaml:"time_precision"`
	// Spans maps spans from services that already trace onto hops, when they
	// carry the device and sequence number as attributes.
	Spans []SpanRule `yaml:"spans"`
	// Redact lists dotted field paths whose values a tap replaces before it
	// sends the payload of a watched device anywhere.
	Redact []string `yaml:"redact"`

	Dir string `yaml:"-"` // the rules file's directory; schema paths are relative to it
}

// SpanRule says which spans from an instrumented service stand for a hop.
type SpanRule struct {
	Name       string `yaml:"name"`
	Service    string `yaml:"service"`     // resource service.name
	Span       string `yaml:"span"`        // span name; empty matches any span of the service
	DeviceAttr string `yaml:"device_attr"` // attribute holding the device
	SeqAttr    string `yaml:"seq_attr"`    // attribute holding the sequence number
	BootAttr   string `yaml:"boot_attr"`   // optional, like boot_field on sources
	Hop        string `yaml:"hop"`         // the hop name: "span:" and a name
}

// SpanRule returns the rule for a span, if any matches.
func (c *Config) SpanRule(service, span string) (SpanRule, bool) {
	for _, r := range c.Spans {
		if r.Service == service && (r.Span == "" || r.Span == span) {
			return r, true
		}
	}
	return SpanRule{}, false
}

// TypedRoute is a route for messages whose type starts with one of Types,
// compared by dotted components ("ems.measurements" matches
// "ems.measurements.BasicMeasurement" but not "ems.measurementsX").
type TypedRoute struct {
	Name  string   `yaml:"name"`
	Types []string `yaml:"types"` // empty: every type (the untyped route)
	Steps []Step   `yaml:"steps"`
}

// Hops returns the route's hop names in order.
func (r TypedRoute) Hops() []string {
	out := make([]string, len(r.Steps))
	for i, s := range r.Steps {
		out[i] = s.Hop
	}
	return out
}

// RequiredHops returns the hops every reading on the route should reach.
func (r TypedRoute) RequiredHops() []string {
	var out []string
	for _, s := range r.Steps {
		if !s.Optional {
			out = append(out, s.Hop)
		}
	}
	return out
}

// Optional returns the route's optional hops.
func (r TypedRoute) Optional() []string {
	out := []string{}
	for _, s := range r.Steps {
		if s.Optional {
			out = append(out, s.Hop)
		}
	}
	return out
}

// Covers reports whether the route is for messages of type t.
func (r TypedRoute) Covers(t string) bool {
	if len(r.Types) == 0 {
		return true
	}
	for _, p := range r.Types {
		if t == p || strings.HasPrefix(t, p+".") {
			return true
		}
	}
	return false
}

// AllRoutes returns every route: the typed ones, then the untyped one if set.
func (c *Config) AllRoutes() []TypedRoute {
	out := append([]TypedRoute(nil), c.Routes...)
	if len(c.Route) > 0 {
		out = append(out, TypedRoute{Name: "all", Steps: c.Route})
	}
	return out
}

// RoutesFor returns the routes a message of type t follows: every typed route
// that covers it, or else the untyped route. An empty type (a source that
// names no type) follows the untyped route, or every typed route if there is
// none.
func (c *Config) RoutesFor(t string) []TypedRoute {
	var out []TypedRoute
	if t != "" {
		for _, r := range c.Routes {
			if len(r.Types) > 0 && r.Covers(t) {
				out = append(out, r)
			}
		}
	}
	if len(out) == 0 && len(c.Route) > 0 {
		return []TypedRoute{{Name: "all", Steps: c.Route}}
	}
	if len(out) == 0 && t == "" {
		return c.Routes
	}
	return out
}

// primary is the route the single-route views use: the untyped route, or the
// first typed one.
func (c *Config) primary() []Step {
	if len(c.Route) > 0 {
		return c.Route
	}
	if len(c.Routes) > 0 {
		return c.Routes[0].Steps
	}
	return nil
}

// allSteps lists the steps of every route, for lookups by hop.
func (c *Config) allSteps() []Step {
	var out []Step
	for _, r := range c.AllRoutes() {
		out = append(out, r.Steps...)
	}
	return out
}

// Step is one hop of the route.
type Step struct {
	Hop string `yaml:"hop"`
	Via string `yaml:"via"` // the service between the previous hop and this one; optional
	// Mapping is how the service maps fields from the previous hop, as a
	// person confirmed it: output field → "input", "input + 273.15", ...
	Mapping map[string]string `yaml:"mapping"`
	// Optional steps happen only on demand, such as serving a reading to
	// someone who asks for it. They are shown when they happen but never
	// make a verdict; they can only come at the end of the route.
	Optional bool `yaml:"optional"`
}

// RequiredHops returns the hops every reading should reach, in order.
func (c *Config) RequiredHops() []string {
	var out []string
	for _, s := range c.primary() {
		if !s.Optional {
			out = append(out, s.Hop)
		}
	}
	return out
}

// Mapping returns the confirmed mapping into hop, parsed; nil if none.
func (c *Config) Mapping(hop string) map[string]mapping.Expr {
	for _, s := range c.allSteps() {
		if s.Hop != hop || len(s.Mapping) == 0 {
			continue
		}
		out := map[string]mapping.Expr{}
		for f, x := range s.Mapping {
			e, err := mapping.ParseExpr(x)
			if err == nil {
				out[f] = e
			}
		}
		return out
	}
	return nil
}

// UnmarshalYAML accepts a step as a plain hop name or as a mapping.
func (s *Step) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		s.Hop = n.Value
		return nil
	}
	type plain Step
	var p plain
	if err := n.Decode(&p); err != nil {
		return err
	}
	*s = Step(p)
	return nil
}

// Hops returns the route's hop names in order.
func (c *Config) Hops() []string {
	steps := c.primary()
	out := make([]string, len(steps))
	for i, s := range steps {
		out[i] = s.Hop
	}
	return out
}

// Via returns the service that carries readings to hop, if the route names one.
func (c *Config) Via(hop string) string {
	for _, s := range c.allSteps() {
		if s.Hop == hop && s.Via != "" {
			return s.Via
		}
	}
	return ""
}

// HopName is how hop events name the place a source is read: kind, a colon,
// then the match, e.g. "kafka:telemetry.raw".
func (s Source) HopName() string { return s.Kind + ":" + s.Match }

// Source is one kind of message a tap reads.
type Source struct {
	Name            string `yaml:"name"`
	Kind            string `yaml:"kind"`  // "mqtt" or "kafka"
	Match           string `yaml:"match"` // MQTT topic filter, or Kafka topic name
	DeviceFromTopic string `yaml:"device_from_topic"`
	DeviceField     string `yaml:"device_field"`
	SeqField        string `yaml:"seq_field"`
	BootField       string `yaml:"boot_field"` // optional; every source of a route must agree on it

	// A dead-letter source holds readings a service rejected. Rejects names
	// the route hop those readings never reached, and ReasonHeader the Kafka
	// header that says why.
	Role         string `yaml:"role"` // empty, "dead-letter" or "aggregate"
	Rejects      string `yaml:"rejects"`
	ReasonHeader string `yaml:"reason_header"`

	// Schema is a JSON Schema file the source's payloads are declared to
	// follow, relative to the rules file. ValidateEvery checks one reading
	// in N (default 1, every reading).
	Schema        string `yaml:"schema"`
	ValidateEvery int    `yaml:"validate_every"`

	// An aggregate source holds outputs that stand for many readings. Each
	// output carries a window record in the WindowHeader Kafka header.
	WindowHeader string `yaml:"window_header"`

	// Format is the payload format: empty or "json", or "protobuf" with Proto.
	Format string     `yaml:"format"`
	Proto  *ProtoSpec `yaml:"proto"`
	// Where the device, sequence number and boot counter are when they are
	// not in a JSON payload: a Kafka header or MQTT 5 user property, or the
	// Kafka record key.
	DeviceHeader  string `yaml:"device_header"`
	DeviceFromKey bool   `yaml:"device_from_key"`
	SeqFrom       string `yaml:"seq_from"`
	BootFrom      string `yaml:"boot_from"`
	// TimeField is a field holding the message's own timestamp, for sources
	// with no sequence number: the journey ID is then derived from the
	// device, the message type and that time.
	TimeField string `yaml:"time_field"`
	// DeviceAlias says the device found above is an alias of this kind, for
	// example "systemId": Hopwake resolves it to the device from the pairs it
	// learned upstream.
	DeviceAlias string `yaml:"device_alias"`
	// AsType is the message type to count readings as, where the source
	// names none (or a different one) but carries readings that started as
	// that type.
	AsType string `yaml:"as_type"`
}

// ProtoSpec says how to decode a Protobuf source.
type ProtoSpec struct {
	Path     string `yaml:"path"`      // directory of .proto files, relative to the rules file
	TypeFrom string `yaml:"type_from"` // header or MQTT 5 user property naming each message's full type
	Type     string `yaml:"type"`      // or one fixed type for every message
}

// ProtoPath returns the source's .proto directory, resolved against the rules
// file's directory.
func (c *Config) ProtoPath(s Source) string {
	if s.Proto == nil || s.Proto.Path == "" || filepath.IsAbs(s.Proto.Path) || c.Dir == "" {
		if s.Proto == nil {
			return ""
		}
		return s.Proto.Path
	}
	return filepath.Join(c.Dir, s.Proto.Path)
}

// Aggregate reports whether the source holds aggregated outputs.
func (s Source) Aggregate() bool { return s.Role == "aggregate" }

// SchemaPath returns the source's schema file, resolved against the rules
// file's directory, or "" when it declares none.
func (c *Config) SchemaPath(s Source) string {
	if s.Schema == "" || filepath.IsAbs(s.Schema) || c.Dir == "" {
		return s.Schema
	}
	return filepath.Join(c.Dir, s.Schema)
}

// DeadLetter reports whether the source holds rejected readings.
func (s Source) DeadLetter() bool { return s.Role == "dead-letter" }

// DeadLetters returns the dead-letter sources.
func (c *Config) DeadLetters() []Source {
	var out []Source
	for _, s := range c.Sources {
		if s.DeadLetter() {
			out = append(out, s)
		}
	}
	return out
}

// LoadConfig reads and checks a rules file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := ParseConfig(b)
	if err != nil {
		return nil, err
	}
	c.Dir = filepath.Dir(path)
	return c, nil
}

// ParseConfig parses and checks rules from YAML.
func ParseConfig(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("journey rules: %w", err)
	}
	if err := c.check(); err != nil {
		return nil, fmt.Errorf("journey rules: %w", err)
	}
	return &c, nil
}

func (c *Config) check() error {
	if c.Tenant == "" {
		return errors.New("tenant is required")
	}
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	for i, s := range c.Sources {
		where := fmt.Sprintf("source %d (%s)", i+1, s.Name)
		switch s.Kind {
		case "mqtt", "kafka", "hook":
		default:
			return fmt.Errorf("%s: kind must be mqtt, kafka or hook, not %q", where, s.Kind)
		}
		if s.Match == "" {
			return fmt.Errorf("%s: match is required", where)
		}
		if s.Kind == "hook" {
			// A service in the pipeline marks readings itself, with the device
			// and sequence number it already knows.
			if s.DeviceFromTopic != "" || s.DeviceField != "" || s.SeqField != "" || s.Role != "" || s.Schema != "" {
				return fmt.Errorf("%s: a hook source takes only a name and a match", where)
			}
			continue
		}
		if s.Aggregate() {
			// An aggregate stands for many readings: no device or sequence.
			if s.Kind != "kafka" || s.WindowHeader == "" {
				return fmt.Errorf("%s: an aggregate source must be kafka with a window_header", where)
			}
			if s.DeviceFromTopic != "" || s.DeviceField != "" || s.SeqField != "" || s.Schema != "" {
				return fmt.Errorf("%s: an aggregate source takes no device, seq or schema fields", where)
			}
			continue
		}
		if s.WindowHeader != "" {
			return fmt.Errorf("%s: window_header needs role: aggregate", where)
		}
		devices := 0
		for _, set := range []bool{s.DeviceFromTopic != "", s.DeviceField != "", s.DeviceHeader != "", s.DeviceFromKey} {
			if set {
				devices++
			}
		}
		if devices != 1 {
			return fmt.Errorf("%s: set exactly one of device_from_topic, device_field, device_header and device_from_key", where)
		}
		if s.DeviceFromKey && s.Kind != "kafka" {
			return fmt.Errorf("%s: device_from_key only applies to kafka sources", where)
		}
		switch s.Format {
		case "", "json":
			if s.Proto != nil {
				return fmt.Errorf("%s: proto needs format: protobuf", where)
			}
		case "protobuf":
			if s.Proto == nil || s.Proto.Path == "" || (s.Proto.TypeFrom == "") == (s.Proto.Type == "") {
				return fmt.Errorf("%s: format protobuf needs proto.path and exactly one of proto.type_from and proto.type", where)
			}
		default:
			return fmt.Errorf("%s: format must be json or protobuf, not %q", where, s.Format)
		}
		if s.DeviceFromTopic != "" {
			if s.Kind != "mqtt" {
				return fmt.Errorf("%s: device_from_topic only applies to mqtt sources", where)
			}
			if strings.Count(s.DeviceFromTopic, "{device}") != 1 {
				return fmt.Errorf("%s: device_from_topic must contain {device} once", where)
			}
		}
		seqs := 0
		for _, set := range []bool{s.SeqField != "", s.SeqFrom != "", s.TimeField != ""} {
			if set {
				seqs++
			}
		}
		if seqs != 1 {
			return fmt.Errorf("%s: set exactly one of seq_field, seq_from and time_field", where)
		}
		if s.BootField != "" && s.BootFrom != "" {
			return fmt.Errorf("%s: set at most one of boot_field and boot_from", where)
		}
		if s.ValidateEvery < 0 {
			return fmt.Errorf("%s: validate_every must be 1 or more", where)
		}
		if s.ValidateEvery > 0 && s.Schema == "" {
			return fmt.Errorf("%s: validate_every needs a schema", where)
		}
		switch s.Role {
		case "":
			if s.Rejects != "" || s.ReasonHeader != "" {
				return fmt.Errorf("%s: rejects and reason_header need role: dead-letter", where)
			}
		case "dead-letter":
			if s.Kind != "kafka" && s.ReasonHeader != "" {
				return fmt.Errorf("%s: reason_header only applies to kafka sources", where)
			}
		default:
			return fmt.Errorf("%s: role must be empty, dead-letter or aggregate, not %q", where, s.Role)
		}
	}
	for i, r := range c.Spans {
		where := fmt.Sprintf("span rule %d (%s)", i+1, r.Name)
		if r.Service == "" || r.DeviceAttr == "" || r.SeqAttr == "" {
			return fmt.Errorf("%s: service, device_attr and seq_attr are required", where)
		}
		if !strings.HasPrefix(r.Hop, "span:") || len(r.Hop) == len("span:") {
			return fmt.Errorf("%s: hop must be \"span:\" and a name, not %q", where, r.Hop)
		}
	}
	known := map[string]bool{}
	for _, s := range c.Sources {
		known[s.HopName()] = true
	}
	for _, r := range c.Spans {
		known[r.Hop] = true
	}
	seen := map[string]bool{}
	names := map[string]bool{}
	for _, r := range c.AllRoutes() {
		where := "route"
		if r.Name != "all" || len(r.Types) > 0 {
			where = fmt.Sprintf("route %q", r.Name)
			if r.Name == "" || names[r.Name] {
				return fmt.Errorf("routes: each typed route needs a unique name")
			}
			names[r.Name] = true
			if len(r.Types) == 0 {
				return fmt.Errorf("%s: types is required (route: is the route for every type)", where)
			}
		}
		if err := checkSteps(where, r.Steps, known); err != nil {
			return err
		}
		for _, st := range r.Steps {
			seen[st.Hop] = true
		}
	}
	for _, s := range c.Sources {
		if s.Aggregate() && seen[s.HopName()] {
			return fmt.Errorf("route: %q is an aggregate source and cannot be a hop of the route", s.HopName())
		}
	}
	for _, s := range c.DeadLetters() {
		if seen[s.HopName()] {
			return fmt.Errorf("route: %q is a dead-letter source and cannot be a hop of the route", s.HopName())
		}
		i := -1
		for _, r := range c.AllRoutes() {
			for k, st := range r.Steps {
				if st.Hop == s.Rejects && k > i {
					i = k
				}
			}
		}
		if i < 1 {
			return fmt.Errorf("source %s: rejects must name a route hop after the first, not %q", s.Name, s.Rejects)
		}
	}
	return nil
}

// Source returns the source a message on a topic belongs to, if any.
func (c *Config) Source(kind, topic string) (Source, bool) {
	for _, s := range c.Sources {
		if s.Kind != kind {
			continue
		}
		if (kind == "mqtt" && topicMatches(s.Match, topic)) || (kind == "kafka" && s.Match == topic) {
			return s, true
		}
	}
	return Source{}, false
}

// Rule returns the extraction rule for a message on a topic, if a source
// of that kind matches it. The first matching source wins.
// checkSteps checks one route's steps.
func checkSteps(where string, steps []Step, known map[string]bool) error {
	if len(steps) == 0 {
		return fmt.Errorf("%s: no steps", where)
	}
	seen := map[string]bool{}
	for i, st := range steps {
		h := st.Hop
		if i == 0 && (st.Via != "" || len(st.Mapping) > 0) {
			return fmt.Errorf("%s: the first hop %q has nothing before it, so it cannot have via or mapping", where, h)
		}
		if st.Optional && i == 0 {
			return fmt.Errorf("%s: the first hop %q cannot be optional", where, h)
		}
		if !st.Optional && i > 0 && steps[i-1].Optional {
			return fmt.Errorf("%s: %q follows an optional hop; optional hops come last", where, h)
		}
		for f, x := range st.Mapping {
			if _, err := mapping.ParseExpr(x); err != nil {
				return fmt.Errorf("%s: %s: %s: %w", where, h, f, err)
			}
		}
		if !known[h] {
			return fmt.Errorf("%s: %q is not a source (use kind:match)", where, h)
		}
		if seen[h] {
			return fmt.Errorf("%s: %q appears twice", where, h)
		}
		seen[h] = true
	}
	return nil
}

func (c *Config) Rule(kind, topic string) (Rule, bool) {
	s, ok := c.Source(kind, topic)
	if !ok {
		return Rule{}, false
	}
	return Rule{Tenant: c.Tenant, TopicPattern: s.DeviceFromTopic, DeviceField: s.DeviceField, SeqField: s.SeqField, BootField: s.BootField,
		DeviceHeader: s.DeviceHeader, DeviceFromKey: s.DeviceFromKey, SeqFrom: s.SeqFrom, BootFrom: s.BootFrom, TimeField: s.TimeField,
		TimePrecision: c.TimePrecision}, true
}

// topicMatches applies MQTT filter rules: + matches one level, # the rest.
func topicMatches(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i, part := range f {
		if part == "#" {
			return true
		}
		if i >= len(t) || (part != "+" && part != t[i]) {
			return false
		}
	}
	return len(f) == len(t)
}
