package journey

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
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
//	  - mqtt:plant/+/+/telemetry
//	  - kafka:telemetry.normalized
//
// Route lists the hops a reading should pass, in order, by HopName.
type Config struct {
	Tenant  string   `yaml:"tenant"`
	Sources []Source `yaml:"sources"`
	Route   []string `yaml:"route"`
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
}

// LoadConfig reads and checks a rules file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseConfig(b)
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
		case "mqtt", "kafka":
		default:
			return fmt.Errorf("%s: kind must be mqtt or kafka, not %q", where, s.Kind)
		}
		if s.Match == "" {
			return fmt.Errorf("%s: match is required", where)
		}
		if (s.DeviceFromTopic == "") == (s.DeviceField == "") {
			return fmt.Errorf("%s: set exactly one of device_from_topic and device_field", where)
		}
		if s.DeviceFromTopic != "" {
			if s.Kind != "mqtt" {
				return fmt.Errorf("%s: device_from_topic only applies to mqtt sources", where)
			}
			if strings.Count(s.DeviceFromTopic, "{device}") != 1 {
				return fmt.Errorf("%s: device_from_topic must contain {device} once", where)
			}
		}
		if s.SeqField == "" {
			return fmt.Errorf("%s: seq_field is required", where)
		}
	}
	known := map[string]bool{}
	for _, s := range c.Sources {
		known[s.HopName()] = true
	}
	seen := map[string]bool{}
	for _, h := range c.Route {
		if !known[h] {
			return fmt.Errorf("route: %q is not a source (use kind:match)", h)
		}
		if seen[h] {
			return fmt.Errorf("route: %q appears twice", h)
		}
		seen[h] = true
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
func (c *Config) Rule(kind, topic string) (Rule, bool) {
	s, ok := c.Source(kind, topic)
	if !ok {
		return Rule{}, false
	}
	return Rule{Tenant: c.Tenant, TopicPattern: s.DeviceFromTopic, DeviceField: s.DeviceField, SeqField: s.SeqField}, true
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
