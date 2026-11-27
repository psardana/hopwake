// Command protosim simulates a fleet of devices that publish Protobuf
// messages over MQTT 5, naming each message's type in a user property.
//
// What the devices send is set by a YAML file: the topic, the property
// names, and per message type how often and a JSON template that is encoded
// with the team's own .proto files. Faults swap a device's template or type
// after a delay, the way a firmware update changes what a device sends.
//
//	protosim -config sim.yaml -protos ./protos
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"gopkg.in/yaml.v3"

	"github.com/psardana/hopwake/internal/protodec"
	"github.com/psardana/hopwake/reference/protosim/cmark"
)

type config struct {
	Broker     string `yaml:"broker"`
	Topic      string `yaml:"topic"` // with {device}
	QoS        byte   `yaml:"qos"`
	Properties struct {
		Type   string `yaml:"type"`   // user property naming the message type
		Device string `yaml:"device"` // user property naming the device, optional
	} `yaml:"properties"`
	Devices struct {
		Count  int    `yaml:"count"`
		Format string `yaml:"format"` // fmt pattern for the device ID, e.g. "S%08d"
		First  int    `yaml:"first"`
	} `yaml:"devices"`
	Messages []message `yaml:"messages"`
	Faults   []fault   `yaml:"faults"`
	Marks    *marks    `yaml:"marks"`
}

// marks says which devices mark their own sends, and where.
type marks struct {
	Topic   string        `yaml:"topic"`   // with {device}
	Hop     string        `yaml:"hop"`     // the device source's match, e.g. ems.sent
	Numbers [2]int        `yaml:"numbers"` // the devices that mark: device numbers from and to, inclusive
	Types   []string      `yaml:"types"`   // the message types they mark
	Every   time.Duration `yaml:"every"`   // how often marks are published
	Queue   int           `yaml:"queue"`   // marks a device can keep
}

type message struct {
	Type     string        `yaml:"type"`
	Every    time.Duration `yaml:"every"`
	Template string        `yaml:"template"` // JSON, a Go text/template
}

// fault changes what some devices send from a moment on.
type fault struct {
	Name     string        `yaml:"name"`
	Devices  []string      `yaml:"devices"`
	After    time.Duration `yaml:"after"`
	Message  string        `yaml:"message"`  // the message type it changes
	Type     string        `yaml:"type"`     // send under this type name instead
	Template string        `yaml:"template"` // send this instead
	Drop     bool          `yaml:"drop"`     // stop sending this message
	Lose     bool          `yaml:"lose"`     // the device sends it, but it never reaches the broker
}

func main() {
	cfgPath := flag.String("config", "sim.yaml", "simulator config")
	protos := flag.String("protos", "protos", "directory of .proto files")
	duration := flag.Duration("duration", 0, "stop after this long (0 runs until interrupted)")
	flag.Parse()

	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	var cfg config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatal(err)
	}
	if b := os.Getenv("SIM_BROKER"); b != "" {
		cfg.Broker = b
	}
	dec, err := protodec.Load(*protos)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	u, err := url.Parse(cfg.Broker)
	if err != nil {
		log.Fatal(err)
	}
	ready := make(chan struct{})
	var once sync.Once
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ReconnectBackoff:              autopaho.NewConstantBackoff(2 * time.Second),
		OnConnectionUp:                func(*autopaho.ConnectionManager, *paho.Connack) { once.Do(func() { close(ready) }) },
		OnConnectError:                func(err error) { log.Printf("mqtt: %v", err) },
		ClientConfig:                  paho.ClientConfig{ClientID: fmt.Sprintf("protosim-%d", os.Getpid())},
	})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-ready:
	case <-ctx.Done():
		return
	}
	start := time.Now()
	for _, f := range cfg.Faults {
		log.Printf(`{"fault":%q,"devices":%q,"after":%q,"message":%q}`, f.Name, strings.Join(f.Devices, ","), f.After, f.Message)
	}

	var sent, failed atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < cfg.Devices.Count; i++ {
		n := cfg.Devices.First + i
		id := fmt.Sprintf(cfg.Devices.Format, n)
		var q *cmark.Queue
		if mk := cfg.Marks; mk != nil && n >= mk.Numbers[0] && n <= mk.Numbers[1] {
			if !cmark.Available {
				log.Fatal("marks: this binary was built without cgo")
			}
			q = cmark.New(max(mk.Queue, 16))
			wg.Add(1)
			go func() {
				defer wg.Done()
				publishMarks(ctx, cm, mk, id, q)
			}()
		}
		for _, m := range cfg.Messages {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d := &deviceState{id: id, counters: map[string]float64{}, marks: q}
				run(ctx, cm, dec, &cfg, d, m, start, &sent, &failed)
			}()
		}
	}
	if mk := cfg.Marks; mk != nil {
		log.Printf("devices %d to %d mark %s on %s", mk.Numbers[0], mk.Numbers[1], strings.Join(mk.Types, ", "), mk.Topic)
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			_ = cm.Disconnect(context.Background())
			log.Printf("stopped: %d sent, %d failed", sent.Load(), failed.Load())
			return
		case <-tick.C:
			log.Printf("%d sent, %d failed", sent.Load(), failed.Load())
		}
	}
}

type deviceState struct {
	id       string
	counters map[string]float64
	now      time.Time    // the time of the message being built: one time per message
	marks    *cmark.Queue // nil when the device does not mark
}

// publishMarks publishes a device's marks on its mark topic, at QoS 1; marks
// stay in the device's queue until a publish succeeds.
func publishMarks(ctx context.Context, cm *autopaho.ConnectionManager, mk *marks, id string, q *cmark.Queue) {
	topic := strings.ReplaceAll(mk.Topic, "{device}", id)
	every := mk.Every
	if every <= 0 {
		every = 2 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		for {
			msg, n := q.Encode(2048)
			if n == 0 {
				break
			}
			if _, err := cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: msg}); err != nil {
				break // the link is down: keep them for next time
			}
			q.Commit(n)
		}
	}
}

// run sends one message type for one device, on its schedule.
func run(ctx context.Context, cm *autopaho.ConnectionManager, dec *protodec.Decoder, cfg *config, d *deviceState, m message, start time.Time, sent, failed *atomic.Uint64) {
	// Spread devices over the interval so they do not publish in bursts.
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(rand.Int64N(int64(m.Every)))):
	}
	tick := time.NewTicker(m.Every)
	defer tick.Stop()
	topic := strings.ReplaceAll(cfg.Topic, "{device}", d.id)
	normal := template.Must(template.New(m.Type).Funcs(funcs(d)).Parse(m.Template))
	for {
		typ, tmpl, drop := m.Type, normal, false
		for _, f := range cfg.Faults {
			if f.Message == m.Type && has(f.Devices, d.id) && time.Since(start) >= f.After {
				if f.Type != "" {
					typ = f.Type
				}
				if f.Template != "" {
					tmpl = template.Must(template.New(f.Name).Funcs(funcs(d)).Parse(f.Template))
				}
				drop = drop || f.Drop
			}
		}
		lose := false
		for _, f := range cfg.Faults {
			if f.Lose && f.Message == m.Type && has(f.Devices, d.id) && time.Since(start) >= f.After {
				lose = true
			}
		}
		if !drop {
			d.now = time.Now().UTC()
			var err error
			if !lose {
				err = publish(ctx, cm, dec, cfg, d, topic, m.Type, typ, tmpl)
			}
			if err != nil {
				failed.Add(1)
			} else {
				sent.Add(1)
				// As far as the device knows, it sent the reading.
				if d.marks != nil && has(cfg.Marks.Types, m.Type) {
					d.marks.MarkTime(cfg.Marks.Hop, false, typ, d.now.UnixMilli(), time.Now().UnixMilli())
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func publish(ctx context.Context, cm *autopaho.ConnectionManager, dec *protodec.Decoder, cfg *config, d *deviceState, topic, encodeAs, typ string, tmpl *template.Template) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return err
	}
	// Encoded with the real type's schema; a fault may still announce it
	// under another type name.
	body, err := dec.Encode(encodeAs, buf.Bytes())
	if err != nil {
		log.Printf("%s %s: %v", d.id, encodeAs, err)
		return err
	}
	props := []paho.UserProperty{{Key: cfg.Properties.Type, Value: typ}}
	if cfg.Properties.Device != "" {
		props = append(props, paho.UserProperty{Key: cfg.Properties.Device, Value: d.id})
	}
	_, err = cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: cfg.QoS, Payload: body, Properties: &paho.PublishProperties{User: props}})
	return err
}

// funcs are what a template can use: now, rand, counter and device.
func funcs(d *deviceState) template.FuncMap {
	return template.FuncMap{
		"now": func() string {
			if d.now.IsZero() {
				return time.Now().UTC().Format(time.RFC3339Nano)
			}
			// Millisecond times, as the device's clock gives them.
			return d.now.Truncate(time.Millisecond).Format(time.RFC3339Nano)
		},
		"device": func() string { return d.id },
		"rand": func(lo, hi float64) float64 {
			return math.Round((lo+rand.Float64()*(hi-lo))*100) / 100
		},
		// counter grows a named meter reading by a random step, like an
		// energy counter that only goes up.
		"counter": func(name string, lo, hi float64) float64 {
			d.counters[name] += lo + rand.Float64()*(hi-lo)
			return math.Round(d.counters[name]*100) / 100
		},
	}
}

func has(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
