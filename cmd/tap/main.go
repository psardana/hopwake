// Command tap watches MQTT topics and Kafka topics from the outside and emits
// a hop event for every reading it sees.
//
// It never sits in the data path: it subscribes to MQTT like any other client
// and reads Kafka in its own consumer group. Each message's journey ID is
// derived from the device and sequence number found by the rules file, and
// the hop event goes out over OTLP.
//
// Every few seconds it also reports its own health per hop: whether it was
// connected, how many messages it read, how many it had to drop and how far
// behind it was. The journey service uses this so that a reading the tap
// missed is not reported as a reading the pipeline lost.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/psardana/hopwake/internal/alias"
	"github.com/psardana/hopwake/internal/capture"
	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
	"github.com/psardana/hopwake/internal/protodec"
	"github.com/psardana/hopwake/internal/schema"
	"github.com/psardana/hopwake/internal/shape"
)

// health keeps per-hop counts for one reporting interval.
type health struct {
	mu   sync.Mutex
	hops map[string]*hopHealth
}

type hopHealth struct {
	seen, unparsed, missed int
	maxDelay               time.Duration
	totalSeen, totalMissed int
}

func (h *health) get(hopName string) *hopHealth {
	x, ok := h.hops[hopName]
	if !ok {
		x = &hopHealth{}
		h.hops[hopName] = x
	}
	return x
}

func (h *health) saw(hopName string, delay time.Duration, parsed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	x := h.get(hopName)
	if !parsed {
		x.unparsed++
		return
	}
	x.seen++
	x.totalSeen++
	if delay > x.maxDelay {
		x.maxDelay = delay
	}
}

func (h *health) miss(hopName string) {
	h.mu.Lock()
	x := h.get(hopName)
	x.missed++
	x.totalMissed++
	h.mu.Unlock()
}

// take returns this interval's counts and starts a new interval.
func (h *health) take(hopName string) hopHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	x := h.get(hopName)
	out := *x
	x.seen, x.unparsed, x.missed, x.maxDelay = 0, 0, 0, 0
	return out
}

// message is one MQTT message waiting for the tap.
type message struct {
	topic   string
	payload []byte
	props   paho.UserProperties // MQTT 5 user properties
	at      time.Time
}

func main() {
	rulesPath := flag.String("rules", env("TAP_RULES", "reference/journey-rules.yaml"), "journey ID rules")
	broker := flag.String("mqtt", env("TAP_MQTT", ""), "MQTT broker URL; empty skips MQTT sources")
	brokers := flag.String("kafka", env("TAP_KAFKA", ""), "Kafka bootstrap servers; empty skips Kafka sources")
	group := flag.String("group", env("TAP_GROUP", "hopwake-tap"), "Kafka consumer group of the tap")
	every := flag.Duration("health-every", 5*time.Second, "how often the tap reports its own health per hop")
	queue := flag.Int("mqtt-queue", 20000, "MQTT messages the tap may hold before it drops and counts them")
	sample := flag.Uint64("sample", envUint("TAP_SAMPLE", 1), "send hop events for one reading in N (counters always cover all); watched devices, rejections and schema breaks are always sent")
	countsEvery := flag.Duration("counts-every", 5*time.Second, "how often the tap sends per-device counters")
	control := flag.String("control", env("TAP_CONTROL", ""), "journey service URL to read the watch list from; empty captures no payloads")
	flag.Parse()

	rules, err := journey.LoadConfig(*rulesPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Hop events go to OTEL_EXPORTER_OTLP_ENDPOINT (default http://localhost:4318).
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Fatalf("otlp: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(65536), sdktrace.WithMaxExportBatchSize(4096)),
		sdktrace.WithIDGenerator(hop.IDGenerator()),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("hopwake-tap"))),
	)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
	}()
	rec := hop.NewRecorder(tp)
	hl := &health{hops: map[string]*hopHealth{}}
	host, _ := os.Hostname()
	tapID := fmt.Sprintf("%s/%d", host, os.Getpid())

	// Declared schemas, per hop, and a counter for checking one reading in N.
	validators := map[string]*schema.Validator{}
	checked := map[string]*atomic.Uint64{}
	for _, s := range rules.Sources {
		if p := rules.SchemaPath(s); p != "" && !s.DeadLetter() {
			v, err := schema.Load(p)
			if err != nil {
				log.Fatal(err)
			}
			validators[s.HopName()] = v
			checked[s.HopName()] = &atomic.Uint64{}
			log.Printf("%s: checking against %s, one reading in %d", s.HopName(), p, max(s.ValidateEvery, 1))
		}
	}

	watched := &watchList{}
	var aliases *alias.Resolver
	if *control != "" {
		go watched.follow(ctx, strings.TrimRight(*control, "/")+"/api/watches", 5*time.Second)
		aliases = alias.NewResolver(ctx, *control, 5*time.Second)
	}

	ctr := &counters{hops: map[string]map[string]hop.Count{}, from: time.Now()}

	var announced sync.Map // hop + fingerprint → struct{}: shapes whose fields were already sent
	// header returns a message header by name; nil for MQTT.
	// .proto directories, compiled once per directory.
	decoders := map[string]*protodec.Decoder{}
	for _, s := range rules.Sources {
		p := rules.ProtoPath(s)
		if s.Format != "protobuf" || decoders[p] != nil {
			continue
		}
		d, err := protodec.Load(p)
		if err != nil {
			log.Fatal(err)
		}
		for f, why := range d.Skipped {
			log.Printf("%s: skipped %s: %s", p, f, why)
		}
		decoders[p] = d
		log.Printf("%s: %d message types", p, len(d.Types()))
	}

	// header returns a Kafka header or MQTT 5 user property by name; key is
	// the Kafka record key.
	observe := func(kind, topic string, payload []byte, header func(string) string, recordKey []byte, delay time.Duration) {
		src, ok := rules.Source(kind, topic)
		if !ok {
			return
		}
		if src.Aggregate() {
			var raw string
			if header != nil {
				raw = header(src.WindowHeader)
			}
			w, err := parseWindow(raw)
			if err != nil {
				hl.saw(src.HopName(), delay, false)
				return
			}
			w.Tenant, w.Hop, w.Output, w.At = rules.Tenant, src.HopName(), string(payload), time.Now()
			// An aggregate's record time is its window's event time, not when it
			// was written, so it says nothing about how far behind the tap is.
			hl.saw(src.HopName(), 0, rec.RecordWindow(ctx, w) == nil)
			return
		}
		// Protobuf payloads are decoded to JSON first, so everything after
		// this works the same for both formats.
		body, msgType, decodeErr := payload, "", ""
		var sh shape.Shape
		if src.Format == "protobuf" {
			msgType = src.Proto.Type
			if src.Proto.TypeFrom != "" && header != nil {
				msgType = header(src.Proto.TypeFrom)
			}
			if msgType == "" {
				decodeErr = "no message type in " + src.Proto.TypeFrom
			} else if res, err := decoders[rules.ProtoPath(src)].Decode(msgType, payload); err != nil {
				decodeErr = err.Error()
			} else {
				body, sh = res.JSON, res.Shape
			}
		} else {
			sh = shape.Of(payload)
		}
		rule, _ := rules.Rule(kind, topic)
		if src.AsType != "" {
			msgType = src.AsType
		}
		meta := journey.Meta{Header: header, Key: recordKey, Type: msgType}
		key, err := rule.ExtractMeta(topic, body, meta)
		if decodeErr != "" {
			// A message that does not decode cannot be numbered from its
			// content. Its journey ID comes from the device and the bytes, so
			// the same bad message is one journey at every hop.
			dev, derr := rule.Device(topic, meta)
			if derr != nil {
				hl.saw(src.HopName(), delay, false)
				return
			}
			sum := sha256.Sum256(payload)
			key, err = journey.Key{Tenant: rules.Tenant, Device: dev, Type: msgType, Time: "undecodable:" + hex.EncodeToString(sum[:8])}, nil
			sh = shape.New([]shape.Field{{Path: "$", Type: "binary"}})
		}
		if err != nil {
			hl.saw(src.HopName(), delay, false)
			return
		}
		// Downstream of an identity change the device goes by another name;
		// resolve it to the device the upstream hops know.
		aliasOf := ""
		if src.DeviceAlias != "" {
			dev, ok := "", false
			if aliases != nil {
				dev, ok = aliases.Device(src.DeviceAlias, key.Device)
			}
			if !ok {
				hl.saw(src.HopName(), delay, false)
				return
			}
			aliasOf = alias.Format(src.DeviceAlias, key.Device)
			key.Device = dev
		}
		fields := ""
		if _, done := announced.LoadOrStore(src.HopName()+" "+sh.Fingerprint, struct{}{}); !done {
			fields = sh.String()
		}
		op, reason := hop.OpReceive, ""
		if decodeErr != "" {
			op, reason = hop.OpError, decodeErr
		} else if v := validators[src.HopName()]; v != nil && checked[src.HopName()].Add(1)%uint64(max(src.ValidateEvery, 1)) == 0 {
			if err := v.Check(body); err != nil {
				op, reason = hop.OpError, err.Error()
			}
		}
		if src.DeadLetter() {
			op, reason = hop.OpDrop, "rejected"
			if header != nil && src.ReasonHeader != "" {
				if r := strings.TrimSpace(header(src.ReasonHeader)); r != "" {
					reason = r
				}
			}
		}
		ctr.add(src.HopName(), key.Device, op != hop.OpReceive)
		payloadCopy := ""
		isWatched := watched.has(key.Device, time.Now())
		if isWatched {
			payloadCopy, _ = capture.Prepare(body, rules.Redact)
		}
		if op == hop.OpReceive && !isWatched && !sampled(key.ID(), *sample) {
			hl.saw(src.HopName(), delay, true)
			return
		}
		err = rec.Record(ctx, hop.Event{
			JourneyID:   key.ID(),
			Tenant:      key.Tenant,
			Device:      key.Device,
			Seq:         key.Seq,
			Hop:         src.HopName(),
			System:      kind,
			Destination: topic,
			Op:          op,
			Reason:      reason,
			Correlation: hop.Derived,
			Bytes:       len(payload),
			At:          time.Now(),
			ShapeFP:     sh.Fingerprint,
			ShapeFields: fields,
			Payload:     payloadCopy,
			MessageType: msgType,
			Alias:       aliasOf,
		})
		hl.saw(src.HopName(), delay, err == nil)
	}

	var mqttHops, kafkaHops []string
	var mqttFilters, kafkaTopics []string
	for _, s := range rules.Sources {
		switch s.Kind {
		case "mqtt":
			mqttFilters = append(mqttFilters, s.Match)
			mqttHops = append(mqttHops, s.HopName())
		case "kafka":
			kafkaTopics = append(kafkaTopics, s.Match)
			kafkaHops = append(kafkaHops, s.HopName())
		}
	}

	var mqttUp, kafkaUp atomic.Bool
	mqttDown := map[string]bool{} // hops whose connection dropped during the interval
	var downMu sync.Mutex
	markDown := func(hops []string) {
		downMu.Lock()
		for _, h := range hops {
			mqttDown[h] = true
		}
		downMu.Unlock()
	}

	if *broker != "" && len(mqttFilters) > 0 {
		// MQTT delivers whether or not the tap keeps up. Messages wait in a
		// bounded queue; when it is full the tap drops them and says so.
		inbox := make(chan message, *queue)
		go func() {
			for m := range inbox {
				props := m.props
				observe("mqtt", m.topic, m.payload, func(name string) string { return props.Get(name) }, nil, time.Since(m.at))
			}
		}()
		u, err := url.Parse(*broker)
		if err != nil {
			log.Fatalf("mqtt: %v", err)
		}
		subs := make([]paho.SubscribeOptions, 0, len(mqttFilters))
		for _, f := range mqttFilters {
			subs = append(subs, paho.SubscribeOptions{Topic: f, QoS: 0}) // QoS 0: the tap must never slow the broker down
		}
		// MQTT 5, so the tap sees user properties such as a message type.
		cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
			ServerUrls:                    []*url.URL{u},
			KeepAlive:                     30,
			CleanStartOnInitialConnection: true,
			SessionExpiryInterval:         0,
			ReconnectBackoff:              autopaho.NewConstantBackoff(2 * time.Second),
			OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
				go func() {
					if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs}); err != nil {
						log.Printf("mqtt subscribe: %v", err)
						return
					}
					mqttUp.Store(true)
					log.Printf("mqtt: watching %s", strings.Join(mqttFilters, ", "))
				}()
			},
			OnConnectionDown: func() bool {
				mqttUp.Store(false)
				markDown(mqttHops)
				return true
			},
			OnConnectError: func(err error) { log.Printf("mqtt: %v", err) },
			ClientConfig: paho.ClientConfig{
				ClientID: fmt.Sprintf("hopwake-tap-%d", os.Getpid()),
				OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
					p := pr.Packet
					var props paho.UserProperties
					if p.Properties != nil {
						props = p.Properties.User
					}
					select {
					case inbox <- message{topic: p.Topic, payload: p.Payload, props: props, at: time.Now()}:
					default:
						if src, ok := rules.Source("mqtt", p.Topic); ok {
							hl.miss(src.HopName())
						}
					}
					return true, nil
				}},
			},
		})
		if err != nil {
			log.Fatalf("mqtt: %v", err)
		}
		defer func() {
			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = cm.Disconnect(c)
		}()
	}

	if *brokers != "" && len(kafkaTopics) > 0 {
		kc, err := kgo.NewClient(
			kgo.SeedBrokers(strings.Split(*brokers, ",")...),
			kgo.ConsumerGroup(*group),
			kgo.ConsumeTopics(kafkaTopics...),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
			kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { kafkaUp.Store(true) }),
		)
		if err != nil {
			log.Fatalf("kafka: %v", err)
		}
		defer kc.Close()
		log.Printf("kafka: watching %s in group %s", strings.Join(kafkaTopics, ", "), *group)
		go func() {
			for ctx.Err() == nil {
				fetches := kc.PollFetches(ctx)
				fetches.EachError(func(t string, p int32, err error) {
					if ctx.Err() == nil {
						log.Printf("kafka %s/%d: %v", t, p, err)
					}
				})
				fetches.EachRecord(func(r *kgo.Record) {
					// How long the record sat in Kafka before the tap read it.
					delay := time.Since(r.Timestamp)
					observe("kafka", r.Topic, r.Value, func(name string) string {
						for _, h := range r.Headers {
							if h.Key == name {
								return string(h.Value)
							}
						}
						return ""
					}, r.Key, delay)
				})
			}
		}()
	}

	// A hop's health is reported only once its source has connected the first
	// time: before that the tap was not running there, which is no news, and
	// reporting "not connected" would read as a blind tap at every start. A
	// tap that stops reporting later still shows up as missing reports.
	started := map[*atomic.Bool]bool{}
	report := func(hops []string, up *atomic.Bool) {
		if !started[up] {
			if !up.Load() {
				for _, h := range hops {
					hl.take(h)
				}
				return
			}
			started[up] = true
		}
		for _, h := range hops {
			x := hl.take(h)
			downMu.Lock()
			wasDown := mqttDown[h]
			delete(mqttDown, h)
			downMu.Unlock()
			_ = rec.RecordHealth(ctx, hop.Health{
				Tenant: rules.Tenant, Hop: h, Tap: tapID,
				Connected: up.Load() && !wasDown,
				Seen:      x.seen, Missed: x.missed, MaxDelay: x.maxDelay,
				Every: *every, At: time.Now(),
			})
		}
	}
	go func() {
		t := time.NewTicker(*countsEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				from, to, byHop := ctr.take()
				for h, m := range byHop {
					_ = rec.RecordCounts(ctx, rules.Tenant, h, from, to, m)
				}
			}
		}
	}()
	if *sample > 1 {
		log.Printf("sending hop events for one reading in %d; counters cover all", *sample)
	}

	tick := time.NewTicker(*every)
	defer tick.Stop()
	logTick := time.NewTicker(30 * time.Second)
	defer logTick.Stop()
	for {
		select {
		case <-ctx.Done():
			logTotals(hl)
			return
		case <-tick.C:
			if *broker != "" {
				report(mqttHops, &mqttUp)
			}
			if *brokers != "" {
				report(kafkaHops, &kafkaUp)
			}
		case <-logTick.C:
			logTotals(hl)
		}
	}
}

func logTotals(hl *health) {
	hl.mu.Lock()
	defer hl.mu.Unlock()
	hops := make([]string, 0, len(hl.hops))
	for h := range hl.hops {
		hops = append(hops, h)
	}
	sort.Strings(hops)
	parts := make([]string, 0, len(hops))
	for _, h := range hops {
		parts = append(parts, fmt.Sprintf("%s %d (missed %d)", h, hl.hops[h].totalSeen, hl.hops[h].totalMissed))
	}
	log.Printf("hop events since start: %s", strings.Join(parts, ", "))
}

// sampled decides from the journey ID whether a reading's hop events are
// sent, so every hop and every tap makes the same choice.
func sampled(journeyID string, n uint64) bool {
	if n <= 1 || len(journeyID) < 8 {
		return true
	}
	x, err := strconv.ParseUint(journeyID[:8], 16, 64)
	return err != nil || x%n == 0
}

// counters counts every message per hop and device between flushes.
type counters struct {
	mu   sync.Mutex
	hops map[string]map[string]hop.Count
	from time.Time
}

func (c *counters) add(hopName, device string, isError bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.hops[hopName]
	if m == nil {
		m = map[string]hop.Count{}
		c.hops[hopName] = m
	}
	x := m[device]
	x.N++
	if isError {
		x.Errors++
	}
	x.Last = time.Now()
	m[device] = x
}

func (c *counters) take() (from, to time.Time, byHop map[string]map[string]hop.Count) {
	c.mu.Lock()
	defer c.mu.Unlock()
	from, to, byHop = c.from, time.Now(), c.hops
	c.hops, c.from = map[string]map[string]hop.Count{}, to
	return from, to, byHop
}

func envUint(key string, fallback uint64) uint64 {
	if v, err := strconv.ParseUint(os.Getenv(key), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
