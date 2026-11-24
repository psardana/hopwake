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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
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
	at      time.Time
}

func main() {
	rulesPath := flag.String("rules", env("TAP_RULES", "reference/journey-rules.yaml"), "journey ID rules")
	broker := flag.String("mqtt", env("TAP_MQTT", ""), "MQTT broker URL; empty skips MQTT sources")
	brokers := flag.String("kafka", env("TAP_KAFKA", ""), "Kafka bootstrap servers; empty skips Kafka sources")
	group := flag.String("group", env("TAP_GROUP", "hopwake-tap"), "Kafka consumer group of the tap")
	every := flag.Duration("health-every", 5*time.Second, "how often the tap reports its own health per hop")
	queue := flag.Int("mqtt-queue", 20000, "MQTT messages the tap may hold before it drops and counts them")
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

	var announced sync.Map // hop + fingerprint → struct{}: shapes whose fields were already sent
	// header returns a message header by name; nil for MQTT.
	observe := func(kind, topic string, payload []byte, header func(string) string, delay time.Duration) {
		src, ok := rules.Source(kind, topic)
		if !ok {
			return
		}
		rule, _ := rules.Rule(kind, topic)
		key, err := rule.Extract(topic, payload)
		if err != nil {
			hl.saw(src.HopName(), delay, false)
			return
		}
		sh := shape.Of(payload)
		fields := ""
		if _, done := announced.LoadOrStore(src.HopName()+" "+sh.Fingerprint, struct{}{}); !done {
			fields = sh.String()
		}
		op, reason := hop.OpReceive, ""
		if src.DeadLetter() {
			op, reason = hop.OpDrop, "rejected"
			if header != nil && src.ReasonHeader != "" {
				if r := strings.TrimSpace(header(src.ReasonHeader)); r != "" {
					reason = r
				}
			}
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
				observe("mqtt", m.topic, m.payload, nil, time.Since(m.at))
			}
		}()
		filters := map[string]byte{}
		for _, f := range mqttFilters {
			filters[f] = 0 // QoS 0: the tap must never slow the broker down
		}
		mc := mqtt.NewClient(mqtt.NewClientOptions().
			AddBroker(*broker).
			SetClientID(fmt.Sprintf("hopwake-tap-%d", os.Getpid())).
			SetCleanSession(true).
			SetAutoReconnect(true).
			SetConnectRetry(true).
			SetConnectRetryInterval(2 * time.Second).
			SetConnectionLostHandler(func(mqtt.Client, error) {
				mqttUp.Store(false)
				markDown(mqttHops)
			}).
			SetOnConnectHandler(func(c mqtt.Client) {
				if t := c.SubscribeMultiple(filters, func(_ mqtt.Client, m mqtt.Message) {
					select {
					case inbox <- message{topic: m.Topic(), payload: m.Payload(), at: time.Now()}:
					default:
						if src, ok := rules.Source("mqtt", m.Topic()); ok {
							hl.miss(src.HopName())
						}
					}
				}); t.Wait() && t.Error() != nil {
					log.Printf("mqtt subscribe: %v", t.Error())
					return
				}
				mqttUp.Store(true)
				log.Printf("mqtt: watching %s", strings.Join(mqttFilters, ", "))
			}))
		mc.Connect()
		defer mc.Disconnect(250)
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
					}, delay)
				})
			}
		}()
	}

	report := func(hops []string, up *atomic.Bool) {
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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
