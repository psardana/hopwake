// Command tap watches MQTT topics and Kafka topics from the outside and emits
// a hop event for every reading it sees.
//
// It never sits in the data path: it subscribes to MQTT like any other client
// and reads Kafka in its own consumer group. Each message's journey ID is
// derived from the device and sequence number found by the rules file, and
// the hop event goes out over OTLP.
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

type counts struct {
	mu       sync.Mutex
	seen     map[string]int
	unparsed map[string]int
}

func (c *counts) add(m map[string]int, hopName string) {
	c.mu.Lock()
	m[hopName]++
	c.mu.Unlock()
}

func (c *counts) report() {
	c.mu.Lock()
	defer c.mu.Unlock()
	hops := make([]string, 0, len(c.seen)+len(c.unparsed))
	for h := range c.seen {
		hops = append(hops, h)
	}
	for h := range c.unparsed {
		if _, ok := c.seen[h]; !ok {
			hops = append(hops, h)
		}
	}
	sort.Strings(hops)
	parts := make([]string, 0, len(hops))
	for _, h := range hops {
		parts = append(parts, fmt.Sprintf("%s %d (unparsed %d)", h, c.seen[h], c.unparsed[h]))
	}
	log.Printf("hop events: %s", strings.Join(parts, ", "))
}

func main() {
	rulesPath := flag.String("rules", env("TAP_RULES", "reference/journey-rules.yaml"), "journey ID rules")
	broker := flag.String("mqtt", env("TAP_MQTT", ""), "MQTT broker URL; empty skips MQTT sources")
	brokers := flag.String("kafka", env("TAP_KAFKA", ""), "Kafka bootstrap servers; empty skips Kafka sources")
	group := flag.String("group", env("TAP_GROUP", "hopwake-tap"), "Kafka consumer group of the tap")
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
	cnt := &counts{seen: map[string]int{}, unparsed: map[string]int{}}
	var announced sync.Map // hop + fingerprint → struct{}: shapes whose fields were already sent

	observe := func(kind, topic string, payload []byte) {
		src, ok := rules.Source(kind, topic)
		if !ok {
			return
		}
		rule, _ := rules.Rule(kind, topic)
		key, err := rule.Extract(topic, payload)
		if err != nil {
			cnt.add(cnt.unparsed, src.HopName())
			return
		}
		sh := shape.Of(payload)
		fields := ""
		if _, done := announced.LoadOrStore(src.HopName()+" "+sh.Fingerprint, struct{}{}); !done {
			fields = sh.String()
		}
		err = rec.Record(ctx, hop.Event{
			JourneyID:   key.ID(),
			Tenant:      key.Tenant,
			Device:      key.Device,
			Seq:         key.Seq,
			Hop:         src.HopName(),
			System:      kind,
			Destination: topic,
			Op:          hop.OpReceive,
			Correlation: hop.Derived,
			Bytes:       len(payload),
			At:          time.Now(),
			ShapeFP:     sh.Fingerprint,
			ShapeFields: fields,
		})
		if err != nil {
			cnt.add(cnt.unparsed, src.HopName())
			return
		}
		cnt.add(cnt.seen, src.HopName())
	}

	var mqttFilters, kafkaTopics []string
	for _, s := range rules.Sources {
		switch s.Kind {
		case "mqtt":
			mqttFilters = append(mqttFilters, s.Match)
		case "kafka":
			kafkaTopics = append(kafkaTopics, s.Match)
		}
	}

	if *broker != "" && len(mqttFilters) > 0 {
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
			SetOnConnectHandler(func(c mqtt.Client) {
				if t := c.SubscribeMultiple(filters, func(_ mqtt.Client, m mqtt.Message) {
					observe("mqtt", m.Topic(), m.Payload())
				}); t.Wait() && t.Error() != nil {
					log.Printf("mqtt subscribe: %v", t.Error())
					return
				}
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
				fetches.EachRecord(func(r *kgo.Record) { observe("kafka", r.Topic, r.Value) })
			}
		}()
	}

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			cnt.report()
			return
		case <-tick.C:
			cnt.report()
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
