// Command bridge forwards device telemetry from MQTT into Kafka.
//
// It subscribes to a topic filter, and produces each payload unchanged to a
// Kafka topic, keyed by the device ID taken from the MQTT topic. The MQTT topic
// and the receive time travel along as Kafka headers.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/psardana/hopwake/reference/telemetry"
)

func main() {
	broker := flag.String("mqtt", env("BRIDGE_MQTT", "tcp://localhost:1883"), "MQTT broker URL")
	filter := flag.String("filter", env("BRIDGE_FILTER", "$share/bridge/plant/+/+/telemetry"), "MQTT topic filter (shared subscriptions allowed)")
	brokers := flag.String("kafka", env("BRIDGE_KAFKA", "localhost:9092"), "Kafka bootstrap servers, comma separated")
	topic := flag.String("topic", env("BRIDGE_TOPIC", "telemetry.raw"), "Kafka topic to produce to")
	deviceLevel := flag.Int("device-level", 2, "zero-based level of the MQTT topic that holds the device ID")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer kc.Close()

	shutdown, err := telemetry.Setup(ctx, "bridge")
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	defer shutdown(context.Background())
	tracer := telemetry.Tracer("hopwake/reference/bridge")

	flt := faultsFromEnv()
	var in, out, failed, dropped atomic.Uint64
	handler := func(_ mqtt.Client, m mqtt.Message) {
		in.Add(1)
		received := time.Now().UTC()
		parts := strings.Split(m.Topic(), "/")
		key := ""
		if *deviceLevel < len(parts) {
			key = parts[*deviceLevel]
		}
		// MQTT 3.1.1 carries no headers, so every reading starts a new trace here.
		rctx, rspan := tracer.Start(context.Background(), "receive plant telemetry",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "mqtt"),
				attribute.String("messaging.destination.name", m.Topic()),
				attribute.String("device.id", key),
			))
		defer rspan.End()
		payload := append([]byte(nil), m.Payload()...)
		drop, copies, delay := flt.decide(key, payload)
		if drop {
			dropped.Add(1)
			return
		}
		mqttTopic := m.Topic()
		send := func() {
			for i := 0; i < copies; i++ {
				rec := &kgo.Record{
					Key:   []byte(key),
					Value: payload,
					Headers: []kgo.RecordHeader{
						{Key: "mqtt_topic", Value: []byte(mqttTopic)},
						{Key: "bridge_received_at", Value: []byte(received.Format(time.RFC3339Nano))},
					},
				}
				pctx, pspan := tracer.Start(rctx, "send "+*topic,
					trace.WithSpanKind(trace.SpanKindProducer),
					trace.WithAttributes(
						attribute.String("messaging.system", "kafka"),
						attribute.String("messaging.destination.name", *topic),
						attribute.String("messaging.kafka.message.key", key),
						attribute.String("device.id", key),
					))
				telemetry.Inject(pctx, rec)
				kc.Produce(ctx, rec, func(_ *kgo.Record, err error) {
					defer pspan.End()
					if err != nil {
						pspan.RecordError(err)
						pspan.SetStatus(codes.Error, "produce failed")
						failed.Add(1)
						return
					}
					out.Add(1)
				})
			}
		}
		if delay > 0 {
			time.AfterFunc(delay, send)
		} else {
			send()
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(*broker).
		SetClientID("bridge-" + hostname()).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			// Subscribe on every (re)connect: a clean session forgets subscriptions.
			if t := c.Subscribe(*filter, 1, handler); t.Wait() && t.Error() != nil {
				log.Printf("subscribe %s: %v", *filter, t.Error())
				return
			}
			log.Printf("subscribed to %s", *filter)
		})
	mc := mqtt.NewClient(opts)
	mc.Connect()
	defer mc.Disconnect(250)

	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := kc.Flush(context.Background()); err != nil {
				log.Printf("flush: %v", err)
			}
			log.Printf("stopped: %d received, %d produced, %d failed, %d dropped by faults", in.Load(), out.Load(), failed.Load(), dropped.Load())
			return
		case <-report.C:
			log.Printf("%d received, %d produced, %d failed, %d dropped by faults", in.Load(), out.Load(), failed.Load(), dropped.Load())
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "local"
	}
	return h
}
