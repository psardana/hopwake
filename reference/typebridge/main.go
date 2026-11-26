// Command typebridge forwards MQTT 5 messages to Kafka, choosing topics by
// the message type a user property names, through a mapping of type
// prefixes to topics. The bytes are forwarded unchanged; the type and the
// device go along as Kafka headers, and the device is the record key.
//
// Faults, set by environment, make it lose some devices' messages:
// FAULT_DROP_DEVICES (comma-separated) from FAULT_AFTER on.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/psardana/hopwake/reference/typemap"
)

func main() {
	broker := flag.String("mqtt", env("BRIDGE_MQTT", "tcp://localhost:1883"), "MQTT broker URL")
	filter := flag.String("filter", env("BRIDGE_FILTER", "devices/+/up/#"), "MQTT topic filter to forward")
	deviceLevel := flag.Int("device-level", 1, "topic level (from 0) holding the device")
	typeProp := flag.String("type-property", "MessageType", "MQTT user property naming the message type")
	brokers := flag.String("kafka", env("BRIDGE_KAFKA", "localhost:9092"), "Kafka bootstrap servers")
	mappingPath := flag.String("mapping", env("BRIDGE_MAPPING", "mapping.yaml"), "file mapping type prefixes to topics")
	mappingKey := flag.String("mapping-key", "bridge.consumer-mapping", "key of the mapping in that file")
	typeHeader := flag.String("type-header", "MessageType", "Kafka header for the message type")
	deviceHeader := flag.String("device-header", "DeviceId", "Kafka header for the device")
	flag.Parse()

	mapping, err := typemap.Load(*mappingPath, *mappingKey)
	if err != nil {
		log.Fatal(err)
	}
	drop := map[string]bool{}
	for _, d := range strings.Split(os.Getenv("FAULT_DROP_DEVICES"), ",") {
		if d = strings.TrimSpace(d); d != "" {
			drop[d] = true
		}
	}
	faultFrom := time.Now()
	if d, err := time.ParseDuration(os.Getenv("FAULT_AFTER")); err == nil {
		faultFrom = faultFrom.Add(d)
	}
	if len(drop) > 0 {
		log.Printf(`{"fault":"drop","devices":%d,"from":%q}`, len(drop), faultFrom.UTC().Format(time.RFC3339))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	kc, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.ProducerLinger(5*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()

	var in, out, unmapped, dropped atomic.Uint64
	u, err := url.Parse(*broker)
	if err != nil {
		log.Fatal(err)
	}
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:       []*url.URL{u},
		KeepAlive:        30,
		ReconnectBackoff: autopaho.NewConstantBackoff(2 * time.Second),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			go func() {
				if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: *filter, QoS: 1}}}); err != nil {
					log.Printf("subscribe: %v", err)
					return
				}
				log.Printf("forwarding %s to %s", *filter, strings.Join(mapping.All(), ", "))
			}()
		},
		OnConnectError: func(err error) { log.Printf("mqtt: %v", err) },
		ClientConfig: paho.ClientConfig{
			ClientID: fmt.Sprintf("typebridge-%d", os.Getpid()),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
				p := pr.Packet
				in.Add(1)
				levels := strings.Split(p.Topic, "/")
				if *deviceLevel >= len(levels) {
					return true, nil
				}
				device := levels[*deviceLevel]
				msgType := ""
				if p.Properties != nil {
					msgType = p.Properties.User.Get(*typeProp)
				}
				topics := mapping.Topics(msgType)
				if len(topics) == 0 {
					unmapped.Add(1)
					log.Printf("no topic for type %q from %s", msgType, device)
					return true, nil
				}
				if drop[device] && !time.Now().Before(faultFrom) {
					dropped.Add(1)
					return true, nil
				}
				for _, t := range topics {
					kc.Produce(ctx, &kgo.Record{Topic: t, Key: []byte(device), Value: p.Payload, Headers: []kgo.RecordHeader{
						{Key: *typeHeader, Value: []byte(msgType)}, {Key: *deviceHeader, Value: []byte(device)},
					}}, func(_ *kgo.Record, err error) {
						if err == nil {
							out.Add(1)
						}
					})
				}
				return true, nil
			}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = cm.Disconnect(context.Background())
			kc.Flush(context.Background())
			return
		case <-tick.C:
			log.Printf("%d received, %d produced, %d without a topic, %d dropped by faults", in.Load(), out.Load(), unmapped.Load(), dropped.Load())
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
