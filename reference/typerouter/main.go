// Command typerouter routes Kafka records to topics by the message type in
// a header, through a mapping of type prefixes to topics. A record whose
// type maps to no topic is logged and counted, not dead-lettered.
//
// Faults, set by environment: FAULT_UNROUTE_TYPES (comma-separated type
// prefixes) are treated as unmapped from FAULT_AFTER on, the way an
// outdated mapping loses a new message type.
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

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/psardana/hopwake/reference/typemap"
)

func main() {
	brokers := flag.String("kafka", env("ROUTER_KAFKA", "localhost:9092"), "Kafka bootstrap servers")
	listen := flag.String("listen", env("ROUTER_LISTEN", "all.in"), "comma-separated topics to route")
	group := flag.String("group", env("ROUTER_GROUP", "typerouter"), "consumer group")
	mappingPath := flag.String("mapping", env("ROUTER_MAPPING", "mapping.yaml"), "file mapping type prefixes to topics")
	mappingKey := flag.String("mapping-key", "routing.consumer-mapping", "key of the mapping in that file")
	typeHeader := flag.String("type-header", "MessageType", "Kafka header holding the message type")
	flag.Parse()

	mapping, err := typemap.Load(*mappingPath, *mappingKey)
	if err != nil {
		log.Fatal(err)
	}
	var unroute []string
	for _, t := range strings.Split(os.Getenv("FAULT_UNROUTE_TYPES"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			unroute = append(unroute, t)
		}
	}
	faultFrom := time.Now()
	if d, err := time.ParseDuration(os.Getenv("FAULT_AFTER")); err == nil {
		faultFrom = faultFrom.Add(d)
	}
	if len(unroute) > 0 {
		log.Printf(`{"fault":"unroute","types":%q,"from":%q}`, strings.Join(unroute, ","), faultFrom.UTC().Format(time.RFC3339))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(strings.Split(*listen, ",")...),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()
	log.Printf("routing %s to %s", *listen, strings.Join(mapping.All(), ", "))

	var in, out, unmapped atomic.Uint64
	report := time.Now()
	for ctx.Err() == nil {
		fetches := kc.PollFetches(ctx)
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				log.Printf("fetch %s/%d: %v", t, p, err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			in.Add(1)
			msgType := ""
			for _, h := range r.Headers {
				if h.Key == *typeHeader {
					msgType = string(h.Value)
				}
			}
			topics := mapping.Topics(msgType)
			if !time.Now().Before(faultFrom) {
				for _, p := range unroute {
					if msgType == p || strings.HasPrefix(msgType, p+".") {
						topics = nil
					}
				}
			}
			if len(topics) == 0 {
				if unmapped.Add(1)%50 == 1 {
					log.Printf("no route for type %q (%d so far)", msgType, unmapped.Load())
				}
				return
			}
			for _, t := range topics {
				kc.Produce(ctx, &kgo.Record{Topic: t, Key: r.Key, Value: r.Value, Headers: r.Headers}, func(_ *kgo.Record, err error) {
					if err == nil {
						out.Add(1)
					}
				})
			}
		})
		if time.Since(report) >= 10*time.Second {
			log.Printf("%d consumed, %d produced, %d without a route", in.Load(), out.Load(), unmapped.Load())
			report = time.Now()
		}
	}
	kc.Flush(context.Background())
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
