// Command journeyprobe checks journey-ID derivation on live traffic.
//
// It reads a copy of the MQTT telemetry and of a downstream Kafka topic,
// derives the journey ID of every message at both hops on its own, and reports
// how many journeys reached the downstream hop, how long they took, and which
// devices lost data in between. No headers are needed.
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

	"github.com/psardana/hopwake/internal/journey"
)

type sighting struct {
	device     string
	seq        uint64
	upstream   time.Time
	downstream time.Time
	repeats    int
}

type probe struct {
	mu       sync.Mutex
	warmEnd  time.Time // upstream sightings before this are ignored: their downstream copies may predate the probe
	seen     map[string]*sighting
	upRule   journey.Rule
	downRule journey.Rule
	skipped  map[string]int
}

func main() {
	broker := flag.String("mqtt", "tcp://localhost:1883", "MQTT broker URL")
	filter := flag.String("filter", "plant/+/+/telemetry", "MQTT topic filter to watch")
	brokers := flag.String("kafka", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "telemetry.normalized", "downstream Kafka topic")
	tenant := flag.String("tenant", "reference", "tenant used in journey IDs")
	grace := flag.Duration("grace", 15*time.Second, "how long a journey may take before it counts as lost")
	every := flag.Duration("every", 10*time.Second, "report interval")
	warmup := flag.Duration("warmup", 5*time.Second, "ignore journeys that start this soon after the probe")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p := &probe{
		seen:     map[string]*sighting{},
		skipped:  map[string]int{},
		warmEnd:  time.Now().Add(*warmup),
		upRule:   journey.Rule{Tenant: *tenant, TopicPattern: "plant/+/{device}/telemetry", SeqField: "seq"},
		downRule: journey.Rule{Tenant: *tenant, DeviceField: "device", SeqField: "seq"},
	}

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kc.Close()
	go func() {
		for ctx.Err() == nil {
			fetches := kc.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) { p.observe(false, "", r.Value) })
		}
	}()

	mc := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(*broker).
		SetClientID(fmt.Sprintf("journeyprobe-%d", os.Getpid())).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			c.Subscribe(*filter, 0, func(_ mqtt.Client, m mqtt.Message) { p.observe(true, m.Topic(), m.Payload()) })
		}))
	if t := mc.Connect(); t.Wait() && t.Error() != nil {
		log.Fatalf("mqtt: %v", t.Error())
	}
	defer mc.Disconnect(250)

	log.Printf("watching %s -> %s, grace %s", *filter, *topic, *grace)
	tick := time.NewTicker(*every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.report(*grace)
		}
	}
}

func (p *probe) observe(upstream bool, topic string, payload []byte) {
	rule, hop := p.downRule, "downstream"
	if upstream {
		rule, hop = p.upRule, "upstream"
	}
	k, err := rule.Extract(topic, payload)
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.skipped[hop]++
		return
	}
	id := k.ID()
	s := p.seen[id]
	if s == nil && now.Before(p.warmEnd) {
		return
	}
	if s == nil {
		s = &sighting{device: k.Device, seq: k.Seq}
		p.seen[id] = s
	}
	switch {
	case upstream && s.upstream.IsZero():
		s.upstream = now
	case !upstream && s.downstream.IsZero():
		s.downstream = now
	default:
		s.repeats++
	}
}

func (p *probe) report(grace time.Duration) {
	cutoff := time.Now().Add(-grace)
	p.mu.Lock()
	var reached, lost, onlyDown, repeats int
	var latencies []time.Duration
	lostBy := map[string][]uint64{}
	for id, s := range p.seen {
		first := s.upstream
		if first.IsZero() {
			first = s.downstream
		}
		if first.After(cutoff) {
			continue // still within its grace period
		}
		repeats += s.repeats
		switch {
		case s.upstream.IsZero():
			onlyDown++ // started before the probe did
		case s.downstream.IsZero():
			lost++
			lostBy[s.device] = append(lostBy[s.device], s.seq)
		default:
			reached++
			latencies = append(latencies, s.downstream.Sub(s.upstream))
		}
		delete(p.seen, id)
	}
	skipped := fmt.Sprintf("%d upstream, %d downstream", p.skipped["upstream"], p.skipped["downstream"])
	p.mu.Unlock()

	total := reached + lost
	if total == 0 {
		log.Printf("no completed journeys yet (unmatched: %d skipped %s)", onlyDown, skipped)
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(q*float64(len(latencies)-1))].Round(time.Millisecond)
	}
	log.Printf("%d journeys: %d reached downstream (%.2f%%), %d lost, %d repeats; latency p50 %s p95 %s; unparsed: %s",
		total, reached, 100*float64(reached)/float64(total), lost, repeats, pct(0.5), pct(0.95), skipped)

	devices := make([]string, 0, len(lostBy))
	for d := range lostBy {
		devices = append(devices, d)
	}
	sort.Slice(devices, func(i, j int) bool { return len(lostBy[devices[i]]) > len(lostBy[devices[j]]) })
	for i, d := range devices {
		if i == 5 {
			log.Printf("  … and %d more devices", len(devices)-5)
			break
		}
		seqs := lostBy[d]
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		log.Printf("  lost %s: %d readings, seq %d to %d", d, len(seqs), seqs[0], seqs[len(seqs)-1])
	}
}
