// Command devicesim simulates a fleet of sensors publishing telemetry over MQTT.
//
// Each device publishes a JSON reading to plant/<site>/<device>/telemetry at a
// fixed interval, with a per-device sequence number that downstream hops can
// use to identify a message.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type reading struct {
	DeviceID  string  `json:"device_id"`
	Site      string  `json:"site"`
	Seq       uint64  `json:"seq"`
	TS        string  `json:"ts"`
	TempC     float64 `json:"temp_c"`
	PressureK float64 `json:"pressure_kpa"`
	Vibration float64 `json:"vibration_mm_s"`
}

type device struct {
	id   string
	site string
	seq  uint64
	base float64
}

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker URL")
	devices := flag.Int("devices", 1000, "number of simulated devices")
	sites := flag.Int("sites", 4, "number of sites the devices are spread over")
	interval := flag.Duration("interval", 5*time.Second, "time between readings per device")
	conns := flag.Int("conns", 10, "MQTT connections shared by the devices")
	qos := flag.Int("qos", 1, "MQTT QoS for published readings (0 or 1)")
	duration := flag.Duration("duration", 0, "stop after this long (0 runs until interrupted)")
	flag.Parse()

	if *devices < 1 || *sites < 1 || *conns < 1 {
		log.Fatal("devices, sites and conns must be at least 1")
	}
	if *qos != 0 && *qos != 1 {
		log.Fatal("qos must be 0 or 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	clients := make([]mqtt.Client, *conns)
	for i := range clients {
		opts := mqtt.NewClientOptions().
			AddBroker(*broker).
			SetClientID(fmt.Sprintf("devicesim-%d-%d", os.Getpid(), i)).
			SetAutoReconnect(true).
			SetConnectRetry(true).
			SetConnectRetryInterval(2 * time.Second)
		c := mqtt.NewClient(opts)
		if t := c.Connect(); t.WaitTimeout(10*time.Second) && t.Error() != nil {
			log.Fatalf("connect %s: %v", *broker, t.Error())
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.Disconnect(250)
		}
	}()

	var sent, failed atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < *devices; i++ {
		d := &device{
			id:   fmt.Sprintf("dev-%05d", i),
			site: fmt.Sprintf("site-%02d", i%*sites),
			base: 18 + rand.Float64()*8,
		}
		client := clients[i%*conns]
		// Spread first readings over one interval so the fleet does not publish in bursts.
		offset := time.Duration(rand.Int64N(int64(*interval)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(ctx, client, d, *interval, offset, byte(*qos), &sent, &failed)
		}()
	}

	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			log.Printf("stopped after %s: %d sent, %d failed", time.Since(start).Round(time.Second), sent.Load(), failed.Load())
			return
		case <-report.C:
			log.Printf("%d sent, %d failed", sent.Load(), failed.Load())
		}
	}
}

func run(ctx context.Context, c mqtt.Client, d *device, every, offset time.Duration, qos byte, sent, failed *atomic.Uint64) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(offset):
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	topic := fmt.Sprintf("plant/%s/%s/telemetry", d.site, d.id)
	for {
		d.seq++
		now := time.Now().UTC()
		phase := float64(now.Unix()%3600) / 3600 * 2 * math.Pi
		body, err := json.Marshal(reading{
			DeviceID:  d.id,
			Site:      d.site,
			Seq:       d.seq,
			TS:        now.Format(time.RFC3339Nano),
			TempC:     round(d.base+2*math.Sin(phase)+rand.NormFloat64()*0.3, 2),
			PressureK: round(101.3+rand.NormFloat64()*0.8, 2),
			Vibration: round(math.Abs(1.5+rand.NormFloat64()*0.5), 3),
		})
		if err != nil {
			failed.Add(1)
		} else if t := c.Publish(topic, qos, false, body); !t.WaitTimeout(5*time.Second) || t.Error() != nil {
			failed.Add(1)
		} else {
			sent.Add(1)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}
