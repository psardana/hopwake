// Command measuresvc consumes Protobuf measurements from Kafka, the way a
// measurement service behind an EMS fleet does:
//
//  1. It keeps each device's last values, keyed by its serial.
//  2. It looks up the device's system ID in master data and, if found, writes
//     a time-series record keyed by the system ID, built from a field map.
//     Without a system ID the record is dropped, with only a counter.
//
// It marks last values stored through a Hopwake hook, teaching Hopwake the
// serial and system ID pair, so hops that only know the system ID can still
// be placed on the reading's journey.
//
// Faults are not simulated here: a missing system ID is already one.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/psardana/hopwake/internal/journey"
	"github.com/psardana/hopwake/internal/protodec"
	"github.com/psardana/hopwake/pkg/hook"
	"github.com/psardana/hopwake/reference/masterdata"
)

// recordMap builds the time-series record from a decoded measurement.
type recordMap struct {
	Type   string `yaml:"type"`  // the record's Protobuf type
	Topic  string `yaml:"topic"` // where records go
	Fields map[string]struct {
		From  string  `yaml:"from"`  // dotted path in the measurement; "$system" or "$seconds" for the system ID and the time
		Scale float64 `yaml:"scale"` // multiply numbers by this; 0 keeps them
	} `yaml:"fields"`
}

const lvcSchema = `CREATE TABLE IF NOT EXISTS last_values (
	serial text NOT NULL,
	message_type text NOT NULL,
	measurement_time timestamptz NOT NULL,
	data jsonb NOT NULL,
	server_time timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (serial, message_type)
)`

func main() {
	brokers := flag.String("kafka", env("SVC_KAFKA", "localhost:9092"), "Kafka bootstrap servers")
	topics := flag.String("topics", env("SVC_TOPICS", "measurements.in"), "comma-separated topics to consume")
	group := flag.String("group", env("SVC_GROUP", "measuresvc"), "consumer group")
	dsn := flag.String("db", env("SVC_DB", "postgres://localhost:5432/lvc"), "Postgres for last values")
	protos := flag.String("protos", env("SVC_PROTOS", "protos"), "directory of the device .proto files")
	recordProtos := flag.String("record-protos", env("SVC_RECORD_PROTOS", "record-protos"), "directory of the time-series record .proto files")
	recordMapPath := flag.String("record-map", env("SVC_RECORD_MAP", "record-map.yaml"), "how to build a time-series record")
	masterPath := flag.String("master", env("SVC_MASTER", "master-data.csv"), "serial,systemId pairs")
	typeHeader := flag.String("type-header", "MessageType", "Kafka header holding the message type")
	timeFields := flag.String("time-fields", "measurement_time", "fields holding a reading's time, tried in order")
	flag.Parse()

	dec, err := protodec.Load(*protos)
	if err != nil {
		log.Fatal(err)
	}
	recDec, err := protodec.Load(*recordProtos)
	if err != nil {
		log.Fatal(err)
	}
	var rm recordMap
	raw, err := os.ReadFile(*recordMapPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &rm); err != nil {
		log.Fatal(err)
	}
	master, err := masterdata.Load(*masterPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := connect(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, lvcSchema); err != nil {
		log.Fatal(err)
	}
	var marks *hook.Hook
	if os.Getenv("HOPWAKE_OTLP_ENDPOINT") != "" {
		if marks, err = hook.New(ctx, hook.Config{Tenant: env("HOPWAKE_TENANT", "reference"), Hop: env("HOPWAKE_HOP", "hook:lvc.stored"), Service: "measuresvc"}); err != nil {
			log.Fatal(err)
		}
		defer marks.Close(context.Background())
	}
	kc, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(strings.Split(*topics, ",")...), kgo.ProducerLinger(5*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()
	log.Printf("consuming %s; records to %s", *topics, rm.Topic)

	var undecodable, noSystem, recorded, stored int
	report := time.Now()
	for ctx.Err() == nil {
		fetches := kc.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			msgType := ""
			for _, h := range r.Headers {
				if h.Key == *typeHeader {
					msgType = string(h.Value)
				}
			}
			serial := string(r.Key)
			res, err := dec.Decode(msgType, r.Value)
			if err != nil {
				undecodable++
				return
			}
			var doc any
			_ = json.Unmarshal(res.JSON, &doc)
			var at time.Time
			for _, f := range strings.Split(*timeFields, ",") {
				if v, ok := journey.Lookup(doc, strings.TrimSpace(f)); ok {
					if s, ok := v.(string); ok {
						at, _ = time.Parse(time.RFC3339Nano, s)
					}
					break
				}
			}
			if at.IsZero() {
				undecodable++
				return
			}
			// 1. Last values, by serial.
			if _, err := pool.Exec(ctx, `INSERT INTO last_values (serial, message_type, measurement_time, data, server_time)
				VALUES ($1, $2, $3, $4, now()) ON CONFLICT (serial, message_type) DO UPDATE
				SET measurement_time = EXCLUDED.measurement_time, data = EXCLUDED.data, server_time = now()`,
				serial, msgType, at, string(res.JSON)); err != nil {
				log.Printf("last values: %v", err)
				return
			}
			stored++
			system, ok := master.System(serial)
			if marks != nil {
				if ok {
					_ = marks.MarkTimeTeach(ctx, hook.Store, serial, "systemId", system, msgType, at)
				} else {
					_ = marks.MarkTime(ctx, hook.Store, serial, msgType, at)
				}
			}
			// 2. The time-series record, by system ID; none without one.
			if !ok {
				noSystem++
				return
			}
			body, err := buildRecord(rm, recDec, doc, system, at)
			if err != nil {
				log.Printf("%s: record: %v", serial, err)
				return
			}
			kc.Produce(ctx, &kgo.Record{Topic: rm.Topic, Key: []byte(system), Value: body}, nil)
			recorded++
		})
		if time.Since(report) >= 10*time.Second {
			log.Printf("%d last values stored, %d records written, %d without a system ID, %d undecodable", stored, recorded, noSystem, undecodable)
			report = time.Now()
		}
	}
}

func buildRecord(rm recordMap, dec *protodec.Decoder, doc any, system string, at time.Time) ([]byte, error) {
	out := map[string]any{}
	for name, f := range rm.Fields {
		var v any
		switch f.From {
		case "$system":
			n, err := strconv.ParseUint(system, 10, 32)
			if err != nil {
				return nil, err
			}
			v = n
		case "$seconds":
			v = at.Unix()
		default:
			x, ok := journey.Lookup(doc, f.From)
			if !ok {
				continue
			}
			if num, ok := x.(float64); ok && f.Scale != 0 {
				x = math.Round(num*f.Scale*1e6) / 1e6
			}
			v = x
		}
		out[name] = v
	}
	js, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return dec.Encode(rm.Type, js)
}

func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	var last error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		last = err
		time.Sleep(2 * time.Second)
	}
	return nil, last
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
