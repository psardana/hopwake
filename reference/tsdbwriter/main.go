// Command tsdbwriter consumes Protobuf records from Kafka and writes one row
// per record into a time-series table, keyed by the record's key field (a
// device, or an alias such as a system ID) and its time. It marks each record
// stored through a Hopwake hook when HOPWAKE_OTLP_ENDPOINT is set; with
// -alias-kind, the mark is made under that alias and Hopwake resolves it.
//
// Faults, set by environment: FAULT_SKIP_KEYS (comma-separated key values)
// are not written from FAULT_AFTER on, the way a consumer bug loses rows.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/psardana/hopwake/internal/journey"
	"github.com/psardana/hopwake/internal/protodec"
	"github.com/psardana/hopwake/pkg/hook"
)

const schema = `CREATE TABLE IF NOT EXISTS series (
	key text NOT NULL,
	message_type text NOT NULL,
	time timestamptz NOT NULL,
	data jsonb NOT NULL,
	stored_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (key, message_type, time)
)`

func main() {
	brokers := flag.String("kafka", env("WRITER_KAFKA", "localhost:9092"), "Kafka bootstrap servers")
	topics := flag.String("topics", env("WRITER_TOPICS", "records.in"), "comma-separated topics to consume")
	group := flag.String("group", env("WRITER_GROUP", "tsdbwriter"), "consumer group")
	dsn := flag.String("db", env("WRITER_DB", "postgres://localhost:5432/tsdb"), "Postgres connection string")
	protos := flag.String("protos", env("WRITER_PROTOS", "protos"), "directory of .proto files")
	fixedType := flag.String("type", "", "the records' Protobuf type; empty reads it from -type-header")
	typeHeader := flag.String("type-header", "MessageType", "Kafka header holding the message type")
	keyField := flag.String("key-field", "", "field holding the row key; empty uses the Kafka record key")
	timeFields := flag.String("time-fields", "measurement_time,timestamp,time", "fields holding a record's time, tried in order")
	asType := flag.String("as-type", "", "message type the readings started as, for Hopwake marks")
	aliasKind := flag.String("alias-kind", "", "the row key is an alias of this kind (e.g. systemId), not the device")
	flag.Parse()

	dec, err := protodec.Load(*protos)
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
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatal(err)
	}
	var stores *hook.Hook
	if os.Getenv("HOPWAKE_OTLP_ENDPOINT") != "" {
		if stores, err = hook.New(ctx, hook.Config{Tenant: env("HOPWAKE_TENANT", "reference"), Hop: env("HOPWAKE_HOP", "hook:tsdb.stored"), Service: "tsdbwriter"}); err != nil {
			log.Fatal(err)
		}
		defer stores.Close(context.Background())
	}
	skip := map[string]bool{}
	for _, d := range strings.Split(os.Getenv("FAULT_SKIP_KEYS"), ",") {
		if d = strings.TrimSpace(d); d != "" {
			skip[d] = true
		}
	}
	faultFrom := time.Now()
	if d, err := time.ParseDuration(os.Getenv("FAULT_AFTER")); err == nil {
		faultFrom = faultFrom.Add(d)
	}
	if len(skip) > 0 {
		log.Printf(`{"fault":"skip","keys":%d,"from":%q}`, len(skip), faultFrom.UTC().Format(time.RFC3339))
	}
	kc, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(strings.Split(*topics, ",")...), kgo.DisableAutoCommit())
	if err != nil {
		log.Fatal(err)
	}
	defer kc.Close()
	log.Printf("writing %s to the series table", *topics)

	type row struct {
		key, msgType string
		at           time.Time
	}
	var written, skipped, bad, unmarked int
	report := time.Now()
	for ctx.Err() == nil {
		fetches := kc.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		batch := &pgx.Batch{}
		var rows []row
		fetches.EachRecord(func(r *kgo.Record) {
			msgType := *fixedType
			if msgType == "" {
				for _, h := range r.Headers {
					if h.Key == *typeHeader {
						msgType = string(h.Value)
					}
				}
			}
			res, err := dec.Decode(msgType, r.Value)
			if err != nil {
				bad++
				return
			}
			var doc any
			if json.Unmarshal(res.JSON, &doc) != nil {
				bad++
				return
			}
			key := string(r.Key)
			if *keyField != "" {
				v, ok := journey.Lookup(doc, *keyField)
				if !ok {
					bad++
					return
				}
				key = fmt.Sprint(v)
			}
			at := timeOf(doc, *timeFields)
			if at.IsZero() || key == "" {
				bad++
				return
			}
			if skip[key] && !time.Now().Before(faultFrom) {
				skipped++
				return
			}
			batch.Queue(`INSERT INTO series (key, message_type, time, data) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
				key, msgType, at, string(res.JSON))
			rows = append(rows, row{key, msgType, at})
		})
		if batch.Len() > 0 {
			if err := pool.SendBatch(ctx, batch).Close(); err != nil {
				log.Printf("write: %v", err)
				time.Sleep(2 * time.Second)
				continue // offsets stay uncommitted, so the batch is read again
			}
			written += len(rows)
			if stores != nil {
				for _, x := range rows {
					t := x.msgType
					if *asType != "" {
						t = *asType
					}
					var err error
					if *aliasKind != "" {
						err = stores.MarkTimeAlias(ctx, hook.Store, *aliasKind, x.key, t, x.at)
					} else {
						err = stores.MarkTime(ctx, hook.Store, x.key, t, x.at)
					}
					if err != nil {
						unmarked++
					}
				}
			}
		}
		if err := kc.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			log.Printf("commit: %v", err)
		}
		if time.Since(report) >= 10*time.Second {
			log.Printf("%d rows written, %d undecodable, %d skipped by faults, %d not marked (alias not yet known)", written, bad, skipped, unmarked)
			report = time.Now()
		}
	}
}

// timeOf reads the first of the listed fields that holds a time: RFC 3339
// text, or seconds since 1970 (Protobuf integers arrive as text).
func timeOf(doc any, fields string) time.Time {
	for _, f := range strings.Split(fields, ",") {
		v, ok := journey.Lookup(doc, strings.TrimSpace(f))
		if !ok {
			continue
		}
		switch x := v.(type) {
		case string:
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return t
			}
			if n, err := strconv.ParseInt(x, 10, 64); err == nil {
				return time.Unix(n, 0)
			}
		case float64:
			return time.Unix(int64(x), 0)
		}
	}
	return time.Time{}
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
