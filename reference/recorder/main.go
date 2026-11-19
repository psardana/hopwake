// Command recorder stores normalised readings and per-site minutes in Postgres.
//
// It consumes telemetry.normalized and telemetry.site_minute, writes them in
// batches (repeats are ignored by primary key), and prunes old readings.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type reading struct {
	Device       string  `json:"device"`
	Site         string  `json:"site"`
	Seq          int64   `json:"seq"`
	ObservedAt   string  `json:"observed_at"`
	TemperatureK float64 `json:"temperature_k"`
	PressurePa   int64   `json:"pressure_pa"`
	Vibration    float64 `json:"vibration_mm_s"`
}

type siteMinute struct {
	Site          string  `json:"site"`
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
	Readings      int     `json:"readings"`
	Devices       int     `json:"devices"`
	MedianTempK   float64 `json:"median_temperature_k"`
	MedianPressPa int64   `json:"median_pressure_pa"`
}

func main() {
	brokers := flag.String("kafka", env("RECORDER_KAFKA", "localhost:9092"), "Kafka bootstrap servers")
	dsn := flag.String("db", env("RECORDER_DB", "postgres://hopwake:hopwake@localhost:5432/reference"), "Postgres connection string")
	readingsTopic := flag.String("readings", env("RECORDER_READINGS", "telemetry.normalized"), "topic with normalised readings")
	minutesTopic := flag.String("minutes", env("RECORDER_MINUTES", "telemetry.site_minute"), "topic with per-site minutes")
	retention := flag.Duration("retention", 24*time.Hour, "how long readings are kept")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := connect(ctx, *dsn)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup("recorder"),
		kgo.ConsumeTopics(*readingsTopic, *minutesTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer kc.Close()

	go prune(ctx, pool, *retention)

	var stored, minutes, skipped int
	report := time.Now()
	for ctx.Err() == nil {
		fetches := kc.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) { log.Printf("fetch %s/%d: %v", t, p, err) })

		batch := &pgx.Batch{}
		fetches.EachRecord(func(r *kgo.Record) {
			switch r.Topic {
			case *readingsTopic:
				var v reading
				if err := json.Unmarshal(r.Value, &v); err != nil || v.Device == "" {
					skipped++
					return
				}
				batch.Queue(`INSERT INTO readings (device, site, seq, observed_at, temperature_k, pressure_pa, vibration_mm_s)
					VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`,
					v.Device, v.Site, v.Seq, v.ObservedAt, v.TemperatureK, v.PressurePa, v.Vibration)
				stored++
			case *minutesTopic:
				var v siteMinute
				if err := json.Unmarshal(r.Value, &v); err != nil || v.Site == "" {
					skipped++
					return
				}
				batch.Queue(`INSERT INTO site_minute (site, window_start, window_end, readings, devices, median_temperature_k, median_pressure_pa)
					VALUES ($1, $2, $3, $4, $5, $6, $7)
					ON CONFLICT (site, window_start) DO UPDATE SET readings = EXCLUDED.readings, devices = EXCLUDED.devices,
					  median_temperature_k = EXCLUDED.median_temperature_k, median_pressure_pa = EXCLUDED.median_pressure_pa`,
					v.Site, v.WindowStart, v.WindowEnd, v.Readings, v.Devices, v.MedianTempK, v.MedianPressPa)
				minutes++
			}
		})
		if batch.Len() > 0 {
			if err := pool.SendBatch(ctx, batch).Close(); err != nil {
				log.Printf("write batch: %v", err)
				time.Sleep(2 * time.Second)
				continue // offsets stay uncommitted, so the batch is read again
			}
		}
		if err := kc.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			log.Printf("commit offsets: %v", err)
		}
		if time.Since(report) >= 10*time.Second {
			log.Printf("%d readings, %d site minutes stored, %d skipped", stored, minutes, skipped)
			report = time.Now()
		}
	}
	log.Printf("stopped: %d readings, %d site minutes stored, %d skipped", stored, minutes, skipped)
}

func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}

func prune(ctx context.Context, pool *pgxpool.Pool, keep time.Duration) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tag, err := pool.Exec(ctx, `DELETE FROM readings WHERE observed_at < now() - make_interval(secs => $1)`, keep.Seconds())
			if err != nil {
				log.Printf("prune: %v", err)
				continue
			}
			if n := tag.RowsAffected(); n > 0 {
				log.Printf("pruned %d readings older than %s", n, keep)
			}
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
