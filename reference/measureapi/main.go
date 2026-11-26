// Command measureapi serves measurements to portals and apps, the way a
// measurement API behind an EMS fleet does:
//
//	GET /v1/systems/{systemId}/measurements/raw?minutes=15   time series, by system ID
//	GET /v1/controllers/{serial}/statistics/latest           last values, by serial
//
// It marks each reading served, the first time a response contains it,
// through a Hopwake hook when HOPWAKE_OTLP_ENDPOINT is set: by serial for
// last values, by system ID alias for the time series.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/psardana/hopwake/pkg/hook"
	"github.com/psardana/hopwake/reference/served"
)

var idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func main() {
	listen := flag.String("listen", env("API_LISTEN", ":8081"), "HTTP address")
	dsn := flag.String("db", env("API_DB", "postgres://localhost:5432/tsdb"), "Postgres with the series and last_values tables")
	seriesType := flag.String("series-type", env("API_SERIES_TYPE", ""), "message type the time series readings started as")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	marks := served.New(ctx, "hook:api.served", "measureapi")
	defer marks.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/systems/{systemId}/measurements/raw", func(w http.ResponseWriter, r *http.Request) {
		system := r.PathValue("systemId")
		if !idRe.MatchString(system) {
			http.Error(w, "bad system id", http.StatusBadRequest)
			return
		}
		minutes, err := strconv.Atoi(r.URL.Query().Get("minutes"))
		if err != nil || minutes <= 0 || minutes > 1440 {
			minutes = 15
		}
		rows, err := pool.Query(r.Context(), `SELECT time, data::text FROM series
			WHERE key = $1 AND time >= now() - make_interval(mins => $2) ORDER BY time DESC LIMIT 2000`, system, minutes)
		if err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		defer rows.Close()
		type point struct {
			SystemID string          `json:"systemId"`
			Time     time.Time       `json:"time"`
			Data     json.RawMessage `json:"data"`
		}
		out := []point{}
		for rows.Next() {
			var p point
			var data string
			if err := rows.Scan(&p.Time, &data); err != nil {
				http.Error(w, "store error", http.StatusInternalServerError)
				return
			}
			p.SystemID, p.Data = system, json.RawMessage(data)
			out = append(out, p)
			marks.MarkAlias(r.Context(), hook.Serve, "systemId", system, *seriesType, p.Time)
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /v1/controllers/{serial}/statistics/latest", func(w http.ResponseWriter, r *http.Request) {
		serial := r.PathValue("serial")
		if !idRe.MatchString(serial) {
			http.Error(w, "bad serial", http.StatusBadRequest)
			return
		}
		rows, err := pool.Query(r.Context(), `SELECT message_type, measurement_time, server_time, data::text FROM last_values WHERE serial = $1`, serial)
		if err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		defer rows.Close()
		type latest struct {
			Serial          string          `json:"serial"`
			MessageType     string          `json:"message_type"`
			MeasurementTime time.Time       `json:"measurement_time"`
			ServerTime      time.Time       `json:"serverTime"`
			Data            json.RawMessage `json:"data"`
		}
		out := []latest{}
		for rows.Next() {
			var x latest
			var data string
			if err := rows.Scan(&x.MessageType, &x.MeasurementTime, &x.ServerTime, &data); err != nil {
				http.Error(w, "store error", http.StatusInternalServerError)
				return
			}
			x.Serial, x.Data = serial, json.RawMessage(data)
			out = append(out, x)
			marks.Mark(r.Context(), hook.Serve, serial, x.MessageType, x.MeasurementTime)
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	log.Printf("measurement API on %s", *listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
