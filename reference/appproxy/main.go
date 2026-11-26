// Command appproxy stands between the mobile apps and the measurement API,
// the way an app measurements proxy does: the apps ask by system ID, the
// proxy finds the system's controller serial in master data and asks the API
// for its latest values. It marks the readings it hands to an app served,
// through a Hopwake hook when HOPWAKE_OTLP_ENDPOINT is set.
//
//	GET /measurements/api/v1/systems/{systemId}/dashboard
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/psardana/hopwake/pkg/hook"
	"github.com/psardana/hopwake/reference/masterdata"
	"github.com/psardana/hopwake/reference/served"
)

func main() {
	listen := flag.String("listen", env("PROXY_LISTEN", ":8082"), "HTTP address")
	api := flag.String("api", env("PROXY_API", "http://localhost:8081"), "measurement API base URL")
	masterPath := flag.String("master", env("PROXY_MASTER", "master-data.csv"), "serial,systemId pairs")
	flag.Parse()
	master, err := masterdata.Load(*masterPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	marks := served.New(ctx, "hook:appproxy.served", "appproxy")
	defer marks.Close()
	client := &http.Client{Timeout: 5 * time.Second}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /measurements/api/v1/systems/{systemId}/dashboard", func(w http.ResponseWriter, r *http.Request) {
		system := r.PathValue("systemId")
		serial, ok := master.Serial(system)
		if !ok {
			http.Error(w, "unknown system", http.StatusNotFound)
			return
		}
		resp, err := client.Get(*api + "/v1/controllers/" + serial + "/statistics/latest")
		if err != nil {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		var latest []struct {
			MessageType     string          `json:"message_type"`
			MeasurementTime time.Time       `json:"measurement_time"`
			Data            json.RawMessage `json:"data"`
		}
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &latest) != nil {
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		for _, x := range latest {
			marks.Mark(r.Context(), hook.Serve, serial, x.MessageType, x.MeasurementTime)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"systemId": system, "latest": latest})
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	log.Printf("app proxy on %s, API %s", *listen, *api)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
