// Command portalbackend serves a partner portal's monitoring view, the way
// a portal monitoring backend does: it asks the measurement API for a
// system's recent time series and returns power figures. It marks the
// readings it hands to the portal served, by system ID alias, through a
// Hopwake hook when HOPWAKE_OTLP_ENDPOINT is set.
//
//	GET /api/portal/monitoring/{systemId}/power?minutes=5
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
	"github.com/psardana/hopwake/reference/served"
)

func main() {
	listen := flag.String("listen", env("PORTAL_LISTEN", ":8083"), "HTTP address")
	api := flag.String("api", env("PORTAL_API", "http://localhost:8081"), "measurement API base URL")
	seriesType := flag.String("series-type", env("PORTAL_SERIES_TYPE", ""), "message type the time series readings started as")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	marks := served.New(ctx, "hook:portal.served", "portalbackend")
	defer marks.Close()
	client := &http.Client{Timeout: 5 * time.Second}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/portal/monitoring/{systemId}/power", func(w http.ResponseWriter, r *http.Request) {
		system := r.PathValue("systemId")
		minutes := r.URL.Query().Get("minutes")
		if minutes == "" {
			minutes = "5"
		}
		resp, err := client.Get(*api + "/v1/systems/" + system + "/measurements/raw?minutes=" + minutes)
		if err != nil {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		var points []struct {
			Time time.Time `json:"time"`
			Data struct {
				HouseConsumptionKwh    float64 `json:"houseConsumptionKwh"`
				HouseholdGenerationKwh float64 `json:"householdGenerationKwh"`
				BatteryLevelPercent    float64 `json:"batteryLevelPercent"`
			} `json:"data"`
		}
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &points) != nil {
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		type power struct {
			Time        time.Time `json:"time"`
			Consumption float64   `json:"consumptionKwh"`
			Generation  float64   `json:"generationKwh"`
			Battery     float64   `json:"batteryPercent"`
		}
		out := make([]power, 0, len(points))
		for _, p := range points {
			out = append(out, power{p.Time, p.Data.HouseConsumptionKwh, p.Data.HouseholdGenerationKwh, p.Data.BatteryLevelPercent})
			marks.MarkAlias(r.Context(), hook.Serve, "systemId", system, *seriesType, p.Time)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	log.Printf("portal backend on %s, API %s", *listen, *api)
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
