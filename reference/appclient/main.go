// Command appclient plays the people who look at the data: app users who
// open their system's dashboard through the app proxy, and partners who
// watch a system's power figures in the portal.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	proxy := flag.String("proxy", env("CLIENT_PROXY", "http://localhost:8082"), "app proxy base URL")
	portal := flag.String("portal", env("CLIENT_PORTAL", "http://localhost:8083"), "portal backend base URL")
	appDevices := flag.String("app-systems", env("CLIENT_APP_SYSTEMS", ""), "comma-separated system IDs whose owners use the app")
	portalDevices := flag.String("portal-systems", env("CLIENT_PORTAL_SYSTEMS", ""), "comma-separated system IDs a partner watches in the portal")
	appEvery := flag.Duration("app-every", 5*time.Second, "how often an app refreshes the live view")
	portalEvery := flag.Duration("portal-every", 20*time.Second, "how often the portal refreshes history")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Timeout: 5 * time.Second}
	var ok, failed atomic.Uint64
	get := func(url string) {
		resp, err := client.Get(url)
		if err != nil {
			failed.Add(1)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			ok.Add(1)
		} else {
			failed.Add(1)
		}
	}
	loop := func(every time.Duration, url func(string) string, devices string) {
		for _, d := range strings.Split(devices, ",") {
			if d = strings.TrimSpace(d); d == "" {
				continue
			}
			go func() {
				t := time.NewTicker(every)
				defer t.Stop()
				for {
					get(url(d))
					select {
					case <-ctx.Done():
						return
					case <-t.C:
					}
				}
			}()
		}
	}
	loop(*appEvery, func(d string) string { return *proxy + "/measurements/api/v1/systems/" + d + "/dashboard" }, *appDevices)
	loop(*portalEvery, func(d string) string { return *portal + "/api/portal/monitoring/" + d + "/power?minutes=5" }, *portalDevices)
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Printf("%d requests answered, %d failed", ok.Load(), failed.Load())
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
