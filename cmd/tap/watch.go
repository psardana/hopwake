package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

// watchList is the set of devices whose payloads the tap captures, read
// from the journey service, each until a time.
type watchList struct {
	mu    sync.RWMutex
	until map[string]time.Time
}

func (w *watchList) has(device string, now time.Time) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	t, ok := w.until[device]
	return ok && now.Before(t)
}

func (w *watchList) set(m map[string]time.Time) {
	w.mu.Lock()
	w.until = m
	w.mu.Unlock()
}

// follow polls url for the watch list until ctx ends. When the journey
// service cannot be reached, the old list stays until its times run out.
func (w *watchList) follow(ctx context.Context, url string, every time.Duration) {
	client := &http.Client{Timeout: 3 * time.Second}
	last := ""
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := client.Do(req); err == nil {
			var list []struct {
				Device string    `json:"device"`
				Until  time.Time `json:"until"`
			}
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&list) == nil {
				m := map[string]time.Time{}
				for _, x := range list {
					m[x.Device] = x.Until
				}
				w.set(m)
				if b, _ := json.Marshal(list); string(b) != last {
					log.Printf("watch list: %d devices", len(list))
					last = string(b)
				}
			}
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}
