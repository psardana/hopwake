// Package alias resolves other names a device goes by further down a
// pipeline, such as a system ID that replaces the device serial in a store,
// back to the device. Hopwake learns each pair at the hop that knows both;
// a Resolver keeps a copy of that list from the journey service.
package alias

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Pair is one alias of a device.
type Pair struct {
	Kind   string `json:"kind"`  // e.g. "systemId"
	Value  string `json:"value"` // e.g. "100042"
	Device string `json:"device"`
}

// Format writes an alias as it travels on hop events: "kind:value".
func Format(kind, value string) string { return kind + ":" + value }

// Parse reads "kind:value".
func Parse(s string) (kind, value string, ok bool) {
	kind, value, ok = strings.Cut(s, ":")
	return kind, value, ok && kind != "" && value != ""
}

// Resolver looks aliases up in a list it refreshes from the journey service.
type Resolver struct {
	url   string
	mu    sync.RWMutex
	pairs map[string]string // kind:value → device
}

// NewResolver follows the alias list at base + "/api/aliases" until ctx ends.
func NewResolver(ctx context.Context, base string, every time.Duration) *Resolver {
	r := &Resolver{url: strings.TrimRight(base, "/") + "/api/aliases", pairs: map[string]string{}}
	go r.follow(ctx, every)
	return r
}

// Device returns the device an alias stands for.
func (r *Resolver) Device(kind, value string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.pairs[Format(kind, value)]
	return d, ok
}

// Set adds a pair directly, for tests and for services that know it.
func (r *Resolver) Set(p Pair) {
	r.mu.Lock()
	r.pairs[Format(p.Kind, p.Value)] = p.Device
	r.mu.Unlock()
}

func (r *Resolver) follow(ctx context.Context, every time.Duration) {
	client := &http.Client{Timeout: 3 * time.Second}
	logged := false
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
		if resp, err := client.Do(req); err == nil {
			var list []Pair
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&list) == nil {
				m := make(map[string]string, len(list))
				for _, p := range list {
					m[Format(p.Kind, p.Value)] = p.Device
				}
				r.mu.Lock()
				r.pairs = m
				r.mu.Unlock()
				if !logged && len(list) > 0 {
					log.Printf("aliases: %d known", len(list))
					logged = true
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
