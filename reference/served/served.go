// Package served marks readings served to someone, each one once, through
// a Hopwake hook. It does nothing when HOPWAKE_OTLP_ENDPOINT is not set.
package served

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/pkg/hook"
)

// Marker remembers what it has marked, so a reading asked for again and
// again is marked once.
type Marker struct {
	h    *hook.Hook
	mu   sync.Mutex
	seen map[string]bool
}

// New starts a marker for one hop.
func New(ctx context.Context, hopName, service string) *Marker {
	m := &Marker{seen: map[string]bool{}}
	if os.Getenv("HOPWAKE_OTLP_ENDPOINT") == "" {
		return m
	}
	tenant := os.Getenv("HOPWAKE_TENANT")
	if tenant == "" {
		tenant = "reference"
	}
	h, err := hook.New(ctx, hook.Config{Tenant: tenant, Hop: hopName, Service: service})
	if err != nil {
		log.Fatal(err)
	}
	m.h = h
	return m
}

func (m *Marker) first(k string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.seen) > 200000 {
		m.seen = map[string]bool{}
	}
	if m.seen[k] {
		return false
	}
	m.seen[k] = true
	return true
}

// Mark marks a reading of a device.
func (m *Marker) Mark(ctx context.Context, op hop.Op, device, messageType string, at time.Time) {
	if m.h != nil && m.first(device+"|"+messageType+"|"+at.UTC().Format(time.RFC3339Nano)) {
		_ = m.h.MarkTime(ctx, op, device, messageType, at)
	}
}

// MarkAlias marks a reading known only by an alias of its device. A reading
// whose alias Hopwake does not know yet is tried again next time.
func (m *Marker) MarkAlias(ctx context.Context, op hop.Op, kind, value, messageType string, at time.Time) {
	if m.h == nil {
		return
	}
	k := kind + ":" + value + "|" + messageType + "|" + at.UTC().Format(time.RFC3339Nano)
	if !m.first(k) {
		return
	}
	if err := m.h.MarkTimeAlias(ctx, op, kind, value, messageType, at); err != nil {
		m.mu.Lock()
		delete(m.seen, k)
		m.mu.Unlock()
	}
}

// Close sends what is queued.
func (m *Marker) Close() {
	if m.h != nil {
		_ = m.h.Close(context.Background())
	}
}
