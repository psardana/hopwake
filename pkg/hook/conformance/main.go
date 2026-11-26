// Command conformance runs the hook conformance script (docs/hooks.md) with
// the Go hook. Run it under hookcheck:
//
//	go run ./cmd/hookcheck -- go run ./pkg/hook/conformance
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/psardana/hopwake/pkg/hook"
)

func main() {
	ctx := context.Background()
	h, err := hook.New(ctx, hook.Config{Tenant: os.Getenv("HOPWAKE_TENANT"), Hop: os.Getenv("HOPWAKE_HOP"), Service: "conformance-go"})
	if err != nil {
		log.Fatal(err)
	}
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	at := time.Date(2026, 11, 18, 10, 0, 0, 750_000_000, time.UTC)
	const typ = "ems.measurements.Measurement"
	must(h.Mark(ctx, hook.Store, "dev-00042", 1683))
	must(h.MarkBoot(ctx, hook.Store, "dev-00042", "7", 12))
	must(h.Mark(ctx, hook.Serve, "dev-00042", 1683))
	must(h.Mark(ctx, hook.Serve, "dev-00042", 1683)) // the same reading served again: not sent
	must(h.MarkTime(ctx, hook.Store, "EMS0000042", typ, at))
	must(h.MarkTimeTeach(ctx, hook.Store, "EMS0000042", "systemId", "100042", typ, at))
	// The alias list arrives shortly after the hook starts.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := h.MarkTimeAlias(ctx, hook.Serve, "systemId", "100042", typ, at)
		if err == nil {
			break
		}
		if !errors.Is(err, hook.ErrUnknownAlias) || time.Now().After(deadline) {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := h.MarkTimeAlias(ctx, hook.Serve, "systemId", "999999", typ, at); !errors.Is(err, hook.ErrUnknownAlias) {
		log.Fatalf("an unknown alias gave %v, want ErrUnknownAlias", err)
	}
	must(h.Close(ctx))
}
