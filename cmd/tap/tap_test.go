package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

func TestSampledAgreesAcrossHopsAndKeepsAboutOneInN(t *testing.T) {
	kept := 0
	for i := 0; i < 4000; i++ {
		id := journey.ID("t", "dev", uint64(i))
		if sampled(id, 4) != sampled(id, 4) {
			t.Fatal("sampling is not deterministic")
		}
		if sampled(id, 4) {
			kept++
		}
		if !sampled(id, 1) {
			t.Fatal("n=1 must keep everything")
		}
	}
	if kept < 800 || kept > 1200 {
		t.Fatalf("kept %d of 4000 at one in 4", kept)
	}
}

func TestCountersTakeStartsANewInterval(t *testing.T) {
	c := &counters{hops: map[string]map[string]hop.Count{}, from: time.Now()}
	c.add("h", "a", false)
	c.add("h", "a", true)
	c.add("h", "b", false)
	_, _, got := c.take()
	if got["h"]["a"].N != 2 || got["h"]["a"].Errors != 1 || got["h"]["b"].N != 1 {
		t.Fatalf("%+v", got)
	}
	if _, _, again := c.take(); len(again) != 0 {
		t.Fatalf("counts carried over: %+v", again)
	}
}

func TestWatchListFollowsTheJourneyService(t *testing.T) {
	until := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"device":"dev-1","until":%q}]`, until)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wl := &watchList{}
	go wl.follow(ctx, srv.URL, 10*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for !wl.has("dev-1", time.Now()) {
		if time.Now().After(deadline) {
			t.Fatal("watch list never arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if wl.has("dev-2", time.Now()) || wl.has("dev-1", time.Now().Add(2*time.Hour)) {
		t.Fatal("watching the wrong device or past the end")
	}
}

func TestHealthCounts(t *testing.T) {
	h := &health{hops: map[string]*hopHealth{}}
	h.saw("x", 2*time.Second, true)
	h.saw("x", 5*time.Second, true)
	h.saw("x", 0, false)
	h.miss("x")
	got := h.take("x")
	if got.seen != 2 || got.unparsed != 1 || got.missed != 1 || got.maxDelay != 5*time.Second {
		t.Fatalf("%+v", got)
	}
	if again := h.take("x"); again.seen != 0 || again.missed != 0 || again.totalSeen != 2 {
		t.Fatalf("interval not reset, or totals lost: %+v", again)
	}
}

func TestTLSConfigNeedsBothHalvesOfACertificate(t *testing.T) {
	if cfg, err := tlsConfig("", "", ""); cfg != nil || err != nil {
		t.Fatalf("no TLS settings gave %v, %v", cfg, err)
	}
	if _, err := tlsConfig("", "client.pem", ""); err == nil {
		t.Fatal("a certificate without its key was accepted")
	}
	if _, err := tlsConfig("/nonexistent/ca.pem", "", ""); err == nil {
		t.Fatal("a missing CA file was accepted")
	}
}
