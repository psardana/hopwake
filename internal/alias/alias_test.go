package alias

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolverFollowsTheList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"kind":"systemId","value":"100042","device":"EMS0000042"}]`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewResolver(ctx, srv.URL, 10*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if d, ok := r.Device("systemId", "100042"); ok {
			if d != "EMS0000042" {
				t.Fatalf("got %s", d)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never resolved")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := r.Device("systemId", "999"); ok {
		t.Fatal("resolved an unknown alias")
	}
	if k, v, ok := Parse("systemId:100042"); !ok || k != "systemId" || v != "100042" {
		t.Fatal("parse")
	}
}
