package fleet

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	got, err := Parse(strings.NewReader("# site-00\ndev-00000\n\n  dev-00001  \ndev-00000\n"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "dev-00000,dev-00001" {
		t.Fatalf("got %v", got)
	}
}

func TestParseRejectsBadIDs(t *testing.T) {
	for _, in := range []string{"dev 00001\n", "dev/00001\n", strings.Repeat("x", 129) + "\n"} {
		if _, err := Parse(strings.NewReader(in)); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
}

func TestReferenceFleetFile(t *testing.T) {
	got, err := Load("../../reference/fleet.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || got[0] != "dev-00000" || got[49] != "dev-00049" {
		t.Fatalf("got %d devices, first %q", len(got), got[0])
	}
}
