package journey

import (
	"os/exec"
	"strings"
	"testing"
)

// The Node hook derives the same journey IDs as Go.
func TestNodeHookAgrees(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	script := `import { journeyId } from './pkg/hook/node/hopwake-hook.ts';
console.log(journeyId('acme', 'dev-00042', 1683), journeyId('acme', 'dev-00042', 1683, '7'), journeyId('t', 'dévice', 18446744073709551615n));`
	cmd := exec.Command(node, "--input-type=module", "-e", script)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	want := strings.Join([]string{ID("acme", "dev-00042", 1683), BootID("acme", "dev-00042", "7", 1683), ID("t", "dévice", 18446744073709551615)}, " ")
	if got := strings.TrimSpace(string(out)); got != want {
		t.Fatalf("node %s\n  go %s", got, want)
	}
}
