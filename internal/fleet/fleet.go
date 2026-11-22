// Package fleet reads the list of devices that are expected to report, so
// that a device which never sent anything shows up as a problem instead of
// simply being absent.
package fleet

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var validID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// Load reads a fleet file: one device ID per line; blank lines and lines
// starting with # are ignored. Repeats are kept once, in first-seen order.
func Load(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads a fleet list from r.
func Parse(r io.Reader) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !validID.MatchString(line) {
			return nil, fmt.Errorf("fleet: line %d: %q is not a device ID", n, line)
		}
		if !seen[line] {
			seen[line] = true
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("fleet: %w", err)
	}
	return out, nil
}
