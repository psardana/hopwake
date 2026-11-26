// Package masterdata reads a device-to-system mapping kept as CSV
// ("serial,systemId" per line), the stand-in for a master-data cache.
package masterdata

import (
	"bufio"
	"os"
	"strings"
)

// Map holds both directions of the mapping.
type Map struct {
	bySerial map[string]string
	bySystem map[string]string
}

// Load reads the file; lines starting with # are comments.
func Load(path string) (*Map, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	m := &Map{bySerial: map[string]string{}, bySystem: map[string]string{}}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		serial, system, ok := strings.Cut(line, ",")
		if !ok {
			continue
		}
		serial, system = strings.TrimSpace(serial), strings.TrimSpace(system)
		m.bySerial[serial], m.bySystem[system] = system, serial
	}
	return m, sc.Err()
}

// System returns a device's system ID.
func (m *Map) System(serial string) (string, bool) { s, ok := m.bySerial[serial]; return s, ok }

// Serial returns the device of a system ID.
func (m *Map) Serial(system string) (string, bool) { s, ok := m.bySystem[system]; return s, ok }
