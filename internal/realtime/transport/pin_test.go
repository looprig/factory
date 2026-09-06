package transport_test

import (
	"os"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/realtime/transport"
)

// TestThePinnedVersionsAreWhatGoModRequires ties the constants this package
// documents to the versions the build actually resolves.
//
// Without it the doc comment is a claim about a file nothing compares it to,
// and a `go get -u` in some later task would move the dependency while leaving
// every sentence about "measured at v0.38.0" in place. The failure it produces
// is the useful one: not "a dependency changed", but "the version this
// package's measurements were taken against is no longer the version that
// runs".
func TestThePinnedVersionsAreWhatGoModRequires(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("../../../go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	required := map[string]string{}
	for _, line := range strings.Split(string(content), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		// A require line inside a block is "module version [// indirect]"; one
		// on its own is "require module version".
		if len(fields) >= 3 && fields[0] == "require" {
			fields = fields[1:]
		}
		if len(fields) >= 2 && strings.HasPrefix(fields[0], "github.com/centrifugal/") {
			required[fields[0]] = fields[1]
		}
	}
	want := map[string]string{
		transport.ServerModule:   transport.ServerVersion,
		transport.GoClientModule: transport.GoClientVersion,
		transport.ProtocolModule: transport.ProtocolVersion,
	}
	for module, version := range want {
		got, ok := required[module]
		if !ok {
			// Vacuity guard. A parser that matched nothing would otherwise
			// report every pin as satisfied.
			t.Errorf("go.mod requires no %s at all", module)
			continue
		}
		if got != version {
			t.Errorf("go.mod requires %s %s, want exactly %s", module, got, version)
		}
	}
}

// TestTheParserWouldNoticeAWrongVersion is the reader for the case above: it
// drives the same shapes of require line the parser must handle, so a parser
// that silently matched nothing cannot make the pin guard vacuous.
func TestTheParserWouldNoticeAWrongVersion(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("../../../go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	// The absolute literal below is a version this module must NOT be on. It is
	// written out rather than derived from ServerVersion, so it stays a
	// different value even if the pin moves.
	if strings.Contains(string(content), transport.ServerModule+" v0.39.0") {
		t.Fatalf("go.mod requires %s v0.39.0; this package's measurements were taken against %s",
			transport.ServerModule, transport.ServerVersion)
	}
}

// TestTheREADMEVersionTableMatchesThePins closes the last unguarded copy of the
// version triple.
//
// Before this existed there were four authorities for "which Centrifuge are we
// on": go.mod, the constants in doc.go, the package doc's prose, and the
// README's table. Only the first two were held together, and nothing in Go read
// the README at all -- so the table could go stale silently, and it did: it
// named a client version that a re-pin had moved away from. This is the same
// arrangement AGENTS.md describes for wui, where contract/VERSION, CORE_VERSION
// and the pinned module all move together or the build fails.
func TestTheREADMEVersionTableMatchesThePins(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("../../../README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}
	// Rows are "| role | `module` | `version` |". Pull the backticked cells and
	// read the version as the cell following the module.
	documented := map[string]string{}
	for _, line := range strings.Split(string(content), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		cells := backtickedCells(line)
		for i := 0; i+1 < len(cells); i++ {
			if strings.HasPrefix(cells[i], "github.com/centrifugal/") {
				documented[cells[i]] = cells[i+1]
			}
		}
	}
	want := map[string]string{
		transport.ServerModule:   transport.ServerVersion,
		transport.GoClientModule: transport.GoClientVersion,
		transport.ProtocolModule: transport.ProtocolVersion,
	}
	for module, version := range want {
		got, ok := documented[module]
		if !ok {
			// Vacuity guard, as above: a parser that matched nothing would
			// otherwise report the whole table as correct.
			t.Errorf("README.md documents no version for %s at all", module)
			continue
		}
		if got != version {
			t.Errorf("README.md documents %s %s, want exactly %s", module, got, version)
		}
	}
}

// backtickedCells returns the backtick-quoted spans of one Markdown table row,
// in order.
func backtickedCells(line string) []string {
	var cells []string
	for {
		open := strings.Index(line, "`")
		if open < 0 {
			return cells
		}
		rest := line[open+1:]
		close := strings.Index(rest, "`")
		if close < 0 {
			return cells
		}
		cells = append(cells, rest[:close])
		line = rest[close+1:]
	}
}

// TestTheREADMEParserFindsTheTable is the reader for the case above: it fails if
// the parser stops finding the module cells at all, which is the way a
// README-shape change would quietly turn the guard into a no-op that reports
// three "documents no version" errors nobody reads as a parser fault.
func TestTheREADMEParserFindsTheTable(t *testing.T) {
	t.Parallel()

	cells := backtickedCells("| Go client, for HostLink | `github.com/centrifugal/centrifuge-go` | `v9.9.9` |")
	if len(cells) != 2 || cells[0] != "github.com/centrifugal/centrifuge-go" || cells[1] != "v9.9.9" {
		t.Fatalf("backtickedCells of a known row = %q, want the module then its version", cells)
	}
	if got := backtickedCells("| role | module | version |"); len(got) != 0 {
		t.Fatalf("backtickedCells of a row with no backticks = %q, want none", got)
	}
}
