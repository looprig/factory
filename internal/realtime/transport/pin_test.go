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
