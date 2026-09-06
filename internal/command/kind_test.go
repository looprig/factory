package command_test

import (
	"testing"

	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

// TestTheKindsAreTheDurableSpellings pins each value as an ABSOLUTE LITERAL.
//
// This is the only place the strings are stated twice, and it has to be, for
// the reason the whole package exists: every other reader now names the
// constant, so a constant whose value changed would move every reader with it
// and nothing would fail. These are wire-visible durable record values shared
// with Host, so changing one is a data-format change and must be a deliberate
// edit to this table rather than a rename that compiles.
func TestTheKindsAreTheDurableSpellings(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		kind sessionstore.CommandKind
		want string
	}{
		{command.KindCreateSession, "create"},
		{command.KindInput, "input"},
		{command.KindInterrupt, "interrupt"},
		{command.KindRestore, "restore"},
		{command.KindGateResponse, "gate_response"},
	} {
		if string(tt.kind) != tt.want {
			t.Errorf("kind = %q, want %q", tt.kind, tt.want)
		}
	}
}

// TestKindsEnumeratesEveryKindExactlyOnce is the anti-drift half: a kind added
// to the constants and not to Kinds would be invisible to every caller that
// sweeps the vocabulary, and a duplicate would make a sweep run one case twice
// while claiming to cover another.
func TestKindsEnumeratesEveryKindExactlyOnce(t *testing.T) {
	t.Parallel()

	kinds := command.Kinds()
	seen := make(map[sessionstore.CommandKind]int, len(kinds))
	for _, kind := range kinds {
		if kind == "" {
			t.Error("Kinds() contains an empty kind")
		}
		seen[kind]++
	}
	for kind, count := range seen {
		if count != 1 {
			t.Errorf("Kinds() names %q %d times, want once", kind, count)
		}
	}
	// An absolute count, so adding a constant without adding it here fails.
	if len(kinds) != 5 {
		t.Errorf("Kinds() has %d entries, want 5", len(kinds))
	}
}

// TestKindsCannotBeMutatedByACaller holds the "function, not variable" claim.
func TestKindsCannotBeMutatedByACaller(t *testing.T) {
	t.Parallel()

	first := command.Kinds()
	first[0] = "tampered"
	if got := command.Kinds()[0]; got != command.KindCreateSession {
		t.Errorf("Kinds()[0] = %q after a caller wrote to a previous result, want %q", got, command.KindCreateSession)
	}
}
