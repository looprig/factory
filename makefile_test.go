package factory_test

import (
	"os"
	"strings"
	"testing"
)

// TestTheTestTargetCannotReplayACachedResult is the reader the `-count=1` in
// the Makefile would otherwise have none of.
//
// It is a one-word deletion with no visible consequence on a healthy tree, and
// the consequence on an unhealthy one is that `make check` reports success on
// code it never ran. A6.1's gate measured it: the case establishing that
// task's headline finding failed in 8 of 14 clean runs, and a clean `make
// check` exited 0 having printed `ok … (cached)` for that very package. No
// behavioural test can catch this, because the thing that goes wrong is that a
// test is NOT EXECUTED.
//
// The recipe is read from the Makefile rather than restated, so moving the flag
// to a variable or another position still passes and removing it does not.
func TestTheTestTargetCannotReplayACachedResult(t *testing.T) {
	t.Parallel()

	recipe := makefileRecipe(t, "test")
	if !strings.Contains(recipe, "go test") {
		t.Fatalf("the `test` recipe does not run `go test`:\n%s", recipe)
	}
	if !strings.Contains(recipe, "-count=1") {
		t.Errorf("the `test` recipe does not pass -count=1, so `make check` may replay a cached pass instead of running the suite:\n%s", recipe)
	}
}

// makefileRecipe returns the command lines of one target.
//
// A recipe line is a line beginning with a TAB, which is make's own rule; the
// recipe ends at the first line that is neither a tab line nor blank. Reading
// the real file is the point -- a copy of the command here would agree with
// itself forever.
func makefileRecipe(t *testing.T, target string) string {
	t.Helper()

	content, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	lines := strings.Split(string(content), "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, target+":") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("the Makefile declares no target %q", target)
	}
	var recipe []string
	for _, line := range lines[start:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, "\t") {
			break
		}
		recipe = append(recipe, line)
	}
	if len(recipe) == 0 {
		t.Fatalf("target %q has an empty recipe", target)
	}
	return strings.Join(recipe, "\n")
}
