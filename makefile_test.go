package factory_test

import (
	"fmt"
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

// makefileRecipe returns the command lines of one target, read from the real
// Makefile. A copy of the command here would agree with itself forever.
func makefileRecipe(t *testing.T, target string) string {
	t.Helper()

	content, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	recipe, err := recipeOf(string(content), target)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return recipe
}

// recipeOf extracts one target's recipe from a Makefile's text.
//
// It is separated from the file read so it can be driven over inputs this
// Makefile does not contain: A6.1's re-gate flagged the scan as a hazard when
// the helper is COPIED into another component, which is a foreseeable outcome
// -- two repositories have already copied the -count=1 pattern this helper
// guards. The rules it implements are make's own:
//
//   - a target line is `name:` at the start of a line, but `name:=` is a
//     variable assignment and not a target;
//   - a recipe line begins with a TAB;
//   - a blank line does not end a recipe, so it is skipped rather than
//     terminating -- and skipping cannot run past the next target, because a
//     target line is by definition not tab-prefixed and therefore stops it.
func recipeOf(content, target string) (string, error) {
	lines := strings.Split(content, "\n")
	start := -1
	for i, line := range lines {
		rest, ok := strings.CutPrefix(line, target+":")
		if !ok || strings.HasPrefix(rest, "=") {
			continue
		}
		start = i + 1
		break
	}
	if start < 0 {
		return "", fmt.Errorf("the Makefile declares no target %q", target)
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
		return "", fmt.Errorf("target %q has an empty recipe", target)
	}
	return strings.Join(recipe, "\n"), nil
}

// TestTheRecipeReaderStopsAtTheNextTarget drives recipeOf over the shapes this
// Makefile does not have, so the helper is safe to lift into another component.
//
// The `:=` row is the one that was wrong: `strings.HasPrefix(line, target+":")`
// also matches a variable assignment spelled without spaces, so a Makefile with
// a `test := ...` line above the `test:` target read the assignment as the
// target and reported an empty recipe -- a false failure in the guard that
// exists to prevent false passes.
func TestTheRecipeReaderStopsAtTheNextTarget(t *testing.T) {
	t.Parallel()

	const makefile = "test:=stale\n" +
		"test:\n" +
		"\tgo test -count=1 ./...\n" +
		"\n" +
		"\techo still the same recipe\n" +
		"\n" +
		"check: test\n" +
		"\techo other\n"

	recipe, err := recipeOf(makefile, "test")
	if err != nil {
		t.Fatalf("recipeOf(_, %q): %v", "test", err)
	}
	if !strings.Contains(recipe, "go test -count=1 ./...") {
		t.Errorf("the recipe does not contain the target's own command:\n%s", recipe)
	}
	// A blank line does not end a make recipe, so the second command belongs to
	// `test` and must be collected...
	if !strings.Contains(recipe, "echo still the same recipe") {
		t.Errorf("a blank line ended the recipe, but make does not:\n%s", recipe)
	}
	// ...while the next TARGET does end it, blank lines in between or not.
	if strings.Contains(recipe, "echo other") {
		t.Errorf("the recipe ran past the `check` target boundary:\n%s", recipe)
	}
	if _, err := recipeOf(makefile, "absent"); err == nil {
		t.Error("recipeOf reported a recipe for a target the Makefile does not declare")
	}
}
