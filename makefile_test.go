package factory_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
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

// TestEveryFuzzTargetInTheModuleIsFuzzed is the reader the `fuzz` stage's
// package enumeration would otherwise have none of.
//
// The variable listed the ROOT package alone until A6.3 added the module's
// first out-of-root target, and the failure was silent in the direction that
// looks safe: the target compiled, `go test` ran its seed corpus, and `-fuzz`
// -- the only mode that explores past the seeds -- was never invoked on it. A
// gate measured the consequence by introducing a real divergence into the
// grammar that target compares: `go test ./... -count=1` stayed green and the
// same tree failed in 22 seconds under `-fuzz`.
//
// The Makefile's own command is EXECUTED rather than restated, so a change to
// how it enumerates is tested rather than duplicated; the expectation beside it
// is derived from the sources through modfiles, which is what makes this fail
// for a package nobody thought of. A restatement of the command would agree
// with itself forever.
func TestEveryFuzzTargetInTheModuleIsFuzzed(t *testing.T) {
	t.Parallel()

	declared := declaredFuzzTargets(t)
	if len(declared) == 0 {
		t.Fatal("no fuzz targets were found in the module's sources, so this test would pass by finding nothing")
	}
	enumerated := makefileFuzzTargets(t)

	for target, path := range declared {
		if !enumerated[target] {
			t.Errorf("%s declares %s, which the Makefile's FUZZ_TARGETS does not enumerate, so `make check` never fuzzes it", path, target)
		}
	}
	for target := range enumerated {
		if _, ok := declared[target]; !ok {
			t.Errorf("the Makefile enumerates %s, which no source file declares", target)
		}
	}
}

// declaredFuzzTargets returns every `func FuzzXxx(*testing.F)` in the module,
// keyed by "importpath|Name", with the file that declares it.
func declaredFuzzTargets(t *testing.T) map[string]string {
	t.Helper()

	module := modulePathFromGoMod(t)
	// modfiles answers with ABSOLUTE paths, so the module root is needed to get
	// back to the import path a package is known by.
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolve the module root: %v", err)
	}
	files, err := modfiles.Files(".")
	if err != nil {
		t.Fatalf("enumerate the module's files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("modfiles enumerated no file at all")
	}
	targets := map[string]string{}
	for _, path := range files {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Fuzz") {
				continue
			}
			// A fuzz TARGET takes exactly one *testing.F. A helper named
			// FuzzSomething that does not is not one, and enumerating it would
			// make this test demand that the Makefile run something `go test`
			// would refuse.
			if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
				continue
			}
			star, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			selector, ok := star.X.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "F" {
				continue
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatalf("relativize %s: %v", path, err)
			}
			targets[importPathOf(module, relative)+"|"+fn.Name.Name] = path
		}
	}
	return targets
}

// importPathOf maps a module-relative file path to its package's import path.
func importPathOf(module, path string) string {
	dir := filepath.ToSlash(filepath.Dir(path))
	if dir == "." {
		return module
	}
	return module + "/" + dir
}

func modulePathFromGoMod(t *testing.T) string {
	t.Helper()

	content, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		if path, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(path)
		}
	}
	t.Fatal("go.mod declares no module path")
	return ""
}

// makefileFuzzTargets runs the Makefile's OWN enumeration command and returns
// what it answers.
//
// The `$$` sequences are make's escape for a literal `$`, so they are undone
// before the command reaches a shell; nothing else about the line is rewritten.
func makefileFuzzTargets(t *testing.T) map[string]bool {
	t.Helper()

	content, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	const variable = "FUZZ_TARGET_LIST = "
	command := ""
	for line := range strings.SplitSeq(string(content), "\n") {
		if rest, ok := strings.CutPrefix(line, variable); ok {
			command = strings.ReplaceAll(rest, "$$", "$")
			break
		}
	}
	if command == "" {
		t.Fatalf("the Makefile declares no %s", strings.TrimSpace(variable))
	}
	output, err := exec.Command("sh", "-c", command).Output()
	if err != nil {
		t.Fatalf("running the Makefile's own enumeration (%s): %v", command, err)
	}
	targets := map[string]bool{}
	for line := range strings.SplitSeq(string(output), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			targets[line] = true
		}
	}
	return targets
}
