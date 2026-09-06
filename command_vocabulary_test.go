package factory_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
)

// The one file allowed to spell a command kind as a string.
const commandVocabularyFile = "internal/command/kind.go"

// commandKindLiteral is one production spelling of a durable command kind.
type commandKindLiteral struct {
	// file is relative to the module root, with forward slashes.
	file string
	// value is the decoded string, so a rewritten escape is still the same
	// spelling and cannot look like a different kind.
	value string
}

// findCommandKindLiterals reports every place a production file writes a
// sessionstore.CommandKind as a STRING rather than naming internal/command's
// constant.
//
// It works on the PARSED file for the same reason import_boundary_test.go
// does: a grep over source text is defeated by a line break or a comment, and
// cannot tell a declaration from prose mentioning one. Two shapes are
// recognised, because both produce a second copy of the vocabulary:
//
//   - a const or var spec typed `…CommandKind` whose value is a string literal;
//   - a conversion `…CommandKind("create")`.
//
// The selector's PACKAGE qualifier is deliberately not matched: an import alias
// would defeat that, and no other CommandKind is in play in this module.
//
// Test files are excluded on purpose, and the exclusion is not free. Its cost,
// stated so a later reader does not have to rediscover it: a vocabulary copy
// declared in an INTERNAL test file of a production package would not be
// reported. No such copy exists today.
//
// It cannot simply be dropped. internal/command/kind_test.go must state all
// five as absolute literals -- that is the only thing that could notice a value
// changing -- and other suites legitimately drive the wire strings directly in
// a shape this detector recognises: guard_composition_test.go:175 converts
// `sessionstore.CommandKind("input")` to exercise the seam with a value the
// production code is not allowed to invent. Scanning test files would report
// those as copies, which would make the guard's output noise rather than a
// finding. The exclusion buys a true signal at the price of that one blind
// spot, and the blind spot is a test file, which cannot admit an RPC.
func findCommandKindLiterals(t *testing.T, root string) []commandKindLiteral {
	t.Helper()

	files, err := modfiles.Files(root)
	if err != nil {
		t.Fatalf("modfiles.Files(%q): %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("modfiles.Files(%q) returned no files; a scan over nothing cannot report anything", root)
	}

	var found []commandKindLiteral
	scanned := 0
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		scanned++
		parsed, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		relative, err := filepath.Rel(root, file)
		if err != nil {
			t.Fatalf("relative path of %s: %v", file, err)
		}
		relative = filepath.ToSlash(relative)

		record := func(expr ast.Expr) {
			lit, ok := expr.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("%s: unquote %s: %v", relative, lit.Value, err)
			}
			found = append(found, commandKindLiteral{file: relative, value: value})
		}

		ast.Inspect(parsed, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				if !isCommandKindType(n.Type) {
					return true
				}
				for _, value := range n.Values {
					record(value)
				}
			case *ast.CallExpr:
				if !isCommandKindType(n.Fun) || len(n.Args) != 1 {
					return true
				}
				record(n.Args[0])
			}
			return true
		})
	}
	if scanned == 0 {
		t.Fatalf("no production file under %q was parsed", root)
	}
	return found
}

// isCommandKindType reports whether an expression names the CommandKind type.
func isCommandKindType(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel != nil && selector.Sel.Name == "CommandKind"
}

// TestTheCommandVocabularyIsSpelledInExactlyOnePlace is the reader
// internal/command's package doc claimed to have and did not.
//
// The doc says two packages holding two private copies would satisfy every test
// either package could write while letting an RPC be admitted under a kind no
// route serves. That was TRUE OF THIS MODULE while it said it:
// internal/admission declared all five as its own absolute literals, and
// internal/admission is precisely where the ClientLink RPCs are routed. Nothing
// tied the two sets together -- no alias, no import, no assertion -- so the
// hazard the package was created to remove was still present in the package's
// most important consumer.
//
// A guard naming the packages it knows about could not have caught that one and
// cannot catch the next, so this derives its subject from the module: EVERY
// production file, enumerated by modfiles, parsed, with the literals decoded.
func TestTheCommandVocabularyIsSpelledInExactlyOnePlace(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}

	found := findCommandKindLiterals(t, root)
	for _, literal := range found {
		if literal.file != commandVocabularyFile {
			t.Errorf("%s spells the command kind %q as a literal; name internal/command's constant instead, or the two copies can drift into an RPC admitted under a kind no route serves",
				literal.file, literal.value)
		}
	}

	// The anti-vacuity half, and it is not decoration: a detector that matched
	// nothing would pass the loop above in silence. The one sanctioned file
	// must hold the whole vocabulary, exactly once each.
	want := []string{"create", "gate_response", "input", "interrupt", "restore"}
	var got []string
	for _, literal := range found {
		if literal.file == commandVocabularyFile {
			got = append(got, literal.value)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("%s spells %v, want exactly %v", commandVocabularyFile, got, want)
	}
}

// TestTheVocabularyScanReportsACopyItHasNeverSeen drives the detector over a
// fixture, because the assertion above passes on a clean tree whether the scan
// works or not.
//
// The fixture carries both recognised shapes and two controls: a file naming
// the CONSTANT -- which is the fix, and must not be reported -- and a file
// whose string is typed as something else.
func TestTheVocabularyScanReportsACopyItHasNeverSeen(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.24\n")
	write("declared.go", `package fixture

import "github.com/looprig/sessionstore"

const CommandCreate sessionstore.CommandKind = "create"
`)
	write("converted.go", `package fixture

import store "github.com/looprig/sessionstore"

func kind() store.CommandKind { return store.CommandKind("gate_response") }
`)
	write("named.go", `package fixture

import (
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

const Restore sessionstore.CommandKind = command.KindRestore
`)
	write("unrelated.go", `package fixture

const Something string = "interrupt"
`)

	found := findCommandKindLiterals(t, root)
	var got []string
	for _, literal := range found {
		got = append(got, literal.file+":"+literal.value)
	}
	slices.Sort(got)
	want := []string{"converted.go:gate_response", "declared.go:create"}
	if !slices.Equal(got, want) {
		t.Errorf("the scan reported %v over the fixture, want %v", got, want)
	}
}
