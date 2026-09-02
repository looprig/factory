package factory_test

import (
	"os"
	"strings"
	"testing"
)

// The prose documents the import boundary in two places, and prose drifts.
// Rule ADDITION is otherwise wired shut -- the case table, sampleImports and
// the fuzz targets all fail on a rule they have not been told about -- but
// nothing stopped a sixth rule from leaving README.md and CLAUDE.md describing
// five. This ties them to the declared list.
//
// The blocks are delimited by HTML comments rather than found by pattern,
// because a guard that hunts for markdown it recognises is defeated by
// reformatting -- the same failure this module has now fixed twice, once for
// go.mod's block form and once for quoted tokens.
const (
	rulesBlockBegin = "<!-- boundary-rules:begin -->"
	rulesBlockEnd   = "<!-- boundary-rules:end -->"
)

func TestDocumentedRulesMatchTheDeclaredRules(t *testing.T) {
	t.Parallel()

	for _, document := range []string{"README.md", "CLAUDE.md"} {
		t.Run(document, func(t *testing.T) {
			t.Parallel()

			lines := ruleBlockLines(t, document)
			if len(lines) != len(boundaryRules) {
				t.Fatalf("%s documents %d rules, but %d are declared:\n  %q",
					document, len(lines), len(boundaryRules), lines)
			}
			for _, rule := range boundaryRules {
				matching := 0
				for _, line := range lines {
					if !strings.Contains(line, rule.docToken) {
						continue
					}
					matching++
					if rule.scope == "" {
						if !strings.Contains(line, "nowhere") && !strings.Contains(line, "everywhere") {
							t.Errorf("%s documents %q without saying it is forbidden everywhere: %q", document, rule.name, line)
						}
						continue
					}
					if !strings.Contains(line, rule.scope) {
						t.Errorf("%s documents %q without naming its scope %q: %q", document, rule.name, rule.scope, line)
					}
				}
				if matching != 1 {
					t.Errorf("%s has %d lines mentioning %q for rule %q, want exactly 1", document, matching, rule.docToken, rule.name)
				}
			}
		})
	}
}

// TestEveryRuleHasADocTokenItsOwnRuleMatches stops the tie above from being
// satisfied by a token that no longer describes the rule. A docToken is a claim
// about what the rule forbids, so the rule must forbid it.
func TestEveryRuleHasADocTokenItsOwnRuleMatches(t *testing.T) {
	t.Parallel()

	if len(boundaryRules) == 0 {
		t.Fatal("no rules are declared, so this test is vacuous")
	}
	seen := map[string]bool{}
	for _, rule := range boundaryRules {
		if strings.TrimSpace(rule.docToken) == "" {
			t.Errorf("rule %q has no docToken, so no document can be held to it", rule.name)
			continue
		}
		if seen[rule.docToken] {
			t.Errorf("docToken %q is used by more than one rule, so a document line cannot be attributed", rule.docToken)
		}
		seen[rule.docToken] = true
		if !rule.matches(rule.docToken) {
			t.Errorf("rule %q documents itself as %q, but its own matcher does not match that", rule.name, rule.docToken)
		}
	}
}

func ruleBlockLines(t *testing.T, document string) []string {
	t.Helper()

	content, err := os.ReadFile(document)
	if err != nil {
		t.Fatalf("read %s: %v", document, err)
	}
	text := string(content)
	begin := strings.Index(text, rulesBlockBegin)
	end := strings.Index(text, rulesBlockEnd)
	if begin < 0 || end < 0 || end < begin {
		t.Fatalf("%s has no %s ... %s block; the documented rules are then held to nothing", document, rulesBlockBegin, rulesBlockEnd)
	}
	if strings.Count(text, rulesBlockBegin) != 1 || strings.Count(text, rulesBlockEnd) != 1 {
		t.Fatalf("%s has more than one boundary-rules block, so which one is authoritative is undefined", document)
	}
	var lines []string
	for _, line := range strings.Split(text[begin+len(rulesBlockBegin):end], "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		t.Fatalf("%s has an empty boundary-rules block", document)
	}
	return lines
}
