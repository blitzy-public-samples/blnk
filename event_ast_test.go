/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blnk

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Structural assertions over the event pipeline's own source, for the small set of
// properties that are genuinely about STRUCTURE rather than about behaviour.
//
// # Why these exist, and what they replaced
//
// A handful of properties in this package are absences: "the relay does not sleep a retry
// delay in process", "the relay does not own a second copy of the sunset decision", "this
// test file forms no opinion of its own about the boundary it tests". An absence cannot be
// observed by running the code — a second sunset comparison is invisible to a behavioural
// test for exactly as long as it happens to agree with the first, which is the whole
// interval before somebody changes one of them.
//
// They used to be asserted with strings.Contains over the file's text, and that instrument
// is wrong in both directions:
//
//	FALSE POSITIVES. The text includes comments and string literals. A comment explaining
//	"the relay must never call time.Sleep" fails an assertion that the file must not contain
//	"time.Sleep" — so the documentation and the test that documents the same rule cannot
//	coexist. One scan was reduced to assembling its needles from concatenated fragments
//	precisely so it would not match itself, which is the point at which a check has stopped
//	being a check.
//
//	FALSE NEGATIVES. A substring pins one spelling. `.Record(` misses `Record (`, misses a
//	method value passed as a function, and misses an equivalent instrument written through an
//	alias. A byte-distance check like "the guard must be within 120 characters of the call"
//	fails when a comment is added between them and passes when the guard encloses something
//	else entirely.
//
// An AST match has neither failure mode for these properties. Comments are not parsed unless
// asked for, so prose about a rule cannot violate it. Identifiers are matched as
// identifiers, so formatting, wrapping and whitespace are irrelevant. And containment is
// asserted as containment — a call inside an if-statement's body — instead of as arithmetic
// over byte offsets.
//
// # What these helpers are NOT for
//
// Anything observable at runtime belongs in a behavioural test, and every AST assertion in
// this package is paired with one. The structure is the part behaviour cannot reach; it is
// never the whole of what is checked.

// parseRepositoryGoFile parses a repository-root Go file into an AST.
//
// COMMENTS ARE DELIBERATELY NOT PARSED. Every assertion built on this asks about code, and
// including comments would reintroduce the exact false positive that made the text scans
// untenable: a comment naming the forbidden construct would count as the construct.
//
// Parameters:
//   - t *testing.T: the test; the parse failing is a hard failure.
//   - name string: a file name relative to the module root, e.g. "event_relay.go".
//
// Returns:
//   - *ast.File: the parsed file, never nil on return.
func parseRepositoryGoFile(t *testing.T, name string) *ast.File {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), name)
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err, "%s must be parseable to assert its structure", path)

	return parsed
}

// selectorCallNames returns the set of method and package-function names CALLED anywhere in
// the file, as they appear after the dot.
//
// It answers "does this file call anything named Sleep / Record / Before?" without caring
// what it is called on, which is the right granularity for the absence rules here: the rule
// is about the operation, and naming the receiver as well would let a rename evade it.
//
// A NAME IN A COMMENT OR A STRING IS NOT A CALL and is absent from this set, which is the
// whole reason for parsing rather than scanning.
//
// Parameters:
//   - file *ast.File: the parsed file.
//
// Returns:
//   - map[string]int: call count keyed by the selected name. Empty, never nil.
func selectorCallNames(file *ast.File) map[string]int {
	names := make(map[string]int)

	ast.Inspect(file, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}

		if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector {
			names[selector.Sel.Name]++
		}

		return true
	})

	return names
}

// qualifiedCallCount counts calls written as pkg.Name(...) — a package-qualified call, or a
// method call on a variable of that name.
//
// Distinguishing the qualifier matters where the bare name is common: "Sleep" alone is
// unambiguous, but a rule about time.Parse should not be satisfied or violated by an
// unrelated Parse method.
//
// Parameters:
//   - file *ast.File: the parsed file.
//   - qualifier string: the identifier before the dot, e.g. "time".
//   - name string: the identifier after it, e.g. "Parse".
//
// Returns:
//   - int: how many such calls appear.
func qualifiedCallCount(file *ast.File, qualifier, name string) int {
	count := 0

	ast.Inspect(file, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}

		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != name {
			return true
		}

		if base, isIdent := selector.X.(*ast.Ident); isIdent && base.Name == qualifier {
			count++
		}

		return true
	})

	return count
}

// qualifiedSelections returns every name the file selects off one package qualifier, whether
// or not the selection is itself a call.
//
// It answers a question the two helpers above cannot: "which members of this package does
// this file touch at all?". selectorCallNames sees `metrics.EventsDispatchedTotal.Add(...)`
// as a call named Add, because that is what it is, and qualifiedCallCount would need the
// forbidden name spelled out one at a time. Enumerating the selections instead lets a rule be
// stated as an ALLOWLIST — this file may reference exactly these members and no others —
// which is the only phrasing that stays correct when a new instrument is added to the package.
//
// A name in a comment or a string is not a selection and is absent from the map, for the same
// reason it is absent from selectorCallNames.
//
// Parameters:
//   - file *ast.File: the parsed file.
//   - qualifier string: the identifier before the dot, e.g. "metrics".
//
// Returns:
//   - map[string]int: reference count keyed by the selected name. Empty, never nil.
func qualifiedSelections(file *ast.File, qualifier string) map[string]int {
	names := make(map[string]int)

	ast.Inspect(file, func(node ast.Node) bool {
		selector, isSelector := node.(*ast.SelectorExpr)
		if !isSelector {
			return true
		}

		if base, isIdent := selector.X.(*ast.Ident); isIdent && base.Name == qualifier {
			names[selector.Sel.Name]++
		}

		return true
	})

	return names
}

// identifierUses returns how many times the file REFERS to an identifier of the given name,
// in any position — a selector's field name, a bare identifier, a key.
//
// This is the AST answer to "does this file read that configuration field at all?". It sees a
// reference wherever one exists and, again, does not see the name in prose.
//
// Parameters:
//   - file *ast.File: the parsed file.
//   - name string: the identifier to look for.
//
// Returns:
//   - int: the number of references.
func identifierUses(file *ast.File, name string) int {
	count := 0

	ast.Inspect(file, func(node ast.Node) bool {
		if identifier, isIdent := node.(*ast.Ident); isIdent && identifier.Name == name {
			count++
		}

		return true
	})

	return count
}

// assignmentTargets returns the names assigned to anywhere in the file, whether by = or :=,
// and whether the target is a bare identifier or a selected field.
//
// It is how "nothing here replaces that field" is stated. A struct field assigned in a
// composite literal is included too, because `Foo{bar: stub}` and `x.bar = stub` are the same
// substitution as far as the rule is concerned.
//
// Parameters:
//   - file *ast.File: the parsed file.
//
// Returns:
//   - map[string]int: assignment count keyed by target name. Empty, never nil.
func assignmentTargets(file *ast.File) map[string]int {
	targets := make(map[string]int)

	record := func(expression ast.Expr) {
		switch target := expression.(type) {
		case *ast.Ident:
			targets[target.Name]++
		case *ast.SelectorExpr:
			targets[target.Sel.Name]++
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch statement := node.(type) {
		case *ast.AssignStmt:
			for _, left := range statement.Lhs {
				record(left)
			}
		case *ast.CompositeLit:
			for _, element := range statement.Elts {
				if pair, isPair := element.(*ast.KeyValueExpr); isPair {
					record(pair.Key)
				}
			}
		}

		return true
	})

	return targets
}

// callsGuardedBy returns the calls that appear INSIDE the body of an if-statement whose
// condition calls guardQualifier.guardName, reported by the called name.
//
// Containment is read from the tree, so it is containment: a guard that encloses the call is
// found, and a guard that merely sits near it in the file is not. That replaces a byte-offset
// comparison whose verdict changed whenever a comment was added between the two.
//
// Nested ifs are covered because the search descends the whole body.
//
// Parameters:
//   - file *ast.File: the parsed file.
//   - guardQualifier string: the package or receiver in the guard condition, e.g. "logrus".
//   - guardName string: the function called in the condition, e.g. "IsLevelEnabled".
//
// Returns:
//   - map[string]int: count of guarded calls keyed by the selected name. Empty, never nil.
func callsGuardedBy(file *ast.File, guardQualifier, guardName string) map[string]int {
	guarded := make(map[string]int)

	ast.Inspect(file, func(node ast.Node) bool {
		branch, isIf := node.(*ast.IfStmt)
		if !isIf || branch.Cond == nil {
			return true
		}

		condition, isCall := branch.Cond.(*ast.CallExpr)
		if !isCall {
			return true
		}

		selector, isSelector := condition.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != guardName {
			return true
		}

		if base, isIdent := selector.X.(*ast.Ident); !isIdent || base.Name != guardQualifier {
			return true
		}

		for name, count := range selectorCallNames(&ast.File{
			Name:  file.Name,
			Decls: []ast.Decl{wrapStatementsAsDecl(branch.Body)},
		}) {
			guarded[name] += count
		}

		return true
	})

	return guarded
}

// wrapStatementsAsDecl puts a block into a throwaway function declaration so it can be
// walked by the same helpers that walk a file.
//
// Reusing one walker for a file and for a block is what keeps "is this call present?" and "is
// this call present inside that guard?" from being two implementations that can disagree.
func wrapStatementsAsDecl(body *ast.BlockStmt) ast.Decl {
	return &ast.FuncDecl{
		Name: ast.NewIdent("guardedBlock"),
		Type: &ast.FuncType{Params: &ast.FieldList{}},
		Body: body,
	}
}
