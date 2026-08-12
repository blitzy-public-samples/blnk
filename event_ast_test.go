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
// A handful of properties in this package are absences: "the relay does not sleep a
// retry delay in process", "the relay does not own a second copy of the sunset
// decision", "this test file forms no opinion of its own about the boundary it tests".
//
// FALSE POSITIVES.
//
// FALSE NEGATIVES.
//
// An AST match has neither failure mode for these properties.
//
// Anything observable at runtime belongs in a behavioural test, and every AST assertion
// in this package is paired with one.

// parseRepositoryGoFile parses a repository-root Go file into an AST.
//
// COMMENTS ARE DELIBERATELY NOT PARSED.
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

// selectorCallNames returns the set of method and package-function names CALLED
// anywhere in the file, as they appear after the dot.
//
// A NAME IN A COMMENT OR A STRING IS NOT A CALL and is absent from this set, which is
// the whole reason for parsing rather than scanning.
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

// qualifiedCallCount counts calls written as pkg.Name(...) — a package-qualified call,
// or a method call on a variable of that name.
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

// qualifiedSelections returns every name the file selects off one package qualifier,
// whether or not the selection is itself a call.
//
// A name in a comment or a string is not a selection and is absent from the map, for
// the same reason it is absent from selectorCallNames.
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

// identifierUses returns how many times the file REFERS to an identifier of the given
// name, in any position — a selector's field name, a bare identifier, a key.
//
// This is the AST answer to "does this file read that configuration field at all?".
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

// assignmentTargets returns the names assigned to anywhere in the file, whether by = or
// :=, and whether the target is a bare identifier or a selected field.
//
// It is how "nothing here replaces that field" is stated.
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
// Containment is read from the tree, so it is containment: a guard that encloses the
// call is found, and a guard that merely sits near it in the file is not.
//
// Parameters:
//   - file *ast.File: the parsed file.
//   - guardQualifier string: the package or receiver in the guard condition, e.g.
//   - guardName string: the function called in the condition, e.g. "IsLevelEnabled".
//
// Returns:
//   - map[string]int: count of guarded calls keyed by the selected name.
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
func wrapStatementsAsDecl(body *ast.BlockStmt) ast.Decl {
	return &ast.FuncDecl{
		Name: ast.NewIdent("guardedBlock"),
		Type: &ast.FuncType{Params: &ast.FieldList{}},
		Body: body,
	}
}

// ---------------------------------------------------------------------------------------
// Source groups
//
// Every helper above answers a question about ONE file. A production file split for size
// answers the same questions across a GROUP, and the questions worth asserting are mostly
// about ABSENCE — that nothing reads a value, sleeps, or writes an instrument. An absence
// assertion narrowed to the remnant of a split file is the worst outcome available: it
// keeps passing, and it stops meaning anything the moment the code moves next door.
//
// So each single-file helper has a group counterpart here, and structural assertions use
// the counterpart.
// ---------------------------------------------------------------------------------------

// parseRepositoryGoFiles parses every non-test member of base's source group.
//
// Parameters:
//   - t *testing.T: the test.
//   - base string: a repository-relative path, e.g. "event_relay.go".
//
// Returns:
//   - []*ast.File: one parsed file per group member, in sorted path order.
func parseRepositoryGoFiles(t *testing.T, base string) []*ast.File {
	t.Helper()

	group := eventSourceGroup(t, base)
	parsedFiles := make([]*ast.File, 0, len(group))

	for _, member := range group {
		parsedFiles = append(parsedFiles, parseRepositoryGoFile(t, member))
	}

	return parsedFiles
}

// sumIdentifierUses totals identifierUses across a parsed group.
func sumIdentifierUses(files []*ast.File, name string) int {
	total := 0
	for _, file := range files {
		total += identifierUses(file, name)
	}

	return total
}

// sumQualifiedCallCount totals qualifiedCallCount across a parsed group.
func sumQualifiedCallCount(files []*ast.File, qualifier, name string) int {
	total := 0
	for _, file := range files {
		total += qualifiedCallCount(file, qualifier, name)
	}

	return total
}

// sumSelectorCallNames merges selectorCallNames across a parsed group.
func sumSelectorCallNames(files []*ast.File) map[string]int {
	merged := map[string]int{}

	for _, file := range files {
		for name, count := range selectorCallNames(file) {
			merged[name] += count
		}
	}

	return merged
}

// sumQualifiedSelections merges qualifiedSelections across a parsed group.
func sumQualifiedSelections(files []*ast.File, qualifier string) map[string]int {
	merged := map[string]int{}

	for _, file := range files {
		for name, count := range qualifiedSelections(file, qualifier) {
			merged[name] += count
		}
	}

	return merged
}

// sumCallsGuardedBy merges callsGuardedBy across a parsed group.
func sumCallsGuardedBy(files []*ast.File, guardQualifier, guardName string) map[string]int {
	merged := map[string]int{}

	for _, file := range files {
		for name, count := range callsGuardedBy(file, guardQualifier, guardName) {
			merged[name] += count
		}
	}

	return merged
}
