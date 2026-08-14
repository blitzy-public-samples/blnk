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

// LINK INTEGRITY FOR EVERY PUBLISHED MARKDOWN FILE.
//
// Two links in README.md's Quick start pointed at documentation pages that no longer
// existed, and both returned 404:
//
//	https://docs.blnkfinance.com/api-reference
//	https://docs.blnkfinance.com/tutorials/quick-start/create-your-first-ledger-balance-and-transaction
//
// A third was broken in a quieter way. The Contributor Covenant badge linked
// code_of_conduct.md, and the file in this repository is CODE_OF_CONDUCT.md. Every path
// GitHub serves is case-sensitive, so that link had always been a 404 in the browser — and
// it survived review precisely because it resolves on a case-insensitive filesystem, which
// is what a good number of contributors develop on. A checker that only calls os.Stat
// reproduces the same blind spot, so the case is compared against the directory entry
// itself here.
//
// # WHAT IS CHECKED HERE, AND WHAT IS DELIBERATELY CHECKED ELSEWHERE
//
// This file is hermetic. It resolves repository-relative links and section anchors with no
// network at all, which is what lets it run on every `go test ./...` — including offline
// and in an air-gapped build — and still mean something. Anchors are the highest-value
// half: the docs cross-reference each other with more than fifty #section links, and a
// renamed heading breaks them silently, with nothing at runtime to notice.
//
// External resolution cannot be hermetic, so it lives in scripts/check-doc-links.sh, run
// from .github/workflows/docs-links.yml. That split is not incidental. The two README
// links above rotted WITHOUT ANY COMMIT TO THIS REPOSITORY — the documentation site
// retired its own pages — so external checking has to be driven by a clock rather than by
// a push, and the final test in this file is what holds that schedule in place.
package blnk

import (
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// docLinkCheckerPath is the external, network-dependent half of this contract.
const docLinkCheckerPath = "scripts/check-doc-links.sh"

// docLinkWorkflowPath drives the checker on a schedule.
const docLinkWorkflowPath = ".github/workflows/docs-links.yml"

// retiredDocumentationSlugs are the exact URL fragments that shipped as 404s. Each stays
// pinned so a revert, a stale copy-paste or a resurrected older draft cannot reintroduce
// one silently: the replacement is a live page, but nothing about the old string stops it
// being typed again.
var retiredDocumentationSlugs = []string{
	"docs.blnkfinance.com/api-reference",
	"create-your-first-ledger-balance-and-transaction",
}

// markdownInlineLink matches a Markdown inline link's target. A target containing
// whitespace is not matched, which correctly excludes the `](url "title")` title form's
// title while still capturing its url.
var markdownInlineLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)

// atxHeading matches a Markdown heading line.
var atxHeading = regexp.MustCompile(`^(#{1,6})\s+(.*)$`)

// markdownFilesUnderContract returns every Markdown file in the module, module-relative.
//
// The enumeration is over the tree rather than over a list, for the same reason the
// hardening and YAML-import contracts are: a documentation page added next month is under
// contract the moment it lands, with nobody having to remember to add it here. A checker
// with a hardcoded file list rots in exactly the way the links it checks do.
func markdownFilesUnderContract(t *testing.T) []string {
	t.Helper()

	root := moduleRootDir(t)

	var found []string

	err := filepath.WalkDir(root, func(fullPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			switch entry.Name() {
			// .git holds objects; vendor and node_modules hold other projects' files;
			// blitzy holds generated run evidence that is not published documentation.
			case ".git", "vendor", "node_modules", "blitzy":
				return fs.SkipDir
			}

			return nil
		}

		if strings.HasSuffix(entry.Name(), ".md") {
			relative, relErr := filepath.Rel(root, fullPath)
			if relErr != nil {
				return relErr
			}

			found = append(found, filepath.ToSlash(relative))
		}

		return nil
	})
	require.NoError(t, err, "the module tree must be walkable to enumerate Markdown files")

	// A repository with no Markdown at all would make every assertion below vacuously
	// true, so the enumeration asserts it found the documentation set it exists to check.
	require.GreaterOrEqual(t, len(found), 10,
		"expected the published documentation set to be discovered, found only %v", found)

	return found
}

// strippedOfFencedCode removes fenced code blocks.
//
// The operations documentation is largely worked examples, and those examples contain both
// Markdown-shaped text and URLs that are illustrations rather than links. Neither renders
// as a link, so neither is one.
func strippedOfFencedCode(source string) string {
	var kept []string

	inFence := false

	for _, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence

			continue
		}

		if !inFence {
			kept = append(kept, line)
		}
	}

	return strings.Join(kept, "\n")
}

// linkTargetsIn returns every inline link target in a Markdown source.
func linkTargetsIn(source string) []string {
	var targets []string

	for _, match := range markdownInlineLink.FindAllStringSubmatch(strippedOfFencedCode(source), -1) {
		targets = append(targets, match[1])
	}

	return targets
}

// githubHeadingSlug reproduces GitHub's heading-to-anchor transformation.
//
// The one subtlety worth stating: each space becomes one hyphen, and runs are NOT
// collapsed. Headings here are written with em-dashes, and an em-dash is punctuation, so
// it is removed and leaves the spaces on either side of it behind — which is why the
// correct anchor for "Storage — size the volume" contains a DOUBLE hyphen. A slug function
// that collapses whitespace declares every one of those anchors broken, which is a false
// alarm on real, working links and is exactly how this kind of check loses its audience.
func githubHeadingSlug(heading string) string {
	slug := strings.ToLower(strings.TrimSpace(heading))

	// Inline code and links contribute their text, not their markup.
	slug = strings.ReplaceAll(slug, "`", "")
	slug = markdownInlineLinkText.ReplaceAllString(slug, "$1")

	// Everything that is not a word character, whitespace or a hyphen is dropped.
	slug = nonSlugCharacter.ReplaceAllString(slug, "")

	return strings.ReplaceAll(slug, " ", "-")
}

var (
	markdownInlineLinkText = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
	nonSlugCharacter       = regexp.MustCompile(`[^\p{L}\p{N}\s_-]`)
)

// headingSlugsIn returns the anchors a Markdown file actually offers.
func headingSlugsIn(source string) map[string]bool {
	slugs := map[string]bool{}

	for _, line := range strings.Split(strippedOfFencedCode(source), "\n") {
		if match := atxHeading.FindStringSubmatch(line); match != nil {
			slugs[githubHeadingSlug(match[2])] = true
		}
	}

	return slugs
}

// existsWithExactCase reports whether a module-relative path exists AND is spelled with
// the same case as the directory entry backing it.
//
// os.Stat alone is not enough. On the case-insensitive filesystems many contributors
// develop on it happily resolves CODE_OF_CONDUCT.md for a link that says
// code_of_conduct.md, and the 404 then appears only once the link is published. Comparing
// against the directory listing gives the same verdict on every platform.
func existsWithExactCase(root, relative string) (bool, error) {
	current := root

	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." {
			continue
		}

		entries, err := os.ReadDir(current)
		if err != nil {
			return false, err
		}

		matched := false

		for _, entry := range entries {
			if entry.Name() == component {
				matched = true

				break
			}
		}

		if !matched {
			return false, nil
		}

		current = filepath.Join(current, component)
	}

	return true, nil
}

// ---------------------------------------------------------------------------
// P7-F04 — repository-relative links
// ---------------------------------------------------------------------------

// A relative link is the one kind of link this repository is wholly responsible for: no
// third party can break it, so a broken one is always our own.
func TestDocumentationLinks_EveryRepositoryRelativeLinkResolves(t *testing.T) {
	root := moduleRootDir(t)
	files := markdownFilesUnderContract(t)

	checked := 0

	for _, file := range files {
		source, err := os.ReadFile(filepath.Join(root, file))
		require.NoErrorf(t, err, "%s must be readable", file)

		for _, target := range linkTargetsIn(string(source)) {
			if isExternalOrInPage(target) {
				continue
			}

			filePart := strings.SplitN(target, "#", 2)[0]
			if filePart == "" {
				continue
			}

			resolved := path.Join(path.Dir(file), filePart)
			checked++

			exists, statErr := existsWithExactCase(root, resolved)
			require.NoErrorf(t, statErr, "%s: resolving %q must not error", file, target)

			assert.Truef(t, exists,
				"%s links %q, which resolves to %q — and no file of exactly that name exists.\n"+
					"Note the CASE: every path GitHub serves is case-sensitive, so a link that\n"+
					"resolves on a case-insensitive filesystem can still be a 404 in the browser.",
				file, target, resolved)
		}
	}

	require.Positive(t, checked, "expected repository-relative links to be found and checked")
}

// The docs cross-reference each other by section. A renamed heading leaves the link
// resolving to the right FILE and the wrong place in it, and nothing anywhere reports it.
func TestDocumentationLinks_EverySectionAnchorResolves(t *testing.T) {
	root := moduleRootDir(t)
	files := markdownFilesUnderContract(t)

	// Anchors are read per target file, so a file is parsed once however often it is linked.
	slugCache := map[string]map[string]bool{}

	slugsFor := func(relative string) map[string]bool {
		if cached, ok := slugCache[relative]; ok {
			return cached
		}

		source, err := os.ReadFile(filepath.Join(root, relative))
		require.NoErrorf(t, err, "%s must be readable to resolve its anchors", relative)

		slugs := headingSlugsIn(string(source))
		slugCache[relative] = slugs

		return slugs
	}

	checked := 0

	for _, file := range files {
		source, err := os.ReadFile(filepath.Join(root, file))
		require.NoErrorf(t, err, "%s must be readable", file)

		for _, target := range linkTargetsIn(string(source)) {
			if isExternalOrInPage(target) || !strings.Contains(target, "#") {
				continue
			}

			parts := strings.SplitN(target, "#", 2)
			filePart, anchor := parts[0], parts[1]

			if filePart == "" || anchor == "" {
				continue
			}

			resolved := path.Join(path.Dir(file), filePart)
			if !strings.HasSuffix(resolved, ".md") {
				continue
			}

			if exists, _ := existsWithExactCase(root, resolved); !exists {
				// Reported by the test above; not repeated here.
				continue
			}

			checked++

			assert.Truef(t, slugsFor(resolved)[anchor],
				"%s links %q, but %s has no heading whose anchor is %q.\n"+
					"A heading was most likely reworded. Update the link, or restore the heading.",
				file, target, resolved, anchor)
		}
	}

	require.Positive(t, checked, "expected cross-document section anchors to be found and checked")
}

// isExternalOrInPage reports whether a target is somebody else's to resolve, or is a
// same-page fragment.
func isExternalOrInPage(target string) bool {
	return strings.HasPrefix(target, "http://") ||
		strings.HasPrefix(target, "https://") ||
		strings.HasPrefix(target, "mailto:") ||
		strings.HasPrefix(target, "#")
}

// The two retired URLs are pinned by exact string. The replacement being live does not
// prevent the dead one being typed, reverted or copied back in.
func TestDocumentationLinks_NoRetiredDocumentationSlugReturns(t *testing.T) {
	root := moduleRootDir(t)

	for _, file := range markdownFilesUnderContract(t) {
		source, err := os.ReadFile(filepath.Join(root, file))
		require.NoErrorf(t, err, "%s must be readable", file)

		// The checker script names these slugs in order to explain itself, and this test
		// names them in order to pin them. Neither is a published link.
		if file == docLinkCheckerPath || strings.HasSuffix(file, "event_docs_links_test.go") {
			continue
		}

		for _, retired := range retiredDocumentationSlugs {
			assert.NotContainsf(t, string(source), retired,
				"%s references %q, a documentation URL that was retired and answers 404.\n"+
					"The live equivalents are https://docs.blnkfinance.com/reference/overview for the\n"+
					"API reference, and https://docs.blnkfinance.com/tutorials/quick-start/wallet-management\n"+
					"for the walkthrough that creates a ledger, a balance and then a transaction.",
				file, retired)
		}
	}
}

// A malformed URL never resolves anywhere, and it is not the external checker's job to
// discover that — it is decidable here, offline.
func TestDocumentationLinks_ExternalLinksAreWellFormed(t *testing.T) {
	root := moduleRootDir(t)

	checked := 0

	for _, file := range markdownFilesUnderContract(t) {
		source, err := os.ReadFile(filepath.Join(root, file))
		require.NoErrorf(t, err, "%s must be readable", file)

		for _, target := range linkTargetsIn(string(source)) {
			if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
				continue
			}

			checked++

			// A malformed percent-escape is the common way this fails in practice: an
			// anchor or query copied by hand loses a hex digit, url.Parse rejects it, and
			// no client resolves it either.
			parsed, parseErr := url.Parse(target)
			assert.NoErrorf(t, parseErr,
				"%s: %q is not a parseable URL, so nothing will resolve it", file, target)

			if parseErr != nil {
				continue
			}

			assert.NotEmptyf(t, parsed.Host,
				"%s: %q has no host, so it cannot resolve", file, target)
			assert.Containsf(t, []string{"http", "https"}, parsed.Scheme,
				"%s: %q must use http or https", file, target)
		}
	}

	require.Positive(t, checked, "expected external links to be found and checked")
}

// ---------------------------------------------------------------------------
// The scheduled half — without this, nothing catches the next retired page
// ---------------------------------------------------------------------------

// The specific way the two README links broke is what this test protects.
//
// Nobody edited them. The documentation site retired the pages, and the repository's
// Markdown became wrong while sitting still. A link check that runs on push and pull
// request only would not have caught it on any commit, because there was no commit — which
// makes the SCHEDULE the load-bearing part of the configuration, not a nicety. A future
// tidy-up that drops the cron in the name of saving CI minutes would restore the original
// blind spot exactly, so the schedule is asserted rather than assumed.
func TestDocumentationLinks_ExternalLivenessIsCheckedOnASchedule(t *testing.T) {
	root := moduleRootDir(t)

	t.Run("the checker script exists and is executable", func(t *testing.T) {
		info, err := os.Stat(filepath.Join(root, docLinkCheckerPath))
		require.NoErrorf(t, err, "%s must exist", docLinkCheckerPath)

		assert.NotZerof(t, info.Mode().Perm()&0o111,
			"%s must be executable, or neither CI nor `make docs_links` can run it",
			docLinkCheckerPath)
	})

	t.Run("a workflow runs it on a schedule, not only on pushes", func(t *testing.T) {
		workflow := readRepoFile(t, docLinkWorkflowPath)

		assert.Containsf(t, workflow, docLinkCheckerPath,
			"%s must run %s", docLinkWorkflowPath, docLinkCheckerPath)

		assert.Containsf(t, workflow, "schedule:",
			"%s must carry a schedule: trigger. The two links this check exists for rotted with\n"+
				"no commit to this repository at all — the documentation site retired the pages — so\n"+
				"a push-triggered check can never observe that class of breakage.",
			docLinkWorkflowPath)

		assert.Containsf(t, workflow, "cron:",
			"%s's schedule: trigger must specify a cron expression", docLinkWorkflowPath)
	})

	t.Run("the hermetic half runs in the ordinary test suite", func(t *testing.T) {
		workflow := readRepoFile(t, docLinkWorkflowPath)

		assert.Containsf(t, workflow, "TestDocumentationLinks",
			"%s must also run the hermetic link tests, so a broken relative link or anchor\n"+
				"fails without waiting on a network round trip", docLinkWorkflowPath)
	})

	t.Run("make exposes the checker locally", func(t *testing.T) {
		makefile := readRepoFile(t, "makefile")

		assert.Containsf(t, makefile, "docs_links:",
			"the makefile must expose a docs_links target so a contributor can run the same\n"+
				"check CI runs, rather than discovering a dead link after review")
		assert.Contains(t, makefile, docLinkCheckerPath,
			"the docs_links target must invoke "+docLinkCheckerPath)
	})
}
