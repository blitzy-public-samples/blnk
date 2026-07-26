// Command scorer is the deterministic, standard-library-only canonical scorer
// for the recon-agent demo/eval pipeline (findings M-01, M-17). It consumes the
// immutable evaluation corpus (ground truth) together with the artifacts a run
// produces — the per-break resolved JSONL and the machine-readable summary —
// and enforces the AAP's measurable success criteria (§0.1.1), exiting NONZERO
// on any failed criterion so `make demo` and CI can gate on it.
//
// Criteria enforced
//
//  1. Exactly six breaks were ingested (breaks_in == 6, six resolved records,
//     every canonical id present).
//  2. At least five of six root-cause LABELS match the corpus ground truth.
//  3. At least one break was auto-resolved (>= 1 clearance). Clearance itself
//     is proven in-code: the remediator marks a break resolved ONLY after a
//     Blnk dry-run reconciliation confirms it left the unmatched set (Rule 5.3);
//     the DB-backed integration test additionally asserts this against a live
//     Blnk. The scorer verifies the resulting auto-resolve OUTCOME count.
//  4. Routing safety (Rule 5.4): no break the corpus marks escalate, and no
//     regulated break, is ever auto-resolved; every regulated break escalates.
//  5. Complete disposition + audit parity: every break reached exactly one
//     terminal outcome (breaks_in == auto_resolved + escalated, so none is
//     silently dropped) and produced at least one audit event
//     (audit_count >= breaks_in). Exact per-ACTION audit parity is asserted by
//     the DB-backed integration test, which can enumerate every store row.
//  6. Fixture integrity: the seed CSV and the corpus still describe exactly the
//     same six canonical breaks (guards against fixture/corpus drift).
//
// Scope / dependency hygiene
//
// This program lives in the ROOT Go module (github.com/blnkfinance/blnk),
// alongside seed/, and imports NOTHING beyond the Go standard library, so it
// adds no dependency to any go.mod/go.sum (Rules 5.6/5.8). It is an evaluation
// harness, explicitly outside the recon-agent coverage floor (Rule 5.9 excludes
// eval/). It never MUTATES the corpus — it only reads it.
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	wantBreaks        = 6
	minCorrectLabels  = 5
	minAutoResolved   = 1
	autoResolveAction = "auto_resolve"
	escalateAction    = "escalate"
)

// canonicalIDs is the fixed set of planted seed break ids the corpus, the seed
// CSV, and every run's artifacts must all agree on.
var canonicalIDs = []string{"EXT-001", "EXT-002", "EXT-003", "EXT-004", "EXT-005", "EXT-006"}

// outcome is the subset of a corpus / resolved record the scorer reasons about.
// In the corpus this holds the EXPECTED outcome; in the resolved artifact the
// same JSON shape (expected_output) holds the REALIZED outcome (the field is
// named expected_output in both files, but the pipeline writes the actual
// root_cause / regulated flag / disposition into the run's copy).
type outcome struct {
	RootCause      string `json:"root_cause"`
	Regulated      bool   `json:"regulated"`
	ExpectedAction string `json:"expected_action"`
}

// record is one line of either the corpus or the resolved artifact.
type record struct {
	ID             string  `json:"id"`
	Scenario       string  `json:"scenario"`
	ExpectedOutput outcome `json:"expected_output"`
}

// summaryReport mirrors the machine-readable recon_summary.json the pipeline
// writes beside the resolved artifact.
type summaryReport struct {
	BreaksIn     int `json:"breaks_in"`
	AutoResolved int `json:"auto_resolved"`
	Escalated    int `json:"escalated"`
	AuditCount   int `json:"audit_count"`
}

// scoreboard accumulates per-criterion pass/fail results and prints a scorecard.
type scoreboard struct {
	failed int
}

func (s *scoreboard) check(name string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		s.failed++
	}
	fmt.Printf("  [%s] %s — %s\n", status, name, detail)
}

// readJSONL decodes a JSON-lines file into records, tolerating blank lines.
func readJSONL(path string) ([]record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var recs []record
	dec := json.NewDecoder(f)
	for {
		var r record
		if err := dec.Decode(&r); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		recs = append(recs, r)
	}
	return recs, nil
}

// baseID strips a run-scoped suffix ("EXT-001-a1b2c3d4" -> "EXT-001"): it returns
// whichever canonical id equals the value or is its prefix before a '-'.
func baseID(id string) string {
	for _, c := range canonicalIDs {
		if id == c || strings.HasPrefix(id, c+"-") {
			return c
		}
	}
	return id
}

// loadCorpus reads the immutable ground-truth corpus keyed by base id.
func loadCorpus(path string) (map[string]record, error) {
	recs, err := readJSONL(path)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]record, len(recs))
	for _, r := range recs {
		byID[baseID(r.ID)] = r
	}
	return byID, nil
}

// loadResolved reads a run's resolved artifact keyed by base id.
func loadResolved(path string) (map[string]record, error) {
	recs, err := readJSONL(path)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]record, len(recs))
	for _, r := range recs {
		byID[baseID(r.ID)] = r
	}
	return byID, nil
}

// loadSummary reads the machine-readable summary report.
func loadSummary(path string) (summaryReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return summaryReport{}, err
	}
	defer f.Close()
	var s summaryReport
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return summaryReport{}, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// csvIDs returns the sorted, de-duplicated id set and row count of the seed CSV,
// verifying the required header schema (case-insensitive).
func csvIDs(path string) ([]string, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	if len(rows) == 0 {
		return nil, 0, fmt.Errorf("%s: empty CSV", path)
	}
	// Locate the ID column from the header (case-insensitive).
	idCol := -1
	for i, h := range rows[0] {
		if strings.EqualFold(strings.TrimSpace(h), "id") {
			idCol = i
			break
		}
	}
	if idCol < 0 {
		return nil, 0, fmt.Errorf("%s: header has no ID column", path)
	}
	seen := map[string]bool{}
	var ids []string
	dataRows := 0
	for _, row := range rows[1:] {
		if len(row) <= idCol {
			continue
		}
		id := strings.TrimSpace(row[idCol])
		if id == "" {
			continue
		}
		dataRows++
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, dataRows, nil
}

// setEqualsCanonical reports whether ids exactly equals the canonical id set.
func setEqualsCanonical(ids []string) bool {
	if len(ids) != len(canonicalIDs) {
		return false
	}
	want := map[string]bool{}
	for _, c := range canonicalIDs {
		want[c] = true
	}
	for _, id := range ids {
		if !want[id] {
			return false
		}
	}
	return true
}

func main() {
	corpusPath := flag.String("corpus", "eval/recon_corpus.jsonl", "path to the immutable evaluation corpus (ground truth)")
	resolvedPath := flag.String("resolved", "recon-agent/recon_resolved.jsonl", "path to the run's resolved artifact JSONL")
	summaryPath := flag.String("summary", "recon-agent/recon_summary.json", "path to the run's machine-readable summary JSON")
	csvPath := flag.String("csv", "seed/external_transactions.csv", "path to the seed external-transactions CSV")
	flag.Parse()

	fmt.Println("recon-agent canonical scorer")

	corpus, err := loadCorpus(*corpusPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scorer: load corpus: %v\n", err)
		os.Exit(2)
	}
	resolved, err := loadResolved(*resolvedPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scorer: load resolved artifact: %v\n", err)
		os.Exit(2)
	}
	summary, err := loadSummary(*summaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scorer: load summary: %v\n", err)
		os.Exit(2)
	}
	seedIDs, seedRows, err := csvIDs(*csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scorer: read seed CSV: %v\n", err)
		os.Exit(2)
	}

	var sb scoreboard

	// Criterion 6 (checked first): fixture integrity — corpus and seed CSV must
	// both describe exactly the same six canonical breaks.
	corpusIDs := make([]string, 0, len(corpus))
	for id := range corpus {
		corpusIDs = append(corpusIDs, id)
	}
	sort.Strings(corpusIDs)
	sb.check("fixture:corpus-ids", setEqualsCanonical(corpusIDs),
		fmt.Sprintf("corpus ids=%v want %v", corpusIDs, canonicalIDs))
	sb.check("fixture:seed-csv-ids", setEqualsCanonical(seedIDs) && seedRows == wantBreaks,
		fmt.Sprintf("seed CSV ids=%v rows=%d want %v (6 rows)", seedIDs, seedRows, canonicalIDs))

	// Criterion 1: exactly six breaks in, six resolved records, all present.
	sb.check("exactly-six:summary", summary.BreaksIn == wantBreaks,
		fmt.Sprintf("summary.breaks_in=%d want %d", summary.BreaksIn, wantBreaks))
	sb.check("exactly-six:resolved-records", len(resolved) == wantBreaks,
		fmt.Sprintf("resolved records=%d want %d", len(resolved), wantBreaks))
	allPresent := true
	var missing []string
	for _, id := range canonicalIDs {
		if _, ok := resolved[id]; !ok {
			allPresent = false
			missing = append(missing, id)
		}
	}
	sb.check("exactly-six:all-present", allPresent,
		fmt.Sprintf("missing=%v", missing))

	// Criterion 2: >= 5/6 correct root-cause labels.
	correct := 0
	var mislabeled []string
	for _, id := range canonicalIDs {
		exp, okE := corpus[id]
		act, okA := resolved[id]
		if !okE || !okA {
			continue
		}
		if act.ExpectedOutput.RootCause == exp.ExpectedOutput.RootCause {
			correct++
		} else {
			mislabeled = append(mislabeled, fmt.Sprintf("%s(got=%s want=%s)", id, act.ExpectedOutput.RootCause, exp.ExpectedOutput.RootCause))
		}
	}
	sb.check("labels:>=5-correct", correct >= minCorrectLabels,
		fmt.Sprintf("correct=%d/%d want >=%d; mislabeled=%v", correct, len(canonicalIDs), minCorrectLabels, mislabeled))

	// Criterion 3: >= 1 auto-resolved.
	autoResolvedActual := 0
	for _, id := range canonicalIDs {
		if act, ok := resolved[id]; ok && act.ExpectedOutput.ExpectedAction == autoResolveAction {
			autoResolvedActual++
		}
	}
	sb.check("clearance:>=1-auto-resolved",
		autoResolvedActual >= minAutoResolved && summary.AutoResolved >= minAutoResolved,
		fmt.Sprintf("resolved-artifact auto=%d summary.auto_resolved=%d want >=%d", autoResolvedActual, summary.AutoResolved, minAutoResolved))

	// Criterion 4: routing safety (Rule 5.4). No escalate-expected break and no
	// regulated break may be auto-resolved; every regulated break escalates.
	routingOK := true
	var violations []string
	for _, id := range canonicalIDs {
		exp, okE := corpus[id]
		act, okA := resolved[id]
		if !okE || !okA {
			continue
		}
		expectedEscalate := exp.ExpectedOutput.ExpectedAction == escalateAction || exp.ExpectedOutput.Regulated
		if expectedEscalate && act.ExpectedOutput.ExpectedAction == autoResolveAction {
			routingOK = false
			violations = append(violations, fmt.Sprintf("%s auto-resolved but corpus expects escalate/regulated", id))
		}
		if act.ExpectedOutput.Regulated && act.ExpectedOutput.ExpectedAction != escalateAction {
			routingOK = false
			violations = append(violations, fmt.Sprintf("%s regulated but not escalated (action=%s)", id, act.ExpectedOutput.ExpectedAction))
		}
	}
	sb.check("routing:no-unsafe-auto-resolution", routingOK,
		fmt.Sprintf("violations=%v", violations))

	// Criterion 5: complete disposition + audit parity floor.
	sb.check("disposition:breaks==auto+escalated",
		summary.BreaksIn == summary.AutoResolved+summary.Escalated,
		fmt.Sprintf("breaks_in=%d auto_resolved=%d escalated=%d", summary.BreaksIn, summary.AutoResolved, summary.Escalated))
	sb.check("audit:>=one-per-break",
		summary.AuditCount >= summary.BreaksIn && summary.AuditCount > 0,
		fmt.Sprintf("audit_count=%d breaks_in=%d", summary.AuditCount, summary.BreaksIn))

	fmt.Printf("\nsummary: breaks_in=%d auto_resolved=%d escalated=%d audit_count=%d; labels=%d/%d\n",
		summary.BreaksIn, summary.AutoResolved, summary.Escalated, summary.AuditCount, correct, len(canonicalIDs))

	if sb.failed > 0 {
		fmt.Printf("SCORER: FAIL (%d criterion/criteria failed)\n", sb.failed)
		os.Exit(1)
	}
	fmt.Println("SCORER: PASS (all acceptance criteria met)")
}
