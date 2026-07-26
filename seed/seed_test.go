// Copyright 2024 Blnk Finance Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// seed_test.go binds the committed seed statement (seed/external_transactions.csv)
// to the committed evaluation corpus (eval/recon_corpus.jsonl) so that
// INCOMPATIBLE seed<->corpus data can never slip through green (QA finding F2 /
// documented release-blocker "contradiction (a)").
//
// It is a pure DATA-CONTRACT test: it reads BOTH committed files off disk and
// asserts they describe the SAME six planted breaks, with consistent
// amount/currency/reference fields and the expected root-cause labels. A drift
// in EITHER file — a dropped or renamed id, a changed currency/amount/reference,
// or a relabeled root cause — fails the test.
//
// Design constraints:
//   - STDLIB ONLY. The root Blnk module's go.mod/go.sum must stay byte-for-byte
//     unchanged (AAP §0.3.2, Rule 5.8), so this test imports nothing outside the
//     Go standard library and therefore adds no dependency.
//   - No running service. It only reads two files, so it runs in the default
//     `go test ./...` (and therefore in `make test`, whose first line is
//     `go test -short ./...` at the repository root) with zero external
//     dependencies.
//   - Ground truth is declared ONCE (expectedBreaks) and both files are checked
//     against it AND against each other, so neither file is treated as
//     self-authoritative.

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Relative paths from the seed package directory (Go runs a package's tests
// with the working directory set to that package's directory).
const (
	seedCSVRelPath    = "external_transactions.csv"
	evalCorpusRelPath = "../eval/recon_corpus.jsonl"
	// plantedBreakCount is the exact number of planted breaks the AAP §0.1.1
	// success criteria are scored against ("a planted 6-break seed").
	plantedBreakCount = 6
	// amountEpsilon tolerates float round-trip when comparing a CSV decimal
	// string (e.g. "1500.00") with the corpus JSON number (e.g. 1500.0).
	amountEpsilon = 1e-6
)

// expectedBreak is the ground-truth shape for one planted break.
type expectedBreak struct {
	rootCause string
	amount    float64
	currency  string
	reference string
}

// expectedBreaks is the AAP/corpus GROUND TRUTH for the six planted breaks
// (AAP §0.1.1: root-cause enumeration timing | amount_drift | reference_mismatch
// | duplicate | missing_internal | currency_mismatch). Both the seed CSV and the
// eval corpus must agree with it exactly; any drift in either file fails this
// test. Declaring the truth here (rather than deriving it from one file) is what
// makes the seed<->corpus binding non-vacuous.
var expectedBreaks = map[string]expectedBreak{
	"EXT-001": {rootCause: "timing", amount: 1500.00, currency: "USD", reference: "INV-1001"},
	"EXT-002": {rootCause: "amount_drift", amount: 250.75, currency: "USD", reference: "INV-1002"},
	"EXT-003": {rootCause: "reference_mismatch", amount: 980.00, currency: "USD", reference: "ACME-2025-03"},
	"EXT-004": {rootCause: "duplicate", amount: 1500.00, currency: "USD", reference: "INV-1001"},
	"EXT-005": {rootCause: "missing_internal", amount: 4200.00, currency: "USD", reference: "UNKN-9001"},
	"EXT-006": {rootCause: "currency_mismatch", amount: 700.00, currency: "EUR", reference: "INV-1006"},
}

// csvRow captures the matchable fields of one external-statement CSV row.
type csvRow struct {
	amount    float64
	currency  string
	reference string
}

// corpusRecord is the minimal subset of an eval-corpus JSONL line this test
// asserts on. The corpus also carries context_note / judging_criteria /
// proposed_rule fields that are irrelevant to the seed<->corpus binding and are
// intentionally ignored.
type corpusRecord struct {
	ID       string `json:"id"`
	Scenario string `json:"scenario"`
	Input    struct {
		ExternalTransaction struct {
			ID        string  `json:"id"`
			Amount    float64 `json:"amount"`
			Currency  string  `json:"currency"`
			Reference string  `json:"reference"`
		} `json:"external_transaction"`
	} `json:"input"`
	ExpectedOutput struct {
		RootCause string `json:"root_cause"`
	} `json:"expected_output"`
}

// readSeedCSV parses seed/external_transactions.csv into a map keyed by the ID
// column. Headers are matched case-insensitively, mirroring Blnk's ingestion
// contract (internal/files/files.go), so a column reorder does not silently
// misalign fields.
func readSeedCSV(t *testing.T) map[string]csvRow {
	t.Helper()
	f, err := os.Open(seedCSVRelPath)
	if err != nil {
		t.Fatalf("open seed CSV %q: %v", seedCSVRelPath, err)
	}
	defer func() { _ = f.Close() }()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // tolerate a stray blank trailing line
	header, err := reader.Read()
	if err != nil {
		t.Fatalf("read seed CSV header: %v", err)
	}
	col := make(map[string]int, len(header))
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, required := range []string{"id", "amount", "currency", "reference"} {
		if _, ok := col[required]; !ok {
			t.Fatalf("seed CSV header missing required column %q (got %v)", required, header)
		}
	}

	rows := make(map[string]csvRow)
	for {
		rec, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read seed CSV row: %v", readErr)
		}
		// Skip a fully blank line (e.g. a trailing newline yields an empty record).
		if len(rec) == 1 && strings.TrimSpace(rec[0]) == "" {
			continue
		}
		id := strings.TrimSpace(rec[col["id"]])
		if id == "" {
			continue
		}
		amount, parseErr := strconv.ParseFloat(strings.TrimSpace(rec[col["amount"]]), 64)
		if parseErr != nil {
			t.Fatalf("seed CSV row %q: unparseable amount %q: %v", id, rec[col["amount"]], parseErr)
		}
		if _, dup := rows[id]; dup {
			t.Fatalf("seed CSV contains duplicate id %q", id)
		}
		rows[id] = csvRow{
			amount:    amount,
			currency:  strings.TrimSpace(rec[col["currency"]]),
			reference: strings.TrimSpace(rec[col["reference"]]),
		}
	}
	return rows
}

// readCorpus parses eval/recon_corpus.jsonl into a map keyed by the record id,
// failing on a malformed line or a duplicate id.
func readCorpus(t *testing.T) map[string]corpusRecord {
	t.Helper()
	raw, err := os.ReadFile(evalCorpusRelPath)
	if err != nil {
		t.Fatalf("read eval corpus %q: %v", evalCorpusRelPath, err)
	}
	records := make(map[string]corpusRecord)
	for lineNo, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		var rec corpusRecord
		if decErr := json.Unmarshal([]byte(trimmed), &rec); decErr != nil {
			t.Fatalf("eval corpus line %d is not valid JSON: %v", lineNo+1, decErr)
		}
		if rec.ID == "" {
			t.Fatalf("eval corpus line %d has an empty id", lineNo+1)
		}
		if _, dup := records[rec.ID]; dup {
			t.Fatalf("eval corpus contains duplicate id %q", rec.ID)
		}
		records[rec.ID] = rec
	}
	return records
}

// TestSeedCorpusAlignment is the F2 guard. It fails on ANY incompatibility
// between the committed seed CSV, the committed eval corpus, and the ground
// truth — the exact class of defect QA's "contradiction (a)" injection
// (renamed/dropped ids, a flipped currency, a relabeled root cause) produces.
func TestSeedCorpusAlignment(t *testing.T) {
	csvRows := readSeedCSV(t)
	corpus := readCorpus(t)

	// --- Counts: exactly the planted 6 in each file. ---
	if len(csvRows) != plantedBreakCount {
		t.Fatalf("seed CSV must contain exactly %d planted breaks; got %d (ids %v)",
			plantedBreakCount, len(csvRows), sortedKeys(csvRows))
	}
	if len(corpus) != plantedBreakCount {
		t.Fatalf("eval corpus must contain exactly %d records (one per planted break); got %d (ids %v)",
			plantedBreakCount, len(corpus), sortedCorpusKeys(corpus))
	}

	// --- Identical id sets across ground truth, CSV, and corpus. ---
	for id := range expectedBreaks {
		if _, ok := csvRows[id]; !ok {
			t.Errorf("seed CSV is missing expected break id %q", id)
		}
		if _, ok := corpus[id]; !ok {
			t.Errorf("eval corpus is missing expected break id %q", id)
		}
	}
	for id := range csvRows {
		if _, ok := expectedBreaks[id]; !ok {
			t.Errorf("seed CSV contains unexpected break id %q (not in the planted 6-break set)", id)
		}
	}
	for id := range corpus {
		if _, ok := expectedBreaks[id]; !ok {
			t.Errorf("eval corpus contains unexpected break id %q (not in the planted 6-break set)", id)
		}
	}
	if t.Failed() {
		// The id sets already disagree; stop here so those failures are the
		// actionable message rather than a cascade of zero-value field mismatches.
		t.FailNow()
	}

	// --- Per-break field + label agreement. ---
	for id, want := range expectedBreaks {
		csvRow := csvRows[id]
		rec := corpus[id]

		// Seed CSV must match the ground truth.
		if !floatsEqual(csvRow.amount, want.amount) {
			t.Errorf("seed CSV %s: amount = %v, want %v", id, csvRow.amount, want.amount)
		}
		if csvRow.currency != want.currency {
			t.Errorf("seed CSV %s: currency = %q, want %q", id, csvRow.currency, want.currency)
		}
		if csvRow.reference != want.reference {
			t.Errorf("seed CSV %s: reference = %q, want %q", id, csvRow.reference, want.reference)
		}

		// Corpus record must match the ground truth.
		ext := rec.Input.ExternalTransaction
		if ext.ID != id {
			t.Errorf("eval corpus %s: input.external_transaction.id = %q, want %q", id, ext.ID, id)
		}
		if !floatsEqual(ext.Amount, want.amount) {
			t.Errorf("eval corpus %s: amount = %v, want %v", id, ext.Amount, want.amount)
		}
		if ext.Currency != want.currency {
			t.Errorf("eval corpus %s: currency = %q, want %q", id, ext.Currency, want.currency)
		}
		if ext.Reference != want.reference {
			t.Errorf("eval corpus %s: reference = %q, want %q", id, ext.Reference, want.reference)
		}

		// Corpus root-cause label must match the ground truth AND be internally
		// consistent (scenario == expected_output.root_cause). This catches a
		// relabeled break in the corpus.
		if rec.Scenario != want.rootCause {
			t.Errorf("eval corpus %s: scenario = %q, want %q", id, rec.Scenario, want.rootCause)
		}
		if rec.ExpectedOutput.RootCause != want.rootCause {
			t.Errorf("eval corpus %s: expected_output.root_cause = %q, want %q", id, rec.ExpectedOutput.RootCause, want.rootCause)
		}
		if rec.Scenario != rec.ExpectedOutput.RootCause {
			t.Errorf("eval corpus %s: scenario %q and expected_output.root_cause %q disagree",
				id, rec.Scenario, rec.ExpectedOutput.RootCause)
		}

		// Seed CSV and corpus must agree field-for-field (the seed<->corpus
		// binding QA's contradiction (a) breaks).
		if !floatsEqual(csvRow.amount, ext.Amount) {
			t.Errorf("%s: seed CSV amount %v disagrees with corpus amount %v", id, csvRow.amount, ext.Amount)
		}
		if csvRow.currency != ext.Currency {
			t.Errorf("%s: seed CSV currency %q disagrees with corpus currency %q", id, csvRow.currency, ext.Currency)
		}
		if csvRow.reference != ext.Reference {
			t.Errorf("%s: seed CSV reference %q disagrees with corpus reference %q", id, csvRow.reference, ext.Reference)
		}
	}
}

// floatsEqual reports whether a and b are equal within amountEpsilon.
func floatsEqual(a, b float64) bool {
	return math.Abs(a-b) < amountEpsilon
}

// sortedKeys returns the ids of a csvRow map for a deterministic failure message.
func sortedKeys(m map[string]csvRow) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return sortStrings(keys)
}

// sortedCorpusKeys returns the ids of a corpus map for a deterministic message.
func sortedCorpusKeys(m map[string]corpusRecord) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return sortStrings(keys)
}

// sortStrings sorts a slice in place (insertion sort; the set is tiny) and
// returns it, avoiding a sort import for a six-element slice.
func sortStrings(s []string) []string {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
	return s
}
