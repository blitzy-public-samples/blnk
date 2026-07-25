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

// Command seed bootstraps the reconciliation demo end-to-end through Blnk's
// public HTTP API. It performs two steps, in order:
//
//  1. Creates an internal ledger, one or more balances, and the internal
//     transactions declared in seed/internal_ledger.json. These form the
//     "internal side" that Blnk's reconciliation engine matches against.
//  2. Uploads the external statement seed/external_transactions.csv (six
//     deliberately planted "breaks") to Blnk via POST /reconciliation/upload.
//
// It is invoked by `make seed`, which runs `go run ./seed` from the repository
// root after sourcing .env.
//
// # Dependency discipline
//
// This program depends ONLY on the Go standard library. It intentionally does
// NOT import any third-party module — not even Blnk's own packages such as
// github.com/blnkfinance/blnk/model. Any non-stdlib import would force an
// addition to Blnk's root go.mod/go.sum, which the additive-preservation rule
// forbids (the root manifests must remain byte-for-byte unchanged). The small
// request/response shapes the seed needs are therefore redeclared locally as
// plain structs that mirror Blnk's documented JSON contracts.
//
// The seed reaches Blnk exclusively through documented HTTP routes
// (/ledgers, /balances, /transactions, /reconciliation/upload), which the
// native-API-only rule explicitly permits for the seed program.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// blnkKeyHeader is the HTTP header Blnk uses for API-key authentication. It
// mirrors the KeyHeader constant defined in api/middleware/auth.go. The header
// is attached to every outbound request: sending it when empty is harmless
// (Blnk skips auth entirely when secure mode is off) and it is required when
// secure mode is on.
const blnkKeyHeader = "X-Blnk-Key"

// httpTimeout bounds every HTTP call so a hung Blnk instance cannot make the
// seed run forever.
const httpTimeout = 30 * time.Second

// Readiness-probe tuning. The probe tolerates a just-started stack by retrying
// on transport (connection-refused) errors only; it never blocks indefinitely.
const (
	readinessAttempts = 10
	readinessDelay    = 1 * time.Second
)

// Response and redirect bounds (M-06). The Blnk responses the seed consumes are
// small JSON documents, so a few MiB is a generous cap that still prevents a
// malicious or misconfigured endpoint from streaming an unbounded body into
// memory. Redirects are restricted to the same origin so the custom X-Blnk-Key
// header — which Go, unlike Authorization/Cookie, does NOT strip on a
// cross-origin redirect — can never be forwarded to a foreign host.
const (
	maxRespBytes int64 = 4 << 20 // 4 MiB cap on any response body the seed reads
	maxRedirects       = 10      // upper bound on a same-origin redirect chain
)

// External-statement input bounds (M-07). The seed validates the CSV strictly
// and with hard limits so a malformed, hostile, or accidentally-huge statement
// fails fast instead of exhausting memory or smuggling non-finite/duplicate
// data past Blnk (which silently coerces an unparseable amount to 0).
const (
	maxCSVFileBytes  int64 = 8 << 20  // 8 MiB cap on the whole statement file
	maxCSVRows             = 100_000  // cap on data rows (excludes the header)
	maxCSVFieldBytes       = 64 << 10 // 64 KiB cap on any single field
	maxAbsAmount           = 1e12     // reject non-finite / absurd-magnitude amounts
)

// config holds the resolved runtime configuration. Values come from
// environment variables (which take precedence over the built-in defaults) and
// may be overridden by command-line flags.
type config struct {
	baseURL    string // Blnk base URL, e.g. http://localhost:5001
	apiKey     string // value sent in the X-Blnk-Key header
	ledgerFile string // path to the internal ledger JSON
	csvFile    string // path to the external transactions CSV
	source     string // source label attached to the CSV upload
}

// seedData mirrors the schema of seed/internal_ledger.json. Only the fields the
// seed reads are declared. The struct tags MUST stay in lock-step with that
// data file — the two form a private contract.
type seedData struct {
	Ledger struct {
		Name     string         `json:"name"`
		MetaData map[string]any `json:"meta_data"`
	} `json:"ledger"`
	Balances []struct {
		Key              string         `json:"key"`
		Currency         string         `json:"currency"`
		Precision        float64        `json:"precision"`
		TrackFundLineage bool           `json:"track_fund_lineage"`
		MetaData         map[string]any `json:"meta_data"`
	} `json:"balances"`
	Transactions []struct {
		Reference      string  `json:"reference"`
		Amount         float64 `json:"amount"`
		Currency       string  `json:"currency"`
		Description    string  `json:"description"`
		Source         string  `json:"source"`
		Destination    string  `json:"destination"`
		Precision      float64 `json:"precision"`
		AllowOverdraft bool    `json:"allow_overdraft"`
		SkipQueue      bool    `json:"skip_queue"`
		Scenario       string  `json:"scenario"`
	} `json:"transactions"`
}

// --- Request payload DTOs (mirror Blnk's api/model JSON keys exactly) ---

// createLedgerRequest is the body for POST /ledgers. Blnk requires a non-empty
// name; meta_data is optional.
type createLedgerRequest struct {
	Name     string         `json:"name"`
	MetaData map[string]any `json:"meta_data,omitempty"`
}

// createBalanceRequest is the body for POST /balances. identity_id is
// deliberately omitted: Blnk only requires it when track_fund_lineage is true.
type createBalanceRequest struct {
	LedgerID         string         `json:"ledger_id"`
	Currency         string         `json:"currency"`
	Precision        float64        `json:"precision"`
	TrackFundLineage bool           `json:"track_fund_lineage"`
	MetaData         map[string]any `json:"meta_data,omitempty"`
}

// recordTransactionRequest is the body for POST /transactions. Only the scalar
// source/destination form is used (never the sources/destinations arrays) so
// Blnk's "exactly one of source|sources" validation is satisfied.
type recordTransactionRequest struct {
	Amount         float64        `json:"amount"`
	Precision      float64        `json:"precision"`
	Currency       string         `json:"currency"`
	Reference      string         `json:"reference"`
	Description    string         `json:"description"`
	Source         string         `json:"source"`
	Destination    string         `json:"destination"`
	AllowOverdraft bool           `json:"allow_overdraft"`
	SkipQueue      bool           `json:"skip_queue"`
	MetaData       map[string]any `json:"meta_data,omitempty"`
}

// --- Response DTOs (only the fields the seed consumes) ---

// createLedgerResponse captures the ledger_id returned by POST /ledgers.
type createLedgerResponse struct {
	LedgerID string `json:"ledger_id"`
}

// createBalanceResponse captures the balance_id returned by POST /balances.
type createBalanceResponse struct {
	BalanceID string `json:"balance_id"`
}

// uploadResponse captures the JSON returned by POST /reconciliation/upload.
type uploadResponse struct {
	UploadID    string `json:"upload_id"`
	RecordCount int    `json:"record_count"`
	Source      string `json:"source"`
}

// blnkClient is a thin standard-library HTTP client for Blnk's public API.
// Every request it issues carries the X-Blnk-Key header.
type blnkClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// newBlnkClient builds a client with a bounded timeout. baseURL has any
// trailing slash trimmed by the caller so that baseURL+path never doubles up.
func newBlnkClient(baseURL, apiKey string) *blnkClient {
	return &blnkClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		http: &http.Client{
			Timeout:       httpTimeout,
			CheckRedirect: sameOriginOnly,
		},
	}
}

// sameOriginOnly is an http.Client.CheckRedirect hook that refuses any redirect
// to a different origin (scheme+host) and caps the length of a same-origin
// redirect chain. This is a security control (M-06): the seed attaches the
// custom X-Blnk-Key header to every request, and Go's http.Client forwards
// custom headers across redirects — it only strips a fixed set (Authorization,
// WWW-Authenticate, Cookie) on a cross-origin hop. Refusing cross-origin
// redirects outright guarantees the key is never delivered to a foreign host.
func sameOriginOnly(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	// via[0] is the original request; every hop must stay on its origin.
	if len(via) > 0 && !sameOrigin(via[0].URL, req.URL) {
		return fmt.Errorf("refusing cross-origin redirect from %s://%s to %s://%s",
			via[0].URL.Scheme, via[0].URL.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

// sameOrigin reports whether two URLs share the same scheme and host
// (case-insensitively), i.e. the same web origin.
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}

// postJSON marshals payload to JSON and POSTs it to baseURL+path with a JSON
// content type and the X-Blnk-Key header. It returns the raw response body and
// HTTP status code. A transport-level failure is returned as an error; the
// caller decides whether a non-2xx status is fatal.
func (c *blnkClient) postJSON(path string, payload any) ([]byte, int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request body for %s: %w", path, err)
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("build request for %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(blnkKeyHeader, c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response body for %s: %w", path, err)
	}
	return respBody, resp.StatusCode, nil
}

// uploadFile POSTs the file at csvPath to baseURL+path as a multipart/form-data
// request containing a "file" file-part (whose filename is the CSV's base name)
// and a "source" field-part. The X-Blnk-Key header is attached. It returns the
// raw response body and HTTP status code.
func (c *blnkClient) uploadFile(path, csvPath, source string) ([]byte, int, error) {
	fileBytes, err := readFileBounded(csvPath, maxCSVFileBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("read csv file %s: %w", csvPath, err)
	}

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(csvPath))
	if err != nil {
		return nil, 0, fmt.Errorf("create multipart file part: %w", err)
	}
	if _, err := part.Write(fileBytes); err != nil {
		return nil, 0, fmt.Errorf("write multipart file part: %w", err)
	}
	if err := writer.WriteField("source", source); err != nil {
		return nil, 0, fmt.Errorf("write multipart source field: %w", err)
	}
	// The multipart writer MUST be closed before the request is sent so the
	// terminating boundary is emitted and Content-Length is correct.
	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return nil, 0, fmt.Errorf("build upload request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set(blnkKeyHeader, c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read upload response body: %w", err)
	}
	return respBody, resp.StatusCode, nil
}

// transactionExistsByRef reports whether an internal transaction with the given
// reference already exists in Blnk, via GET /transactions/reference/:reference.
// A 2xx response means the transaction exists; a 404 means it does not. Any
// other status — or a transport error — is returned as an error so a caller
// never mistakes an ambiguous backend failure for "absent". It underpins the
// seed's idempotency guard (so a re-run neither creates orphan ledgers/balances
// nor aborts on a duplicate-reference conflict).
func (c *blnkClient) transactionExistsByRef(reference string) (bool, error) {
	endpoint := c.baseURL + "/transactions/reference/" + url.PathEscape(reference)
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("build request for reference %q: %w", reference, err)
	}
	req.Header.Set(blnkKeyHeader, c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("GET /transactions/reference/%s: %w", reference, err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain the body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case is2xx(resp.StatusCode):
		return true, nil
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("GET /transactions/reference/%s returned unexpected HTTP %d", reference, resp.StatusCode)
	}
}

// waitForReady best-effort waits for Blnk to accept connections by polling the
// unauthenticated GET /health endpoint. It retries only on transport errors
// (e.g. connection refused while the stack is still starting) and returns as
// soon as the server produces ANY HTTP response. It is bounded by
// readinessAttempts and never blocks indefinitely; if the server never comes
// up, it returns quietly and lets the first real request surface a clear error.
func (c *blnkClient) waitForReady() {
	for attempt := 1; attempt <= readinessAttempts; attempt++ {
		req, err := http.NewRequest(http.MethodGet, c.baseURL+"/health", nil)
		if err != nil {
			// A malformed base URL is a configuration error; do not spin on it.
			return
		}
		req.Header.Set(blnkKeyHeader, c.apiKey)

		resp, err := c.http.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return
		}

		if attempt < readinessAttempts {
			log.Printf("waiting for Blnk at %s (attempt %d/%d): %v", c.baseURL, attempt, readinessAttempts, err)
			time.Sleep(readinessDelay)
		}
	}
}

// loadConfig resolves configuration from environment variables and flags. Flag
// defaults are seeded from the environment (or a built-in default), and an
// explicitly-provided flag overrides the environment. Effective precedence is
// therefore: flag > environment variable > built-in default.
func loadConfig() config {
	baseURL := flag.String("blnk-url", envOr("BLNK_BASE_URL", "http://localhost:5001"),
		"Blnk base URL")
	apiKey := flag.String("blnk-key", envOr("BLNK_API_KEY", ""),
		"Blnk API key sent in the X-Blnk-Key header")
	ledgerFile := flag.String("ledger-file", envOr("SEED_LEDGER_FILE", filepath.Join("seed", "internal_ledger.json")),
		"path to the internal ledger JSON file")
	csvFile := flag.String("csv-file", envOr("SEED_CSV_FILE", filepath.Join("seed", "external_transactions.csv")),
		"path to the external transactions CSV file")
	source := flag.String("source", envOr("SEED_SOURCE", "seed-bank"),
		"source label attached to the external CSV upload")
	flag.Parse()

	return config{
		baseURL:    strings.TrimRight(*baseURL, "/"),
		apiKey:     *apiKey,
		ledgerFile: *ledgerFile,
		csvFile:    *csvFile,
		source:     *source,
	}
}

// envOr returns the value of the environment variable named key when it is set
// and non-empty; otherwise it returns def. An empty environment value is
// treated as unset so a stray `KEY=` in .env falls back to the default.
func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

// is2xx reports whether status is a 2xx success code. Blnk returns 201 Created
// for ledger/balance/transaction creation and 200 OK for the CSV upload, so the
// seed accepts any 2xx status uniformly.
func is2xx(status int) bool {
	return status >= 200 && status < 300
}

// readFileBounded reads at most limit bytes from the file at path, returning an
// error if the file exceeds the limit (M-06/M-07). It guards the upload path
// against a statement that grew — or was swapped for a symlink to something
// enormous — between validation and upload, so the seed never buffers an
// unbounded file into memory.
func readFileBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	// Read one extra byte so a file exactly at the limit is accepted while a
	// larger one is reliably detected.
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("file %s exceeds the %d-byte limit", path, limit)
	}
	return data, nil
}

// loadSeedData reads and decodes the internal ledger JSON file into seedData,
// failing with an actionable error if the file is missing, invalid, or lacks a
// ledger name.
func loadSeedData(path string) (*seedData, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read seed file %s: %w", path, err)
	}

	var data seedData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("parse seed file %s as JSON: %w", path, err)
	}
	if strings.TrimSpace(data.Ledger.Name) == "" {
		return nil, fmt.Errorf("seed file %s: ledger.name is required and must be non-empty", path)
	}
	return &data, nil
}

// internalSeedState classifies whether the internal transactions declared by
// the seed are already present in Blnk. It drives the idempotency guard.
type internalSeedState int

const (
	// seedAbsent means none of the declared internal transaction references
	// exist yet — a clean, first-time run.
	seedAbsent internalSeedState = iota
	// seedPresent means every declared internal transaction reference already
	// exists — the environment is already seeded, so seeding is a no-op.
	seedPresent
	// seedPartial means some (but not all) declared references exist — an
	// ambiguous/inconsistent state the seed refuses to build on top of.
	seedPartial
)

// checkInternalSeedState probes each declared internal transaction reference
// (via the native GET /transactions/reference/:reference route) and reports
// whether the internal side is fully absent, fully present, or partially
// present, along with the number of references found. It is the read-only
// preflight that makes the seed idempotent: it runs BEFORE any ledger, balance,
// or transaction is created, so a re-run never leaves orphan objects behind.
func checkInternalSeedState(client *blnkClient, data *seedData) (state internalSeedState, present int, err error) {
	total := len(data.Transactions)
	if total == 0 {
		return seedAbsent, 0, nil
	}

	for _, t := range data.Transactions {
		exists, probeErr := client.transactionExistsByRef(t.Reference)
		if probeErr != nil {
			return seedPartial, present, probeErr
		}
		if exists {
			present++
		}
	}

	switch present {
	case 0:
		return seedAbsent, 0, nil
	case total:
		return seedPresent, present, nil
	default:
		return seedPartial, present, nil
	}
}

// requiredCSVColumns are the columns the external statement MUST provide. They
// mirror the case-insensitive column contract enforced by
// internal/files/files.go. The seed validates them locally so a malformed
// statement fails fast with a clear message instead of being silently accepted
// by Blnk, which coerces an unparseable amount to 0 and an unparseable date to
// the zero time rather than rejecting the row.
var requiredCSVColumns = []string{"id", "amount", "currency", "reference", "description", "date"}

// validateExternalCSV reads and strictly validates the external statement CSV,
// returning the number of data rows (excluding the header). Validation is
// bounded and streaming (M-07): the file is capped at maxCSVFileBytes, the
// number of data rows at maxCSVRows, and each field at maxCSVFieldBytes, so a
// malformed or hostile statement fails fast rather than exhausting memory.
//
// A row is rejected if a required column is missing, the field count is wrong,
// any field is too large, a required field is empty, an external id is
// duplicated, the amount is not a FINITE float in (0, maxAbsAmount], or the
// date is not valid RFC3339. Rejecting NaN/±Inf and non-numeric amounts matters
// because Blnk silently coerces an unparseable amount to 0; likewise it coerces
// an unparseable date to the zero time. The returned count is later asserted
// against the record_count Blnk reports for the upload, so a silently dropped
// or added row is caught too.
func validateExternalCSV(csvPath string) (int, error) {
	// Fast pre-check on the file size before opening for read, so an obviously
	// oversized statement is rejected without streaming any of it.
	if info, statErr := os.Stat(csvPath); statErr == nil && info.Size() > maxCSVFileBytes {
		return 0, fmt.Errorf("csv %s is %d bytes, exceeding the %d-byte limit",
			csvPath, info.Size(), maxCSVFileBytes)
	}

	f, err := os.Open(csvPath)
	if err != nil {
		return 0, fmt.Errorf("open csv file %s: %w", csvPath, err)
	}
	defer func() { _ = f.Close() }()

	// Defense-in-depth byte bound: even if os.Stat under-reported (a symlink,
	// concurrent growth, or a stat error above), never read more than the cap
	// (+1 to keep a file exactly at the limit valid). Reading is row-by-row; the
	// whole file is never buffered.
	reader := csv.NewReader(io.LimitReader(f, maxCSVFileBytes+1))
	// Disable the automatic fields-per-record check so we can surface a clearer,
	// row-numbered error message ourselves.
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("read csv header from %s: %w", csvPath, err)
	}

	// Build a case-insensitive column-name -> index map (mirrors files.go).
	colIndex := make(map[string]int, len(header))
	for i, name := range header {
		colIndex[strings.ToLower(strings.TrimSpace(name))] = i
	}
	for _, col := range requiredCSVColumns {
		if _, ok := colIndex[col]; !ok {
			return 0, fmt.Errorf("csv %s is missing required column %q (need: %s)",
				csvPath, col, strings.Join(requiredCSVColumns, ", "))
		}
	}

	idIdx := colIndex["id"]
	seenIDs := make(map[string]int) // external id -> the first line it appeared on

	dataRows := 0
	line := 1 // the header row was already consumed
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		line++
		if readErr != nil {
			return 0, fmt.Errorf("read csv %s row %d: %w", csvPath, line, readErr)
		}

		// Cap the number of data rows so an enormous statement cannot make the
		// seed run unbounded.
		if dataRows >= maxCSVRows {
			return 0, fmt.Errorf("csv %s exceeds the %d-row limit", csvPath, maxCSVRows)
		}

		if len(record) != len(header) {
			return 0, fmt.Errorf("csv %s row %d: expected %d fields but found %d",
				csvPath, line, len(header), len(record))
		}

		// Bound each field's size so a single monster cell cannot balloon memory.
		for i, field := range record {
			if len(field) > maxCSVFieldBytes {
				return 0, fmt.Errorf("csv %s row %d: field %d is %d bytes, exceeding the %d-byte limit",
					csvPath, line, i+1, len(field), maxCSVFieldBytes)
			}
		}

		for _, col := range requiredCSVColumns {
			if strings.TrimSpace(record[colIndex[col]]) == "" {
				return 0, fmt.Errorf("csv %s row %d: required field %q is empty", csvPath, line, col)
			}
		}

		// External id must be unique across the statement: Blnk keys external
		// transactions by id, so a duplicate would collide on upload/reconcile.
		id := strings.TrimSpace(record[idIdx])
		if first, dup := seenIDs[id]; dup {
			return 0, fmt.Errorf("csv %s row %d: duplicate id %q (first seen on row %d)",
				csvPath, line, id, first)
		}
		seenIDs[id] = line

		// Amount must parse as a FINITE float within a sane magnitude and be
		// non-zero. Blnk would otherwise coerce an unparseable amount to 0; a
		// NaN/±Inf or absurd magnitude is rejected here so it can never enter
		// reconciliation.
		amountStr := strings.TrimSpace(record[colIndex["amount"]])
		amount, convErr := strconv.ParseFloat(amountStr, 64)
		if convErr != nil {
			return 0, fmt.Errorf("csv %s row %d: amount %q is not a valid number", csvPath, line, amountStr)
		}
		if math.IsNaN(amount) || math.IsInf(amount, 0) {
			return 0, fmt.Errorf("csv %s row %d: amount %q is not finite", csvPath, line, amountStr)
		}
		if amount == 0 {
			return 0, fmt.Errorf("csv %s row %d: amount must be non-zero", csvPath, line)
		}
		if math.Abs(amount) > maxAbsAmount {
			return 0, fmt.Errorf("csv %s row %d: amount %q exceeds the maximum magnitude %g",
				csvPath, line, amountStr, float64(maxAbsAmount))
		}

		// Date must parse as RFC3339; Blnk would otherwise coerce it to the zero time.
		dateStr := strings.TrimSpace(record[colIndex["date"]])
		if _, convErr := time.Parse(time.RFC3339, dateStr); convErr != nil {
			return 0, fmt.Errorf("csv %s row %d: date %q is not a valid RFC3339 timestamp", csvPath, line, dateStr)
		}
		dataRows++
	}

	if dataRows == 0 {
		return 0, fmt.Errorf("csv %s contains a header but no data rows", csvPath)
	}
	return dataRows, nil
}

// createLedger creates the internal ledger via POST /ledgers and returns the
// generated ledger_id.
func createLedger(client *blnkClient, data *seedData) (string, error) {
	req := createLedgerRequest{Name: data.Ledger.Name, MetaData: data.Ledger.MetaData}

	body, status, err := client.postJSON("/ledgers", req)
	if err != nil {
		return "", err
	}
	if !is2xx(status) {
		return "", fmt.Errorf("POST /ledgers returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}

	var resp createLedgerResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode ledger response: %w (body=%s)", err, string(body))
	}
	if resp.LedgerID == "" {
		return "", fmt.Errorf("ledger response did not include a ledger_id (body=%s)", string(body))
	}
	return resp.LedgerID, nil
}

// createBalances creates each declared balance via POST /balances and returns a
// map of balance key -> generated balance_id used later for endpoint
// substitution in transactions.
func createBalances(client *blnkClient, data *seedData, ledgerID string) (map[string]string, error) {
	balanceIDs := make(map[string]string, len(data.Balances))

	for i, b := range data.Balances {
		req := createBalanceRequest{
			LedgerID:         ledgerID,
			Currency:         b.Currency,
			Precision:        b.Precision,
			TrackFundLineage: b.TrackFundLineage,
			MetaData:         b.MetaData,
		}

		body, status, err := client.postJSON("/balances", req)
		if err != nil {
			return nil, err
		}
		if !is2xx(status) {
			return nil, fmt.Errorf("POST /balances (key=%q) returned HTTP %d: %s",
				b.Key, status, strings.TrimSpace(string(body)))
		}

		var resp createBalanceResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode balance response (key=%q): %w (body=%s)", b.Key, err, string(body))
		}
		if resp.BalanceID == "" {
			return nil, fmt.Errorf("balance response did not include a balance_id (key=%q, body=%s)", b.Key, string(body))
		}

		if b.Key != "" {
			balanceIDs[b.Key] = resp.BalanceID
		}
		log.Printf("created balance[%d] key=%q currency=%s -> balance_id=%s", i, b.Key, b.Currency, resp.BalanceID)
	}
	return balanceIDs, nil
}

// createTransactions creates each declared internal transaction via
// POST /transactions and returns the number successfully created. Endpoints are
// resolved with the balance-key substitution rule, and each transaction is sent
// with skip_queue=true (applied synchronously so it is immediately visible to
// reconciliation) and allow_overdraft=true.
func createTransactions(client *blnkClient, data *seedData, balanceIDs map[string]string) (int, error) {
	count := 0

	for i, t := range data.Transactions {
		source := resolveEndpoint(t.Source, balanceIDs)
		destination := resolveEndpoint(t.Destination, balanceIDs)

		// Copy the documentation-only scenario label into meta_data for
		// traceability. When empty, omitempty drops the field entirely.
		var meta map[string]any
		if t.Scenario != "" {
			meta = map[string]any{"scenario": t.Scenario}
		}

		req := recordTransactionRequest{
			Amount:         t.Amount,
			Precision:      t.Precision,
			Currency:       t.Currency,
			Reference:      t.Reference,
			Description:    t.Description,
			Source:         source,
			Destination:    destination,
			AllowOverdraft: t.AllowOverdraft,
			SkipQueue:      t.SkipQueue,
			MetaData:       meta,
		}

		body, status, err := client.postJSON("/transactions", req)
		if err != nil {
			return count, err
		}
		if !is2xx(status) {
			return count, fmt.Errorf("POST /transactions (reference=%q) returned HTTP %d: %s",
				t.Reference, status, strings.TrimSpace(string(body)))
		}

		count++
		log.Printf("created transaction[%d] reference=%s amount=%s %s (%s -> %s)",
			i, t.Reference, formatAmount(t.Amount), t.Currency, source, destination)
	}
	return count, nil
}

// resolveEndpoint applies the balance-key substitution rule: if value equals a
// declared balance key, it returns that balance's created balance_id; otherwise
// it passes value through unchanged (so an indicator like "@world" is preserved
// and Blnk auto-creates the corresponding balance).
func resolveEndpoint(value string, balanceIDs map[string]string) string {
	if id, ok := balanceIDs[value]; ok {
		return id
	}
	return value
}

// uploadExternal uploads the external CSV statement via
// POST /reconciliation/upload and returns the parsed response.
func uploadExternal(client *blnkClient, csvPath, source string) (*uploadResponse, error) {
	body, status, err := client.uploadFile("/reconciliation/upload", csvPath, source)
	if err != nil {
		return nil, err
	}
	if !is2xx(status) {
		return nil, fmt.Errorf("POST /reconciliation/upload returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}

	var resp uploadResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode upload response: %w (body=%s)", err, string(body))
	}
	if resp.UploadID == "" {
		return nil, fmt.Errorf("upload response did not include an upload_id (body=%s)", string(body))
	}
	return &resp, nil
}

// formatAmount renders a float amount compactly for human-readable log lines
// (e.g. 1500.00 -> "1500", 250.75 -> "250.75"). It is used only for logging and
// does not affect the numeric value sent to Blnk.
func formatAmount(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", f), "0"), ".")
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("[seed] ")

	cfg := loadConfig()

	data, err := loadSeedData(cfg.ledgerFile)
	if err != nil {
		log.Fatalf("failed to load seed data: %v", err)
	}

	// Validate the external statement up front so a malformed CSV fails fast —
	// before any ledger/balance/transaction is created — and so its data-row
	// count can be asserted against Blnk's reported record_count after upload.
	expectedRecords, err := validateExternalCSV(cfg.csvFile)
	if err != nil {
		log.Fatalf("external statement validation failed: %v", err)
	}

	client := newBlnkClient(cfg.baseURL, cfg.apiKey)

	// Tolerate a just-started stack; best-effort, never blocks indefinitely.
	client.waitForReady()

	// Idempotency guard: probe whether the internal side already exists BEFORE
	// creating anything, so a re-run neither creates orphan ledgers/balances nor
	// aborts on a duplicate-reference conflict.
	state, present, err := checkInternalSeedState(client, data)
	if err != nil {
		log.Fatalf("failed to probe existing seed state: %v", err)
	}
	switch state {
	case seedPresent:
		// Already fully seeded — do nothing (idempotent no-op). Re-creating the
		// ledger/balances would orphan objects and re-uploading the CSV would
		// collide on the external transaction primary key.
		log.Printf("internal side already seeded: all %d internal transaction(s) present; "+
			"skipping ledger, balance, transaction creation and CSV upload (idempotent no-op)",
			len(data.Transactions))
		fmt.Printf("Seed skipped: already seeded (internal_transactions=%d present)\n", present)
		return
	case seedPartial:
		// Ambiguous/inconsistent state — refuse to build on top of it. Nothing
		// new has been created at this point.
		log.Fatalf("refusing to seed: %d of %d internal transaction(s) already present — "+
			"the environment is in a partial/inconsistent state. Reset the database before "+
			"re-seeding. No ledger, balance, or transaction was created.",
			present, len(data.Transactions))
	case seedAbsent:
		// Clean environment — fall through to a fresh seed below.
	}

	ledgerID, err := createLedger(client, data)
	if err != nil {
		log.Fatalf("failed to create ledger: %v", err)
	}
	log.Printf("created ledger %q -> ledger_id=%s", data.Ledger.Name, ledgerID)

	balanceIDs, err := createBalances(client, data, ledgerID)
	if err != nil {
		log.Fatalf("failed to create balances: %v", err)
	}

	txnCount, err := createTransactions(client, data, balanceIDs)
	if err != nil {
		log.Fatalf("failed to create internal transactions: %v", err)
	}

	upload, err := uploadExternal(client, cfg.csvFile, cfg.source)
	if err != nil {
		log.Fatalf("failed to upload external statement: %v", err)
	}

	// Assert Blnk ingested exactly the number of data rows the CSV contains.
	// Blnk tolerates and silently coerces malformed rows, so without this check
	// a dropped or altered row would go unnoticed.
	if upload.RecordCount != expectedRecords {
		log.Fatalf("upload record_count mismatch: Blnk reported %d but %s contains %d data row(s); "+
			"the upload may have silently dropped or altered rows",
			upload.RecordCount, cfg.csvFile, expectedRecords)
	}

	// One-line internal-side summary for operators and CI logs.
	log.Printf("internal side ready: ledger_id=%s balances=%d internal_transactions=%d",
		ledgerID, len(balanceIDs), txnCount)

	// Required output: a human-readable summary plus parseable key=value lines
	// so the demo/operator can capture the upload identifiers programmatically.
	fmt.Printf("Seed complete: upload_id=%s record_count=%d source=%s\n",
		upload.UploadID, upload.RecordCount, upload.Source)
	fmt.Printf("upload_id=%s\n", upload.UploadID)
	fmt.Printf("record_count=%d\n", upload.RecordCount)
	fmt.Printf("source=%s\n", upload.Source)
}
