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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
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
		http:    &http.Client{Timeout: httpTimeout},
	}
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

	respBody, err := io.ReadAll(resp.Body)
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
	fileBytes, err := os.ReadFile(csvPath)
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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read upload response body: %w", err)
	}
	return respBody, resp.StatusCode, nil
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

	client := newBlnkClient(cfg.baseURL, cfg.apiKey)

	// Tolerate a just-started stack; best-effort, never blocks indefinitely.
	client.waitForReady()

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
