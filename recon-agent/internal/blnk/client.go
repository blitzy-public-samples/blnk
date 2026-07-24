package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// keyHeader is Blnk's API-key header. Every request this client issues
	// attaches it (Blnk gates all /reconciliation/* routes behind API-key auth).
	// The name is fixed by Blnk (api/middleware/auth.go: KeyHeader).
	keyHeader = "X-Blnk-Key"

	// Blnk reconciliation route paths (registered in Blnk api/api.go).
	routeUpload        = "/reconciliation/upload"
	routeStart         = "/reconciliation/start"
	routeStartInstant  = "/reconciliation/start-instant"
	routeReconByID     = "/reconciliation/" // + reconciliation id
	routeMatchingRules = "/reconciliation/matching-rules"
	// routeHealth is Blnk's unauthenticated liveness endpoint, used by Ready to
	// await dependency readiness at startup (finding L1).
	routeHealth = "/health"

	// probeStrategy is the reconciliation strategy used by ProbeBreak. A single
	// external transaction is probed one-to-one against the internal ledger.
	// Blnk accepts {one_to_one, one_to_many, many_to_one}; one_to_one is the
	// documented default for per-transaction reconciliation.
	probeStrategy = "one_to_one"
)

// Blnk reconciliation status values (mirrors Blnk reconciliation.go). ProbeBreak
// polls until the run reaches a terminal status before reading the counts.
const (
	statusCompleted = "completed"
	statusFailed    = "failed"
)

// Default bounds for ProbeBreak's polling loop. StartInstantReconciliation is
// asynchronous on the Blnk side: it returns a reconciliation_id immediately and
// populates the match/unmatch counts only once a background goroutine finishes.
// ProbeBreak therefore polls GET /reconciliation/:id until the run completes.
const (
	defaultProbeInterval = 100 * time.Millisecond
	defaultProbeTimeout  = 30 * time.Second
	defaultHTTPTimeout   = 60 * time.Second
)

// Client is the sole HTTP gateway to Blnk's reconciliation API. It wraps the
// six reconciliation routes plus the ProbeBreak break-identity bridge and
// attaches the X-Blnk-Key header to every request.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client

	// probeInterval and probeTimeout bound ProbeBreak's polling loop. They are
	// unexported and set to sane defaults by NewClient; tests may override them.
	probeInterval time.Duration
	probeTimeout  time.Duration
}

// NewClient constructs a Client. baseURL originates from BLNK_BASE_URL and
// apiKey from BLNK_API_KEY (both injected by cmd/main.go via the config
// package). A trailing slash on baseURL is trimmed so route joins are clean.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL:       strings.TrimRight(baseURL, "/"),
		apiKey:        apiKey,
		httpClient:    &http.Client{Timeout: defaultHTTPTimeout},
		probeInterval: defaultProbeInterval,
		probeTimeout:  defaultProbeTimeout,
	}
}

// Ready reports whether Blnk is reachable and serving, by issuing GET /health.
// cmd/main.go calls it (with bounded retries) to await dependency readiness
// before running the pipeline, so the agent never triages against a not-yet-up
// Blnk and reports a false-green result (finding L1). A transport error or a
// 5xx response means not-ready; any other response means reachable.
func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+routeHealth, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("blnk health check returned status %d", resp.StatusCode)
	}
	return nil
}

// UploadExternalData uploads an external statement via multipart/form-data
// (file part "file" + text field "source") to POST /reconciliation/upload.
func (c *Client) UploadExternalData(ctx context.Context, source, filename string, file io.Reader) (UploadResponse, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	part, err := w.CreateFormFile("file", filename)
	if err != nil {
		return UploadResponse{}, fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return UploadResponse{}, fmt.Errorf("copy file contents: %w", err)
	}
	if err := w.WriteField("source", source); err != nil {
		return UploadResponse{}, fmt.Errorf("write source field: %w", err)
	}
	if err := w.Close(); err != nil {
		return UploadResponse{}, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+routeUpload, &body)
	if err != nil {
		return UploadResponse{}, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set(keyHeader, c.apiKey)

	var out UploadResponse
	if err := c.do(req, http.StatusOK, &out); err != nil {
		return UploadResponse{}, err
	}
	return out, nil
}

// StartReconciliation kicks off a run against a previously uploaded batch via
// POST /reconciliation/start and returns the reconciliation id.
func (c *Client) StartReconciliation(ctx context.Context, req StartReconciliationRequest) (string, error) {
	httpReq, err := c.newJSONRequest(ctx, http.MethodPost, routeStart, req)
	if err != nil {
		return "", err
	}
	var out StartReconciliationResponse
	if err := c.do(httpReq, http.StatusOK, &out); err != nil {
		return "", err
	}
	return out.ReconciliationID, nil
}

// InstantReconciliation runs a reconciliation over inline external transactions
// via POST /reconciliation/start-instant and returns the reconciliation id.
func (c *Client) InstantReconciliation(ctx context.Context, req InstantReconciliationRequest) (string, error) {
	httpReq, err := c.newJSONRequest(ctx, http.MethodPost, routeStartInstant, req)
	if err != nil {
		return "", err
	}
	var out StartReconciliationResponse
	if err := c.do(httpReq, http.StatusOK, &out); err != nil {
		return "", err
	}
	return out.ReconciliationID, nil
}

// GetReconciliation reads a reconciliation's current state (including its
// integer match/unmatch counts) via GET /reconciliation/:id.
func (c *Client) GetReconciliation(ctx context.Context, reconciliationID string) (Reconciliation, error) {
	req, err := c.newJSONRequest(ctx, http.MethodGet, routeReconByID+url.PathEscape(reconciliationID), nil)
	if err != nil {
		return Reconciliation{}, err
	}
	var out Reconciliation
	if err := c.do(req, http.StatusOK, &out); err != nil {
		return Reconciliation{}, err
	}
	return out, nil
}

// CreateMatchingRule applies an agent-proposed rule via
// POST /reconciliation/matching-rules and returns the created rule (201).
func (c *Client) CreateMatchingRule(ctx context.Context, rule MatchingRule) (MatchingRule, error) {
	req, err := c.newJSONRequest(ctx, http.MethodPost, routeMatchingRules, rule)
	if err != nil {
		return MatchingRule{}, err
	}
	var out MatchingRule
	if err := c.do(req, http.StatusCreated, &out); err != nil {
		return MatchingRule{}, err
	}
	return out, nil
}

// UpdateMatchingRule updates an existing rule via
// PUT /reconciliation/matching-rules/:id and returns the updated rule (200).
func (c *Client) UpdateMatchingRule(ctx context.Context, ruleID string, rule MatchingRule) (MatchingRule, error) {
	req, err := c.newJSONRequest(ctx, http.MethodPut, routeMatchingRules+"/"+url.PathEscape(ruleID), rule)
	if err != nil {
		return MatchingRule{}, err
	}
	var out MatchingRule
	if err := c.do(req, http.StatusOK, &out); err != nil {
		return MatchingRule{}, err
	}
	return out, nil
}

// DeleteMatchingRule removes a matching rule via
// DELETE /reconciliation/matching-rules/:id (Blnk returns 200 with a message).
// It is used to roll back a rule the agent created for an auto-remediation
// attempt that did not clear, so a failed attempt leaves no orphan rule behind
// in the shared Blnk catalog (finding F5). A not-found rule is reported as an
// error for the caller to treat as best-effort (the orphan-prevention goal is
// already met when the rule is gone, however it got there).
func (c *Client) DeleteMatchingRule(ctx context.Context, ruleID string) error {
	req, err := c.newJSONRequest(ctx, http.MethodDelete, routeMatchingRules+"/"+url.PathEscape(ruleID), nil)
	if err != nil {
		return err
	}
	return c.do(req, http.StatusOK, nil)
}

// ProbeBreak is the break-identity bridge and the DETERMINISTIC ARBITER for
// whether a break is cleared (Rule 5.3). It submits a SINGLE external
// transaction through POST /reconciliation/start-instant with dry_run=true,
// then polls GET /reconciliation/:id until the (asynchronous) run reaches a
// terminal status and inspects the integer unmatched count:
//
//	unmatched_transactions == 0  => cleared = true  (the break resolved)
//	unmatched_transactions == 1  => cleared = false (still a break)
//
// reconID is the reconciliation id of the probe; callers record it on the
// resulting audit event. A failed run or a timeout is returned as an error so
// the caller can fail closed (Rule 5.7) and route the break to HITL. LLM
// confidence must never substitute for this deterministic check.
func (c *Client) ProbeBreak(ctx context.Context, txn ExternalTransaction, matchingRuleIDs []string) (cleared bool, reconID string, err error) {
	// Double-persist elimination (findings F3/F8). Blnk's start-instant
	// UNCONDITIONALLY persists every submitted external transaction by its OWN id
	// via a plain INSERT (database/reconciliation.go RecordExternalTransaction —
	// no ON CONFLICT), EVEN under dry_run (reconciliation.go
	// StartInstantReconciliation stores before matching). blnk.external_transactions
	// keys on the transaction id (TEXT PRIMARY KEY), so submitting a given
	// external id more than once collides on external_transactions_pkey and Blnk
	// returns HTTP 500 (RECON_START_FAILED) — which previously made the second
	// probe of a break (detect, then auto-remediation confirm) fail and made a
	// rerun of a resolved id fail.
	//
	// Blnk matches an external transaction against internal bookings
	// FIELD-TO-FIELD — amount/date/reference/description/currency — and NEVER by
	// the transaction id (reconciliation.go matchesRules / findMatchingInternal
	// Transaction), so the matched/unmatched verdict is invariant under the id.
	// We therefore submit a FRESH, unique ephemeral id on every probe while
	// preserving all matchable fields: the dry-run yields the identical verdict,
	// but no id is ever inserted twice, so repeated probes of the same break —
	// within one run and across reruns against a shared Blnk database — never
	// collide. The caller's txn value is left unmodified (probeTxn is a copy).
	probeTxn := txn
	probeTxn.ID = "probe-" + uuid.NewString()

	reconID, err = c.InstantReconciliation(ctx, InstantReconciliationRequest{
		ExternalTransactions: []ExternalTransaction{probeTxn},
		Strategy:             probeStrategy,
		DryRun:               true,
		MatchingRuleIDs:      matchingRuleIDs,
	})
	if err != nil {
		return false, "", fmt.Errorf("probe start-instant: %w", err)
	}

	pollCtx, cancel := context.WithTimeout(ctx, c.probeTimeout)
	defer cancel()

	ticker := time.NewTicker(c.probeInterval)
	defer ticker.Stop()

	for {
		recon, gErr := c.GetReconciliation(pollCtx, reconID)
		if gErr == nil {
			switch recon.Status {
			case statusCompleted:
				return recon.UnmatchedTransactions == 0, reconID, nil
			case statusFailed:
				return false, reconID, fmt.Errorf("probe reconciliation %s failed", reconID)
			}
		}
		select {
		case <-pollCtx.Done():
			return false, reconID, fmt.Errorf("probe reconciliation %s did not complete: %w", reconID, pollCtx.Err())
		case <-ticker.C:
			// poll again
		}
	}
}

// newJSONRequest builds an HTTP request with the X-Blnk-Key header attached and,
// when body is non-nil, a JSON payload plus Content-Type: application/json.
func (c *Client) newJSONRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(keyHeader, c.apiKey)
	return req, nil
}

// do executes req, requires the given status code, and decodes a JSON response
// body into out when out is non-nil.
func (c *Client) do(req *http.Request, wantStatus int, out any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != wantStatus {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: unexpected status %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", req.Method, req.URL.Path, err)
		}
	}
	return nil
}
