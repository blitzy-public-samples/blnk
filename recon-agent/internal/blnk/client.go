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
	reconID, err = c.InstantReconciliation(ctx, InstantReconciliationRequest{
		ExternalTransactions: []ExternalTransaction{txn},
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
