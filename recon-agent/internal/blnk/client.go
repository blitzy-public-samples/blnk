package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	// routeReadinessProbe is a sentinel reconciliation id used by Ready to
	// exercise the AUTHENTICATED GET /reconciliation/:id contract (M-05, Rule
	// 5.1). Blnk returns 404 for an unknown id ONLY after the request has passed
	// API-key auth, so a 200/404 response proves both that the reconciliation
	// surface is reachable AND that the agent's X-Blnk-Key is accepted. Readiness
	// therefore stays strictly within the mandated /reconciliation/* surface
	// instead of calling Blnk's out-of-scope /health endpoint, and it no longer
	// treats an auth failure (401/403) or a server error (5xx) as "ready".
	routeReadinessProbe = "recon-agent-readiness-probe"

	// probeStrategy is the reconciliation strategy used by ProbeBreak. A single
	// external transaction is probed one-to-one against the internal ledger.
	// Blnk accepts {one_to_one, one_to_many, many_to_one}; one_to_one is the
	// documented default for per-transaction reconciliation. It aliases the
	// exported StrategyOneToOne so the literal is declared exactly once.
	probeStrategy = StrategyOneToOne
)

// Exported Blnk-wire vocabulary (finding m-01): the single source of truth for
// the reconciliation strategy name and Blnk's terminal run-status strings that
// the agent must speak on Blnk's HTTP contract. cmd consumes these exported
// constants instead of re-declaring the same literals. They live here — rather
// than in internal/model alongside the agent's own lifecycle/grammar
// vocabulary — because internal/model imports internal/blnk, so internal/blnk
// must not import internal/model (that would cycle); and these values describe
// Blnk's HTTP contract, not the agent's domain, making internal/blnk their
// correct home.
const (
	// StrategyOneToOne is Blnk's per-transaction reconciliation strategy.
	StrategyOneToOne = "one_to_one"
	// ReconStatusCompleted is Blnk's terminal "run finished" status.
	ReconStatusCompleted = "completed"
	// ReconStatusFailed is Blnk's terminal "run failed" status.
	ReconStatusFailed = "failed"
)

// Blnk reconciliation status values (mirrors Blnk reconciliation.go). ProbeBreak
// polls until the run reaches a terminal status before reading the counts. These
// unexported names alias the exported constants above so the literals are
// declared exactly once (finding m-01).
const (
	statusCompleted = ReconStatusCompleted
	statusFailed    = ReconStatusFailed
)

// Default bounds for ProbeBreak's polling loop. StartInstantReconciliation is
// asynchronous on the Blnk side: it returns a reconciliation_id immediately and
// populates the match/unmatch counts only once a background goroutine finishes.
// ProbeBreak therefore polls GET /reconciliation/:id until the run completes.
const (
	defaultProbeInterval = 100 * time.Millisecond
	defaultProbeTimeout  = 30 * time.Second
	defaultHTTPTimeout   = 60 * time.Second

	// maxRedirects caps how many redirects the client will follow before giving
	// up, matching net/http's own default. Blnk's reconciliation API is not
	// expected to redirect at all; this is a safety backstop.
	maxRedirects = 10

	// defaultMaxResponseBytes bounds how many bytes of a response body the client
	// will read/decode (M-06). Every Blnk reconciliation response the agent
	// consumes (upload receipt, reconciliation state with integer counts, a
	// single matching rule) is small, so this generous cap cannot truncate a
	// legitimate payload yet prevents an unbounded or hostile body from
	// exhausting memory.
	defaultMaxResponseBytes int64 = 4 << 20 // 4 MiB
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

	// maxRespBytes bounds how many bytes of any response body the client reads
	// or JSON-decodes (M-06). Set by NewClient; tests may shrink it to assert the
	// bound is enforced.
	maxRespBytes int64
}

// NewClient constructs a Client. baseURL originates from BLNK_BASE_URL and
// apiKey from BLNK_API_KEY (both injected by cmd/main.go via the config
// package). A trailing slash on baseURL is trimmed so route joins are clean.
//
// The underlying http.Client installs a same-origin redirect policy (M-06):
// because the X-Blnk-Key header is a CUSTOM header, net/http would NOT strip it
// on a cross-host redirect (it only strips well-known sensitive headers), so a
// redirect to a foreign origin could otherwise leak the Blnk API key. The
// client refuses any redirect that leaves the original scheme+host.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout:       defaultHTTPTimeout,
			CheckRedirect: sameOriginOnly,
		},
		probeInterval: defaultProbeInterval,
		probeTimeout:  defaultProbeTimeout,
		maxRespBytes:  defaultMaxResponseBytes,
	}
}

// sameOriginOnly is the http.Client CheckRedirect policy: it permits a redirect
// only while it stays on the SAME origin (scheme+host) as the original request,
// and refuses any cross-origin redirect so the custom X-Blnk-Key header can
// never be transmitted to a different host (M-06). It also caps the redirect
// chain length.
func sameOriginOnly(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	if len(via) >= maxRedirects {
		return errors.New("blnk client: stopped after too many redirects")
	}
	orig := via[0].URL
	if !sameOrigin(orig, req.URL) {
		return fmt.Errorf(
			"blnk client: refusing cross-origin redirect from %q to %q (would leak the API key)",
			orig.Host, req.URL.Host)
	}
	return nil
}

// sameOrigin reports whether two URLs share scheme and host (host includes any
// explicit port), i.e. they are the same web origin.
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}

// Ready reports whether Blnk's reconciliation API is reachable AND the agent is
// authenticated, by issuing an AUTHENTICATED GET /reconciliation/:id against a
// sentinel id (M-05, Rule 5.1). cmd/main.go calls it (with bounded retries) to
// await dependency readiness before running the pipeline, so the agent never
// triages against a not-yet-up Blnk and reports a false-green result.
//
// The readiness contract stays strictly within the mandated /reconciliation/*
// surface (never Blnk's out-of-scope /health) and requires an EXPECTED status:
//
//   - 200 or 404 => ready. The request passed Blnk's API-key auth middleware and
//     the route answered; 404 simply means the sentinel id does not exist, which
//     is the expected healthy response for an unknown id.
//   - transport error, 401/403 (auth failure), 400, or 5xx => NOT ready. Unlike
//     the previous implementation, an auth failure or server error is no longer
//     treated as "ready".
func (c *Client) Ready(ctx context.Context) error {
	req, err := c.newJSONRequest(ctx, http.MethodGet, routeReconByID+routeReadinessProbe, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, c.maxRespBytes))
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("blnk not ready: GET %s returned status %d", req.URL.Path, resp.StatusCode)
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

// ProbeBreak is the SINGLE-TRANSACTION break-identity bridge and a
// DETERMINISTIC ARBITER (Rule 5.3) for whether one break is cleared. It submits
// a SINGLE external transaction through POST /reconciliation/start-instant with
// dry_run=true, then polls GET /reconciliation/:id until the (asynchronous) run
// reaches a terminal status and inspects the integer unmatched count:
//
//	unmatched_transactions == 0  => cleared = true  (the break resolved)
//	unmatched_transactions == 1  => cleared = false (still a break)
//
// reconID is the reconciliation id of the probe; callers record it on the
// resulting audit event. A failed run or a timeout is returned as an error so
// the caller can fail closed (Rule 5.7) and route the break to HITL. LLM
// confidence must never substitute for this deterministic check.
//
// RESIDUAL CACHE LIMITATION (findings C-02/C-03, Rule 5.8). Because ProbeBreak
// submits each probe as its OWN ephemeral single-transaction upload, running
// several probes in sequence trips a defect in Blnk's PROTECTED core: the
// external-transaction pagination cache
// (database/reconciliation.go GetExternalTransactionsPaginated) keys ONLY on
// (batch_size, offset) and OMITS the upload id, so within its 5-minute TTL every
// later probe reads the FIRST probe's cached row instead of its own. That core
// defect must NOT be modified (Rule 5.8 / AAP §0.6.2). ProbeBreak is therefore
// retained ONLY for the single, human-triggered HITL re_drive of one break — a
// lone, non-sequential invocation for which the collision does not manifest. The
// automated pipeline's break detection and clearance confirmation instead use
// the cache-safe, upload-scoped ReconcileUpload / ConfirmClearedOverUpload
// bridge below, which funnels every reconciliation through the ONE persisted
// upload and so makes the shared cache benign.
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

// ReconcileUpload runs a cache-safe, UPLOAD-SCOPED dry-run reconciliation over
// an ALREADY-PERSISTED upload and returns Blnk's authoritative integer unmatched
// count plus the reconciliation id (findings C-02/C-03). It is the cache-safe
// replacement for sequential per-transaction ProbeBreak probing.
//
// Blnk's external-transaction pagination cache
// (database/reconciliation.go GetExternalTransactionsPaginated) keys ONLY on
// (batch_size, offset) and OMITS the upload id, so within its 5-minute TTL the
// FIRST reconciliation's rows are returned to every later reconciliation
// regardless of upload id. That defect lives in Blnk's PROTECTED core and MUST
// NOT be modified (Rule 5.8 / AAP §0.6.2). The agent instead makes the shared
// cache BENIGN by funnelling detection AND every clearance confirmation through
// the SAME persisted upload: every such reconciliation reads exactly that one
// upload's rows, so a cache hit returns the rows the caller intended rather than
// a stale different-upload page. Callers MUST pass the SAME uploadID they
// detected over. The run is a dry run (never mutating Blnk state) and is polled
// to a terminal status exactly like ProbeBreak.
func (c *Client) ReconcileUpload(ctx context.Context, uploadID string, matchingRuleIDs []string) (unmatched int, reconID string, err error) {
	reconID, err = c.StartReconciliation(ctx, StartReconciliationRequest{
		UploadID:        uploadID,
		Strategy:        probeStrategy,
		DryRun:          true,
		MatchingRuleIDs: matchingRuleIDs,
	})
	if err != nil {
		return 0, "", fmt.Errorf("reconcile upload start: %w", err)
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
				return recon.UnmatchedTransactions, reconID, nil
			case statusFailed:
				return 0, reconID, fmt.Errorf("reconcile upload %s failed", reconID)
			}
		}
		select {
		case <-pollCtx.Done():
			return 0, reconID, fmt.Errorf("reconcile upload %s did not complete: %w", reconID, pollCtx.Err())
		case <-ticker.C:
			// poll again
		}
	}
}

// ConfirmClearedOverUpload is the cache-safe DETERMINISTIC ARBITER (Rule 5.3)
// for whether a proposed matching rule cleared its break (finding C-03). It runs
// a dry-run reconciliation over the break's ORIGINAL persisted upload with the
// single proposed ruleID and reports the break cleared when Blnk's authoritative
// unmatched count drops strictly BELOW baselineUnmatched — the count under the
// strict detection rule, i.e. with no safe rule applied. A strict decrease is
// live proof that adding this rule moved at least one external transaction out
// of the unmatched set; LLM confidence never substitutes for it. reconID is the
// confirming reconciliation id the caller records on the resolved audit event.
//
// IDENTITY PRECISION (residual, Rule 5.8). Blnk's HTTP surface returns only
// integer counts — never the unmatched id list — and its matcher applies a rule
// GLOBALLY to every row in the upload, so when two breaks share a match profile
// (e.g. a duplicate posting that also fits a timing break's relaxed rule) the
// count alone cannot attribute the clearance to one specific id. Attributing the
// clearance to the break whose safe rule was just added is the strongest
// attribution obtainable over the native HTTP contract (Rule 5.1) without the
// forbidden upload-scoped cache/list fix in Blnk's protected core (Rule 5.8 /
// AAP §0.6.2). The resolution remains genuinely Blnk-arbitrated (the unmatched
// set demonstrably shrank), satisfying Rule 5.3.
func (c *Client) ConfirmClearedOverUpload(ctx context.Context, uploadID, ruleID string, baselineUnmatched int) (cleared bool, reconID string, err error) {
	unmatched, reconID, err := c.ReconcileUpload(ctx, uploadID, []string{ruleID})
	if err != nil {
		return false, "", err
	}
	return unmatched < baselineUnmatched, reconID, nil
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
		// M-06: bound how much of the response body is read/decoded so a hostile
		// or malfunctioning endpoint cannot exhaust memory with an unbounded body.
		if err := json.NewDecoder(io.LimitReader(resp.Body, c.maxRespBytes)).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", req.Method, req.URL.Path, err)
		}
	}
	return nil
}
