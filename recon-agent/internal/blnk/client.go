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

	// routeHealth is Blnk's liveness endpoint. Ready GETs it to confirm Blnk is
	// reachable (finding F13). Unlike the former sentinel-reconciliation-id probe
	// — which requested a deliberately nonexistent reconciliation id and made
	// Blnk log a spurious ERROR on every agent boot — GET /health is a
	// purpose-built, auth-bypassed liveness signal (Blnk cmd/server.go
	// healthCheckHandler; api/middleware/auth.go skips auth for "/health") that
	// returns 200 {"status":"UP"} when healthy and NEVER emits an error log.
	// Readiness therefore produces no misleading Blnk errors. Reaching /health is
	// consistent with Rule 5.1: it is a read-only liveness check on the same Blnk
	// service that hosts the /reconciliation/* routes, not a bypass of them.
	routeHealth = "/health"

	// probeStrategy is the reconciliation strategy used by ProbeBreak and
	// EstablishMainReconciliation. It is MANY-TO-ONE, not one-to-one, and this
	// choice is load-bearing for correctness (findings F02/F18, Rule 5.8).
	//
	// Blnk accepts {one_to_one, one_to_many, many_to_one}. A one_to_one probe
	// reads the external side through GetExternalTransactionsPaginated, whose
	// cache key (Blnk database/reconciliation.go) is (batch_size, offset) and
	// OMITS the upload id, so within its 5-minute TTL a SECOND single-transaction
	// probe silently reads the FIRST probe's cached page instead of its own — a
	// protected-core defect (Rule 5.8 / AAP §0.6.2) that makes sequential
	// per-break one_to_one probes collide and return each other's verdict. A
	// many_to_one probe instead reads the external side through
	// FetchAndGroupExternalTransactions, whose cache key INCLUDES the upload id,
	// so each probe's OWN ephemeral upload keys a distinct entry and sequential
	// per-break probes never collide. many_to_one is therefore the only
	// cache-safe way to adjudicate each break with its own dry-run over Blnk's
	// count-only HTTP surface without touching its protected core.
	probeStrategy = StrategyManyToOne

	// probeGroupingCriteria is the grouping field ProbeBreak and
	// EstablishMainReconciliation pass to Blnk's many_to_one reconciler. Blnk
	// groups the external side by this field; "reference" is a stable,
	// always-present field on every external transaction the agent submits, so
	// grouping is deterministic and never empty.
	probeGroupingCriteria = "reference"
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
	// StrategyOneToOne is Blnk's per-transaction reconciliation strategy. It is
	// retained as part of the exported Blnk-wire vocabulary but is deliberately
	// NOT used for the agent's per-break probes: sequential one_to_one probes
	// collide on Blnk's upload-agnostic external-transaction pagination cache
	// (see probeStrategy). The agent probes with StrategyManyToOne instead.
	StrategyOneToOne = "one_to_one"
	// StrategyManyToOne is Blnk's many-to-one reconciliation strategy. The agent
	// probes each break with it because its external-side read is upload-scoped
	// (cache key includes the upload id), so per-break probes never collide (see
	// probeStrategy, findings F02/F18/Rule 5.8).
	StrategyManyToOne = "many_to_one"
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

// Ready reports whether Blnk is reachable, by issuing a read-only GET /health
// (finding F13). cmd/main.go calls it (with bounded retries) to await dependency
// readiness before running the pipeline, so the agent never triages against a
// not-yet-up Blnk and reports a false-green result; the HITL /readyz handler
// calls it (bounded) so readiness reflects the Blnk dependency truthfully rather
// than reporting ready while Blnk is down (finding F08).
//
// Probing GET /health — Blnk's purpose-built, auth-bypassed liveness endpoint —
// rather than the former sentinel GET /reconciliation/:id is what fixed F13: the
// sentinel probe requested a deliberately nonexistent reconciliation id and made
// Blnk log a spurious ERROR on every agent boot, whereas /health never logs an
// error. Reaching /health is consistent with Rule 5.1: it is a read-only
// liveness check on the SAME Blnk service that hosts the /reconciliation/*
// routes, not a bypass of them. The readiness verdict requires an EXPECTED
// healthy response:
//
//   - 200 with an "UP" status body => ready.
//   - transport error, or any non-200 (including a 503 "DOWN" when a Blnk
//     dependency such as its DB is degraded) => NOT ready. A degraded Blnk is
//     correctly reported as not-ready rather than mistaken for reachable.
func (c *Client) Ready(ctx context.Context) error {
	// F13: probe Blnk's purpose-built liveness endpoint (GET /health) rather than
	// a deliberately nonexistent reconciliation id. The former sentinel probe
	// made Blnk log a spurious ERROR on every agent boot; /health is auth-bypassed
	// and never logs an error, so readiness produces no misleading Blnk errors.
	req, err := c.newJSONRequest(ctx, http.MethodGet, routeHealth, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, c.maxRespBytes))
	// Blnk returns 200 {"status":"UP"} when healthy and 503 {"status":"DOWN"}
	// when a dependency (config/DB) is unavailable. Ready ONLY when Blnk reports
	// 200 AND an UP status, so a degraded Blnk (or any non-200) is correctly
	// reported as not-ready rather than being mistaken for reachable.
	if resp.StatusCode == http.StatusOK && strings.Contains(string(body), "UP") {
		return nil
	}
	return fmt.Errorf("blnk not ready: GET %s returned status %d", req.URL.Path, resp.StatusCode)
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

// ProbeBreak is the SINGLE-TRANSACTION break-identity bridge and the
// DETERMINISTIC ARBITER (Rule 5.3) for whether ONE break is cleared. It is the
// unit of per-break adjudication used BOTH by the automated pipeline (to confirm
// an auto-remediation, findings F02/F18) AND by the human-triggered HITL
// re_drive of a single break (finding F16). It submits a SINGLE external
// transaction through POST /reconciliation/start-instant with dry_run=true and
// strategy=many_to_one, then polls GET /reconciliation/:id until the
// (asynchronous) run reaches a terminal status and reads Blnk's integer counts.
//
// VERDICT — matched_transactions >= 1 (NOT unmatched == 0). Under many_to_one
// Blnk drives the reconciliation over the INTERNAL ledger and groups the
// external side by reference; the run's unmatched count therefore includes
// UNRELATED internal bookings that share no external counterpart and is NOT a
// clean signal for a single external probe. The matched count, however, is
// clean: because the probe submits EXACTLY ONE external transaction, any match
// Blnk reports necessarily involves THAT transaction. Hence:
//
//	matched_transactions >= 1  => cleared = true  (Blnk matched the break)
//	matched_transactions == 0  => cleared = false (still an unmatched break)
//
// COLLISION-FREE PER-BREAK PROBING (findings F02/F18, Rule 5.8). Each probe is
// its OWN ephemeral single-transaction upload. Under many_to_one, Blnk reads the
// external side through FetchAndGroupExternalTransactions, whose cache key
// INCLUDES the upload id, so each probe keys a DISTINCT cache entry and
// sequential per-break probes never read each other's page. (A one_to_one probe
// would instead read through GetExternalTransactionsPaginated, whose cache key
// OMITS the upload id — a protected-core defect, Rule 5.8 / AAP §0.6.2 — making
// sequential one_to_one probes collide within the cache's 5-minute TTL. That is
// precisely why the agent probes with many_to_one.) This lets the pipeline
// adjudicate EACH break with its OWN dry-run and resolve the confirmed ones
// independently, even when a sibling break in the same run remains unmatched
// (F18) — replacing the earlier all-or-nothing whole-cohort dry-run.
//
// reconID is the reconciliation id of the probe; callers record it as the
// clearance proof on the resulting audit event (Rule 5.3). A failed run or a
// timeout is returned as an error so the caller can fail closed (Rule 5.7) and
// route the break to HITL. LLM confidence must never substitute for this check.
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
		GroupingCriteria:     probeGroupingCriteria,
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
				// matched >= 1 proves Blnk matched the single probed transaction
				// (see the VERDICT note above). unmatched is NOT consulted: under
				// many_to_one it is contaminated by unrelated internal bookings.
				return recon.MatchedTransactions >= 1, reconID, nil
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

// EstablishMainReconciliation runs the pipeline's INITIAL reconciliation over
// the already-uploaded statement and returns its reconciliation id (the
// "main_recon_id"). It implements the mandated Upload -> start -> read topology
// (finding F02): it POSTs /reconciliation/start for the given upload with
// dry_run=true, then polls GET /reconciliation/:id until the (asynchronous) run
// reaches a terminal status, so the returned id is the id of a reconciliation
// Blnk actually completed. Callers persist it as each break's main_recon_id
// provenance, giving every break a directly traceable pointer to the batch
// reconciliation that surfaced it (findings F02/F13).
//
// It is a DRY RUN: it never mutates Blnk state, consistent with the deterministic
// arbiter rule (Rule 5.3) that only a Blnk dry-run — never LLM confidence — may
// speak to clearance. Blnk requires at least one matching rule id for both
// /reconciliation/start and /reconciliation/start-instant, so the caller passes
// the ids of the rules it created for this run's auto-eligible breaks; the
// initial run's integer counts are not consulted for any per-break decision
// (those come from the individual ProbeBreak dry-runs), so the specific rules
// only need to be valid.
//
// STRATEGY. It uses many_to_one with reference grouping — the SAME cache-safe
// strategy as ProbeBreak (see probeStrategy) — so the initial run over the real
// upload cannot poison the per-break probes: FetchAndGroupExternalTransactions
// keys its cache on the upload id, and the real upload's id differs from every
// probe's ephemeral upload id, so their cache entries are disjoint.
//
// A start error, a failed run, or a timeout is returned to the caller so a
// missing initial reconciliation surfaces rather than being silently ignored.
func (c *Client) EstablishMainReconciliation(ctx context.Context, uploadID string, matchingRuleIDs []string) (reconID string, err error) {
	if strings.TrimSpace(uploadID) == "" {
		return "", fmt.Errorf("establish main reconciliation: empty upload id")
	}
	if len(matchingRuleIDs) == 0 {
		return "", fmt.Errorf("establish main reconciliation: at least one matching rule id is required")
	}

	reconID, err = c.StartReconciliation(ctx, StartReconciliationRequest{
		UploadID:         uploadID,
		Strategy:         probeStrategy,
		GroupingCriteria: probeGroupingCriteria,
		DryRun:           true,
		MatchingRuleIDs:  matchingRuleIDs,
	})
	if err != nil {
		return "", fmt.Errorf("establish main reconciliation start: %w", err)
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
				return reconID, nil
			case statusFailed:
				return reconID, fmt.Errorf("main reconciliation %s failed", reconID)
			}
		}
		select {
		case <-pollCtx.Done():
			return reconID, fmt.Errorf("main reconciliation %s did not complete: %w", reconID, pollCtx.Err())
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
		// M-06: bound how much of the response body is read/decoded so a hostile
		// or malfunctioning endpoint cannot exhaust memory with an unbounded body.
		if err := json.NewDecoder(io.LimitReader(resp.Body, c.maxRespBytes)).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", req.Method, req.URL.Path, err)
		}
	}
	return nil
}
