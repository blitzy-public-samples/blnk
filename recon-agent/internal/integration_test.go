//go:build integration

package internal_test

// Package internal_test hosts the recon-agent's module-level, build-tag-guarded
// END-TO-END integration test. It drives the ENTIRE pipeline
// (config -> store -> audit -> blnk HTTP client -> classifier -> remediator ->
// hitl) against a LIVE Blnk daemon + PostgreSQL, exactly as cmd/main.go wires it
// in production, and asserts the AAP's measurable success criteria.
//
// WHY A BUILD TAG (`//go:build integration`):
//   - This is intentionally the ONLY .go file directly under recon-agent/internal/
//     (every other unit of code lives in a sub-package). Under the DEFAULT build
//     tags a plain `make test` (`go test ./...`, `go build ./...`, `go vet ./...`)
//     SILENTLY SKIPS this directory — there is no "build constraints exclude all
//     Go files" error — so the fast, service-free unit suites in the sub-packages
//     (which use httptest / sqlmock mocks and carry the >=80% coverage floor,
//     Rule 5.9) run without any external services.
//   - Under `go test -tags integration ./...` this file compiles as the external
//     test package `internal_test` and TestReconAgentPipeline runs. That tagged
//     run is what CI executes (Gate 10) with PostgreSQL + Blnk + recon-agent stood
//     up, giving a real end-to-end signal (Gate 9: >=1 integration test).
//
// RULE 5.1 (native-API-only): this file imports NO github.com/blnkfinance/blnk/...
// package. It reaches Blnk's ledger service exclusively through the recon-agent's
// own internal/blnk HTTP client. The only direct HTTP the test performs itself is
// (a) the deterministic stub OpenAI-compatible LLM it hosts locally and (b) an
// optional Blnk /health readiness poll in TestMain (readiness gating only, not a
// reconciliation operation, and importing nothing from Blnk).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/hitl"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/remediator"
	"github.com/blnkfinance/recon-agent/internal/store"
)

const (
	// integrationReviewer is the human reviewer id attributed to the HITL
	// decisions this test submits over HTTP, so their audit events are
	// attributable to this run.
	integrationReviewer = "integration-reviewer"

	// uploadSource is the external-statement source label sent on the upload
	// request (Blnk records it; it is NOT a CSV column — see
	// internal/files/files.go and seed/external_transactions.csv).
	uploadSource = "integration-seed-bank"

	// scenarioTagPrefix / scenarioTagSuffix bracket the machine-readable scenario
	// marker embedded in each break's description. The local stub LLM keys its
	// deterministic classification off this marker, which survives run-scoped
	// unique ids (the marker is stable across runs while the txn id is not).
	scenarioTagPrefix = "[[scenario:"
	scenarioTagSuffix = "]]"

	// uploadRowIDSuffix namespaces the ids written to the uploaded statement so
	// they never collide with the ids the remediator later submits to Blnk's
	// start-instant. Blnk persists every external transaction — whether via
	// /reconciliation/upload or /reconciliation/start-instant — with a plain
	// INSERT keyed by the transaction's OWN id (RecordExternalTransaction,
	// database/reconciliation.go: no ON CONFLICT), so blnk.external_transactions
	// enforces a global, once-only primary key per id. ProbeBreak (the
	// deterministic-arbiter bridge, Rule 5.3) re-submits a break BY ITS OWN id via
	// start-instant, so pre-uploading that same id would turn the probe's insert
	// into a duplicate-key failure. This test's upload exists only to exercise
	// UploadExternalData and yield a real upload_id for provenance; its rows
	// therefore carry a distinct id namespace while the remediated breaks keep
	// their probe ids. record_count still equals the planted break count, and the
	// agent never reads blnk.external_transactions (Rule 5.1), so the distinct
	// namespace is invisible to every assertion.
	uploadRowIDSuffix = "-UP"

	// probeVerifyIDSuffix stamps the independent, collision-free cross-check probe
	// (assertProbeConfirmsClearance). The remediator's auto path has already
	// probed — and therefore persisted — the resolved break's real id, so the
	// cross-check re-probes a fresh-id clone that carries the SAME reference and
	// amount. Blnk matches external against internal FIELD-TO-FIELD and ignores
	// both the transaction id and criteria.Value (reconciliation.go
	// matchesString/matchesGroupAmount), so the clone clears exactly when the
	// original did while avoiding the once-only-id constraint above.
	probeVerifyIDSuffix = "-VERIFY"
)

// scenarioSpec is the deterministic classification the stub LLM returns for a
// break carrying the matching scenario marker, plus the pipeline outcome this
// test expects for it. It is the SINGLE source of truth shared by (a) the stub
// LLM (what the model "says") and (b) the assertions (what the remediator must
// therefore do), so the two can never silently drift apart.
type scenarioSpec struct {
	// name is the scenario marker embedded in the break description and matched
	// by the stub LLM (e.g. "timing").
	name string
	// rootCause is the label the stub LLM returns and the classifier normalizes.
	rootCause model.RootCause
	// confidence is the calibrated confidence the stub LLM returns, in [0,1].
	confidence float64
	// regulated marks the break as touching a regulated flow. A regulated break
	// is NEVER auto-remediated regardless of confidence (Rule 5.4).
	regulated bool
	// ruleField / ruleOperator describe the single-criterion Blnk matching rule
	// the stub LLM proposes. Empty ruleField means "no proposed_rule" (the model
	// deliberately omits a rule for causes that must escalate).
	ruleField    string
	ruleOperator string
	// autoEligible is true when this break passes the remediator's
	// confidence/regulation gate (Rule 5.4) AND carries an auto-eligible root
	// cause (timing / amount_drift / reference_mismatch) AND proposes a safe
	// {field,equals} rule — i.e. the remediator drives it down the auto-path
	// (propose rule -> Blnk dry-run -> confirm cleared). Whether Blnk actually
	// clears it is decided by the LIVE ledger, not by this flag (deterministic
	// arbiter, Rule 5.3): the suite asserts only the robust invariant that a
	// break which DID resolve was auto-eligible, never a fixed per-scenario
	// clearance outcome (the shared Blnk database is not owned by this test).
	autoEligible bool
}

// escalates reports whether the confidence/regulation gate (Rule 5.4) alone
// forces this break to HITL: a regulated break, or one below the auto-remediation
// threshold, is escalated and never auto-actioned. (Auto-INeligible root causes
// such as duplicate/missing_internal also escalate, but via a separate gate; this
// helper captures only the Rule 5.4 arm the AAP success criterion names
// explicitly.)
func (s scenarioSpec) gatedToHITL(threshold float64) bool {
	return s.regulated || s.confidence < threshold
}

// scenarios enumerates the classifications the stub LLM returns, one per planted
// break. They are calibrated against the EMPIRICALLY VERIFIED behavior of Blnk's
// reconciliation engine (confirmed by probing a live daemon during authoring),
// because this suite runs the real deterministic arbiter — not a mock:
//
//  1. A single-transaction dry-run clears a break through an {amount,equals}
//     criterion: Blnk derives the internal-candidate SQL window from a rule's
//     amount/date criteria (reconciliation.go calculateMatchingBounds +
//     GetTransactionsByCriteria), so an amount-equality rule surfaces the
//     matching internal booking and clears the probe. String-only criteria
//     (reference/description) yield a nil amount/date window and do not surface
//     a counterpart, so they never clear in this deployment.
//  2. The classifier does NOT carry allowable_drift onto a proposed rule
//     (classifier.rawProposedRule has no drift field; parseClassification builds
//     the criterion with drift 0), so an auto-applied {amount,equals} clears
//     when the external amount equals an internal booking's amount.
//
// Three breaks are AUTO-ELIGIBLE: they pass the confidence/regulation gate, carry
// an auto-eligible root cause, and propose an {amount,equals} rule, so the
// remediator drives each down the auto-path and Blnk — the deterministic arbiter,
// Rule 5.3 — decides clearance against the ledger `make seed` establishes:
//   - timing             -> amount correct, posted late  (amount 1500 == INV-1001)
//   - amount_drift       -> amount slightly drifted       (amount 250.75)
//   - reference_mismatch -> reference wrong, amount right (amount 980 == INV-1003)
//
// timing and reference_mismatch match a SEEDED internal booking exactly, so they
// clear reliably; amount_drift clears only if some internal booking shares its
// amount. The suite therefore asserts the robust invariant "a break that resolved
// was auto-eligible" plus ">=1 auto-resolved" (met by the two seed-backed breaks),
// never a fixed per-scenario clearance outcome, because the shared Blnk database
// is not owned by this test and may already hold arbitrary amounts.
//
// The remaining four each exercise a DISTINCT escalation arm, so HITL routing and
// the confidence/regulation gate (Rule 5.4) are covered end to end against the
// LIVE engine, and none is ever auto-actioned:
//   - duplicate, missing_internal -> auto-INeligible root cause (escalates
//     regardless of confidence; never probed)
//   - currency_mismatch -> regulated == true              (Rule 5.4)
//   - low_confidence    -> confidence < threshold          (Rule 5.4; escalates at
//     the gate before any rule is proposed)
//
// Blnk ignores criteria.Value and matches field-to-field (reconciliation.go
// matchesGroupAmount), so each proposed rule's Value is left empty; only the
// Field/Operator (and, for clearance, the external amount) affect the outcome.
var scenarios = []scenarioSpec{
	{name: "timing", rootCause: model.RootCauseTiming, confidence: 0.95, regulated: false, ruleField: "amount", ruleOperator: "equals", autoEligible: true},
	{name: "amount_drift", rootCause: model.RootCauseAmountDrift, confidence: 0.90, regulated: false, ruleField: "amount", ruleOperator: "equals", autoEligible: true},
	{name: "reference_mismatch", rootCause: model.RootCauseReferenceMismatch, confidence: 0.88, regulated: false, ruleField: "amount", ruleOperator: "equals", autoEligible: true},
	{name: "duplicate", rootCause: model.RootCauseDuplicate, confidence: 0.93, regulated: false},
	{name: "missing_internal", rootCause: model.RootCauseMissingInternal, confidence: 0.91, regulated: false},
	{name: "currency_mismatch", rootCause: model.RootCauseCurrencyMismatch, confidence: 0.90, regulated: true},
	{name: "low_confidence", rootCause: model.RootCauseTiming, confidence: 0.40, regulated: false, ruleField: "amount", ruleOperator: "equals"},
}

// specByName returns the scenario spec for a marker, or ok=false when none
// matches (the stub LLM then falls back to a safe "unknown" classification).
func specByName(name string) (scenarioSpec, bool) {
	for _, s := range scenarios {
		if s.name == name {
			return s, true
		}
	}
	return scenarioSpec{}, false
}

// breakInput pairs a scenario with the concrete external-statement fields of one
// planted break. Its references/amounts mirror seed/external_transactions.csv so
// the auto-resolvable breaks match the internal ledger seeded by `make seed`.
type breakInput struct {
	spec        scenarioSpec
	idSuffix    string
	amount      float64
	currency    string
	reference   string
	description string
}

// runBreaks builds the planted-break set for one test run, stamping every
// external transaction id with runID so repeated runs against a shared agent
// database never collide (the remediator is idempotent per id, so reusing ids
// across runs would make a re-run a no-op and defeat the per-run assertions).
// The amounts are chosen against the ledger `make seed` establishes (INV-1001
// 1500, INV-1002 250, INV-1003 980, INV-1006 700 — all USD): the seed-backed
// auto-eligible breaks (timing == 1500, reference_mismatch == 980) match an
// internal booking's amount EXACTLY, so an {amount,equals} probe clears them and
// guarantees the ">=1 auto-resolved" criterion regardless of any other data in
// the shared Blnk database (see scenarios for the full eligibility rationale).
func runBreaks(runID string) []breakInput {
	base := []breakInput{
		// Auto-eligible; clears reliably: external amount == seeded INV-1001 (1500).
		{spec: mustSpec("timing"), idSuffix: "TIMING", amount: 1500.00, currency: "USD", reference: "INV-1001", description: "Vendor payout ACH settlement posted at value date T+2"},
		// Auto-eligible; amount slightly drifted (250.75). Blnk decides clearance
		// against the live ledger — the suite asserts only that a resolved break
		// was auto-eligible, never that this specific break must clear or escalate.
		{spec: mustSpec("amount_drift"), idSuffix: "DRIFT", amount: 250.75, currency: "USD", reference: "INV-1002", description: "Card capture net of 0.75 processor fee drift"},
		// Auto-eligible; clears reliably: reference differs but amount == seeded INV-1003 (980).
		{spec: mustSpec("reference_mismatch"), idSuffix: "REFMISS", amount: 980.00, currency: "USD", reference: "ACME-2025-03", description: "Wire credit with mismatched vendor reference code"},
		{spec: mustSpec("duplicate"), idSuffix: "DUP", amount: 1500.00, currency: "USD", reference: "INV-1001", description: "Duplicate re-posting of vendor payout INV-1001"},
		{spec: mustSpec("missing_internal"), idSuffix: "MISSING", amount: 4200.00, currency: "USD", reference: "UNKN-9001", description: "Unrecognized inbound deposit with no internal booking"},
		{spec: mustSpec("currency_mismatch"), idSuffix: "CCY", amount: 700.00, currency: "EUR", reference: "INV-1006", description: "Cross-border SEPA credit booked internally in USD"},
		{spec: mustSpec("low_confidence"), idSuffix: "LOWCONF", amount: 15.00, currency: "USD", reference: "AMBIG-4242", description: "Ambiguous micro-credit with insufficient evidence to classify confidently"},
	}
	for i := range base {
		base[i].idSuffix = fmt.Sprintf("IT-%s-%s", runID, base[i].idSuffix)
	}
	return base
}

// mustSpec looks up a scenario spec by name and panics when it is missing. It is
// used only for the compile-time-constant scenario names above, so a panic here
// is a programming error in this test, never a runtime/environment condition.
func mustSpec(name string) scenarioSpec {
	s, ok := specByName(name)
	if !ok {
		panic("integration_test: unknown scenario " + name)
	}
	return s
}

// externalTxnID is the run-scoped external transaction id for a planted break.
func (b breakInput) externalTxnID() string { return b.idSuffix }

// txn renders the planted break as the Blnk external-transaction DTO the pipeline
// consumes. The scenario marker is appended to the description so the stub LLM
// can classify deterministically regardless of the run-scoped id.
func (b breakInput) txn() blnk.ExternalTransaction {
	return blnk.ExternalTransaction{
		ID:          b.externalTxnID(),
		Amount:      b.amount,
		Currency:    b.currency,
		Reference:   b.reference,
		Description: fmt.Sprintf("%s %s%s%s", b.description, scenarioTagPrefix, b.spec.name, scenarioTagSuffix),
		Date:        time.Date(2025, 1, 6, 9, 0, 0, 0, time.UTC),
		Source:      uploadSource,
	}
}

// ---------------------------------------------------------------------------
// Deterministic stub OpenAI-compatible LLM
//
// The classifier is wired through the REAL go-openai client (Rule 5.6 config-
// driven), so to make classification deterministic and independent of any
// external model this test hosts a local OpenAI-compatible Chat Completions
// endpoint. Pointing cfg.LLMBaseURL at this server makes classifier.New(cfg)
// exercise the genuine client end to end while the reply is fixed per scenario.
// (Absent this, an unset/blank LLM_API_KEY would make every break fail closed to
// HITL — Rule 5.7 — and no break could auto-remediate, defeating the ">=1
// auto-remediated" success criterion.)
// ---------------------------------------------------------------------------

// stubProposedRule / stubClassification mirror the strict-JSON shape the real
// classifier parses from the model reply (see internal/classifier/classifier.go
// rawClassification). The stub marshals a stubClassification as the assistant
// message content.
type stubProposedRule struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type stubClassification struct {
	RootCause    string            `json:"root_cause"`
	Confidence   float64           `json:"confidence"`
	Regulated    bool              `json:"regulated"`
	ProposedRule *stubProposedRule `json:"proposed_rule,omitempty"`
	Rationale    string            `json:"rationale"`
}

// stubChatResponse (+ choice/message) is the minimal subset of the OpenAI Chat
// Completions response the go-openai client decodes: it reads choices[0].message
// .content. The field names match go-openai's JSON tags, so the response is
// hand-crafted here without importing the go-openai package into the test.
type stubChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type stubChatChoice struct {
	Index        int             `json:"index"`
	Message      stubChatMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

type stubChatResponse struct {
	ID      string           `json:"id"`
	Object  string           `json:"object"`
	Created int64            `json:"created"`
	Model   string           `json:"model"`
	Choices []stubChatChoice `json:"choices"`
}

// detectScenario finds the scenario marker embedded in the prompt and returns
// its spec. When no marker is present it returns a safe "unknown" classification
// with no proposed rule, which the remediator escalates — this should not happen
// for the planted breaks and exists only as a defensive default.
func detectScenario(prompt string) scenarioSpec {
	for _, s := range scenarios {
		if strings.Contains(prompt, scenarioTagPrefix+s.name+scenarioTagSuffix) {
			return s
		}
	}
	return scenarioSpec{name: "unknown", rootCause: model.RootCauseUnknown, confidence: 0}
}

// classificationContentFor builds the strict-JSON classification the stub returns
// for the break described by prompt. The proposed rule's Value is left empty
// because Blnk matches external against internal field-to-field and ignores
// criteria.Value (reconciliation.go), so only Field/Operator affect clearance.
func classificationContentFor(prompt string) string {
	spec := detectScenario(prompt)
	cls := stubClassification{
		RootCause:  string(spec.rootCause),
		Confidence: spec.confidence,
		Regulated:  spec.regulated,
		Rationale:  fmt.Sprintf("stub classification for scenario %q", spec.name),
	}
	if spec.ruleField != "" {
		cls.ProposedRule = &stubProposedRule{Field: spec.ruleField, Operator: spec.ruleOperator, Value: ""}
	}
	b, err := json.Marshal(cls)
	if err != nil {
		// Marshaling a fixed struct cannot fail in practice; surface a valid
		// fallback object so the classifier still parses a well-formed reply.
		return `{"root_cause":"unknown","confidence":0,"regulated":false,"rationale":"stub marshal error"}`
	}
	return string(b)
}

// newStubLLM starts a local httptest server that answers the go-openai client's
// POST {BaseURL}/chat/completions with a deterministic per-scenario
// classification. The caller must Close it (via t.Cleanup).
func newStubLLM(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad chat request", http.StatusBadRequest)
			return
		}
		var sb strings.Builder
		for _, m := range req.Messages {
			sb.WriteString(m.Content)
			sb.WriteByte('\n')
		}
		resp := stubChatResponse{
			ID:      "chatcmpl-stub",
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			Model:   "kimi-k3-stub",
			Choices: []stubChatChoice{{
				Index:        0,
				Message:      stubChatMessage{Role: "assistant", Content: classificationContentFor(sb.String())},
				FinishReason: "stop",
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ---------------------------------------------------------------------------
// Assertion / statement helpers
// ---------------------------------------------------------------------------

// buildStatementCSV renders the planted breaks as an external-statement CSV whose
// header columns match those Blnk's ingestion requires (internal/files/files.go):
// ID, Amount, Currency, Reference, Description, Date. Commas inside a description
// are sanitized so the row stays well-formed.
func buildStatementCSV(breaks []breakInput) string {
	var sb strings.Builder
	sb.WriteString("ID,Amount,Currency,Reference,Description,Date\n")
	for _, b := range breaks {
		txn := b.txn()
		// Upload-scoped id (see uploadRowIDSuffix): keeps the persisted statement
		// rows disjoint from the ids the remediator probes by, so ProbeBreak's
		// start-instant insert is never a duplicate of an uploaded id.
		sb.WriteString(fmt.Sprintf("%s,%.2f,%s,%s,%s,%s\n",
			txn.ID+uploadRowIDSuffix,
			txn.Amount,
			txn.Currency,
			txn.Reference,
			strings.ReplaceAll(txn.Description, ",", ";"),
			txn.Date.Format(time.RFC3339),
		))
	}
	return sb.String()
}

// runIDSet returns the set of run-scoped external txn ids, used to filter shared
// store listings (which may contain rows from other runs) down to this run.
func runIDSet(breaks []breakInput) map[string]bool {
	ids := make(map[string]bool, len(breaks))
	for _, b := range breaks {
		ids[b.externalTxnID()] = true
	}
	return ids
}

// auditByTxn groups this run's audit events by external txn id, preserving order.
func auditByTxn(events []model.AuditEvent, ids map[string]bool) map[string][]model.AuditEvent {
	out := make(map[string][]model.AuditEvent)
	for _, e := range events {
		if ids[e.ExternalTxnID] {
			out[e.ExternalTxnID] = append(out[e.ExternalTxnID], e)
		}
	}
	return out
}

// hasAction reports whether any event in events carries the given action.
func hasAction(events []model.AuditEvent, action string) bool {
	for _, e := range events {
		if e.Action == action {
			return true
		}
	}
	return false
}

// resolvedWithRecon returns the resolved events that carry a non-empty confirming
// reconciliation id in provenance — the deterministic-arbiter evidence Rule 5.3
// requires for a resolution.
func resolvedWithRecon(events []model.AuditEvent) []model.AuditEvent {
	var out []model.AuditEvent
	for _, e := range events {
		if e.Action == audit.ActionResolved && strings.TrimSpace(e.Provenance.ReconID) != "" {
			out = append(out, e)
		}
	}
	return out
}

// inHITLQueue reports whether externalTxnID is currently in the agent HITL queue.
func inHITLQueue(items []store.HITLItem, externalTxnID string) bool {
	for _, it := range items {
		if it.ExternalTxnID == externalTxnID {
			return true
		}
	}
	return false
}

// breakByID returns the store break row for an external txn id from a listing.
func breakByID(breaks []store.Break, externalTxnID string) (store.Break, bool) {
	for _, b := range breaks {
		if b.Classification.ExternalTxnID == externalTxnID {
			return b, true
		}
	}
	return store.Break{}, false
}

// ---------------------------------------------------------------------------
// TestMain — optional readiness gating
// ---------------------------------------------------------------------------

// TestMain optionally waits for the live Blnk daemon to become reachable before
// running the suite, smoothing over CI startup races (Blnk may still be booting
// when the test binary starts). It performs ONLY readiness gating: a best-effort
// GET of <BLNK_BASE_URL>/health, treating ANY HTTP response (200/401/404/503) as
// "reachable" and retrying only on dial errors. It imports nothing from Blnk and
// issues no reconciliation call, so Rule 5.1 is preserved — the agent still
// reaches Blnk's reconciliation surface exclusively through internal/blnk. When
// BLNK_BASE_URL is unset the test itself skips, so this is a no-op.
func TestMain(m *testing.M) {
	if base := strings.TrimSpace(os.Getenv("BLNK_BASE_URL")); base != "" {
		waitForBlnk(base, 30*time.Second)
	}
	os.Exit(m.Run())
}

// waitForBlnk polls baseURL+"/health" until it receives any HTTP response or the
// timeout elapses. It never fails the run: if Blnk never answers, the test's own
// operations surface the real error (or the test skips when env is unset).
func waitForBlnk(baseURL string, timeout time.Duration) {
	client := &http.Client{Timeout: 2 * time.Second}
	url := strings.TrimRight(baseURL, "/") + "/health"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return // any HTTP status means the server is reachable
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// TestReconAgentPipeline — end-to-end pipeline against a LIVE Blnk daemon
// ---------------------------------------------------------------------------

// TestReconAgentPipeline drives the entire recon-agent pipeline end to end and
// asserts the AAP's measurable success criteria (§0.1.1):
//   - >=1 break auto-remediated AND confirmed cleared by a Blnk dry-run
//     (Rule 5.3: the resolved audit event carries a confirming recon_id);
//   - 100% of pipeline actions emit an append-only AuditEvent (every processed
//     break has an audit trail; every resolved event carries a recon_id);
//   - every break with confidence < CONF_AUTO_THRESHOLD OR regulated == true is
//     routed to the HITL queue and never auto-actioned (Rule 5.4);
//   - the HITL accept / re_drive / reject routes are each invoked over HTTP and
//     (on the auditable paths) write a decision AuditEvent (Gate 13).
//
// It wires the SAME object graph as cmd/main.go (Gate 9) and reaches Blnk only
// through internal/blnk (Rule 5.1). Classification is served by a local
// deterministic stub OpenAI-compatible endpoint so the run is reproducible and
// never fails closed for a missing key; every other collaborator (Blnk, the
// agent database, the confidence threshold) is the real, env-configured one.
func TestReconAgentPipeline(t *testing.T) {
	// --- Defensive environment gating: degrade gracefully when mis-invoked
	// outside CI. In CI these are always set. ---
	if strings.TrimSpace(os.Getenv("BLNK_BASE_URL")) == "" || strings.TrimSpace(os.Getenv("AGENT_DATABASE_URL")) == "" {
		t.Skip("integration env not configured (requires BLNK_BASE_URL and AGENT_DATABASE_URL)")
	}

	// --- Deterministic stub LLM (started before config so we can repoint at it). ---
	llm := newStubLLM(t)

	// --- Load config exactly as the service does, then repoint inference at the
	// local stub. Rule 5.6: the model name stays config-driven; we only override
	// the endpoint/key so classification is deterministic and service-free. ---
	cfg, err := config.Load()
	require.NoError(t, err, "config.Load must succeed with the integration env set")
	cfg.LLMBaseURL = llm.URL
	cfg.LLMApiKey = "integration-stub-key"
	if strings.TrimSpace(cfg.LLMModel) == "" {
		cfg.LLMModel = "kimi-k3"
	}

	// Bound the whole pipeline. Individual Blnk calls are additionally bounded by
	// the client's own HTTP timeout and ProbeBreak's polling timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// --- Wire the real object graph exactly like cmd/main.go (Gate 9). ---
	st, err := store.New(cfg.AgentDatabaseURL)
	require.NoError(t, err, "store.New")
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.Migrate(ctx), "store.Migrate must create the agent schema and tables")

	auditWriter, err := audit.New(st)
	require.NoError(t, err, "audit.New")

	blnkClient := blnk.NewClient(cfg.BlnkBaseURL, cfg.BlnkApiKey)
	cls := classifier.New(cfg)
	rem := remediator.New(cls, blnkClient, auditWriter, st, cfg.ConfAutoThreshold, cfg.LLMModel)
	hitlServer := hitl.NewServer(cfg, st, auditWriter, blnkClient)

	// --- Seed/upload a small external statement (exercises UploadExternalData and
	// yields a real upload id for provenance). The internal ledger these breaks
	// reconcile against is provided by `make seed` (AAP-endorsed). Run-scoped ids
	// keep repeated runs against a shared agent DB independent. ---
	runID := strings.SplitN(uuid.NewString(), "-", 2)[0]
	breaks := runBreaks(runID)
	statement := buildStatementCSV(breaks)
	upload, err := blnkClient.UploadExternalData(ctx, uploadSource, "integration_statement.csv", strings.NewReader(statement))
	require.NoError(t, err, "UploadExternalData must succeed against live Blnk")
	require.NotEmpty(t, upload.UploadID, "upload must return an upload_id")
	require.Equal(t, len(breaks), upload.RecordCount, "record_count must equal the planted break count")

	// --- Run the remediator over every planted break. Handle is terminal and
	// idempotent and returns an error only on agent-side infra failure; its auto
	// path exercises the ProbeBreak dry-run bridge (deterministic arbiter,
	// Rule 5.3) internally. ---
	for _, b := range breaks {
		require.NoError(t, rem.Handle(ctx, b.txn(), upload.UploadID),
			"remediator.Handle must reach a safely-recorded terminal outcome for %s (%s)", b.externalTxnID(), b.spec.name)
	}

	// --- Gather post-pipeline state, filtered to this run's ids (the agent
	// database is shared, so other runs' rows must be excluded). ---
	ids := runIDSet(breaks)

	allBreaks, err := st.ListBreaks(ctx)
	require.NoError(t, err, "ListBreaks")
	allAudit, err := st.ListAudit(ctx)
	require.NoError(t, err, "ListAudit")
	hitlItems, err := st.ListHITL(ctx)
	require.NoError(t, err, "ListHITL")

	byTxn := auditByTxn(allAudit, ids)
	var runAudit []model.AuditEvent
	for _, e := range allAudit {
		if ids[e.ExternalTxnID] {
			runAudit = append(runAudit, e)
		}
	}

	// === Success criterion 1: >=1 break auto-remediated AND confirmed cleared by
	// a Blnk dry-run (Rule 5.3 — the resolved event carries a recon_id). ===
	resolved := resolvedWithRecon(runAudit)
	require.GreaterOrEqualf(t, len(resolved), 1,
		"at least one break must be auto-remediated and confirmed cleared by a Blnk dry-run (resolved event with recon_id); got %d", len(resolved))

	resolvedIDs := make(map[string]bool, len(resolved))
	for _, e := range resolved {
		resolvedIDs[e.ExternalTxnID] = true
		row, ok := breakByID(allBreaks, e.ExternalTxnID)
		require.Truef(t, ok, "resolved break %s must have a store row", e.ExternalTxnID)
		require.Equalf(t, "auto-resolved", row.Status, "resolved break %s must have status auto-resolved", e.ExternalTxnID)
	}
	// Robust invariant (Rules 5.3 + 5.4): a break may resolve ONLY if it was
	// auto-eligible (passed the confidence/regulation gate with an auto-eligible
	// root cause and a proposed rule). Nothing gated or auto-ineligible may ever
	// slip through to a resolved state, whatever the shared ledger contains.
	for _, b := range breaks {
		if resolvedIDs[b.externalTxnID()] {
			require.Truef(t, b.spec.autoEligible,
				"only auto-eligible breaks may resolve; %s (%s) resolved but is not auto-eligible", b.externalTxnID(), b.spec.name)
		}
	}

	// === Success criterion 2: 100% of pipeline actions emit an AuditEvent. ===
	for _, b := range breaks {
		id := b.externalTxnID()
		evs := byTxn[id]
		require.NotEmptyf(t, evs, "break %s (%s) must have at least one audit event (100%% of actions audited)", id, b.spec.name)

		row, ok := breakByID(allBreaks, id)
		require.Truef(t, ok, "break %s must have a store row", id)
		switch row.Status {
		case "auto-resolved":
			require.Truef(t, hasAction(evs, audit.ActionClassified), "auto-resolved %s must have a classified event", id)
			require.Truef(t, hasAction(evs, audit.ActionResolved), "auto-resolved %s must have a resolved event", id)
		case "queued":
			require.Truef(t, hasAction(evs, audit.ActionEscalated), "queued %s must have an escalated event", id)
		}
	}
	// Rule 5.3 backstop: no resolved event in this run may lack a confirming recon_id.
	for _, e := range runAudit {
		if e.Action == audit.ActionResolved {
			require.NotEmptyf(t, strings.TrimSpace(e.Provenance.ReconID),
				"resolved event for %s must carry a confirming recon_id (Rule 5.3)", e.ExternalTxnID)
		}
	}

	// === Success criterion 3: every break with confidence < threshold OR
	// regulated == true is routed to HITL and never auto-actioned (Rule 5.4).
	// Checked BEFORE the HITL HTTP exercise drains the queue. ===
	for _, b := range breaks {
		if !b.spec.gatedToHITL(cfg.ConfAutoThreshold) {
			continue
		}
		id := b.externalTxnID()
		require.Truef(t, inHITLQueue(hitlItems, id),
			"gated break %s (%s: regulated=%v confidence=%.2f) must be in the HITL queue (Rule 5.4)",
			id, b.spec.name, b.spec.regulated, b.spec.confidence)
		require.Falsef(t, hasAction(byTxn[id], audit.ActionResolved),
			"gated break %s (%s) must never be auto-resolved (Rule 5.4)", id, b.spec.name)
		row, ok := breakByID(allBreaks, id)
		require.Truef(t, ok, "gated break %s must have a store row", id)
		require.Equalf(t, "queued", row.Status, "gated break %s must be queued", id)
	}

	// --- Explicit ProbeBreak dry-run bridge cross-check (Rule 5.3): independently
	// re-probe one auto-resolved break with its created rule and confirm Blnk (the
	// deterministic arbiter) reports it cleared with a non-empty recon id. ---
	assertProbeConfirmsClearance(ctx, t, blnkClient, allBreaks, breaks, resolved[0].ExternalTxnID)

	// === HITL surface exercised over HTTP (Gate 13): every decision verb is
	// invoked against the real Gin router, and the auditable verbs each write a
	// decision AuditEvent. ===
	hitlHTTP := httptest.NewServer(hitlServer.Router())
	defer hitlHTTP.Close()

	assertHTTPStatus(ctx, t, hitlHTTP.URL+"/healthz", http.StatusOK)
	assertBreaksListed(ctx, t, hitlHTTP.URL+"/breaks", breaks)

	// A distinct queued break per decision verb.
	acceptID := scenarioID(breaks, "duplicate")
	rejectID := scenarioID(breaks, "missing_internal")
	reDriveID := scenarioID(breaks, "low_confidence")
	require.NotEmpty(t, acceptID, "duplicate break id")
	require.NotEmpty(t, rejectID, "missing_internal break id")
	require.NotEmpty(t, reDriveID, "low_confidence break id")

	acceptStatus := postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{ExternalTxnID: acceptID, Decision: audit.DecisionAccept, Reviewer: integrationReviewer, Note: "integration accept"})
	require.Equal(t, http.StatusOK, acceptStatus, "POST /decisions accept")

	rejectStatus := postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{ExternalTxnID: rejectID, Decision: audit.DecisionReject, Reviewer: integrationReviewer, Note: "integration reject"})
	require.Equal(t, http.StatusOK, rejectStatus, "POST /decisions reject")

	// re_drive re-tests clearance via a Blnk dry-run. Per finding L2 the HITL
	// handler now loads the break's FULL external transaction and probes with a
	// grammar-conformant matching rule built from its fields (a non-empty
	// matching_rule_ids set is required by Blnk's start-instant), so against a
	// live Blnk the probe succeeds and this returns 200 — writing a re_driven
	// audit event (whether or not Blnk reports the break cleared). Only an
	// upstream Blnk failure (rule creation or probe) surfaces as 502 (finding
	// L2), never 500. Either outcome invokes the re_drive branch (Gate 13).
	reDriveStatus := postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{ExternalTxnID: reDriveID, Decision: audit.DecisionReDrive, Reviewer: integrationReviewer, Note: "integration re_drive"})
	require.Contains(t, []int{http.StatusOK, http.StatusBadGateway}, reDriveStatus, "POST /decisions re_drive must be handled (200 or 502)")

	// --- Decisions must be audited (Gate 13) and accept/reject must drain HITL. ---
	postAudit, err := st.ListAudit(ctx)
	require.NoError(t, err, "ListAudit (post-decision)")
	postByTxn := auditByTxn(postAudit, ids)
	require.Truef(t, hasAction(postByTxn[acceptID], audit.ActionAccepted), "accept must write an accepted audit event (Gate 13)")
	require.Truef(t, hasAction(postByTxn[rejectID], audit.ActionRejected), "reject must write a rejected audit event (Gate 13)")
	if reDriveStatus == http.StatusOK {
		require.Truef(t, hasAction(postByTxn[reDriveID], audit.ActionReDriven), "successful re_drive must write a re_driven audit event (Gate 13)")
	}

	finalHITL, err := st.ListHITL(ctx)
	require.NoError(t, err, "ListHITL (post-decision)")
	require.Falsef(t, inHITLQueue(finalHITL, acceptID), "accepted break %s must be dequeued from HITL", acceptID)
	require.Falsef(t, inHITLQueue(finalHITL, rejectID), "rejected break %s must be dequeued from HITL", rejectID)
}

// ---------------------------------------------------------------------------
// HTTP + probe assertion helpers
// ---------------------------------------------------------------------------

// scenarioID returns the run-scoped external txn id of the planted break for a
// scenario name, or "" when none matches.
func scenarioID(breaks []breakInput, scenario string) string {
	for _, b := range breaks {
		if b.spec.name == scenario {
			return b.externalTxnID()
		}
	}
	return ""
}

// postDecision submits a HITL decision as JSON to <baseURL>/decisions and returns
// the HTTP status code.
func postDecision(ctx context.Context, t *testing.T, baseURL string, d model.HITLDecision) int {
	t.Helper()
	body, err := json.Marshal(d)
	require.NoError(t, err, "marshal decision")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/decisions", strings.NewReader(string(body)))
	require.NoError(t, err, "build decision request")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "POST /decisions")
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// assertHTTPStatus GETs url and asserts the response status equals want.
func assertHTTPStatus(ctx context.Context, t *testing.T, url string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err, "build GET request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET %s", url)
	defer func() { _ = resp.Body.Close() }()
	require.Equalf(t, want, resp.StatusCode, "GET %s status", url)
}

// assertBreaksListed GETs the HITL /breaks JSON view and asserts every planted
// break of this run is present.
func assertBreaksListed(ctx context.Context, t *testing.T, url string, breaks []breakInput) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err, "build GET /breaks request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET /breaks")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /breaks status")

	var payload struct {
		Breaks []struct {
			Classification struct {
				ExternalTxnID string `json:"external_txn_id"`
			} `json:"classification"`
			Status string `json:"status"`
		} `json:"breaks"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload), "decode /breaks JSON")

	got := make(map[string]bool, len(payload.Breaks))
	for _, b := range payload.Breaks {
		got[b.Classification.ExternalTxnID] = true
	}
	for _, b := range breaks {
		require.Truef(t, got[b.externalTxnID()], "GET /breaks must include run break %s", b.externalTxnID())
	}
}

// assertProbeConfirmsClearance independently re-probes an auto-resolved break
// with its persisted created rule and asserts Blnk's dry-run confirms clearance
// with a non-empty recon id — the deterministic-arbiter bridge (Rule 5.3).
func assertProbeConfirmsClearance(ctx context.Context, t *testing.T, client *blnk.Client, rows []store.Break, breaks []breakInput, externalTxnID string) {
	t.Helper()
	row, ok := breakByID(rows, externalTxnID)
	require.Truef(t, ok, "resolved break %s must have a store row", externalTxnID)
	require.NotEmptyf(t, row.CreatedRuleID, "auto-resolved break %s must record its created Blnk rule id", externalTxnID)

	var input breakInput
	found := false
	for _, b := range breaks {
		if b.externalTxnID() == externalTxnID {
			input, found = b, true
			break
		}
	}
	require.Truef(t, found, "resolved break %s must be one of the planted breaks", externalTxnID)

	// Re-probe an independent clone carrying the SAME reference/amount (all Blnk
	// matches on) but a FRESH id: start-instant persists the submitted external
	// txn by its own id with no upsert, and the remediator's auto path has already
	// probed (hence persisted) this break's real id, so re-submitting that id
	// would be a duplicate-key failure. The fresh-id clone yields an independent,
	// collision-free confirmation of the identical match (see probeVerifyIDSuffix).
	probe := input.txn()
	probe.ID = probe.ID + probeVerifyIDSuffix
	cleared, reconID, err := client.ProbeBreak(ctx, probe, []string{row.CreatedRuleID})
	require.NoErrorf(t, err, "ProbeBreak dry-run for %s must succeed", externalTxnID)
	require.Truef(t, cleared, "Blnk dry-run must confirm %s cleared (deterministic arbiter, Rule 5.3)", externalTxnID)
	require.NotEmptyf(t, reconID, "ProbeBreak must return a confirming recon id for %s", externalTxnID)
}
