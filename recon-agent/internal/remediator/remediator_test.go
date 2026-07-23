package remediator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// ---- Compile-time proof the fakes satisfy the unexported consumer ports. ----

var (
	_ classifierPort = (*fakeClassifier)(nil)
	_ blnkPort       = (*fakeBlnk)(nil)
	_ auditPort      = (*fakeAudit)(nil)
	_ storePort      = (*fakeStore)(nil)
)

// ---- Fakes (stdlib only; no testify, no go-sqlmock). ----

type fakeClassifier struct {
	ret   model.BreakClassification
	err   error
	calls int
}

func (f *fakeClassifier) Classify(ctx context.Context, txn blnk.ExternalTransaction) (model.BreakClassification, error) {
	f.calls++
	return f.ret, f.err
}

type fakeBlnk struct {
	created          blnk.MatchingRule
	createErr        error
	cleared          bool
	reconID          string
	probeErr         error
	createCalls      int
	probeCalls       int
	lastProbeRuleIDs []string
}

func (f *fakeBlnk) CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error) {
	f.createCalls++
	if f.createErr != nil {
		return blnk.MatchingRule{}, f.createErr
	}
	return f.created, nil
}

func (f *fakeBlnk) ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (bool, string, error) {
	f.probeCalls++
	f.lastProbeRuleIDs = matchingRuleIDs
	if f.probeErr != nil {
		return false, "", f.probeErr
	}
	return f.cleared, f.reconID, nil
}

type fakeAudit struct {
	events       []model.AuditEvent
	failOnAction string
}

func (f *fakeAudit) Record(ctx context.Context, ev model.AuditEvent) error {
	if f.failOnAction != "" && ev.Action == f.failOnAction {
		return fmt.Errorf("audit: injected failure on %q", ev.Action)
	}
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeAudit) has(action string) bool {
	_, ok := f.find(action)
	return ok
}

func (f *fakeAudit) find(action string) (model.AuditEvent, bool) {
	for _, e := range f.events {
		if e.Action == action {
			return e, true
		}
	}
	return model.AuditEvent{}, false
}

type fakeStore struct {
	statuses    map[string]string
	breaks      map[string]model.BreakClassification
	hitl        map[string]string
	upsertErr   error
	setErr      error
	enqErr      error
	upsertCalls int
	setCalls    int
	enqCalls    int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		statuses: map[string]string{},
		breaks:   map[string]model.BreakClassification{},
		hitl:     map[string]string{},
	}
}

func (f *fakeStore) UpsertBreak(ctx context.Context, c model.BreakClassification, status string) error {
	f.upsertCalls++
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.statuses[c.ExternalTxnID] = status
	f.breaks[c.ExternalTxnID] = c
	return nil
}

func (f *fakeStore) SetBreakStatus(ctx context.Context, externalTxnID, status string) error {
	f.setCalls++
	if f.setErr != nil {
		return f.setErr
	}
	f.statuses[externalTxnID] = status
	return nil
}

func (f *fakeStore) EnqueueHITL(ctx context.Context, externalTxnID, reason string) error {
	f.enqCalls++
	if f.enqErr != nil {
		return f.enqErr
	}
	f.hitl[externalTxnID] = reason
	return nil
}

// ---- Helpers ----

const (
	testThreshold = 0.85
	testModel     = "kimi-k3"
)

func sampleTxn() blnk.ExternalTransaction {
	return blnk.ExternalTransaction{ID: "ext-1", Amount: 100, Currency: "USD", Source: "bank-x"}
}

func validRule() *blnk.MatchingRule {
	return &blnk.MatchingRule{
		Name: "amount-eq",
		Criteria: []blnk.MatchingCriteria{
			{Field: "amount", Operator: "equals", Value: "100"},
		},
	}
}

func newRemediator(fc *fakeClassifier, fb *fakeBlnk, fa *fakeAudit, fs *fakeStore) *Remediator {
	return New(fc, fb, fa, fs, testThreshold, testModel)
}

// ---- (a) High-confidence, non-regulated, dry-run cleared => auto-resolved. ----

func TestHandle_AutoResolved_WhenBlnkConfirmsClearance(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{
		// ExternalTxnID intentionally empty to prove the defensive re-key.
		RootCause:    model.RootCauseAmountDrift,
		Confidence:   0.95,
		Regulated:    false,
		ProposedRule: validRule(),
		Rationale:    "amount within drift",
	}}
	fb := &fakeBlnk{created: blnk.MatchingRule{RuleID: "rule-123"}, cleared: true, reconID: "recon-777"}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}

	if got := fs.statuses["ext-1"]; got != statusAutoResolved {
		t.Errorf("final status = %q, want %q", got, statusAutoResolved)
	}
	if !fa.has("classified") {
		t.Error("expected a classified audit event (Rule 5.5)")
	}
	resolved, ok := fa.find("resolved")
	if !ok {
		t.Fatal("expected a resolved audit event")
	}
	if resolved.Provenance.ReconID != "recon-777" {
		t.Errorf("resolved event recon_id = %q, want %q (Rule 5.3)", resolved.Provenance.ReconID, "recon-777")
	}
	if resolved.Provenance.Model != testModel {
		t.Errorf("resolved event provenance model = %q, want %q", resolved.Provenance.Model, testModel)
	}
	if fa.has("escalated") {
		t.Error("did not expect an escalated event on the auto-resolved path")
	}
	if fb.createCalls != 1 || fb.probeCalls != 1 {
		t.Errorf("createCalls=%d probeCalls=%d, want 1/1", fb.createCalls, fb.probeCalls)
	}
	if len(fb.lastProbeRuleIDs) != 1 || fb.lastProbeRuleIDs[0] != "rule-123" {
		t.Errorf("ProbeBreak matchingRuleIDs = %v, want [rule-123]", fb.lastProbeRuleIDs)
	}
}

// ---- (b) Low confidence => HITL, no resolution, no Blnk calls (Rule 5.4). ----

func TestHandle_LowConfidence_RoutesToHITL(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{
		ExternalTxnID: "ext-1",
		RootCause:     model.RootCauseTiming,
		Confidence:    0.5,
		ProposedRule:  validRule(),
	}}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if got := fs.statuses["ext-1"]; got != statusQueued {
		t.Errorf("status = %q, want %q", got, statusQueued)
	}
	if fa.has("resolved") {
		t.Error("Rule 5.4 violated: low-confidence break must NOT have a resolved event")
	}
	if !fa.has("classified") || !fa.has("escalated") {
		t.Error("expected classified + escalated audit events")
	}
	if fb.createCalls != 0 || fb.probeCalls != 0 {
		t.Errorf("Blnk must not be called for a gated break; createCalls=%d probeCalls=%d", fb.createCalls, fb.probeCalls)
	}
	if fs.hitl["ext-1"] != reasonLowConfidence {
		t.Errorf("HITL reason = %q, want %q", fs.hitl["ext-1"], reasonLowConfidence)
	}
}

// ---- (c) Regulated at high confidence => HITL (Rule 5.4). ----

func TestHandle_Regulated_RoutesToHITL_EvenAtHighConfidence(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{
		ExternalTxnID: "ext-1",
		Confidence:    0.99,
		Regulated:     true,
		ProposedRule:  validRule(),
	}}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if got := fs.statuses["ext-1"]; got != statusQueued {
		t.Errorf("status = %q, want %q", got, statusQueued)
	}
	if fa.has("resolved") {
		t.Error("Rule 5.4 violated: regulated break must NOT be auto-resolved")
	}
	if fb.createCalls != 0 || fb.probeCalls != 0 {
		t.Errorf("regulated break must not touch Blnk; createCalls=%d probeCalls=%d", fb.createCalls, fb.probeCalls)
	}
	if fs.hitl["ext-1"] != reasonRegulated {
		t.Errorf("HITL reason = %q, want %q", fs.hitl["ext-1"], reasonRegulated)
	}
}

// ---- (d) Classifier terminal failure => fail-closed HITL (Rule 5.7). ----

func TestHandle_ClassifierFailure_FailsClosedToHITL(t *testing.T) {
	fc := &fakeClassifier{err: fmt.Errorf("upstream: %w", classifier.ErrClassificationFailed)}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fc.calls != 1 {
		t.Errorf("classifier called %d times, want 1", fc.calls)
	}
	if fa.has("classified") {
		t.Error("no classified event expected when classification failed")
	}
	if fa.has("resolved") {
		t.Error("Rule 5.7 violated: failed classification must never auto-resolve")
	}
	if !fa.has("escalated") {
		t.Error("expected an escalated audit event (fail-closed)")
	}
	if fb.createCalls != 0 || fb.probeCalls != 0 {
		t.Errorf("no Blnk calls expected on classifier failure; createCalls=%d probeCalls=%d", fb.createCalls, fb.probeCalls)
	}
	if got := fs.statuses["ext-1"]; got != statusQueued {
		t.Errorf("status = %q, want %q", got, statusQueued)
	}
	if b, ok := fs.breaks["ext-1"]; !ok {
		t.Error("expected the stub break to be keyed by txn ID")
	} else if b.RootCause != model.RootCauseUnknown {
		t.Errorf("stub RootCause = %q, want %q", b.RootCause, model.RootCauseUnknown)
	}
	if !strings.Contains(fs.hitl["ext-1"], reasonClassifierFailed) {
		t.Errorf("HITL reason = %q, want it to contain %q", fs.hitl["ext-1"], reasonClassifierFailed)
	}
}

// ---- (d2) Non-sentinel classifier error still routes to HITL. ----

func TestHandle_ClassifierNonSentinelError_StillHITL(t *testing.T) {
	fc := &fakeClassifier{err: errors.New("network blip")}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if !fa.has("escalated") || fa.has("resolved") || fa.has("classified") {
		t.Error("non-sentinel classifier error must fail-closed to HITL with no classified/resolved event")
	}
	reason := fs.hitl["ext-1"]
	if !strings.Contains(reason, reasonClassifierFailed) || !strings.Contains(reason, "network blip") {
		t.Errorf("HITL reason = %q, want it to note the unexpected error", reason)
	}
}

// ---- (e1) Rule creation error => HITL. ----

func TestHandle_CreateMatchingRuleError_RoutesToHITL(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
	fb := &fakeBlnk{createErr: errors.New("blnk 500")}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fb.createCalls != 1 || fb.probeCalls != 0 {
		t.Errorf("createCalls=%d probeCalls=%d, want 1/0", fb.createCalls, fb.probeCalls)
	}
	if fa.has("resolved") || !fa.has("escalated") {
		t.Error("rule-creation failure must escalate, never resolve")
	}
	if !strings.HasPrefix(fs.hitl["ext-1"], reasonRuleCreateFailed) {
		t.Errorf("HITL reason = %q, want prefix %q", fs.hitl["ext-1"], reasonRuleCreateFailed)
	}
}

// ---- (e2) Probe error => HITL. ----

func TestHandle_ProbeError_RoutesToHITL(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
	fb := &fakeBlnk{created: blnk.MatchingRule{RuleID: "rule-9"}, probeErr: errors.New("timeout")}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fb.createCalls != 1 || fb.probeCalls != 1 {
		t.Errorf("createCalls=%d probeCalls=%d, want 1/1", fb.createCalls, fb.probeCalls)
	}
	if fa.has("resolved") || !fa.has("escalated") {
		t.Error("probe failure must escalate, never resolve")
	}
	if !strings.HasPrefix(fs.hitl["ext-1"], reasonProbeFailed) {
		t.Errorf("HITL reason = %q, want prefix %q", fs.hitl["ext-1"], reasonProbeFailed)
	}
}

// ---- (e3) Dry-run not cleared => HITL (Rule 5.3: Blnk is the arbiter). ----

func TestHandle_NotCleared_RoutesToHITL(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
	fb := &fakeBlnk{created: blnk.MatchingRule{RuleID: "rule-9"}, cleared: false, reconID: "recon-x"}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fa.has("resolved") {
		t.Error("Rule 5.3 violated: uncleared dry-run must never be marked resolved")
	}
	if !fa.has("escalated") {
		t.Error("expected escalation when Blnk did not confirm clearance")
	}
	if fs.hitl["ext-1"] != reasonNotCleared {
		t.Errorf("HITL reason = %q, want %q", fs.hitl["ext-1"], reasonNotCleared)
	}
	if got := fs.statuses["ext-1"]; got != statusQueued {
		t.Errorf("status = %q, want %q", got, statusQueued)
	}
}

// ---- (f) No proposed rule => HITL. ----

func TestHandle_NoProposedRule_RoutesToHITL(t *testing.T) {
	fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: nil}}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fb.createCalls != 0 {
		t.Errorf("no rule must be created when none proposed; createCalls=%d", fb.createCalls)
	}
	if fa.has("resolved") || !fa.has("escalated") {
		t.Error("missing rule must escalate, never resolve")
	}
	if fs.hitl["ext-1"] != reasonMissingRule {
		t.Errorf("HITL reason = %q, want %q", fs.hitl["ext-1"], reasonMissingRule)
	}
}

// ---- (g) Proposed rule fails grammar validation => HITL (Rule 5.2). ----

func TestHandle_GrammarRejection_RoutesToHITL(t *testing.T) {
	bad := &blnk.MatchingRule{
		Name:     "bogus",
		Criteria: []blnk.MatchingCriteria{{Field: "not_a_field", Operator: "equals", Value: "1"}},
	}
	fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.95, ProposedRule: bad}}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := newRemediator(fc, fb, fa, fs)

	if err := r.Handle(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Handle returned unexpected error: %v", err)
	}
	if fb.createCalls != 0 || fb.probeCalls != 0 {
		t.Errorf("out-of-grammar rule must never be POSTed; createCalls=%d probeCalls=%d", fb.createCalls, fb.probeCalls)
	}
	if fa.has("resolved") || !fa.has("escalated") {
		t.Error("grammar rejection must escalate, never resolve")
	}
	if !strings.HasPrefix(fs.hitl["ext-1"], reasonGrammarReject) {
		t.Errorf("HITL reason = %q, want prefix %q", fs.hitl["ext-1"], reasonGrammarReject)
	}
}

// ---- (h) Infrastructure failures bubble up as non-nil Handle errors. ----

func TestHandle_InfraErrors_Propagate(t *testing.T) {
	t.Run("classified upsert error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
		fs := newFakeStore()
		fs.upsertErr = errors.New("db down")
		r := newRemediator(fc, &fakeBlnk{}, &fakeAudit{}, fs)
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when the classified upsert fails")
		}
	})

	t.Run("classified audit error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
		fa := &fakeAudit{failOnAction: "classified"}
		r := newRemediator(fc, &fakeBlnk{}, fa, newFakeStore())
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when the classified audit write fails")
		}
	})

	t.Run("resolve SetBreakStatus error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
		fb := &fakeBlnk{created: blnk.MatchingRule{RuleID: "rule-9"}, cleared: true, reconID: "recon-1"}
		fs := newFakeStore()
		fs.setErr = errors.New("set failed")
		r := newRemediator(fc, fb, &fakeAudit{}, fs)
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when SetBreakStatus fails")
		}
	})

	t.Run("resolved audit error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.9, ProposedRule: validRule()}}
		fb := &fakeBlnk{created: blnk.MatchingRule{RuleID: "rule-9"}, cleared: true, reconID: "recon-1"}
		fa := &fakeAudit{failOnAction: "resolved"}
		r := newRemediator(fc, fb, fa, newFakeStore())
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when the resolved audit write fails")
		}
	})

	t.Run("escalate enqueue error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.5, ProposedRule: validRule()}}
		fs := newFakeStore()
		fs.enqErr = errors.New("queue down")
		r := newRemediator(fc, &fakeBlnk{}, &fakeAudit{}, fs)
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when EnqueueHITL fails")
		}
	})

	t.Run("escalate upsert error via fail-closed path", func(t *testing.T) {
		fc := &fakeClassifier{err: classifier.ErrClassificationFailed}
		fs := newFakeStore()
		fs.upsertErr = errors.New("db down")
		r := newRemediator(fc, &fakeBlnk{}, &fakeAudit{}, fs)
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when escalate's upsert fails")
		}
	})

	t.Run("escalate audit error", func(t *testing.T) {
		fc := &fakeClassifier{ret: model.BreakClassification{ExternalTxnID: "ext-1", Confidence: 0.5, ProposedRule: validRule()}}
		fa := &fakeAudit{failOnAction: "escalated"}
		r := newRemediator(fc, &fakeBlnk{}, fa, newFakeStore())
		if err := r.Handle(context.Background(), sampleTxn()); err == nil {
			t.Fatal("expected a non-nil error when the escalated audit write fails")
		}
	})
}

// ---- (i) Constructor wiring. ----

func TestNew_WiresDependencies(t *testing.T) {
	fc := &fakeClassifier{}
	fb := &fakeBlnk{}
	fa := &fakeAudit{}
	fs := newFakeStore()
	r := New(fc, fb, fa, fs, 0.85, "kimi-k3")
	if r.cls == nil || r.blnkClient == nil || r.auditWriter == nil || r.store == nil {
		t.Fatal("New must wire all four collaborators")
	}
	if r.threshold != 0.85 {
		t.Errorf("threshold = %v, want 0.85", r.threshold)
	}
	if r.llmModel != "kimi-k3" {
		t.Errorf("llmModel = %q, want %q", r.llmModel, "kimi-k3")
	}
}
