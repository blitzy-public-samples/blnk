// White-box tests (package audit) for the append-only audit writer. They
// exercise every finding-driven behavior added at this checkpoint:
//   - C-06: transaction-bound recording via RecordTx / TxSink.
//   - C-07: nil-sink rejection, required-field validation, and the
//     ClearanceProof verified-probe type gating Resolved.
//   - M-13: the closed action set + rule/probe builders, machine-readable
//     evidence stamping, and rejection of arbitrary decisions.
//   - M-14: trusted server-clock RFC3339Nano timestamps overriding any
//     caller-supplied value.
//   - M-20: this file itself, providing full statement coverage of audit.go.
//   - Rule 5.5: the writer exposes no mutating (update/delete) path.
package audit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// -----------------------------------------------------------------------------
// Test doubles
// -----------------------------------------------------------------------------

// recordingSink implements the unexported sink interface, capturing every
// appended event and optionally returning a programmed error.
type recordingSink struct {
	events []model.AuditEvent
	err    error
}

func (s *recordingSink) InsertAudit(_ context.Context, e model.AuditEvent) error {
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, e)
	return nil
}

// txRecordingSink implements BOTH sink and TxSink, so New discovers its
// transaction-bound capability.
type txRecordingSink struct {
	recordingSink
	txEvents []model.AuditEvent
	txErr    error
	lastTx   *sql.Tx
}

func (s *txRecordingSink) InsertAuditTx(_ context.Context, tx *sql.Tx, e model.AuditEvent) error {
	s.lastTx = tx
	if s.txErr != nil {
		return s.txErr
	}
	s.txEvents = append(s.txEvents, e)
	return nil
}

// Minimal database/sql/driver fake, sufficient to hand out a real *sql.Tx for
// the RecordTx success path. Mirrors the pattern in internal/store/store_test.go.
type fakeConnector struct{}

func (fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{}, nil }
func (fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("audit test: prepare not supported")
}
func (fakeConn) Close() error              { return nil }
func (fakeConn) Begin() (driver.Tx, error) { return fakeTx{}, nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

func newTx(t *testing.T) *sql.Tx {
	t.Helper()
	db := sql.OpenDB(fakeConnector{})
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin fake tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(); _ = db.Close() })
	return tx
}

func validEvent() model.AuditEvent {
	return model.AuditEvent{
		ExternalTxnID: "EXT-001",
		Actor:         ActorAgent,
		Action:        ActionClassified,
		Rationale:     "looks like a timing break",
		Confidence:    0.5,
		Provenance:    model.Provenance{Model: "kimi-k3", Source: "bank-x"},
	}
}

// -----------------------------------------------------------------------------
// New / constructor validation (C-07, C-06 discovery)
// -----------------------------------------------------------------------------

func TestNew_NilSink(t *testing.T) {
	w, err := New(nil)
	if !errors.Is(err, ErrNilSink) {
		t.Fatalf("New(nil): want ErrNilSink, got %v", err)
	}
	if w != nil {
		t.Fatalf("New(nil): want nil Writer, got %+v", w)
	}
}

func TestNew_SinkOnly_NoTxSupport(t *testing.T) {
	w, err := New(&recordingSink{})
	if err != nil {
		t.Fatalf("New: unexpected error %v", err)
	}
	if w.txSink != nil {
		t.Fatal("sink without InsertAuditTx must not be discovered as a TxSink")
	}
	// RecordTx must refuse when the sink has no transaction-bound path.
	if err := w.RecordTx(context.Background(), newTx(t), validEvent()); !errors.Is(err, ErrTxSinkUnsupported) {
		t.Fatalf("RecordTx: want ErrTxSinkUnsupported, got %v", err)
	}
}

func TestNew_DiscoversTxSink(t *testing.T) {
	w, err := New(&txRecordingSink{})
	if err != nil {
		t.Fatalf("New: unexpected error %v", err)
	}
	if w.txSink == nil {
		t.Fatal("a sink implementing InsertAuditTx must be discovered as a TxSink")
	}
}

// -----------------------------------------------------------------------------
// Record: success, normalization, and validation (C-07, M-14)
// -----------------------------------------------------------------------------

func TestRecord_Success_NormalizesIDAndTimestamp(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)

	before := time.Now().UTC()
	if err := w.Record(context.Background(), validEvent()); err != nil {
		t.Fatalf("Record: %v", err)
	}
	after := time.Now().UTC()

	if len(fs.events) != 1 {
		t.Fatalf("want 1 recorded event, got %d", len(fs.events))
	}
	got := fs.events[0]
	if _, err := uuid.Parse(got.EventID); err != nil {
		t.Fatalf("EventID is not a UUID: %q (%v)", got.EventID, err)
	}
	ts, err := time.Parse(time.RFC3339Nano, got.Timestamp)
	if err != nil {
		t.Fatalf("Timestamp not RFC3339Nano: %q (%v)", got.Timestamp, err)
	}
	if ts.Before(before.Add(-time.Second)) || ts.After(after.Add(time.Second)) {
		t.Fatalf("Timestamp %v not within record window [%v,%v]", ts, before, after)
	}
}

func TestRecord_PreservesValidCallerEventID(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	ev := validEvent()
	id := uuid.NewString()
	ev.EventID = id
	if err := w.Record(context.Background(), ev); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if fs.events[0].EventID != id {
		t.Fatalf("valid caller EventID must be preserved: want %s got %s", id, fs.events[0].EventID)
	}
}

func TestRecord_RejectsMalformedEventID(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	ev := validEvent()
	ev.EventID = "not-a-uuid"
	if err := w.Record(context.Background(), ev); !errors.Is(err, ErrInvalidEventID) {
		t.Fatalf("want ErrInvalidEventID, got %v", err)
	}
	if len(fs.events) != 0 {
		t.Fatal("a malformed event id must not be written")
	}
}

// M-14: a caller-supplied timestamp is never trusted; the server clock wins.
func TestRecord_OverridesCallerTimestamp(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	ev := validEvent()
	ev.Timestamp = "1999-01-01T00:00:00Z"
	if err := w.Record(context.Background(), ev); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got := fs.events[0].Timestamp
	if got == "1999-01-01T00:00:00Z" {
		t.Fatal("caller timestamp must be overridden (M-14)")
	}
	ts, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("overridden timestamp not RFC3339Nano: %q (%v)", got, err)
	}
	if time.Since(ts) > time.Minute {
		t.Fatalf("overridden timestamp %v is not from the server clock", ts)
	}
}

func TestRecord_ValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*model.AuditEvent)
		want error
	}{
		{"empty external txn id", func(e *model.AuditEvent) { e.ExternalTxnID = "" }, ErrEmptyExternalTxnID},
		{"whitespace external txn id", func(e *model.AuditEvent) { e.ExternalTxnID = "   " }, ErrEmptyExternalTxnID},
		{"empty actor", func(e *model.AuditEvent) { e.Actor = "" }, ErrEmptyActor},
		{"whitespace actor", func(e *model.AuditEvent) { e.Actor = "  " }, ErrEmptyActor},
		{"unknown action", func(e *model.AuditEvent) { e.Action = "frobnicate" }, ErrInvalidAction},
		{"empty action", func(e *model.AuditEvent) { e.Action = "" }, ErrInvalidAction},
		{"confidence NaN", func(e *model.AuditEvent) { e.Confidence = math.NaN() }, ErrInvalidConfidence},
		{"confidence +Inf", func(e *model.AuditEvent) { e.Confidence = math.Inf(1) }, ErrInvalidConfidence},
		{"confidence -Inf", func(e *model.AuditEvent) { e.Confidence = math.Inf(-1) }, ErrInvalidConfidence},
		{"confidence below 0", func(e *model.AuditEvent) { e.Confidence = -0.01 }, ErrInvalidConfidence},
		{"confidence above 1", func(e *model.AuditEvent) { e.Confidence = 1.01 }, ErrInvalidConfidence},
		{"resolved without recon id", func(e *model.AuditEvent) {
			e.Action = ActionResolved
			e.Provenance.ReconID = ""
		}, ErrEmptyReconID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &recordingSink{}
			w, _ := New(fs)
			ev := validEvent()
			tc.mut(&ev)
			if err := w.Record(context.Background(), ev); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if len(fs.events) != 0 {
				t.Fatal("an invalid event must not be written (fail-safe)")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// M-12: non-empty rationale + action-specific provenance validation
// -----------------------------------------------------------------------------

// TestRecord_RejectsEmptyRationale proves a directly-constructed event with no
// rationale is rejected (finding M-12: every action must explain WHY). The
// builders synthesize a default, so this guards the raw Record path.
func TestRecord_RejectsEmptyRationale(t *testing.T) {
	for _, r := range []string{"", "   ", "\t\n"} {
		fs := &recordingSink{}
		w, _ := New(fs)
		ev := validEvent()
		ev.Rationale = r
		if err := w.Record(context.Background(), ev); !errors.Is(err, ErrEmptyRationale) {
			t.Fatalf("rationale %q: want ErrEmptyRationale, got %v", r, err)
		}
		if len(fs.events) != 0 {
			t.Fatal("an event with no rationale must not be written")
		}
	}
}

// TestRecord_ActionSpecificProvenance proves each action is rejected when it
// lacks the machine-readable evidence its action requires, and accepted once
// that evidence is present (finding M-12). Events are built directly (not via
// the evidence-stamping builders) so the validation itself is exercised.
func TestRecord_ActionSpecificProvenance(t *testing.T) {
	base := func(action string, prov model.Provenance) model.AuditEvent {
		return model.AuditEvent{
			ExternalTxnID: "EXT-100",
			Actor:         ActorAgent,
			Action:        action,
			Rationale:     "non-empty rationale",
			Confidence:    0.5,
			Provenance:    prov,
		}
	}
	cases := []struct {
		name    string
		missing model.AuditEvent
		wantErr error
		present model.AuditEvent
	}{
		{
			name:    "classified requires model",
			missing: base(ActionClassified, model.Provenance{Source: "bank-x"}),
			wantErr: ErrMissingProvenance,
			present: base(ActionClassified, model.Provenance{Model: "kimi-k3"}),
		},
		{
			name:    "rule_proposed requires rule_field",
			missing: base(ActionRuleProposed, model.Provenance{RuleID: "rule_1"}),
			wantErr: ErrMissingProvenance,
			present: base(ActionRuleProposed, model.Provenance{RuleField: "amount"}),
		},
		{
			name:    "rule_created requires rule_id",
			missing: base(ActionRuleCreated, model.Provenance{RuleField: "amount"}),
			wantErr: ErrMissingProvenance,
			present: base(ActionRuleCreated, model.Provenance{RuleID: "rule_1"}),
		},
		{
			name:    "probed requires recon_id",
			missing: base(ActionProbed, model.Provenance{}),
			wantErr: ErrMissingProvenance,
			present: base(ActionProbed, model.Provenance{ReconID: "recon_p"}),
		},
		{
			// resolved keeps its own distinct sentinel (Rule 5.3 deterministic
			// arbiter proof) rather than the generic missing-provenance error.
			name:    "resolved requires recon_id (distinct sentinel)",
			missing: base(ActionResolved, model.Provenance{Model: "kimi-k3"}),
			wantErr: ErrEmptyReconID,
			present: base(ActionResolved, model.Provenance{ReconID: "recon_r"}),
		},
		{
			// rule_compensated (finding M-11) names the exact rule that was
			// deleted (or failed to delete) via its rule_id, so the compensation
			// trail is traceable to a specific Blnk rule.
			name:    "rule_compensated requires rule_id",
			missing: base(ActionRuleCompensated, model.Provenance{Model: "kimi-k3"}),
			wantErr: ErrMissingProvenance,
			present: base(ActionRuleCompensated, model.Provenance{RuleID: "rule_1"}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &recordingSink{}
			w, _ := New(fs)
			if err := w.Record(context.Background(), tc.missing); !errors.Is(err, tc.wantErr) {
				t.Fatalf("missing evidence: want %v, got %v", tc.wantErr, err)
			}
			if len(fs.events) != 0 {
				t.Fatal("an under-attributed event must not be written")
			}
			if err := w.Record(context.Background(), tc.present); err != nil {
				t.Fatalf("present evidence: unexpected error %v", err)
			}
			if len(fs.events) != 1 {
				t.Fatal("a fully-attributed event must be written")
			}
		})
	}
}

// TestRecord_ActionsWithoutExtraProvenance proves the actions that intentionally
// require no extra provenance (finding M-12 / m-04) are accepted with only the
// required actor + rationale: a probe-error escalation carries no model, and a
// "not cleared" re_driven decision carries no recon_id.
func TestRecord_ActionsWithoutExtraProvenance(t *testing.T) {
	for _, action := range []string{ActionEscalated, ActionAccepted, ActionReDriven, ActionRejected} {
		fs := &recordingSink{}
		w, _ := New(fs)
		ev := model.AuditEvent{
			ExternalTxnID: "EXT-101",
			Actor:         "alice", // reviewer for decisions; agent for escalated
			Action:        action,
			Rationale:     "decided",
			Confidence:    0,
			Provenance:    model.Provenance{}, // deliberately bare
		}
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("action %q with only actor+rationale must be recordable, got %v", action, err)
		}
		if len(fs.events) != 1 {
			t.Fatalf("action %q must be written", action)
		}
	}
}

// TestBuilders_SynthesizeDefaultRationale proves each builder that accepts a
// caller rationale substitutes a non-empty default when given none, so the
// resulting event passes the M-12 non-empty-rationale rule and is recordable.
func TestBuilders_SynthesizeDefaultRationale(t *testing.T) {
	proof, err := NewClearanceProof("recon_x", true)
	if err != nil {
		t.Fatalf("NewClearanceProof: %v", err)
	}
	events := map[string]model.AuditEvent{
		"classified": Classified(
			model.BreakClassification{ExternalTxnID: "EXT-200", RootCause: model.RootCauseTiming, Confidence: 0.9},
			model.Provenance{Model: "kimi-k3"},
		),
		"resolved":  Resolved("EXT-201", "", 0.9, model.Provenance{Model: "kimi-k3"}, proof),
		"escalated": Escalated("EXT-202", "", 0.3, model.Provenance{}),
		"decision":  Decision(model.HITLDecision{ExternalTxnID: "EXT-203", Decision: DecisionAccept, Reviewer: "alice"}, model.Provenance{}),
	}
	for name, ev := range events {
		if strings.TrimSpace(ev.Rationale) == "" {
			t.Fatalf("%s: builder must synthesize a non-empty default rationale", name)
		}
		fs := &recordingSink{}
		w, _ := New(fs)
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("%s: default-rationale event must be recordable, got %v", name, err)
		}
	}
	// The synthesized defaults are informative, not generic placeholders.
	if !strings.Contains(events["classified"].Rationale, "timing") {
		t.Fatalf("classified default should name the root cause: %q", events["classified"].Rationale)
	}
	// A classification with neither rationale NOR root cause still yields a
	// non-empty, recordable rationale that names the unknown root cause.
	blank := Classified(model.BreakClassification{ExternalTxnID: "EXT-204"}, model.Provenance{Model: "kimi-k3"})
	if !strings.Contains(blank.Rationale, string(model.RootCauseUnknown)) {
		t.Fatalf("classified default with no root cause should name %q: %q", model.RootCauseUnknown, blank.Rationale)
	}
	blankSink := &recordingSink{}
	blankWriter, _ := New(blankSink)
	if err := blankWriter.Record(context.Background(), blank); err != nil {
		t.Fatalf("blank-classification default must be recordable: %v", err)
	}
	if !strings.Contains(events["resolved"].Rationale, "recon_x") {
		t.Fatalf("resolved default should name the confirming recon id: %q", events["resolved"].Rationale)
	}
	if !strings.Contains(events["decision"].Rationale, DecisionAccept) {
		t.Fatalf("decision default should name the verb: %q", events["decision"].Rationale)
	}
}

func TestRecord_PropagatesSinkError(t *testing.T) {
	sentinel := errors.New("db down")
	fs := &recordingSink{err: sentinel}
	w, _ := New(fs)
	if err := w.Record(context.Background(), validEvent()); !errors.Is(err, sentinel) {
		t.Fatalf("want sink error, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// RecordTx: transaction-bound recording (C-06)
// -----------------------------------------------------------------------------

func TestRecordTx_NilTx(t *testing.T) {
	w, _ := New(&txRecordingSink{})
	if err := w.RecordTx(context.Background(), nil, validEvent()); !errors.Is(err, ErrNilTx) {
		t.Fatalf("want ErrNilTx, got %v", err)
	}
}

func TestRecordTx_Success(t *testing.T) {
	ts := &txRecordingSink{}
	w, _ := New(ts)
	tx := newTx(t)
	if err := w.RecordTx(context.Background(), tx, validEvent()); err != nil {
		t.Fatalf("RecordTx: %v", err)
	}
	if len(ts.txEvents) != 1 {
		t.Fatalf("want 1 tx-recorded event, got %d", len(ts.txEvents))
	}
	if len(ts.events) != 0 {
		t.Fatal("RecordTx must not use the non-transactional path")
	}
	if ts.lastTx != tx {
		t.Fatal("the caller transaction must be threaded through to InsertAuditTx (atomic composition)")
	}
	if _, err := uuid.Parse(ts.txEvents[0].EventID); err != nil {
		t.Fatalf("tx event not normalized (EventID): %v", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, ts.txEvents[0].Timestamp); err != nil {
		t.Fatalf("tx event not normalized (Timestamp): %v", err)
	}
}

func TestRecordTx_ValidationError(t *testing.T) {
	ts := &txRecordingSink{}
	w, _ := New(ts)
	ev := validEvent()
	ev.Actor = ""
	if err := w.RecordTx(context.Background(), newTx(t), ev); !errors.Is(err, ErrEmptyActor) {
		t.Fatalf("want ErrEmptyActor, got %v", err)
	}
	if len(ts.txEvents) != 0 {
		t.Fatal("an invalid event must not be written on the transaction")
	}
}

func TestRecordTx_PropagatesSinkError(t *testing.T) {
	sentinel := errors.New("tx insert failed")
	ts := &txRecordingSink{txErr: sentinel}
	w, _ := New(ts)
	if err := w.RecordTx(context.Background(), newTx(t), validEvent()); !errors.Is(err, sentinel) {
		t.Fatalf("want tx sink error, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// ClearanceProof + Resolved (C-07, Rule 5.3)
// -----------------------------------------------------------------------------

func TestNewClearanceProof(t *testing.T) {
	if _, err := NewClearanceProof("recon_1", false); !errors.Is(err, ErrNotCleared) {
		t.Fatalf("cleared=false: want ErrNotCleared, got %v", err)
	}
	if _, err := NewClearanceProof("", true); !errors.Is(err, ErrEmptyReconID) {
		t.Fatalf("empty recon id: want ErrEmptyReconID, got %v", err)
	}
	if _, err := NewClearanceProof("   ", true); !errors.Is(err, ErrEmptyReconID) {
		t.Fatalf("whitespace recon id: want ErrEmptyReconID, got %v", err)
	}
	p, err := NewClearanceProof("  recon_9  ", true)
	if err != nil {
		t.Fatalf("valid proof: %v", err)
	}
	if p.ReconID() != "recon_9" {
		t.Fatalf("recon id must be trimmed: got %q", p.ReconID())
	}
}

func TestResolved_FromProof_IsRecordable(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	proof, err := NewClearanceProof("recon_42", true)
	if err != nil {
		t.Fatalf("proof: %v", err)
	}
	ev := Resolved("EXT-002", "cleared by dry-run", 0.91, model.Provenance{Model: "kimi-k3"}, proof)
	if ev.Action != ActionResolved {
		t.Fatalf("action: want %s got %s", ActionResolved, ev.Action)
	}
	if ev.Provenance.ReconID != "recon_42" {
		t.Fatalf("resolved event must stamp the confirming recon id, got %q", ev.Provenance.ReconID)
	}
	if err := w.Record(context.Background(), ev); err != nil {
		t.Fatalf("Record resolved: %v", err)
	}
}

// Defense-in-depth: even a zero-value proof (no clearance) cannot persist a
// resolution, because validateEvent rejects a resolved event with no recon id.
func TestResolved_ZeroProof_Rejected(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	ev := Resolved("EXT-003", "should not persist", 0.99, model.Provenance{}, ClearanceProof{})
	if err := w.Record(context.Background(), ev); !errors.Is(err, ErrEmptyReconID) {
		t.Fatalf("want ErrEmptyReconID, got %v", err)
	}
	if len(fs.events) != 0 {
		t.Fatal("an unproven resolution must never be written (Rule 5.3)")
	}
}

// -----------------------------------------------------------------------------
// Evidence-stamping builders (M-13)
// -----------------------------------------------------------------------------

func classificationWithRule() model.BreakClassification {
	return model.BreakClassification{
		ExternalTxnID: "EXT-004",
		RootCause:     model.RootCauseAmountDrift,
		Confidence:    0.72,
		Regulated:     true,
		Rationale:     "amount differs within tolerance",
		ProposedRule: &blnk.MatchingRule{
			RuleID: "rule_amt",
			Criteria: []blnk.MatchingCriteria{
				{Field: "amount", Operator: "less_than"},
			},
		},
	}
}

func TestClassified_StampsEvidence(t *testing.T) {
	c := classificationWithRule()
	ev := Classified(c, model.Provenance{Model: "kimi-k3"})
	if ev.Action != ActionClassified || ev.Actor != ActorAgent {
		t.Fatalf("unexpected action/actor: %s/%s", ev.Action, ev.Actor)
	}
	if ev.Confidence != c.Confidence || ev.Rationale != c.Rationale {
		t.Fatal("confidence/rationale not carried")
	}
	p := ev.Provenance
	if p.RootCause != string(model.RootCauseAmountDrift) || !p.Regulated {
		t.Fatalf("root cause/regulated not stamped: %+v", p)
	}
	if p.RuleID != "rule_amt" || p.RuleField != "amount" || p.RuleOperator != "less_than" {
		t.Fatalf("rule identity not stamped: %+v", p)
	}
}

func TestClassified_NoRule(t *testing.T) {
	c := model.BreakClassification{ExternalTxnID: "EXT-005", RootCause: model.RootCauseUnknown}
	p := Classified(c, model.Provenance{}).Provenance
	if p.RuleID != "" || p.RuleField != "" || p.RuleOperator != "" {
		t.Fatalf("no rule expected, got %+v", p)
	}
	if p.RootCause != string(model.RootCauseUnknown) {
		t.Fatalf("root cause should still be stamped, got %q", p.RootCause)
	}
}

func TestClassified_RuleWithoutCriteria(t *testing.T) {
	c := model.BreakClassification{
		ExternalTxnID: "EXT-006",
		RootCause:     model.RootCauseTiming,
		ProposedRule:  &blnk.MatchingRule{RuleID: "rule_x"},
	}
	p := Classified(c, model.Provenance{}).Provenance
	if p.RuleID != "rule_x" {
		t.Fatalf("rule id should be stamped, got %q", p.RuleID)
	}
	if p.RuleField != "" || p.RuleOperator != "" {
		t.Fatalf("no criteria => no field/operator, got %+v", p)
	}
}

func TestWithClassificationEvidence_DoesNotMutateInput(t *testing.T) {
	in := model.Provenance{Model: "kimi-k3", Source: "bank-x"}
	out := WithClassificationEvidence(in, classificationWithRule())
	if in.RootCause != "" || in.Regulated || in.RuleID != "" {
		t.Fatalf("input provenance was mutated: %+v", in)
	}
	if out.Model != "kimi-k3" || out.Source != "bank-x" {
		t.Fatal("existing provenance fields must be preserved in the copy")
	}
	if out.RootCause == "" || !out.Regulated || out.RuleID == "" {
		t.Fatalf("evidence not applied to the copy: %+v", out)
	}
}

func TestRuleProposedAndCreated(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	ref := RuleRef{ID: "rule_1", Field: "reference", Operator: "equals"}

	prop := RuleProposed("EXT-007", ref, 0.8, model.Provenance{})
	if prop.Action != ActionRuleProposed {
		t.Fatalf("action: want %s got %s", ActionRuleProposed, prop.Action)
	}
	created := RuleCreated("EXT-007", ref, 0.8, model.Provenance{})
	if created.Action != ActionRuleCreated {
		t.Fatalf("action: want %s got %s", ActionRuleCreated, created.Action)
	}
	for _, ev := range []model.AuditEvent{prop, created} {
		if ev.Provenance.RuleID != "rule_1" || ev.Provenance.RuleField != "reference" || ev.Provenance.RuleOperator != "equals" {
			t.Fatalf("rule evidence not stamped: %+v", ev.Provenance)
		}
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("Record rule event: %v", err)
		}
	}
}

// TestRuleCompensated covers the finding M-11 audit builder that records the
// OUTCOME of compensating a rule the agent created for a non-clearing attempt.
// The compensated rule's id is stamped into provenance; a successful delete and
// a FAILED delete each synthesize a self-describing default rationale (the
// failure one flagging manual cleanup — the durable failure evidence M-11
// requires); and a caller-supplied rationale is preserved.
func TestRuleCompensated(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)

	// Successful deletion.
	del := RuleCompensated("EXT-011", "rule_1", true, "", 0.9, model.Provenance{Model: "kimi-k3"})
	if del.Action != ActionRuleCompensated || del.Actor != ActorAgent {
		t.Fatalf("unexpected action/actor: %+v", del)
	}
	if del.Provenance.RuleID != "rule_1" {
		t.Fatalf("compensated rule id must be stamped into provenance: %+v", del.Provenance)
	}
	if !strings.Contains(del.Rationale, "rule_1") {
		t.Fatalf("deleted default rationale should name the rule: %q", del.Rationale)
	}
	if err := w.Record(context.Background(), del); err != nil {
		t.Fatalf("Record compensated (deleted): %v", err)
	}

	// FAILED deletion — the default rationale must flag that the orphan may still
	// exist and needs manual cleanup.
	failed := RuleCompensated("EXT-012", "rule_2", false, "", 0.9, model.Provenance{Model: "kimi-k3"})
	if !strings.Contains(strings.ToLower(failed.Rationale), "manual cleanup") {
		t.Fatalf("failed default rationale must flag manual cleanup: %q", failed.Rationale)
	}
	if err := w.Record(context.Background(), failed); err != nil {
		t.Fatalf("Record compensated (failed): %v", err)
	}

	// A caller-supplied rationale is preserved verbatim.
	custom := RuleCompensated("EXT-013", "rule_3", true, "custom reason", 0.9, model.Provenance{Model: "kimi-k3"})
	if custom.Rationale != "custom reason" {
		t.Fatalf("caller rationale must be preserved, got %q", custom.Rationale)
	}
}

func TestProbed(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)

	cleared := Probed("EXT-008", "recon_c", true, model.Provenance{})
	if cleared.Action != ActionProbed || cleared.Provenance.ReconID != "recon_c" {
		t.Fatalf("cleared probe wrong: %+v", cleared)
	}
	if !strings.Contains(cleared.Rationale, "cleared") {
		t.Fatalf("cleared probe rationale: %q", cleared.Rationale)
	}
	unmatched := Probed("EXT-008", "recon_u", false, model.Provenance{})
	if !strings.Contains(unmatched.Rationale, "still unmatched") {
		t.Fatalf("unmatched probe rationale: %q", unmatched.Rationale)
	}
	for _, ev := range []model.AuditEvent{cleared, unmatched} {
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("Record probe event: %v", err)
		}
	}
}

func TestEscalated(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	prov := WithClassificationEvidence(model.Provenance{}, classificationWithRule())
	ev := Escalated("EXT-009", "regulated flow", 0.3, prov)
	if ev.Action != ActionEscalated || ev.Actor != ActorAgent {
		t.Fatalf("unexpected: %+v", ev)
	}
	if !ev.Provenance.Regulated {
		t.Fatal("escalation should carry the regulated flag when enriched")
	}
	if err := w.Record(context.Background(), ev); err != nil {
		t.Fatalf("Record escalated: %v", err)
	}
}

// -----------------------------------------------------------------------------
// HITL decisions (M-13: no arbitrary decision may pass through)
// -----------------------------------------------------------------------------

func TestDecision_ValidVerbs(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	cases := map[string]string{
		DecisionAccept:  ActionAccepted,
		DecisionReDrive: ActionReDriven,
		DecisionReject:  ActionRejected,
	}
	for verb, wantAction := range cases {
		d := model.HITLDecision{ExternalTxnID: "EXT-010", Decision: verb, Reviewer: "alice", Note: "n"}
		ev := Decision(d, model.Provenance{})
		if ev.Action != wantAction {
			t.Fatalf("%s -> want %s got %s", verb, wantAction, ev.Action)
		}
		if ev.Actor != "alice" {
			t.Fatalf("actor must be the reviewer, got %q", ev.Actor)
		}
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("Record decision %s: %v", verb, err)
		}
	}
}

func TestDecision_UnknownVerbRejected(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	d := model.HITLDecision{ExternalTxnID: "EXT-011", Decision: "approve", Reviewer: "bob"}
	ev := Decision(d, model.Provenance{})
	if ev.Action != "" {
		t.Fatalf("unknown verb must map to empty action, got %q", ev.Action)
	}
	if err := w.Record(context.Background(), ev); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("want ErrInvalidAction, got %v", err)
	}
	if len(fs.events) != 0 {
		t.Fatal("an arbitrary decision must not be recorded")
	}
}

func TestDecision_EmptyReviewerRejected(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	d := model.HITLDecision{ExternalTxnID: "EXT-012", Decision: DecisionAccept, Reviewer: ""}
	if err := w.Record(context.Background(), Decision(d, model.Provenance{})); !errors.Is(err, ErrEmptyActor) {
		t.Fatalf("want ErrEmptyActor, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// F16: the finer-grained re_drive outcome actions are DISTINCT, recordable, and
// carry the right evidence — cleared alone bears the confirming reconciliation id.
// -----------------------------------------------------------------------------

func TestReDriveOutcomeConstructors(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	d := model.HITLDecision{ExternalTxnID: "EXT-R1", Decision: DecisionReDrive, Reviewer: "alice"}

	attempted := ReDriveAttempted(d, model.Provenance{Model: "kimi-k3"})
	if attempted.Action != ActionReDriveAttempted || attempted.Actor != "alice" {
		t.Fatalf("attempted wrong: %+v", attempted)
	}
	if attempted.Provenance.ReconID != "" {
		t.Fatalf("attempted must carry no recon id, got %q", attempted.Provenance.ReconID)
	}

	cleared := ReDriveCleared(d, "recon_ok", model.Provenance{Model: "kimi-k3"})
	if cleared.Action != ActionReDriveCleared || cleared.Actor != "alice" {
		t.Fatalf("cleared wrong: %+v", cleared)
	}
	if cleared.Provenance.ReconID != "recon_ok" {
		t.Fatalf("cleared must carry the confirming recon id, got %q", cleared.Provenance.ReconID)
	}
	if !strings.Contains(cleared.Rationale, "recon_ok") {
		t.Fatalf("cleared rationale should name the confirming recon: %q", cleared.Rationale)
	}

	unmatched := ReDriveUnmatched(d, model.Provenance{Model: "kimi-k3"})
	if unmatched.Action != ActionReDriveUnmatched || unmatched.Actor != "alice" {
		t.Fatalf("unmatched wrong: %+v", unmatched)
	}
	if unmatched.Provenance.ReconID != "" {
		t.Fatalf("unmatched must carry NO recon id (m-04), got %q", unmatched.Provenance.ReconID)
	}

	failed := ReDriveFailed(d, "re_drive probe failed (Blnk upstream)", model.Provenance{Model: "kimi-k3"})
	if failed.Action != ActionReDriveFailed || failed.Actor != "alice" {
		t.Fatalf("failed wrong: %+v", failed)
	}
	if failed.Provenance.ReconID != "" {
		t.Fatalf("failed must carry no recon id, got %q", failed.Provenance.ReconID)
	}
	if !strings.Contains(strings.ToLower(failed.Rationale), "probe failed") {
		t.Fatalf("failed rationale should carry the reason: %q", failed.Rationale)
	}

	// All four are in the closed set and record cleanly.
	for _, ev := range []model.AuditEvent{attempted, cleared, unmatched, failed} {
		if err := w.Record(context.Background(), ev); err != nil {
			t.Fatalf("Record %s: %v", ev.Action, err)
		}
	}
}

// TestReDriveCleared_RequiresReconID proves a cleared re_drive is impossible to
// record without its confirming reconciliation id — the deterministic-arbiter
// proof (Rule 5.3), surfaced via the same ErrEmptyReconID sentinel as `resolved`.
func TestReDriveCleared_RequiresReconID(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	d := model.HITLDecision{ExternalTxnID: "EXT-R2", Decision: DecisionReDrive, Reviewer: "alice"}
	if err := w.Record(context.Background(), ReDriveCleared(d, "", model.Provenance{})); !errors.Is(err, ErrEmptyReconID) {
		t.Fatalf("want ErrEmptyReconID for a blank-proof re_drive_cleared, got %v", err)
	}
	if err := w.Record(context.Background(), ReDriveCleared(d, "   ", model.Provenance{})); !errors.Is(err, ErrEmptyReconID) {
		t.Fatalf("want ErrEmptyReconID for a whitespace-proof re_drive_cleared, got %v", err)
	}
	if len(fs.events) != 0 {
		t.Fatal("a proofless re_drive_cleared must not be recorded")
	}
}

// TestReDriveOutcomes_EmptyReviewerRejected proves every re_drive outcome, like
// any human-attributed decision, requires a non-empty actor (M-13).
func TestReDriveOutcomes_EmptyReviewerRejected(t *testing.T) {
	fs := &recordingSink{}
	w, _ := New(fs)
	d := model.HITLDecision{ExternalTxnID: "EXT-R3", Decision: DecisionReDrive, Reviewer: ""}
	evs := []model.AuditEvent{
		ReDriveAttempted(d, model.Provenance{}),
		ReDriveCleared(d, "recon_ok", model.Provenance{}),
		ReDriveUnmatched(d, model.Provenance{}),
		ReDriveFailed(d, "boom", model.Provenance{}),
	}
	for _, ev := range evs {
		if err := w.Record(context.Background(), ev); !errors.Is(err, ErrEmptyActor) {
			t.Fatalf("%s with empty reviewer: want ErrEmptyActor, got %v", ev.Action, err)
		}
	}
}

// TestReDriveOutcomes_ReviewerNoteBecomesRationale proves a reviewer's note is
// preserved as the rationale across the attempt/cleared/unmatched outcomes.
func TestReDriveOutcomes_ReviewerNoteBecomesRationale(t *testing.T) {
	d := model.HITLDecision{ExternalTxnID: "EXT-R4", Decision: DecisionReDrive, Reviewer: "alice", Note: "manual retry after fix"}
	for _, ev := range []model.AuditEvent{
		ReDriveAttempted(d, model.Provenance{}),
		ReDriveCleared(d, "recon_ok", model.Provenance{}),
		ReDriveUnmatched(d, model.Provenance{}),
	} {
		if ev.Rationale != "manual retry after fix" {
			t.Fatalf("%s should preserve the reviewer note, got %q", ev.Action, ev.Rationale)
		}
	}
}

// TestReDriveActions_InClosedSet proves the four new actions are members of the
// closed action set (isAllowedAction), so Record accepts them and the append-only
// DB action-domain CHECK must include them too.
func TestReDriveActions_InClosedSet(t *testing.T) {
	for _, a := range []string{ActionReDriveAttempted, ActionReDriveCleared, ActionReDriveUnmatched, ActionReDriveFailed} {
		if !isAllowedAction(a) {
			t.Fatalf("action %q must be in the closed set", a)
		}
	}
}

func TestDecisionAction(t *testing.T) {
	cases := map[string]string{
		DecisionAccept:  ActionAccepted,
		DecisionReDrive: ActionReDriven,
		DecisionReject:  ActionRejected,
		"approve":       "",
		"":              "",
	}
	for verb, want := range cases {
		if got := DecisionAction(verb); got != want {
			t.Fatalf("DecisionAction(%q): want %q got %q", verb, want, got)
		}
	}
}

func TestIsValidDecision(t *testing.T) {
	for _, verb := range []string{DecisionAccept, DecisionReDrive, DecisionReject} {
		if !IsValidDecision(verb) {
			t.Fatalf("%q should be valid", verb)
		}
	}
	for _, verb := range []string{"approve", "", "ACCEPT", "accepted"} {
		if IsValidDecision(verb) {
			t.Fatalf("%q should be invalid", verb)
		}
	}
}

// -----------------------------------------------------------------------------
// Rule 5.5: the writer exposes no mutating path, and every DB verb is an INSERT.
// -----------------------------------------------------------------------------

func TestAppendOnly_NoMutatingMethods(t *testing.T) {
	wt := reflect.TypeOf(&Writer{})
	sawRecord, sawRecordTx := false, false
	for i := 0; i < wt.NumMethod(); i++ {
		name := wt.Method(i).Name
		switch name {
		case "Record":
			sawRecord = true
		case "RecordTx":
			sawRecordTx = true
		}
		lower := strings.ToLower(name)
		for _, forbidden := range []string{"update", "delete", "remove", "mutate", "drop"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("Writer exposes a mutating method %q (violates Rule 5.5 append-only)", name)
			}
		}
	}
	if !sawRecord || !sawRecordTx {
		t.Fatal("Writer must expose Record and RecordTx")
	}

	// The persistence interfaces must each expose exactly one INSERT method.
	for _, it := range []reflect.Type{
		reflect.TypeOf((*sink)(nil)).Elem(),
		reflect.TypeOf((*TxSink)(nil)).Elem(),
	} {
		if it.NumMethod() != 1 {
			t.Fatalf("%s must expose exactly one method, has %d", it, it.NumMethod())
		}
		if m := it.Method(0).Name; !strings.HasPrefix(m, "InsertAudit") {
			t.Fatalf("%s method must be an InsertAudit variant, got %q", it, m)
		}
	}
}
