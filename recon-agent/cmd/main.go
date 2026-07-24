// Command recon-agent is the executable entrypoint for the reconciliation
// sidecar. It composes the runtime object graph, drives the break-triage
// pipeline once, and then either exits (one-shot mode) or blocks serving the
// human-in-the-loop (HITL) API and status page (serve mode).
//
// Modes are selected by the -once flag:
//
//	-once=false (default) SERVE mode: run the pipeline once, then start and
//	                      block on the HITL/status server on HITL_PORT. This is
//	                      what the docker-compose recon-agent service runs.
//	-once=true            ONE-SHOT mode: run the pipeline once, print the
//	                      summary table, and exit. This is what `make demo`
//	                      (go run ./cmd -once) invokes to stay within the
//	                      <= 60 second budget.
//
// The agent reaches Blnk exclusively over HTTP through internal/blnk; this file
// imports only recon-agent's own internal packages plus the standard library
// and never any Blnk internal package (Rule 5.1).
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/hitl"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/remediator"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Default filesystem locations for the seed corpus and the evaluation log. Both
// are resolved relative to the module directory (recon-agent/), which is the
// working directory used by `make demo` (cd recon-agent && go run ./cmd -once).
// In the container image the seed/ and eval/ directories are absent, so both
// paths are handled gracefully when missing.
const (
	defaultCSVPath  = "../seed/external_transactions.csv"
	defaultEvalPath = "../eval/recon_corpus.jsonl"
	defaultSource   = "seed-bank"
)

// statusAutoResolved mirrors the terminal status the remediator writes to
// agent_break when a break is deterministically cleared by a Blnk dry-run. It
// is duplicated here (rather than imported) to keep main decoupled from the
// remediator's unexported constants.
const statusAutoResolved = "auto-resolved"

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	once := flag.Bool("once", false, "run the pipeline once, print the summary table, and exit (used by `make demo`); when false, serve the HITL API and status page")
	csvPath := flag.String("csv", defaultCSVPath, "path to the external-transactions CSV to ingest")
	evalPath := flag.String("eval", defaultEvalPath, "path to the JSONL evaluation corpus to append resolved breaks to")
	source := flag.String("source", defaultSource, "source label attached to ingested external transactions")
	flag.Parse()

	if err := run(*once, *csvPath, *evalPath, *source); err != nil {
		log.Fatalf("recon-agent: fatal: %v", err)
	}
}

// run composes the object graph (Gate 9: every component reachable from main),
// executes the pipeline once, prints the summary, and then either returns
// (one-shot) or blocks serving the HITL surface (serve mode).
func run(once bool, csvPath, evalPath, source string) error {
	ctx := context.Background()

	// 1. Configuration — the single write-site consumer of the config struct
	//    (Gate 12). Every field below is read on a path reachable from here.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// 2. Persistence: open the agent store and apply the additive agent schema
	//    (CREATE SCHEMA agent + agent_break/agent_audit/agent_hitl_queue).
	st, err := store.New(cfg.AgentDatabaseURL)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Printf("recon-agent: closing store: %v", cerr)
		}
	}()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate store: %w", err)
	}

	// 3. Wire the remaining components in dependency order. audit.New returns an
	//    error (it rejects a nil sink); *store.Store is a valid, non-nil sink.
	auditWriter, err := audit.New(st)
	if err != nil {
		return fmt.Errorf("init audit writer: %w", err)
	}
	blnkClient := blnk.NewClient(cfg.BlnkBaseURL, cfg.BlnkApiKey)
	cls := classifier.New(cfg)
	rem := remediator.New(cls, blnkClient, auditWriter, st, cfg.ConfAutoThreshold, cfg.LLMModel)
	srv := hitl.NewServer(cfg, st, auditWriter, blnkClient)

	// 4. Drive the triage pipeline once and print the summary table.
	s := runPipeline(ctx, blnkClient, rem, st, csvPath, evalPath, source)
	printSummary(os.Stdout, s)

	// 5. One-shot mode exits here; serve mode blocks on the HITL server.
	if once {
		return nil
	}

	addr := ":" + cfg.HitlPort
	log.Printf("recon-agent: serving HITL API and status page on %s", addr)
	return srv.Run(addr)
}

// summary captures the demo scorecard printed after the pipeline runs.
type summary struct {
	breaksIn     int
	autoResolved int
	escalated    int
	auditCount   int
}

// runPipeline ingests the external statement, derives the set of breaks, hands
// each break to the remediator (which classifies, gates, and either
// auto-remediates via a Blnk dry-run or escalates to HITL), appends resolved
// breaks to the evaluation corpus, and returns the summary counts.
func runPipeline(ctx context.Context, blnkClient *blnk.Client, rem *remediator.Remediator, st *store.Store, csvPath, evalPath, source string) summary {
	txns, err := loadExternalTransactions(csvPath, source)
	if err != nil {
		log.Printf("recon-agent: could not load external transactions from %q: %v (continuing with zero breaks)", csvPath, err)
		txns = nil
	}
	log.Printf("recon-agent: loaded %d external transaction(s) from %q", len(txns), csvPath)

	txnByID := make(map[string]blnk.ExternalTransaction, len(txns))
	for _, t := range txns {
		txnByID[t.ID] = t
	}

	breaks := deriveBreaks(ctx, blnkClient, txns)
	log.Printf("recon-agent: derived %d break(s) to triage", len(breaks))

	// The recon-agent triages a locally-loaded seed statement rather than
	// issuing its own Blnk upload, so there is no Blnk-assigned upload id. A
	// single run-scoped batch id is synthesized for audit provenance: the
	// remediator stamps it onto every AuditEvent a break emits so the
	// append-only trail attributes each action to this run and per-run
	// summaries scope correctly.
	uploadID := fmt.Sprintf("recon-run-%s-%d", source, time.Now().UTC().Unix())

	// Each break is triaged independently; a per-break agent-side infra failure
	// is logged and skipped so one bad break never aborts the run.
	for _, txn := range breaks {
		if err := rem.Handle(ctx, txn, uploadID); err != nil {
			log.Printf("recon-agent: remediator failed for break %q: %v", txn.ID, err)
			continue
		}
	}

	// Tally results from the store (the durable source of truth).
	breakRows, err := st.ListBreaks(ctx)
	if err != nil {
		log.Printf("recon-agent: list breaks: %v", err)
	}
	autoResolved := 0
	for _, b := range breakRows {
		if b.Status == statusAutoResolved {
			autoResolved++
		}
	}

	// Append every resolved break to the evaluation corpus (best-effort).
	if err := appendEvalRecords(evalPath, breakRows, txnByID); err != nil {
		log.Printf("recon-agent: could not append eval records to %q: %v", evalPath, err)
	}

	hitlRows, err := st.ListHITL(ctx)
	if err != nil {
		log.Printf("recon-agent: list hitl queue: %v", err)
	}
	auditRows, err := st.ListAudit(ctx)
	if err != nil {
		log.Printf("recon-agent: list audit trail: %v", err)
	}

	return summary{
		breaksIn:     len(breaks),
		autoResolved: autoResolved,
		escalated:    len(hitlRows),
		auditCount:   len(auditRows),
	}
}

// deriveBreaks determines, per external transaction, whether it is an unmatched
// "break" using Blnk as the deterministic arbiter (Rule 5.3). It submits each
// transaction through the single-txn dry-run probe; a row is treated as matched
// (skipped) ONLY when the probe returns without error AND reports it cleared.
// Any probe error or a not-cleared result yields a break — fail-closed, so no
// break is ever silently dropped.
func deriveBreaks(ctx context.Context, blnkClient *blnk.Client, txns []blnk.ExternalTransaction) []blnk.ExternalTransaction {
	breaks := make([]blnk.ExternalTransaction, 0, len(txns))
	for _, txn := range txns {
		cleared, _, err := blnkClient.ProbeBreak(ctx, txn, nil)
		if err != nil {
			log.Printf("recon-agent: probe for %q did not confirm a match, treating as a break: %v", txn.ID, err)
		}
		if err == nil && cleared {
			continue
		}
		breaks = append(breaks, txn)
	}
	return breaks
}

// loadExternalTransactions parses the seed CSV. The header is matched
// case-insensitively and expects the columns dictated by Blnk's external-file
// contract: ID, Amount, Currency, Reference, Description, Date. The source is
// supplied by the caller (the CSV carries no source column). A missing file is
// surfaced as an error for the caller to handle gracefully.
func loadExternalTransactions(path, source string) ([]blnk.ExternalTransaction, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %w", err)
	}
	if len(records) < 2 {
		return nil, nil
	}

	col := make(map[string]int, len(records[0]))
	for i, h := range records[0] {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	field := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}

	txns := make([]blnk.ExternalTransaction, 0, len(records)-1)
	for _, row := range records[1:] {
		id := field(row, "id")
		if id == "" {
			continue
		}
		amount, aerr := strconv.ParseFloat(field(row, "amount"), 64)
		if aerr != nil {
			log.Printf("recon-agent: row %q has an unparseable amount %q: %v", id, field(row, "amount"), aerr)
		}
		txns = append(txns, blnk.ExternalTransaction{
			ID:          id,
			Amount:      amount,
			Currency:    field(row, "currency"),
			Reference:   field(row, "reference"),
			Description: field(row, "description"),
			Date:        parseDate(field(row, "date")),
			Source:      source,
		})
	}
	return txns, nil
}

// parseDate accepts RFC3339 timestamps and falls back to a bare calendar date;
// an unparseable value yields the zero time (date is not used for matching).
func parseDate(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// evalRecord is one JSONL line in eval/recon_corpus.jsonl, using the required
// five top-level keys: id, scenario, input, expected_output, judging_criteria.
type evalRecord struct {
	ID              string       `json:"id"`
	Scenario        string       `json:"scenario"`
	Input           evalInput    `json:"input"`
	ExpectedOutput  evalExpected `json:"expected_output"`
	JudgingCriteria string       `json:"judging_criteria"`
}

type evalInput struct {
	ExternalTransaction blnk.ExternalTransaction `json:"external_transaction"`
	InternalCandidate   any                      `json:"internal_candidate"`
	ContextNote         string                   `json:"context_note"`
}

type evalExpected struct {
	RootCause      model.RootCause    `json:"root_cause"`
	Regulated      bool               `json:"regulated"`
	ExpectedAction string             `json:"expected_action"`
	ProposedRule   *blnk.MatchingRule `json:"proposed_rule"`
}

// appendEvalRecords appends one JSONL record for each auto-resolved break. It
// is best-effort: any I/O error is returned for the caller to log, never fatal.
func appendEvalRecords(path string, breaks []store.Break, txnByID map[string]blnk.ExternalTransaction) error {
	resolved := make([]store.Break, 0, len(breaks))
	for _, b := range breaks {
		if b.Status == statusAutoResolved {
			resolved = append(resolved, b)
		}
	}
	if len(resolved) == 0 {
		return nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, b := range resolved {
		c := b.Classification
		rec := evalRecord{
			ID:       c.ExternalTxnID,
			Scenario: fmt.Sprintf("auto-remediated %s break", c.RootCause),
			Input: evalInput{
				ExternalTransaction: txnByID[c.ExternalTxnID],
				InternalCandidate:   nil,
				ContextNote:         c.Rationale,
			},
			ExpectedOutput: evalExpected{
				RootCause:      c.RootCause,
				Regulated:      c.Regulated,
				ExpectedAction: "auto_remediate",
				ProposedRule:   c.ProposedRule,
			},
			JudgingCriteria: "root_cause label matches and a Blnk dry-run reconciliation confirms the external transaction left the unmatched set",
		}
		if err := enc.Encode(&rec); err != nil {
			return err
		}
	}
	return nil
}

// printSummary renders the demo scorecard: breaks in / auto-resolved /
// escalated / audit count.
func printSummary(w io.Writer, s summary) {
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "=== recon-agent pipeline summary ===")
	fmt.Fprintf(w, "  %-16s %d\n", "breaks in", s.breaksIn)
	fmt.Fprintf(w, "  %-16s %d\n", "auto-resolved", s.autoResolved)
	fmt.Fprintf(w, "  %-16s %d\n", "escalated", s.escalated)
	fmt.Fprintf(w, "  %-16s %d\n", "audit events", s.auditCount)
	fmt.Fprintln(w, "====================================")
}
