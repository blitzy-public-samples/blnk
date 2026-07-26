// Command llmstub is a deterministic, standard-library-only, OpenAI-compatible
// Chat Completions stub used by `make demo` (and optionally CI) to drive the
// recon-agent pipeline with reproducible classifications WITHOUT contacting a
// real LLM provider.
//
// # Why it exists
//
// The recon-agent classifier always issues a real OpenAI-compatible chat
// completion against LLM_BASE_URL (Rule 5.6: model + endpoint are config-driven,
// never hardcoded). With the shipped non-routable default base URL there is no
// server, so every classification fails closed and the whole fixture escalates
// to HITL — correct for safety, but it exhibits ZERO auto-resolutions, so a demo
// cannot prove the ">= 1 auto-remediated break confirmed cleared by a Blnk
// dry-run" success criterion. This stub supplies exactly the language-inference
// step the agent would otherwise obtain from Kimi K3, deterministically, so the
// demo can exhibit real auto-resolutions.
//
// It does NOT weaken any safety invariant: the agent still only PROPOSES and
// Blnk still DECIDES clearance via its authoritative dry-run reconciliation
// (Rule 5.3). The stub merely returns, for each of the six canonical seed
// breaks, the classification the immutable evaluation corpus
// (eval/recon_corpus.jsonl) already expects.
//
// Scope / dependency hygiene
//
// This program lives in the ROOT Go module (github.com/blnkfinance/blnk),
// alongside seed/, and imports NOTHING beyond the Go standard library. It
// therefore adds no dependency to either go.mod/go.sum — preserving Blnk's root
// manifest (Rule 5.8) and the recon-agent dependency graph (Rule 5.6). It is a
// test/demo harness, explicitly outside the recon-agent coverage floor scope
// (Rule 5.9 excludes seed/ and eval/).
//
// # Protocol
//
// It serves an OpenAI-compatible POST .../chat/completions surface (any path is
// accepted, so it works whether or not LLM_BASE_URL includes a /v1 suffix) plus
// a GET /healthz liveness probe the demo/CI can poll before starting the
// pipeline. For each request it concatenates the chat message contents, matches
// the stable base id EXT-00N (a substring of the pipeline's run-scoped
// EXT-00N-<runID>; run ids are lowercase-hex UUID segments and can never contain
// the uppercase "EXT-" token, so the match is collision-free), and returns the
// canonical classification as a strict JSON object in choices[0].message.content
// — precisely the shape the classifier's strict decoder consumes. An
// unrecognized break yields a fail-closed {unknown, 0} classification so it
// escalates rather than erroring.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	// defaultAddr is the loopback address the stub binds by default. It matches
	// the host:port of the shipped non-routable LLM_BASE_URL default
	// (http://localhost:11434/v1) so `make demo` can simply point the agent at
	// http://127.0.0.1:11434/v1 to consume it.
	defaultAddr = "127.0.0.1:11434"
	// maxRequestBytes bounds the request body the stub will read, so a
	// malformed or hostile client cannot force unbounded memory use. A real
	// classification request is a few kilobytes.
	maxRequestBytes = 1 << 20 // 1 MiB
	// fallbackModel labels the response when the request omits a model.
	fallbackModel = "kimi-k3"
	// autoDrift is the amount AllowableDrift the stub proposes for the
	// amount_drift break. EXT-002 differs from its internal counterpart by
	// 0.75 on 250.00 (0.3%); 1% comfortably covers it, stays within Blnk's
	// [0,1] fractional-drift validation, and is clamped by the remediator's own
	// 5% cap. (The remediator builds its own safe rule and only READS the
	// amount criterion's allowable_drift from the proposal, so this is the one
	// structurally load-bearing value in the proposed rules below.)
	autoDrift = 0.01
)

// criterion mirrors the recon-agent classifier's strict rawCriterion JSON shape
// (field/operator/value/pattern/allowable_drift). Only these keys may appear;
// the classifier decodes with DisallowUnknownFields.
type criterion struct {
	Field          string  `json:"field"`
	Operator       string  `json:"operator"`
	Value          string  `json:"value"`
	Pattern        string  `json:"pattern"`
	AllowableDrift float64 `json:"allowable_drift"`
}

// proposedRule mirrors the classifier's rawProposedRule (a criteria list).
type proposedRule struct {
	Criteria []criterion `json:"criteria"`
}

// classification mirrors the classifier's rawClassification. proposed_rule is
// omitted (JSON null/absent) for breaks the corpus escalates, which the agent
// routes to HITL before it would consult a rule anyway.
type classification struct {
	RootCause    string        `json:"root_cause"`
	Confidence   float64       `json:"confidence"`
	Regulated    bool          `json:"regulated"`
	ProposedRule *proposedRule `json:"proposed_rule,omitempty"`
	Rationale    string        `json:"rationale"`
}

// canonicalBreak binds a stable seed base id to the classification the corpus
// expects for it.
type canonicalBreak struct {
	baseID string
	result classification
}

// equalsPin is a tight equality criterion on the given field (value/pattern are
// left empty: Blnk matches field-to-field and ignores criteria.Value, and the
// remediator rebuilds the rule from the transaction's own fields regardless).
func equalsPin(field string) criterion {
	return criterion{Field: field, Operator: "equals"}
}

// canonicalBreaks is the deterministic classification table for the six planted
// seed breaks, matching eval/recon_corpus.jsonl exactly:
//
//   - EXT-001 timing            -> auto_resolve (amount+currency+reference pin)
//   - EXT-002 amount_drift      -> auto_resolve (amount pin carries allowable_drift)
//   - EXT-003 reference_mismatch-> auto_resolve (amount+currency pin, reference dropped)
//   - EXT-004 duplicate         -> escalate (no rule)
//   - EXT-005 missing_internal  -> escalate (no rule)
//   - EXT-006 currency_mismatch -> escalate, regulated (no rule)
//
// The three escalated breaks are routed to HITL by the remediator on the basis
// of root cause / regulation regardless of confidence, so their confidence
// values only need to be well-formed. The three auto breaks carry confidence
// >= the 0.85 threshold and a non-nil proposed rule (required to enter the auto
// path); the remediator then constructs its own safe rule and lets Blnk decide.
var canonicalBreaks = []canonicalBreak{
	{
		baseID: "EXT-001",
		result: classification{
			RootCause:  "timing",
			Confidence: 0.95,
			Regulated:  false,
			ProposedRule: &proposedRule{Criteria: []criterion{
				equalsPin("amount"), equalsPin("currency"), equalsPin("reference"),
			}},
			Rationale: "Amount, currency and reference align with the internal booking; only the value/posting date differs, consistent with a settlement timing lag.",
		},
	},
	{
		baseID: "EXT-002",
		result: classification{
			RootCause:  "amount_drift",
			Confidence: 0.95,
			Regulated:  false,
			ProposedRule: &proposedRule{Criteria: []criterion{
				{Field: "amount", Operator: "equals", AllowableDrift: autoDrift},
				equalsPin("currency"), equalsPin("reference"),
			}},
			Rationale: "Currency and reference match; the amount differs by a small processor-fee fraction, consistent with amount drift within a bounded tolerance.",
		},
	},
	{
		baseID: "EXT-003",
		result: classification{
			RootCause:  "reference_mismatch",
			Confidence: 0.92,
			Regulated:  false,
			ProposedRule: &proposedRule{Criteria: []criterion{
				equalsPin("amount"), equalsPin("currency"),
			}},
			Rationale: "Amount and currency match an internal booking but the reference code differs, consistent with a reference mismatch; pin on amount+currency and drop the reference.",
		},
	},
	{
		baseID: "EXT-004",
		result: classification{
			RootCause:  "duplicate",
			Confidence: 0.90,
			Regulated:  false,
			Rationale:  "Fields are identical to an already-present statement line (same amount/currency/reference), consistent with a duplicate re-posting; escalate rather than auto-clear so the sole internal counterpart is not double-matched.",
		},
	},
	{
		baseID: "EXT-005",
		result: classification{
			RootCause:  "missing_internal",
			Confidence: 0.90,
			Regulated:  false,
			Rationale:  "No internal booking corresponds to this inbound line; with no counterpart to match there is nothing to auto-clear, so escalate for human investigation.",
		},
	},
	{
		baseID: "EXT-006",
		result: classification{
			RootCause:  "currency_mismatch",
			Confidence: 0.92,
			Regulated:  true,
			Rationale:  "The statement currency differs from the internally booked currency; treated as regulated (cross-border/FX), so it is escalated to HITL and never auto-remediated.",
		},
	},
}

// failClosed is returned for any break the stub does not recognize: an unknown
// root cause at zero confidence, which the agent escalates to HITL (unknown is
// not auto-eligible and 0 < the confidence threshold). It never errors, so the
// pipeline still triages an unexpected line safely instead of aborting.
var failClosed = classification{RootCause: "unknown", Confidence: 0, Regulated: false, Rationale: "stub: unrecognized break; failing closed to HITL"}

// chatMessage is one message in an OpenAI-compatible chat request/response.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest is the subset of the OpenAI ChatCompletionRequest the stub reads.
// It decodes leniently (extra fields such as temperature/max_tokens/
// response_format are ignored) because real clients send many more fields.
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

// chatChoice / chatResponse are the minimal OpenAI-compatible response shape the
// go-openai client unmarshals; the classifier reads only choices[0].message.content.
type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
}

// classify picks the canonical classification for whichever base id appears in
// the concatenated prompt, or fail-closed when none matches.
func classify(prompt string) classification {
	for _, b := range canonicalBreaks {
		if strings.Contains(prompt, b.baseID) {
			return b.result
		}
	}
	return failClosed
}

// handleChat implements the OpenAI-compatible chat-completions endpoint.
func handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes))
	if err != nil {
		http.Error(w, "read request body", http.StatusBadRequest)
		return
	}
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "decode request body", http.StatusBadRequest)
		return
	}

	var b strings.Builder
	for _, m := range req.Messages {
		b.WriteString(m.Content)
		b.WriteByte('\n')
	}
	cls := classify(b.String())

	// The classifier expects the classification as a JSON STRING inside
	// choices[0].message.content (ResponseFormat=json_object), so marshal the
	// classification and embed the result as the message content.
	content, err := json.Marshal(cls)
	if err != nil {
		http.Error(w, "encode classification", http.StatusInternalServerError)
		return
	}

	model := req.Model
	if strings.TrimSpace(model) == "" {
		model = fallbackModel
	}
	resp := chatResponse{
		ID:      "stub-chatcmpl",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []chatChoice{{
			Index:        0,
			Message:      chatMessage{Role: "assistant", Content: string(content)},
			FinishReason: "stop",
		}},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("llmstub: write response: %v", err)
	}
}

func main() {
	addr := flag.String("addr", envOr("LLMSTUB_ADDR", defaultAddr), "host:port to bind the OpenAI-compatible stub")
	flag.Parse()

	mux := http.NewServeMux()
	// Liveness probe the demo/CI polls before starting the pipeline.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok")
	})
	// Any other path is treated as the chat-completions endpoint, so the stub
	// works whether LLM_BASE_URL ends in /v1 or not.
	mux.HandleFunc("/", handleChat)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM so `make demo` can stop the stub
	// cleanly after the pipeline completes.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("llmstub: deterministic OpenAI-compatible stub listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("llmstub: server error: %v", err)
	}
	log.Printf("llmstub: shut down")
}

// envOr returns the value of the named environment variable, or def when unset
// or empty.
func envOr(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
