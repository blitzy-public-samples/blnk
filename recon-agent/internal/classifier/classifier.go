// Package classifier classifies reconciliation "breaks" (unmatched external
// transactions) by their root cause using an open-source model served over an
// OpenAI-compatible endpoint (Kimi K3 by default). It is deliberately the only
// package that talks to the LLM.
//
// Safety posture:
//   - Rule 5.6: inference uses ONLY github.com/sashabaranov/go-openai, targets
//     the configured LLM_MODEL (never a hardcoded literal) over LLM_BASE_URL.
//   - Rule 5.7: on LLM error/timeout after a retry cap of 2, Classify returns an
//     error wrapping ErrClassificationFailed so the remediator fails closed and
//     routes the break to HITL — never dropped, never auto-resolved.
//   - Rule 5.2: any model-proposed matching rule is validated against Blnk's
//     grammar (grammar.go) and dropped if out-of-domain, so an invalid rule can
//     never be attached or POSTed.
package classifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	openai "github.com/sashabaranov/go-openai"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// defaultMaxRetries is the retry cap mandated by Rule 5.7. A cap of 2 means up
// to 2 retries after the initial attempt (3 attempts total) before failing
// closed.
const defaultMaxRetries = 2

// defaultPerAttemptTimeout bounds a SINGLE inference attempt. go-openai's
// DefaultConfig installs an http.Client with NO Timeout, so absent an explicit
// bound a hung/slow endpoint would block Classify forever under a non-deadline
// context (e.g. context.Background()) — defeating the fail-closed guarantee
// (Rule 5.7) and the <=60s demo budget. Classify derives a per-attempt context
// deadline from this value so every attempt is self-bounded regardless of the
// caller's context; after the retry cap the break fails closed and is routed to
// HITL.
//
// It is sized (with the retry cap of 2) so a single break's worst-case
// classification under a hung endpoint — (maxRetries+1) * perAttemptTimeout =
// 3 * 15s = 45s — stays STRICTLY under cmd.pipelineTimeout (55s), so even a
// fully hung endpoint leaves headroom for the fail-closed escalation write to
// run within the same pipeline budget (findings MINOR-1 / #5: the previous 30s
// value let one break's three attempts alone consume the whole 90s pipeline
// budget). 15s remains generous for a healthy Kimi K3 classification response
// yet finite.
const defaultPerAttemptTimeout = 15 * time.Second

// defaultHTTPClientTimeout is a hard backstop on the underlying HTTP client that
// covers the entire request/response exchange (connect, TLS, headers, body). It
// is set above defaultPerAttemptTimeout so that, in normal operation, the
// per-attempt context deadline is the binding limit; the client Timeout only
// engages if the per-attempt bound is ever disabled. Together they guarantee no
// LLM call can hang unbounded even when the caller supplies context.Background().
// Kept tight (findings MINOR-1 / #5) so the backstop, too, cannot stall the
// pipeline.
const defaultHTTPClientTimeout = 20 * time.Second

// deterministicTemperature forces near-deterministic sampling for demo
// reproducibility. go-openai marshals Temperature with `omitempty`, so a literal
// 0 would be dropped and the server default (~1.0) applied; the smallest
// non-zero float32 is the documented way to request temperature ~0.
const deterministicTemperature float32 = math.SmallestNonzeroFloat32

// defaultMaxTokens hard-bounds the completion length (C-06). A triage verdict is
// a small JSON object (root cause, confidence, regulated flag, a handful of
// proposed criteria, and a short rationale); capping the response prevents a
// hostile or malfunctioning endpoint from returning an unbounded stream that
// would inflate cost/latency and complicate strict parsing. It is generous
// enough for the full multi-criteria schema yet finite.
const defaultMaxTokens = 768

// maxReplyBytes hard-bounds how many bytes of assistant content the parser will
// scan (C-06). Combined with defaultMaxTokens on the request side, this caps the
// blast radius of an endpoint that ignores the token limit and streams an
// oversized body: content beyond this bound is rejected rather than parsed.
const maxReplyBytes = 16 * 1024

// ErrClassificationFailed is the sentinel returned (wrapped) when classification
// cannot be completed within the retry cap. Callers detect it with errors.Is and
// MUST fail closed (route the break to HITL) — Rule 5.7.
var ErrClassificationFailed = errors.New("classifier: classification failed after retries")

// knownRootCauses is the accepted root-cause enumeration; anything else is
// normalized to RootCauseUnknown.
var knownRootCauses = map[model.RootCause]bool{
	model.RootCauseTiming:            true,
	model.RootCauseAmountDrift:       true,
	model.RootCauseReferenceMismatch: true,
	model.RootCauseDuplicate:         true,
	model.RootCauseMissingInternal:   true,
	model.RootCauseCurrencyMismatch:  true,
	model.RootCauseUnknown:           true,
}

// chatClient is the minimal slice of the go-openai client used here. It lets
// tests substitute a fake, while production uses *openai.Client.
type chatClient interface {
	CreateChatCompletion(ctx context.Context, req openai.ChatCompletionRequest) (openai.ChatCompletionResponse, error)
}

// Classifier performs LLM-backed break classification.
type Classifier struct {
	client      chatClient
	model       string
	temperature float32
	maxRetries  int

	// perAttemptTimeout bounds each individual inference attempt (Rule 5.7 /
	// AAP §0.1.2 fail-closed). Classify derives a context deadline from it so a
	// hung or slow endpoint cannot stall a single attempt beyond this value even
	// when the caller passes a context without a deadline. A value <= 0 disables
	// the per-attempt bound (the http.Client.Timeout backstop still applies).
	perAttemptTimeout time.Duration
}

// New builds a Classifier from configuration. The OpenAI-compatible client is
// pointed at cfg.LLMBaseURL and every request carries Model = cfg.LLMModel, so
// the model name is fully config-driven (Rule 5.6).
//
// It also installs explicit timeouts so inference can never hang unbounded
// (Rule 5.7 / AAP §0.1.2). go-openai's DefaultConfig uses an http.Client with no
// Timeout; New overrides it with a bounded client (defaultHTTPClientTimeout) as
// a hard backstop, and Classify additionally bounds every attempt with a
// per-attempt context deadline (defaultPerAttemptTimeout). Under a hung endpoint
// with a non-deadline context, Classify therefore returns after the retry cap
// and fails closed instead of blocking forever.
func New(cfg config.Config) *Classifier {
	oaCfg := openai.DefaultConfig(cfg.LLMApiKey)
	oaCfg.BaseURL = cfg.LLMBaseURL
	oaCfg.HTTPClient = &http.Client{Timeout: defaultHTTPClientTimeout}
	return &Classifier{
		client:            openai.NewClientWithConfig(oaCfg),
		model:             cfg.LLMModel,
		temperature:       deterministicTemperature,
		maxRetries:        defaultMaxRetries,
		perAttemptTimeout: defaultPerAttemptTimeout,
	}
}

// rawCriterion is the on-the-wire shape of ONE proposed matching criterion
// (M-08). A safe Blnk-native rule is generally multi-dimensional — e.g. pin
// reference+currency AND allow a bounded amount drift — so the schema models a
// LIST of criteria, each carrying the optional Pattern / AllowableDrift fields
// Blnk's MatchingCriteria supports. Field/Operator are grammar-validated
// (Rule 5.2) before the rule is attached.
type rawCriterion struct {
	Field          string  `json:"field"`
	Operator       string  `json:"operator"`
	Value          string  `json:"value"`
	Pattern        string  `json:"pattern"`
	AllowableDrift float64 `json:"allowable_drift"`
}

// rawProposedRule is the on-the-wire shape the model emits for a proposed rule:
// a complete, multi-criteria representation rather than a single field/operator.
type rawProposedRule struct {
	Criteria []rawCriterion `json:"criteria"`
}

// rawClassification is the intermediate JSON shape parsed from the model reply
// before it is normalized into model.BreakClassification. The struct is decoded
// with DisallowUnknownFields (M-08): any field the model invents outside this
// closed schema is treated as ambiguous output and rejected (fail closed).
type rawClassification struct {
	RootCause    string           `json:"root_cause"`
	Confidence   float64          `json:"confidence"`
	Regulated    bool             `json:"regulated"`
	ProposedRule *rawProposedRule `json:"proposed_rule,omitempty"`
	Rationale    string           `json:"rationale"`
}

// Classify determines the root cause of a single break. On success it returns a
// fully-normalized BreakClassification. On failure (transport error, empty
// response, or unparseable reply) it retries up to the cap and then returns a
// BreakClassification stub (ID set, RootCauseUnknown, zero confidence) together
// with an error wrapping ErrClassificationFailed so the caller fails closed.
func (c *Classifier) Classify(ctx context.Context, txn blnk.ExternalTransaction) (model.BreakClassification, error) {
	req := openai.ChatCompletionRequest{
		Model:       c.model,
		Temperature: c.temperature,
		// C-06: constrain the reply to a single JSON object and bound its length.
		// ResponseFormat asks the OpenAI-compatible endpoint to emit strict JSON
		// (no prose/Markdown), which pairs with the strict decoder in
		// parseClassification; MaxTokens caps the completion so a hostile or
		// malfunctioning endpoint cannot stream an unbounded body.
		MaxTokens:      defaultMaxTokens,
		ResponseFormat: &openai.ChatCompletionResponseFormat{Type: openai.ChatCompletionResponseFormatTypeJSONObject},
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleSystem, Content: systemPrompt},
			{Role: openai.ChatMessageRoleUser, Content: buildUserPrompt(txn)},
		},
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		resp, err := c.createChatCompletion(ctx, req)
		if err != nil {
			lastErr = err
			// If the CALLER's context is done (cancelled or its own deadline
			// exceeded), stop retrying: the caller has given up, so further
			// attempts would only waste the retry budget and delay the
			// fail-closed return. A per-attempt timeout firing does NOT mark the
			// caller's context done (only the derived attempt context), so a
			// genuinely slow or hung endpoint still retries up to the cap before
			// failing closed (Rule 5.7).
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if len(resp.Choices) == 0 {
			lastErr = errors.New("empty choices in chat completion response")
			continue
		}
		bc, err := parseClassification(resp.Choices[0].Message.Content, txn)
		if err != nil {
			lastErr = err
			continue
		}
		return bc, nil
	}

	stub := model.BreakClassification{
		ExternalTxnID: txn.ID,
		RootCause:     model.RootCauseUnknown,
		Confidence:    0,
	}
	return stub, fmt.Errorf("%w: %v", ErrClassificationFailed, lastErr)
}

// createChatCompletion issues a single inference attempt bounded by an explicit
// per-attempt deadline derived from ctx (Rule 5.7 / AAP §0.1.2). This guarantees
// a hung or slow endpoint cannot block one attempt beyond perAttemptTimeout even
// when the caller supplies a context without a deadline. When the caller's
// context already carries an earlier deadline, that earlier deadline wins because
// context.WithTimeout takes the minimum of the two. A perAttemptTimeout <= 0
// disables the per-attempt bound (the client-level Timeout backstop still
// applies). The derived context is always cancelled before returning so no timer
// leaks between attempts.
func (c *Classifier) createChatCompletion(ctx context.Context, req openai.ChatCompletionRequest) (openai.ChatCompletionResponse, error) {
	if c.perAttemptTimeout <= 0 {
		return c.client.CreateChatCompletion(ctx, req)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, c.perAttemptTimeout)
	defer cancel()
	return c.client.CreateChatCompletion(attemptCtx, req)
}

// parseClassification strictly decodes a raw model reply into a
// BreakClassification. The external transaction id is taken authoritatively from
// txn (never from the model). Any proposed rule is grammar-validated (Rule 5.2)
// and dropped if it falls outside Blnk's domain.
//
// Parsing is deliberately STRICT (M-08 / C-06). The reply is size-capped, a
// single well-formed Markdown code fence is stripped, and the JSON is decoded
// with DisallowUnknownFields and a no-trailing-content check. Anything the model
// emits that is not exactly one object of the closed schema — extra fields,
// trailing prose, multiple objects, non-JSON — is a parse error, which makes
// Classify retry and ultimately fail closed to HITL (Rule 5.7). Confidence is
// REJECTED (not clamped) when it is not a finite probability in [0,1] (M-09):
// promoting a malformed/over-range value would let it clear the Rule 5.4 gate,
// so a malformed confidence must instead route the break to a human.
func parseClassification(content string, txn blnk.ExternalTransaction) (model.BreakClassification, error) {
	if len(content) > maxReplyBytes {
		return model.BreakClassification{}, fmt.Errorf("model reply exceeds %d-byte cap (%d bytes)", maxReplyBytes, len(content))
	}

	raw, err := decodeStrictClassification(content)
	if err != nil {
		return model.BreakClassification{}, err
	}

	// M-09: reject non-finite / out-of-range confidence outright. Failing the
	// parse here routes the break to HITL (fail closed) instead of silently
	// promoting an over-range value to an auto-eligible one.
	if !isFiniteProbability(raw.Confidence) {
		return model.BreakClassification{}, fmt.Errorf(
			"confidence %v is not a finite probability in [0,1]; failing closed", raw.Confidence)
	}

	bc := model.BreakClassification{
		ExternalTxnID: txn.ID,
		RootCause:     normalizeRootCause(raw.RootCause),
		Confidence:    raw.Confidence,
		Regulated:     raw.Regulated,
		Rationale:     strings.TrimSpace(raw.Rationale),
	}

	if raw.ProposedRule != nil && len(raw.ProposedRule.Criteria) > 0 {
		criteria := make([]blnk.MatchingCriteria, 0, len(raw.ProposedRule.Criteria))
		for _, rc := range raw.ProposedRule.Criteria {
			criteria = append(criteria, blnk.MatchingCriteria{
				Field:          strings.ToLower(strings.TrimSpace(rc.Field)),
				Operator:       strings.ToLower(strings.TrimSpace(rc.Operator)),
				Value:          rc.Value,
				Pattern:        rc.Pattern,
				AllowableDrift: rc.AllowableDrift,
			})
		}
		rule := blnk.MatchingRule{
			Name:        fmt.Sprintf("agent-proposed-%s", txn.ID),
			Description: "agent-proposed matching rule",
			Criteria:    criteria,
		}
		// Rule 5.2: attach only if EVERY criterion conforms to Blnk's grammar;
		// otherwise the out-of-grammar rule is dropped and never leaves the agent.
		// The remediator additionally replaces this advisory rule with a
		// deterministic, root-cause-specific safe profile before any POST (C-06).
		if err := ValidateRule(rule); err == nil {
			bc.ProposedRule = &rule
		}
	}

	return bc, nil
}

// decodeStrictClassification strips a single surrounding Markdown code fence (a
// benign, deterministic wrapper some endpoints add) and then decodes the reply
// as exactly one JSON object of the closed rawClassification schema. It uses
// DisallowUnknownFields to reject invented fields and verifies there is no
// trailing content after the object. It replaces the previous tolerant
// first-'{'-to-last-'}' extraction, which silently accepted objects buried in
// arbitrary surrounding prose (M-08).
func decodeStrictClassification(content string) (rawClassification, error) {
	s := stripCodeFence(strings.TrimSpace(content))
	if s == "" {
		return rawClassification{}, errors.New("empty model reply")
	}

	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()

	var raw rawClassification
	if err := dec.Decode(&raw); err != nil {
		return rawClassification{}, fmt.Errorf("strict-decode classification: %w", err)
	}
	// Reject any trailing tokens (a second object, prose, etc.): a compliant
	// reply is exactly one JSON object.
	if _, err := dec.Token(); err != io.EOF {
		return rawClassification{}, errors.New("unexpected trailing content after JSON object")
	}
	return raw, nil
}

// stripCodeFence removes a single pair of surrounding triple-backtick Markdown
// fences (optionally tagged, e.g. ```json) when present, returning the inner
// content. When no complete fence is present the input is returned unchanged.
// This is intentionally narrow: it only unwraps a clean fenced block and never
// hunts for JSON embedded in free prose.
func stripCodeFence(s string) string {
	if !strings.HasPrefix(s, "```") {
		return s
	}
	// Drop the opening fence line (the backticks plus any language tag such as
	// "json"), then trim everything from the closing fence onward.
	nl := strings.IndexByte(s, '\n')
	if nl < 0 {
		return s
	}
	inner := s[nl+1:]
	if i := strings.LastIndex(inner, "```"); i >= 0 {
		inner = inner[:i]
	}
	return strings.TrimSpace(inner)
}

// normalizeRootCause lower-cases and trims the model's label and maps it to a
// known enum member, defaulting to RootCauseUnknown.
func normalizeRootCause(s string) model.RootCause {
	rc := model.RootCause(strings.ToLower(strings.TrimSpace(s)))
	if knownRootCauses[rc] {
		return rc
	}
	return model.RootCauseUnknown
}

// isFiniteProbability reports whether v is a finite real number in the closed
// interval [0,1]. It is the strict confidence predicate used to REJECT (M-09) —
// rather than clamp — a malformed model confidence. A value that fails this test
// (NaN, ±Inf, negative, a percentage like 95, or a slightly-over-one artifact)
// makes parseClassification fail closed so the break is routed to HITL and can
// never satisfy the Rule 5.4 auto-remediation gate.
func isFiniteProbability(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 1
}

// truncate shortens s to at most n runes for safe inclusion in error messages.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
