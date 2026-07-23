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
	"math"
	"strings"

	openai "github.com/sashabaranov/go-openai"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// defaultMaxRetries is the retry cap mandated by Rule 5.7. A cap of 2 means up
// to 2 retries after the initial attempt (3 attempts total) before failing
// closed.
const defaultMaxRetries = 2

// deterministicTemperature forces near-deterministic sampling for demo
// reproducibility. go-openai marshals Temperature with `omitempty`, so a literal
// 0 would be dropped and the server default (~1.0) applied; the smallest
// non-zero float32 is the documented way to request temperature ~0.
const deterministicTemperature float32 = math.SmallestNonzeroFloat32

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
}

// New builds a Classifier from configuration. The OpenAI-compatible client is
// pointed at cfg.LLMBaseURL and every request carries Model = cfg.LLMModel, so
// the model name is fully config-driven (Rule 5.6).
func New(cfg config.Config) *Classifier {
	oaCfg := openai.DefaultConfig(cfg.LLMApiKey)
	oaCfg.BaseURL = cfg.LLMBaseURL
	return &Classifier{
		client:      openai.NewClientWithConfig(oaCfg),
		model:       cfg.LLMModel,
		temperature: deterministicTemperature,
		maxRetries:  defaultMaxRetries,
	}
}

// rawProposedRule is the on-the-wire shape the model emits for a proposed rule.
type rawProposedRule struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// rawClassification is the intermediate JSON shape parsed from the model reply
// before it is normalized into model.BreakClassification.
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
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleSystem, Content: systemPrompt},
			{Role: openai.ChatMessageRoleUser, Content: buildUserPrompt(txn)},
		},
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		resp, err := c.client.CreateChatCompletion(ctx, req)
		if err != nil {
			lastErr = err
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

// parseClassification extracts the JSON object from a raw model reply and
// normalizes it into a BreakClassification. The external transaction id is taken
// authoritatively from txn (never from the model). Any proposed rule is
// grammar-validated (Rule 5.2) and dropped if it falls outside Blnk's domain.
func parseClassification(content string, txn blnk.ExternalTransaction) (model.BreakClassification, error) {
	jsonStr := extractJSONObject(content)
	if jsonStr == "" {
		return model.BreakClassification{}, fmt.Errorf("no JSON object found in reply: %q", truncate(content, 120))
	}

	var raw rawClassification
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return model.BreakClassification{}, fmt.Errorf("unmarshal classification: %w", err)
	}

	bc := model.BreakClassification{
		ExternalTxnID: txn.ID,
		RootCause:     normalizeRootCause(raw.RootCause),
		Confidence:    clampConfidence(raw.Confidence),
		Regulated:     raw.Regulated,
		Rationale:     strings.TrimSpace(raw.Rationale),
	}

	if raw.ProposedRule != nil {
		rule := blnk.MatchingRule{
			Name:        fmt.Sprintf("agent-proposed-%s", txn.ID),
			Description: "agent-proposed matching rule",
			Criteria: []blnk.MatchingCriteria{{
				Field:    strings.ToLower(strings.TrimSpace(raw.ProposedRule.Field)),
				Operator: strings.ToLower(strings.TrimSpace(raw.ProposedRule.Operator)),
				Value:    raw.ProposedRule.Value,
			}},
		}
		// Rule 5.2: attach only if it conforms to Blnk's grammar; otherwise the
		// out-of-grammar rule is silently dropped and never leaves the agent.
		if err := ValidateRule(rule); err == nil {
			bc.ProposedRule = &rule
		}
	}

	return bc, nil
}

// extractJSONObject returns the substring spanning the first '{' to the last '}'
// (inclusive), tolerating Markdown code fences or incidental prose around the
// JSON. It returns "" when no plausible object is present.
func extractJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start < 0 || end < 0 || end < start {
		return ""
	}
	return s[start : end+1]
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

// clampConfidence constrains confidence to the closed interval [0, 1] and maps
// NaN to 0.
func clampConfidence(v float64) float64 {
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// truncate shortens s to at most n runes for safe inclusion in error messages.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
