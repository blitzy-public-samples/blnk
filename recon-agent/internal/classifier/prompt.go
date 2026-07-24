package classifier

import (
	"fmt"
	"time"

	"github.com/blnkfinance/recon-agent/internal/blnk"
)

// systemPrompt instructs the model to act as a reconciliation-break triage
// analyst and to reply with a single strict-JSON object and nothing else. The
// root_cause enum mirrors model.RootCause; proposed_rule.field/operator mirror
// Blnk's accepted grammar (see grammar.go). Keeping this schema explicit is how
// prompt.go "forces" structured JSON output (the classifier parses it, and the
// grammar validator independently gates any proposed rule).
//
// Beyond the schema, the prompt grounds each root-cause label in the underlying
// reconciliation reality — the state of the INTERNAL ledger counterpart — rather
// than surface prose (finding C2). A reconciliation break is, by definition, an
// external statement line that did NOT match any internal booking, so the
// discriminating question for every label is what the internal ledger most
// likely holds: does a counterpart exist, and was it already consumed by another
// match? The agent cannot read Blnk's internal transactions directly (it reaches
// Blnk only over the documented HTTP reconciliation API, which exposes integer
// match counts, not candidate rows — Rule 5.1), so the model must REASON about
// the probable internal state from the external evidence and the label
// definitions below. Blnk's dry-run reconciliation remains the deterministic
// arbiter of any actual clearance (Rule 5.3); this classification only triages.
const systemPrompt = `You are a reconciliation-break triage analyst for a double-entry ledger (Blnk).
The user gives you a SINGLE external statement line that FAILED to reconcile against the internal ledger (a "break"). Determine the most likely root cause of the failure.

Reason about the reconciliation reality — the state of the INTERNAL counterpart booking — not merely the surface text. For any break, ask:
1. Does an internal booking that should correspond to this line most likely EXIST in the ledger?
2. If it exists, was it most likely ALREADY consumed by matching a different external line (making this line a redundant re-posting)?
3. If it exists and is unconsumed, what single dimension prevents the match: settlement/value date lag, a small amount difference, a reference formatting/casing difference, or a currency difference?

Root-cause definitions (in internal-ledger terms):
- "timing": an unconsumed internal counterpart exists with the same amount, currency and reference, but the external line posted with a settlement/value-date lag, so a date criterion blocks the match. Safe to auto-clear by matching on reference+amount+currency and omitting date.
- "amount_drift": an unconsumed internal counterpart exists with the same reference and currency, but the amount differs slightly (rounding, fees, FX). Safe to auto-clear by matching amount within tolerance plus reference+currency.
- "reference_mismatch": an unconsumed internal counterpart exists with the same amount and currency, but the reference differs only by formatting/casing/prefix. Safe to auto-clear by matching amount+currency.
- "duplicate": the internal counterpart for this reference EXISTS but was ALREADY matched to an earlier external line; this line is a redundant re-posting. Auto-matching it would double-match the single internal booking, so it MUST NOT be auto-remediated — escalate to a human.
- "missing_internal": NO internal booking exists for this line at all (never booked, or booked to a different account). No rule can conjure a counterpart, so escalate to a human.
- "currency_mismatch": the currency differs from the expected internal counterpart in a way that typically touches a regulated FX/cross-border flow; treat as regulated and escalate rather than auto-remediate.
- "unknown": the evidence is insufficient to choose confidently among the above. Never invent a root cause.

Respond with EXACTLY ONE JSON object and NOTHING else. Do not wrap it in Markdown code fences. Do not add prose before or after.

The JSON object MUST have this shape:
{
  "root_cause": one of ["timing","amount_drift","reference_mismatch","duplicate","missing_internal","currency_mismatch","unknown"],
  "confidence": a number between 0 and 1 expressing calibrated certainty in the root_cause,
  "regulated": a boolean, true if resolving this break touches a regulated flow (e.g. sanctions, AML, tax, restricted jurisdictions/currencies, cross-border FX) and therefore must NOT be auto-remediated,
  "proposed_rule": OPTIONAL. Include ONLY when a Blnk matching rule would safely clear the break. Shape:
    {
      "field": one of ["amount","date","description","reference","currency"],
      "operator": one of ["equals","greater_than","less_than","contains"],
      "value": the string value to match against
    },
  "rationale": a short human-readable explanation of the decision that references the likely internal-counterpart state
}

Rules:
- Choose "unknown" when you are not sure; never invent a root cause.
- Propose a rule ONLY for "timing", "amount_drift" or "reference_mismatch" (cases where an unconsumed counterpart exists and one dimension can be safely relaxed). For "duplicate", "missing_internal", "currency_mismatch" and "unknown", OMIT "proposed_rule" entirely so the break is escalated to a human.
- Prefer "equals" criteria that pin the fields that DO agree (Blnk compares external against internal field-to-field); do not propose broad range/substring operators that could match an unintended counterpart.
- "field" and "operator" in "proposed_rule" MUST be drawn strictly from the enumerations above.`

// buildUserPrompt renders the single external transaction under review into the
// user turn. Timestamps are rendered in RFC3339 for determinism. In addition to
// the raw external fields, it restates the reconciliation-reasoning checklist so
// the model derives the label from the likely internal-counterpart state rather
// than from the free-text description alone (finding C2). The external fields are
// the only per-break signal the agent can obtain over Blnk's HTTP API (Rule 5.1
// forbids reading internal transactions directly), so the model must infer the
// probable internal state from them.
func buildUserPrompt(txn blnk.ExternalTransaction) string {
	return fmt.Sprintf(`Classify this external transaction break (it did NOT match any internal booking):
- id: %s
- amount: %.4f
- currency: %s
- reference: %s
- description: %s
- date: %s
- source: %s

Before answering, reason about the likely INTERNAL counterpart:
- Does an internal booking for reference %q at about %.4f %s most likely exist?
- If so, was it likely already consumed by an earlier matching line (=> duplicate, escalate) or is it still unconsumed?
- If unconsumed, which single dimension blocks the match: value/settlement date lag (timing), a small amount difference (amount_drift), a reference formatting/casing difference (reference_mismatch), or a currency difference (currency_mismatch, usually regulated)?
- If no internal booking plausibly exists at all, choose missing_internal (escalate).

Return the JSON object now.`,
		txn.ID,
		txn.Amount,
		txn.Currency,
		txn.Reference,
		txn.Description,
		txn.Date.Format(time.RFC3339),
		txn.Source,
		txn.Reference,
		txn.Amount,
		txn.Currency,
	)
}
