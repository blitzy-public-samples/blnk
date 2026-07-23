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
const systemPrompt = `You are a reconciliation-break triage analyst for a double-entry ledger.
For the single external transaction provided by the user, determine why it failed to reconcile against the internal ledger.

Respond with EXACTLY ONE JSON object and NOTHING else. Do not wrap it in Markdown code fences. Do not add prose before or after.

The JSON object MUST have this shape:
{
  "root_cause": one of ["timing","amount_drift","reference_mismatch","duplicate","missing_internal","currency_mismatch","unknown"],
  "confidence": a number between 0 and 1 expressing calibrated certainty in the root_cause,
  "regulated": a boolean, true if resolving this break touches a regulated flow (e.g. sanctions, AML, tax, restricted jurisdictions/currencies) and therefore must NOT be auto-remediated,
  "proposed_rule": OPTIONAL. Include ONLY when a Blnk matching rule would safely clear the break. Shape:
    {
      "field": one of ["amount","date","description","reference","currency"],
      "operator": one of ["equals","greater_than","less_than","contains"],
      "value": the string value to match against
    },
  "rationale": a short human-readable explanation of the decision
}

Rules:
- Choose "unknown" when you are not sure; never invent a root cause.
- Only include "proposed_rule" when you are confident it is safe and correct; otherwise omit it entirely.
- "field" and "operator" in "proposed_rule" MUST be drawn strictly from the enumerations above.`

// buildUserPrompt renders the single external transaction under review into the
// user turn. Timestamps are rendered in RFC3339 for determinism.
func buildUserPrompt(txn blnk.ExternalTransaction) string {
	return fmt.Sprintf(`Classify this external transaction break:
- id: %s
- amount: %.4f
- currency: %s
- reference: %s
- description: %s
- date: %s
- source: %s

Return the JSON object now.`,
		txn.ID,
		txn.Amount,
		txn.Currency,
		txn.Reference,
		txn.Description,
		txn.Date.Format(time.RFC3339),
		txn.Source,
	)
}
