package classifier

import (
	"errors"
	"fmt"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// Blnk's accepted matching-rule grammar, confirmed in the Blnk root file
// reconciliation.go: validateField (fields) and validateOperator (operators).
// The agent MUST validate any LLM-proposed rule against this grammar BEFORE it
// is ever POSTed to Blnk (Rule 5.2). Anything outside these sets is rejected so
// an invalid rule can never reach Blnk. The set members are drawn from the
// canonical grammar constants in internal/model (finding m-01) so this
// validator, cmd's detection rule, and hitl's re_drive rule builder share one
// vocabulary and can never diverge.
var (
	allowedFields = map[string]bool{
		model.FieldAmount:      true,
		model.FieldDate:        true,
		model.FieldDescription: true,
		model.FieldReference:   true,
		model.FieldCurrency:    true,
	}
	allowedOperators = map[string]bool{
		model.OperatorEquals:      true,
		model.OperatorGreaterThan: true,
		model.OperatorLessThan:    true,
		model.OperatorContains:    true,
	}
)

// Grammar validation sentinel errors. Callers may use errors.Is to distinguish
// the failure mode.
var (
	// ErrInvalidField indicates a criterion Field outside Blnk's accepted domain.
	ErrInvalidField = errors.New("classifier: matching criteria field outside Blnk grammar")
	// ErrInvalidOperator indicates a criterion Operator outside Blnk's accepted domain.
	ErrInvalidOperator = errors.New("classifier: matching criteria operator outside Blnk grammar")
	// ErrNoCriteria indicates a proposed rule that carries no criteria at all.
	ErrNoCriteria = errors.New("classifier: proposed matching rule has no criteria")
)

// AllowedFields returns a copy of Blnk's accepted matching-rule field domain.
func AllowedFields() []string {
	out := make([]string, 0, len(allowedFields))
	for f := range allowedFields {
		out = append(out, f)
	}
	return out
}

// AllowedOperators returns a copy of Blnk's accepted matching-rule operator domain.
func AllowedOperators() []string {
	out := make([]string, 0, len(allowedOperators))
	for o := range allowedOperators {
		out = append(out, o)
	}
	return out
}

// ValidateCriteria reports whether a single matching criterion conforms to
// Blnk's accepted grammar. Both Field and Operator must be members of the
// accepted domains; otherwise a wrapped sentinel error is returned.
func ValidateCriteria(c blnk.MatchingCriteria) error {
	if !allowedOperators[c.Operator] {
		return fmt.Errorf("%w: %q", ErrInvalidOperator, c.Operator)
	}
	if !allowedFields[c.Field] {
		return fmt.Errorf("%w: %q", ErrInvalidField, c.Field)
	}
	return nil
}

// ValidateRule reports whether every criterion of a proposed matching rule
// conforms to Blnk's grammar. A rule with no criteria is rejected. This is the
// gate that must pass before a rule is attached to a classification or POSTed
// to Blnk (Rule 5.2).
func ValidateRule(r blnk.MatchingRule) error {
	if len(r.Criteria) == 0 {
		return ErrNoCriteria
	}
	for _, c := range r.Criteria {
		if err := ValidateCriteria(c); err != nil {
			return err
		}
	}
	return nil
}
