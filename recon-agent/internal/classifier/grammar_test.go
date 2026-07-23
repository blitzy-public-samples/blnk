package classifier

import (
	"errors"
	"testing"

	"github.com/blnkfinance/recon-agent/internal/blnk"
)

func TestValidateCriteria(t *testing.T) {
	tests := []struct {
		name     string
		criteria blnk.MatchingCriteria
		wantErr  error // nil => accepted
	}{
		{"amount equals", blnk.MatchingCriteria{Field: "amount", Operator: "equals"}, nil},
		{"date greater_than", blnk.MatchingCriteria{Field: "date", Operator: "greater_than"}, nil},
		{"description contains", blnk.MatchingCriteria{Field: "description", Operator: "contains"}, nil},
		{"reference equals", blnk.MatchingCriteria{Field: "reference", Operator: "equals"}, nil},
		{"currency less_than", blnk.MatchingCriteria{Field: "currency", Operator: "less_than"}, nil},

		{"bad field", blnk.MatchingCriteria{Field: "vendor", Operator: "equals"}, ErrInvalidField},
		{"empty field", blnk.MatchingCriteria{Field: "", Operator: "equals"}, ErrInvalidField},
		{"bad operator", blnk.MatchingCriteria{Field: "amount", Operator: "regex"}, ErrInvalidOperator},
		{"empty operator", blnk.MatchingCriteria{Field: "amount", Operator: ""}, ErrInvalidOperator},
		// Operator is validated before Field, matching Blnk's own ordering.
		{"both bad -> operator first", blnk.MatchingCriteria{Field: "vendor", Operator: "regex"}, ErrInvalidOperator},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCriteria(tt.criteria)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateCriteria(%+v) = %v, want nil", tt.criteria, err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateCriteria(%+v) = %v, want errors.Is %v", tt.criteria, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRule(t *testing.T) {
	tests := []struct {
		name    string
		rule    blnk.MatchingRule
		wantErr error
	}{
		{
			name: "single valid criterion",
			rule: blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{
				{Field: "reference", Operator: "equals", Value: "INV-1"},
			}},
			wantErr: nil,
		},
		{
			name: "multiple valid criteria",
			rule: blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{
				{Field: "amount", Operator: "equals", Value: "100"},
				{Field: "currency", Operator: "equals", Value: "USD"},
			}},
			wantErr: nil,
		},
		{
			name:    "no criteria rejected",
			rule:    blnk.MatchingRule{Criteria: nil},
			wantErr: ErrNoCriteria,
		},
		{
			name: "one bad criterion rejects whole rule (field)",
			rule: blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{
				{Field: "amount", Operator: "equals"},
				{Field: "vendor", Operator: "equals"},
			}},
			wantErr: ErrInvalidField,
		},
		{
			name: "one bad criterion rejects whole rule (operator)",
			rule: blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{
				{Field: "amount", Operator: "between"},
			}},
			wantErr: ErrInvalidOperator,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRule(tt.rule)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateRule = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateRule = %v, want errors.Is %v", err, tt.wantErr)
			}
		})
	}
}

func TestAllowedDomainsExposed(t *testing.T) {
	if len(AllowedFields()) != 5 {
		t.Fatalf("AllowedFields size = %d, want 5", len(AllowedFields()))
	}
	if len(AllowedOperators()) != 4 {
		t.Fatalf("AllowedOperators size = %d, want 4", len(AllowedOperators()))
	}
}
