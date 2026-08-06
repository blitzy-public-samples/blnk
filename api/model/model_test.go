package model

import (
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin/binding"
	"github.com/stretchr/testify/assert"
)

func TestSourceOrSourcesValidation(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name:        "Valid with Source",
			transaction: RecordTransaction{Source: "source1"},
			wantErr:     false,
		},
		{
			name:        "Valid with Sources",
			transaction: RecordTransaction{Sources: []model.Distribution{{Identifier: "source1", Distribution: "100"}}},
			wantErr:     false,
		},
		{
			name:        "Invalid with both Source and Sources",
			transaction: RecordTransaction{Source: "source1", Sources: []model.Distribution{{Identifier: "source2", Distribution: "100"}}},
			wantErr:     true,
		},
		{
			name:        "Invalid with neither Source nor Sources",
			transaction: RecordTransaction{},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sourceOrSourcesValidation(&tt.transaction)(nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDestinationOrDestinationsValidation(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name:        "Valid with Destination",
			transaction: RecordTransaction{Destination: "dest1"},
			wantErr:     false,
		},
		{
			name:        "Valid with Destinations",
			transaction: RecordTransaction{Destinations: []model.Distribution{{Identifier: "dest1", Distribution: "100"}}},
			wantErr:     false,
		},
		{
			name:        "Invalid with both Destination and Destinations",
			transaction: RecordTransaction{Destination: "dest1", Destinations: []model.Distribution{{Identifier: "dest2", Distribution: "100"}}},
			wantErr:     true,
		},
		{
			name:        "Invalid with neither Destination nor Destinations",
			transaction: RecordTransaction{},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := destinationOrDestinationsValidation(&tt.transaction)(nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateLedger(t *testing.T) {
	tests := []struct {
		name    string
		ledger  CreateLedger
		wantErr bool
	}{
		{
			name:    "Valid Ledger",
			ledger:  CreateLedger{Name: "Test Ledger"},
			wantErr: false,
		},
		{
			name:    "Invalid Ledger - Empty Name",
			ledger:  CreateLedger{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ledger.ValidateCreateLedger()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateUpdateLedger(t *testing.T) {
	tests := []struct {
		name    string
		ledger  UpdateLedger
		wantErr bool
	}{
		{
			name:    "Valid Update Ledger",
			ledger:  UpdateLedger{Name: "Updated Ledger"},
			wantErr: false,
		},
		{
			name:    "Invalid Update Ledger - Empty Name",
			ledger:  UpdateLedger{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ledger.ValidateUpdateLedger()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateBalance(t *testing.T) {
	tests := []struct {
		name    string
		balance CreateBalance
		wantErr bool
	}{
		{
			name:    "Valid Balance",
			balance: CreateBalance{LedgerId: "ledger1", Currency: "USD"},
			wantErr: false,
		},
		{
			name:    "Invalid Balance - Missing LedgerId",
			balance: CreateBalance{Currency: "USD"},
			wantErr: true,
		},
		{
			name:    "Invalid Balance - Missing Currency",
			balance: CreateBalance{LedgerId: "ledger1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.balance.ValidateCreateBalance()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateAccount(t *testing.T) {
	tests := []struct {
		name    string
		account CreateAccount
		wantErr bool
	}{
		{
			name:    "Valid Account with LedgerId",
			account: CreateAccount{LedgerId: "ledger1", IdentityId: "identity1", Currency: "USD"},
			wantErr: false,
		},
		{
			name:    "Valid Account with BalanceId",
			account: CreateAccount{BalanceId: "balance1"},
			wantErr: false,
		},
		{
			name:    "Invalid Account - Missing Required Fields",
			account: CreateAccount{},
			wantErr: true,
		},
		{
			name:    "Invalid Account - Both LedgerId and BalanceId",
			account: CreateAccount{LedgerId: "ledger1", BalanceId: "balance1", IdentityId: "identity1", Currency: "USD"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.account.ValidateCreateAccount()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRecordTransaction(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name: "Valid Transaction",
			transaction: RecordTransaction{
				Amount:      100,
				Currency:    "USD",
				Reference:   "ref1",
				Description: "Test transaction",
				Source:      "source1",
				Destination: "dest1",
				SkipQueue:   true,
			},
			wantErr: false,
		},
		{
			name: "Valid Transaction - Integer Precision",
			transaction: RecordTransaction{
				Amount:      50,
				Precision:   1000,
				Currency:    "USD",
				Reference:   "ref_precision_int",
				Description: "Integer precision transaction",
				Source:      "source1",
				Destination: "dest1",
			},
			wantErr: false,
		},
		{
			name: "Invalid Transaction - Non-integer Precision",
			transaction: RecordTransaction{
				Amount:      50,
				Precision:   10.5,
				Currency:    "USD",
				Reference:   "ref_precision_float",
				Description: "Fractional precision transaction",
				Source:      "source1",
				Destination: "dest1",
			},
			wantErr: true,
		},
		{
			name: "Invalid Transaction - Missing Required Fields",
			transaction: RecordTransaction{
				Amount: 100,
			},
			wantErr: true,
		},
		{
			name: "Invalid Transaction - Invalid ScheduledFor",
			transaction: RecordTransaction{
				Amount:       100,
				Currency:     "USD",
				Reference:    "ref1",
				Description:  "Test transaction",
				Source:       "source1",
				Destination:  "dest1",
				ScheduledFor: "invalid-date",
				SkipQueue:    true,
			},
			wantErr: true,
		},
		{
			name: "Valid Transaction - Valid InflightCommitDate",
			transaction: RecordTransaction{
				Amount:             100,
				Currency:           "USD",
				Reference:          "ref1",
				Description:        "Test transaction",
				Source:             "source1",
				Destination:        "dest1",
				Inflight:           true,
				InflightCommitDate: "2024-04-22T15:28:03+00:00",
			},
			wantErr: false,
		},
		{
			name: "Invalid Transaction - Invalid InflightCommitDate",
			transaction: RecordTransaction{
				Amount:             100,
				Currency:           "USD",
				Reference:          "ref1",
				Description:        "Test transaction",
				Source:             "source1",
				Destination:        "dest1",
				Inflight:           true,
				InflightCommitDate: "invalid-date",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.transaction.ValidateRecordTransaction()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToLedger(t *testing.T) {
	createLedger := CreateLedger{
		Name:     "Test Ledger",
		MetaData: map[string]interface{}{"key": "value"},
	}

	ledger := createLedger.ToLedger()

	assert.Equal(t, createLedger.Name, ledger.Name)
	assert.Equal(t, createLedger.MetaData, ledger.MetaData)
}

func TestToBalance(t *testing.T) {
	createBalance := CreateBalance{
		LedgerId:   "ledger1",
		IdentityId: "identity1",
		Currency:   "USD",
		MetaData:   map[string]interface{}{"key": "value"},
		Precision:  2,
	}

	balance := createBalance.ToBalance()

	assert.Equal(t, createBalance.LedgerId, balance.LedgerID)
	assert.Equal(t, createBalance.IdentityId, balance.IdentityID)
	assert.Equal(t, createBalance.Currency, balance.Currency)
	assert.Equal(t, createBalance.MetaData, balance.MetaData)
}

func TestToAccount(t *testing.T) {
	createAccount := CreateAccount{
		BalanceId:  "balance1",
		LedgerId:   "ledger1",
		IdentityId: "identity1",
		Currency:   "USD",
		Number:     "123456",
		BankName:   "Test Bank",
		MetaData:   map[string]interface{}{"key": "value"},
	}

	account := createAccount.ToAccount()

	assert.Equal(t, createAccount.BalanceId, account.BalanceID)
	assert.Equal(t, createAccount.LedgerId, account.LedgerID)
	assert.Equal(t, createAccount.IdentityId, account.IdentityID)
	assert.Equal(t, createAccount.Currency, account.Currency)
	assert.Equal(t, createAccount.Number, account.Number)
	assert.Equal(t, createAccount.BankName, account.BankName)
	assert.Equal(t, createAccount.MetaData, account.MetaData)
}

func TestToTransaction(t *testing.T) {
	now := time.Now()
	scheduledFor := now.Add(24 * time.Hour)
	inflightExpiryDate := now.Add(48 * time.Hour)
	inflightCommitDate := now.Add(72 * time.Hour)

	recordTransaction := RecordTransaction{
		Currency:           "USD",
		Source:             "source1",
		Description:        "Test transaction",
		Reference:          "ref1",
		ScheduledFor:       scheduledFor.Format(time.RFC3339),
		Destination:        "dest1",
		Amount:             100,
		AllowOverDraft:     true,
		MetaData:           map[string]interface{}{"key": "value"},
		Sources:            []model.Distribution{{Identifier: "source1", Distribution: "100"}},
		Destinations:       []model.Distribution{{Identifier: "dest1", Distribution: "100"}},
		Inflight:           true,
		Precision:          2,
		InflightExpiryDate: inflightExpiryDate.Format(time.RFC3339),
		InflightCommitDate: inflightCommitDate.Format(time.RFC3339),
		SkipQueue:          true,
	}

	transaction := recordTransaction.ToTransaction()

	assert.Equal(t, recordTransaction.Currency, transaction.Currency)
	assert.Equal(t, recordTransaction.Source, transaction.Source)
	assert.Equal(t, recordTransaction.Description, transaction.Description)
	assert.Equal(t, recordTransaction.Reference, transaction.Reference)
	assert.Equal(t, recordTransaction.Destination, transaction.Destination)
	assert.Equal(t, recordTransaction.Amount, transaction.Amount)
	assert.Equal(t, recordTransaction.AllowOverDraft, transaction.AllowOverdraft)
	assert.Equal(t, recordTransaction.MetaData, transaction.MetaData)
	assert.Equal(t, recordTransaction.Sources, transaction.Sources)
	assert.Equal(t, recordTransaction.Destinations, transaction.Destinations)
	assert.Equal(t, recordTransaction.Inflight, transaction.Inflight)
	assert.Equal(t, recordTransaction.Precision, transaction.Precision)
	assert.Equal(t, recordTransaction.SkipQueue, transaction.SkipQueue)
	assert.False(t, transaction.InflightCommitDate.IsZero(), "InflightCommitDate should be parsed and set")
	assert.WithinDuration(t, inflightCommitDate, transaction.InflightCommitDate, time.Second)
}

func TestToTransactionInflightCommitDateEmpty(t *testing.T) {
	recordTransaction := RecordTransaction{
		Currency:    "USD",
		Source:      "source1",
		Description: "Test transaction",
		Reference:   "ref1",
		Destination: "dest1",
		Amount:      100,
		Inflight:    true,
	}

	transaction := recordTransaction.ToTransaction()

	assert.True(t, transaction.InflightCommitDate.IsZero(), "InflightCommitDate should be zero when not provided")
}

func TestValidateCreateBalanceMonitor(t *testing.T) {
	tests := []struct {
		name    string
		monitor CreateBalanceMonitor
		wantErr bool
	}{
		{
			name: "Valid balance monitor",
			monitor: CreateBalanceMonitor{
				BalanceId: "bln_123",
				Condition: MonitorCondition{
					Field:     "balance",
					Operator:  ">",
					Value:     100,
					Precision: 100,
				},
			},
			wantErr: false,
		},
		{
			name: "Missing balance ID",
			monitor: CreateBalanceMonitor{
				Condition: MonitorCondition{
					Field:     "balance",
					Operator:  ">",
					Value:     100,
					Precision: 100,
				},
			},
			wantErr: true,
		},
		{
			name: "Missing condition",
			monitor: CreateBalanceMonitor{
				BalanceId: "bln_123",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.monitor.ValidateCreateBalanceMonitor()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateMonitorCondition(t *testing.T) {
	tests := []struct {
		name      string
		condition MonitorCondition
		wantErr   bool
	}{
		{
			name: "Valid condition - balance",
			condition: MonitorCondition{
				Field:     "balance",
				Operator:  ">",
				Value:     100,
				Precision: 100,
			},
			wantErr: false,
		},
		{
			name: "Valid condition - credit_balance",
			condition: MonitorCondition{
				Field:     "credit_balance",
				Operator:  "<",
				Value:     500,
				Precision: 100,
			},
			wantErr: false,
		},
		{
			name: "Valid condition - debit_balance",
			condition: MonitorCondition{
				Field:     "debit_balance",
				Operator:  ">=",
				Value:     1,
				Precision: 100,
			},
			wantErr: false,
		},
		{
			name: "Valid condition - inflight_balance",
			condition: MonitorCondition{
				Field:     "inflight_balance",
				Operator:  "<=",
				Value:     1000,
				Precision: 100,
			},
			wantErr: false,
		},
		{
			name: "Invalid field",
			condition: MonitorCondition{
				Field:     "invalid_field",
				Operator:  ">",
				Value:     100,
				Precision: 100,
			},
			wantErr: true,
		},
		{
			name: "Missing field",
			condition: MonitorCondition{
				Operator:  ">",
				Value:     100,
				Precision: 100,
			},
			wantErr: true,
		},
		{
			name: "Missing operator",
			condition: MonitorCondition{
				Field:     "balance",
				Value:     100,
				Precision: 100,
			},
			wantErr: true,
		},
		{
			name: "Missing precision",
			condition: MonitorCondition{
				Field:    "balance",
				Operator: ">",
				Value:    100,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.condition.ValidateMonitorCondition()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToBalanceMonitor(t *testing.T) {
	createMonitor := CreateBalanceMonitor{
		BalanceId:   "bln_test_123",
		CallBackURL: "https://example.com/webhook",
		Condition: MonitorCondition{
			Field:     "balance",
			Operator:  ">",
			Value:     1000,
			Precision: 100,
		},
	}

	monitor := createMonitor.ToBalanceMonitor()

	assert.Equal(t, createMonitor.BalanceId, monitor.BalanceID)
	assert.Equal(t, createMonitor.CallBackURL, monitor.CallBackURL)
	assert.Equal(t, createMonitor.Condition.Field, monitor.Condition.Field)
	assert.Equal(t, createMonitor.Condition.Operator, monitor.Condition.Operator)
	assert.Equal(t, createMonitor.Condition.Value, monitor.Condition.Value)
	assert.Equal(t, createMonitor.Condition.Precision, monitor.Condition.Precision)
}

// TestValidateCreateSubscriber_BoundsTheTopicGrant checks the request-body
// validation for POST /subscribers.
//
// Both directions are asserted. The refusals are the resource bound and the
// authorization narrowing; the acceptances are what keeps that bound from breaking
// legitimate requests — a subscriber registered with no grant at all is the
// registry's normal fail-closed state, and a sixteen-topic grant is exactly what a
// topic-prefix migration needs.
func TestValidateCreateSubscriber_BoundsTheTopicGrant(t *testing.T) {
	oversized := make([]string, model.MaxSubscriberTopics+1)
	for i := range oversized {
		oversized[i] = "blnk.transactions"
	}

	tests := []struct {
		name    string
		body    CreateSubscriber
		wantErr bool
		reason  string
	}{
		{
			name:    "name only, no grant",
			body:    CreateSubscriber{Name: "settlement consumer"},
			wantErr: false,
			reason:  "registration and provisioning are separate steps; a subscriber authorised for nothing is the fail-closed default",
		},
		{
			name:    "an empty grant is an explicit revocation and is valid",
			body:    CreateSubscriber{Name: "settlement consumer", AuthorizedTopics: []string{}},
			wantErr: false,
		},
		{
			name: "a well-formed grant",
			body: CreateSubscriber{
				Name:             "settlement consumer",
				AuthorizedTopics: []string{"blnk.transactions", "blnk.balances"},
			},
			wantErr: false,
		},
		{
			name: "a dead-letter topic",
			body: CreateSubscriber{
				Name:             "wants the dlt",
				AuthorizedTopics: []string{"blnk.transactions", "blnk.transactions.dlt"},
			},
			wantErr: true,
			reason: "a `<topic>.dlt` sibling carries Blnk's OWN failure records — the original bytes " +
				"of every event that exhausted its retry budget, across every ledger — so it has no " +
				"subscriber audience and is not grantable, however well-formed the name is",
		},
		{
			name:    "name is required",
			body:    CreateSubscriber{AuthorizedTopics: []string{"blnk.transactions"}},
			wantErr: true,
			reason:  "an unnamed principal cannot be triaged, and the column is NOT NULL",
		},
		{
			name:    "more topics than the ceiling",
			body:    CreateSubscriber{Name: "greedy", AuthorizedTopics: oversized},
			wantErr: true,
			reason:  "every entry becomes an ACL binding and an element of a stored array",
		},
		{
			name: "a topic belonging to another system",
			body: CreateSubscriber{
				Name:             "cross tenant",
				AuthorizedTopics: []string{"blnk.transactions", "someone-else.orders"},
			},
			wantErr: true,
			reason:  "granting access to another system's topic is not a decision Blnk may make on a subscriber's behalf",
		},
		{
			name: "an internal Kafka topic",
			body: CreateSubscriber{
				Name:             "internals",
				AuthorizedTopics: []string{"__consumer_offsets"},
			},
			wantErr: true,
			reason:  "broker internals are never grantable through the subscriber registry",
		},
		{
			name: "a duplicate entry",
			body: CreateSubscriber{
				Name:             "duplicated",
				AuthorizedTopics: []string{"blnk.balances", "blnk.balances"},
			},
			wantErr: true,
			reason:  "a duplicate inflates the ACL request and the stored row while granting nothing additional",
		},
		{
			name: "a blank entry",
			body: CreateSubscriber{
				Name:             "blank",
				AuthorizedTopics: []string{"blnk.balances", "  "},
			},
			wantErr: true,
			reason:  "a blank grant authorises nothing yet still consumes a binding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.body
			err := body.ValidateCreateSubscriber()
			if tt.wantErr {
				assert.Error(t, err, tt.reason)
				return
			}
			assert.NoError(t, err, tt.reason)
		})
	}
}

// TestValidateUpdateSubscriber_ValidatesTheGrantOnlyWhenPresent pins the
// omitted-versus-empty distinction the update shape exists for.
//
// nil means the caller is not touching the authorised set, so an update to an
// unrelated field must not be rejected on account of it. A PRESENT empty array is a
// deliberate revocation of every grant and must be accepted. A present non-empty
// grant replaces the whole set and so is validated exactly as strictly as on create —
// otherwise the update path would be the way around the create path's bounds.
func TestValidateUpdateSubscriber_ValidatesTheGrantOnlyWhenPresent(t *testing.T) {
	name := "renamed"
	oversized := make([]string, model.MaxSubscriberTopics+1)
	for i := range oversized {
		oversized[i] = "blnk.identities"
	}

	t.Run("an omitted grant is not validated", func(t *testing.T) {
		body := UpdateSubscriber{Name: &name}
		assert.NoError(t, body.ValidateUpdateSubscriber(),
			"an update that does not mention the grant must not be rejected because of it")
	})

	t.Run("a present empty grant revokes everything and is valid", func(t *testing.T) {
		body := UpdateSubscriber{AuthorizedTopics: []string{}}
		assert.NoError(t, body.ValidateUpdateSubscriber(),
			"revoking every topic is a legitimate operation and must remain expressible")
	})

	t.Run("a present grant is bounded exactly as on create", func(t *testing.T) {
		body := UpdateSubscriber{AuthorizedTopics: oversized}
		assert.Error(t, body.ValidateUpdateSubscriber(),
			"an update replaces the whole grant, so a bound enforced only on create is no bound at all")
	})

	t.Run("a present grant is narrowed to the owned taxonomy", func(t *testing.T) {
		body := UpdateSubscriber{AuthorizedTopics: []string{"someone-else.orders"}}
		assert.Error(t, body.ValidateUpdateSubscriber())
	})
}

// TestSubscriberDTOs_CapTheTopicArrayInTheBindingTags asserts the caps are applied
// by the BINDER, before any handler code runs.
//
// This is a different guarantee from the Validate methods above and is why both
// exist. A handler that forgets to call ValidateCreateSubscriber still cannot accept
// an unbounded topic array, because gin applies these tags while decoding the body —
// so the dimensions that bound allocation hold regardless of handler code. The
// assertion goes through gin's own validator, which is the exact code path a bind
// takes.
func TestSubscriberDTOs_CapTheTopicArrayInTheBindingTags(t *testing.T) {
	oversizedCount := make([]string, model.MaxSubscriberTopics+1)
	for i := range oversizedCount {
		oversizedCount[i] = "blnk.transactions"
	}
	oversizedElement := []string{strings.Repeat("t", model.MaxTopicNameLength+1)}

	t.Run("create caps the count", func(t *testing.T) {
		assert.Error(t, binding.Validator.ValidateStruct(&CreateSubscriber{
			Name: "greedy", AuthorizedTopics: oversizedCount,
		}), "the binder must refuse an oversized topic array without any handler code running")
	})

	t.Run("create caps the element length", func(t *testing.T) {
		assert.Error(t, binding.Validator.ValidateStruct(&CreateSubscriber{
			Name: "greedy", AuthorizedTopics: oversizedElement,
		}), "dive,max must bound each element, not only the array")
	})

	t.Run("create accepts a grant at the limits", func(t *testing.T) {
		atLimit := make([]string, model.MaxSubscriberTopics)
		for i := range atLimit {
			atLimit[i] = strings.Repeat("t", model.MaxTopicNameLength)
		}
		assert.NoError(t, binding.Validator.ValidateStruct(&CreateSubscriber{
			Name: "at the limit", AuthorizedTopics: atLimit,
		}), "the limits themselves must be accepted; an off-by-one here would refuse a legitimate migration grant")
	})

	t.Run("update caps the count", func(t *testing.T) {
		assert.Error(t, binding.Validator.ValidateStruct(&UpdateSubscriber{
			AuthorizedTopics: oversizedCount,
		}), "the update body carries the same caps as the create body")
	})

	t.Run("update caps the element length", func(t *testing.T) {
		assert.Error(t, binding.Validator.ValidateStruct(&UpdateSubscriber{
			AuthorizedTopics: oversizedElement,
		}))
	})
}
