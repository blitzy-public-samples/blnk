/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"
	"time"

	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
)

// IDataSource defines the interface for data source operations, grouping related functionalities.
type IDataSource interface {
	transaction     // Interface for transaction-related operations
	ledger          // Interface for ledger-related operations
	balance         // Interface for balance-related operations
	identity        // Interface for identity-related operations
	balanceMonitor  // Interface for balance monitoring operations
	account         // Interface for account-related operations
	reconciliation  // Interface for reconciliation-related operations
	apikey          // Interface for API key operations
	lineage         // Interface for fund lineage operations
	chain           // Interface for hash-chain operations
	eventOutbox     // Interface for event outbox operations
	eventSubscriber // Interface for event subscriber operations
}

// transaction defines methods for handling transactions.
type transaction interface {
	RecordTransaction(cxt context.Context, txn *model.Transaction) (*model.Transaction, error)                                                               // Records a new transaction
	RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) // Records a transaction with balance updates atomically
	// The three atomic writers below take their event outbox rows as a VARIADIC
	// parameter, and that is a deliberate, load-bearing choice rather than a
	// stylistic one. DO NOT "tidy" it into a positional parameter.
	//
	// Event rows have to be inserted inside the very same database transaction as
	// the ledger mutation that produced them, which is what makes an event and its
	// mutation commit or roll back together. Threading them through these writers
	// is therefore unavoidable. Making the parameter variadic is what keeps every
	// pre-existing caller source-compatible while doing so: a variadic tail may be
	// omitted entirely, so callers that pass only lineage outboxes still compile
	// untouched. Callers that pass nothing here are opting out of event capture,
	// exactly as a nil lineage outbox opts out of lineage capture.
	//
	// A positional sixth parameter would break callers this change is explicitly
	// not permitted to edit — the transaction coalescing path in
	// transaction_coalescing.go, which belongs to the frozen transaction-processing
	// pipeline, plus the pre-existing atomic-writer tests in
	// database/transactions_test.go and the mock argument list in
	// transaction_benchmark_test.go. Widening positionally is not a bigger diff, it
	// is an impossible one.
	RecordTransactionWithBalancesAndOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance, outbox *model.LineageOutbox, eventOutbox ...*model.EventOutbox) (*model.Transaction, error) // Records a transaction with balance updates and optional lineage and event outbox entries atomically
	RecordTransactionsWithBalancesAndOutboxes(ctx context.Context, txns []*model.Transaction, sourceBalance, destinationBalance *model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error)
	RecordTransactionsWithBalanceSetAndOutboxes(ctx context.Context, txns []*model.Transaction, balances []*model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error)
	GetTransaction(cxt context.Context, id string) (*model.Transaction, error)                                                                      // Retrieves a transaction by ID
	IsParentTransactionVoid(cxt context.Context, parentID string) (bool, error)                                                                     // Checks if a parent transaction is void
	GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error)                                                           // Retrieves a transaction by reference
	TransactionExistsByRef(ctx context.Context, reference string) (bool, error)                                                                     // Checks if a transaction exists by reference
	GetExistingTransactionReferences(ctx context.Context, references []string) (map[string]struct{}, error)                                         // Gets existing transaction references in bulk
	GetAllTransactions(cxt context.Context, limit, offset int) ([]model.Transaction, error)                                                         // Retrieves all transactions
	GetTotalCommittedTransactions(cxt context.Context, parentID string) (*big.Int, error)                                                           // Gets the total count of committed transactions for a parent
	GetTransactionsPaginated(ctx context.Context, id string, batchSize int, offset int64) ([]*model.Transaction, error)                             // Retrieves transactions in a paginated manner
	GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)   // Retrieves inflight transactions by parent ID
	GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) // Retrieves refundable transactions by parent ID
	GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error)              // Groups transactions based on specified criteria
	UpdateLedgerMetadata(id string, metadata map[string]interface{}) error
	UpdateTransactionMetadata(ctx context.Context, id string, metadata map[string]interface{}) error
	UpdateBalanceMetadata(ctx context.Context, id string, metadata map[string]interface{}) error
	UpdateIdentityMetadata(id string, metadata map[string]interface{}) error
	TransactionExistsByIDOrParentID(ctx context.Context, id string) (bool, error)
	GetTransactionsByParent(ctx context.Context, parentID string, limit int, offset int64) ([]*model.Transaction, error) // Retrieves transactions by parent ID with pagination
	IsTransactionRefunded(ctx context.Context, transaction *model.Transaction) (bool, error)                             // Checks if a transaction has already been refunded
	GetTransactionsByCriteria(ctx context.Context, minAmount, maxAmount *float64, currency *string, minDate, maxDate *time.Time, limit int, offset int64) ([]*model.Transaction, error)
	GetTransactionsByShadowFor(ctx context.Context, parentTransactionID string) ([]model.Transaction, error)              // Retrieves shadow transactions by parent transaction ID
	GetStuckQueuedTransactions(ctx context.Context, threshold time.Duration, batchSize int) ([]*model.Transaction, error) // Retrieves stuck QUEUED transactions with no child
	GetQueuedTransactionsForCoalescing(ctx context.Context, source, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error)
	GetQueuedTransactionsForSourceCoalescing(ctx context.Context, source, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error)
	GetQueuedTransactionsForDestinationCoalescing(ctx context.Context, destination, currency, excludeTransactionID string, createdAtOrAfter time.Time, limit int) ([]*model.Transaction, error)
	CountQueuedTransactionsForPairLane(ctx context.Context, source, destination, currency, lane string) (int, error)

	// Advanced filtering methods
	GetAllTransactionsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Transaction, error)                                              // Retrieves transactions with advanced filtering
	GetAllTransactionsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Transaction, *int64, error) // Retrieves transactions with filtering, sorting, and count
}

// ledger defines methods for handling ledgers.
type ledger interface {
	CreateLedger(ledger model.Ledger) (model.Ledger, error)  // Creates a new ledger
	GetAllLedgers(limit, offset int) ([]model.Ledger, error) // Retrieves all ledgers (legacy)
	GetLedgerByID(id string) (*model.Ledger, error)          // Retrieves a ledger by ID
	UpdateLedger(id, name string) (*model.Ledger, error)     // Updates a ledger's name

	// Advanced filtering methods
	GetAllLedgersWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Ledger, error)                                              // Retrieves ledgers with advanced filtering
	GetAllLedgersWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Ledger, *int64, error) // Retrieves ledgers with filtering, sorting, and count
}

// balance defines methods for handling balances.
type balance interface {
	CreateBalance(balance model.Balance) (model.Balance, error)                                                            // Creates a new balance
	GetBalanceByID(id string, include []string, withQueued bool) (*model.Balance, error)                                   // Retrieves a balance by ID with additional data and queued status
	GetBalanceByIDLite(id string) (*model.Balance, error)                                                                  // Retrieves a balance by ID with minimal data
	GetBalancesByIDsLite(ctx context.Context, ids []string) (map[string]*model.Balance, error)                             // Retrieves multiple balances by IDs with minimal data (batch query)
	GetAllBalances(limit, offset int) ([]model.Balance, error)                                                             // Retrieves all balances (legacy)
	UpdateBalance(balance *model.Balance) error                                                                            // Updates a balance
	GetBalanceByIndicator(indicator, currency string) (*model.Balance, error)                                              // Retrieves a balance by indicator and currency
	UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error                            // Updates multiple balances
	GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error)                                         // Retrieves balances between source and destination
	TakeBalanceSnapshots(ctx context.Context, batchSize int) (int, error)                                                  // Takes balance snapshots
	GetBalanceAtTime(ctx context.Context, balanceID string, targetTime time.Time, fromSource bool) (*model.Balance, error) // Retrieves a balance at a specific time
	UpdateBalanceIdentity(balanceID string, identityID string) error                                                       // Updates only the identity_id of a balance

	// Advanced filtering methods
	GetAllBalancesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Balance, error)                                              // Retrieves balances with advanced filtering
	GetAllBalancesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Balance, *int64, error) // Retrieves balances with filtering, sorting, and count
}

// account defines methods for handling accounts.
type account interface {
	CreateAccount(account model.Account) (model.Account, error)         // Creates a new account
	GetAccountByID(id string, include []string) (*model.Account, error) // Retrieves an account by ID with additional data
	GetAllAccounts() ([]model.Account, error)                           // Retrieves all accounts (legacy)
	GetAccountByNumber(number string) (*model.Account, error)           // Retrieves an account by its number
	UpdateAccount(account *model.Account) error                         // Updates an account
	DeleteAccount(id string) error                                      // Deletes an account

	// Advanced filtering methods
	GetAllAccountsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Account, error)                                              // Retrieves accounts with advanced filtering
	GetAllAccountsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Account, *int64, error) // Retrieves accounts with filtering, sorting, and count
}

// balanceMonitor defines methods for monitoring balances.
type balanceMonitor interface {
	CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) // Creates a new balance monitor
	GetMonitorByID(id string) (*model.BalanceMonitor, error)                  // Retrieves a balance monitor by ID
	GetAllMonitors() ([]model.BalanceMonitor, error)                          // Retrieves all balance monitors
	GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error)      // Retrieves monitors for a specific balance
	UpdateMonitor(monitor *model.BalanceMonitor) error                        // Updates a balance monitor
	DeleteMonitor(id string) error                                            // Deletes a balance monitor
}

// identity defines methods for handling identities.
type identity interface {
	CreateIdentity(identity model.Identity) (model.Identity, error)        // Creates a new identity
	GetIdentityByID(id string) (*model.Identity, error)                    // Retrieves an identity by ID
	GetAllIdentities() ([]model.Identity, error)                           // Retrieves all identities (legacy)
	GetAllIdentitiesPaginated(limit, offset int) ([]model.Identity, error) // Retrieves identities with pagination (legacy)
	UpdateIdentity(identity *model.Identity) error                         // Updates an identity
	DeleteIdentity(id string) error                                        // Deletes an identity

	// Advanced filtering methods
	GetAllIdentitiesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Identity, error)                                              // Retrieves identities with advanced filtering
	GetAllIdentitiesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Identity, *int64, error) // Retrieves identities with filtering, sorting, and count
}

// reconciliation defines methods for handling reconciliation processes.
type reconciliation interface {
	RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error                                                                                          // Records a new reconciliation
	GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error)                                                                                    // Retrieves a reconciliation by ID
	UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error                                                   // Updates the status of a reconciliation
	GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error)                                                                 // Retrieves reconciliations by upload ID
	RecordMatch(ctx context.Context, match *model.Match) error                                                                                                          // Records a match in reconciliation
	GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error)                                                                  // Retrieves matches by reconciliation ID
	GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error)                           // Retrieves external transactions in a paginated manner
	RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, reconciliationID string) error                                                        // Records an external transaction
	RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error                                                                                             // Records a matching rule
	GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error)                                                                                                // Retrieves all matching rules
	GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error)                                                                                        // Retrieves a matching rule by ID
	UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error                                                                                             // Updates a matching rule
	DeleteMatchingRule(ctx context.Context, id string) error                                                                                                            // Deletes a matching rule
	SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error                                               // Saves reconciliation progress
	LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error)                                                      // Loads reconciliation progress
	RecordMatches(ctx context.Context, reconciliationID string, matches []model.Match) error                                                                            // Records matches for a reconciliation
	RecordUnmatched(ctx context.Context, reconciliationID string, results []string) error                                                                               // Records unmatched results for a reconciliation
	FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) // Fetches and groups external transactions based on criteria
}

type apikey interface {
	CreateAPIKey(ctx context.Context, name, ownerID string, scopes []string, expiresAt time.Time) (*model.APIKey, error) // Creates a new API key
	GetAPIKey(ctx context.Context, key string) (*model.APIKey, error)                                                    // Retrieves an API key by its key string
	RevokeAPIKey(ctx context.Context, id, ownerID string) error                                                          // Revokes an API key
	ListAPIKeys(ctx context.Context, ownerID string) ([]*model.APIKey, error)                                            // Lists all API keys for a specific owner
	UpdateLastUsed(ctx context.Context, id string) error                                                                 // Updates the last_used_at timestamp for an API key
}

// lineage defines methods for fund lineage tracking operations.
type lineage interface {
	UpsertLineageMapping(ctx context.Context, mapping model.LineageMapping) error                               // Creates or updates a lineage mapping
	GetLineageMappings(ctx context.Context, balanceID string) ([]model.LineageMapping, error)                   // Retrieves all lineage mappings for a balance
	GetLineageMappingByProvider(ctx context.Context, balanceID, provider string) (*model.LineageMapping, error) // Retrieves a specific lineage mapping
	DeleteLineageMapping(ctx context.Context, id int64) error                                                   // Deletes a lineage mapping

	// Outbox methods for atomic lineage processing
	InsertLineageOutboxInTx(ctx context.Context, tx *sql.Tx, outbox *model.LineageOutbox) error                              // Inserts outbox entry within a transaction
	InsertLineageOutbox(ctx context.Context, outbox *model.LineageOutbox) error                                              // Inserts outbox entry directly (for shadow work)
	ClaimPendingOutboxEntries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.LineageOutbox, error) // Claims pending entries for processing
	MarkOutboxCompleted(ctx context.Context, id int64) error                                                                 // Marks an outbox entry as completed
	MarkOutboxFailed(ctx context.Context, id int64, errMsg string) error                                                     // Marks an outbox entry as failed
	GetOutboxByTransactionID(ctx context.Context, transactionID string) (*model.LineageOutbox, error)                        // Gets outbox entry by transaction ID
	HasPendingCreditOutbox(ctx context.Context, balanceID string) (bool, error)                                              // Checks if there are pending credit outbox entries for a balance
}

// eventOutbox defines methods for the Kafka event-publishing transactional
// outbox: blnk.event_outbox, and the relay state machine that drains it.
//
// It sits beside the lineage outbox above deliberately, because it reuses that
// proven shape — an in-transaction insert, a FIFO claim that takes a lease, and
// explicit terminal-state transitions — rather than inventing a second pattern.
// The two are nonetheless SEPARATE CONTRACTS over SEPARATE TABLES served by
// SEPARATE RELAYS. They are not merged, and the lineage declarations above are
// not modified or reused by name, so the two state machines can evolve
// independently.
//
// # Why the insert has to happen inside a caller-supplied transaction
//
// InsertEventOutboxInTx takes an existing *sql.Tx so the event row commits in
// the SAME database transaction as the ledger mutation that produced it. That is
// the whole point of the outbox: the mutation and its event commit or roll back
// together, so there is no window in which a balance moved but the event was
// lost, and none in which an event describes a mutation that was rolled back.
// This yields exactly-once semantics ON THE WRITE SIDE. Kafka delivery itself
// stays at-least-once, which is why the event_id column is uniquely indexed and
// why duplicate suppression on it is a documented subscriber obligation.
//
// # Vocabulary
//
// Statuses are the model.EventOutboxStatus* values — pending, processing,
// dispatched, failed, dead_lettered — and NOT the four-value lineage
// model.OutboxStatus* set. The terminal success state here is DISPATCHED, not
// "completed": nothing in this contract should be named as though a row could
// complete. The int64 ids taken by the Mark* methods are the BIGSERIAL surrogate
// key, whereas GetEventByID takes the business event_id UUID; the two are not
// interchangeable.
//
// # The one method with a limited lifetime
//
// MarkWebhookDispatched serves the 30-day window during which Kafka publishing
// and legacy HTTP webhook delivery run side by side FROM THE SAME CLAIMED ROW —
// which is what makes the two transports carry byte-identical payloads
// structurally rather than by careful coding. The relay calls it once it has
// enqueued the legacy task, so a row republished to Kafka after a crash does not
// enqueue a second webhook. It is the only member of this contract that the
// webhook sunset makes redundant; every other method outlives the sunset.
type eventOutbox interface {
	// Insert methods for atomic event capture
	InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error // Inserts an event outbox entry within an existing transaction
	InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error                 // Inserts an event outbox entry directly, outside any ledger transaction

	// Relay state machine: claim a batch, then drive each row to a terminal state.
	//
	// EVERY TRANSITION IS CONDITIONAL ON THE CLAIM TOKEN the claim issued, and
	// returns a typed conflict when no row matches. That is not defensive
	// bookkeeping: a transition matching on id alone let a worker whose lease had
	// expired overwrite the newer state of a row another instance had since taken,
	// let two workers each record an attempt against one claim and double-spend the
	// retry budget, and let a late call move a terminal row back out of its terminal
	// state. A caller that receives the conflict has lost the row and must stop
	// working on it — it must NOT treat its own publish as recorded.
	//
	// ClaimPendingEventOutbox additionally guarantees that AT MOST ONE ROW PER
	// PARTITION KEY is claimable at any instant, across all relay instances. FOR
	// UPDATE SKIP LOCKED alone does not give that: it stops two relays claiming the
	// same row but lets one skip an earlier locked row and claim a LATER row with
	// the same key, and because Kafka preserves append order rather than
	// occurred_at, a subscriber then observes one aggregate's events out of order
	// with nothing anywhere to show it happened.
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)                   // Claims pending entries FIFO for publishing, one row per partition key, taking a lease and stamping a claim token
	MarkEventDispatched(ctx context.Context, id int64, claimToken string) error                                                            // Marks a claimed entry dispatched after the broker acknowledges the publish
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration) (model.EventFailureOutcome, error) // Records a failed attempt against a claimed entry, schedules its next due instant and reports whether the budget is now spent

	// MarkEventDeadLettered completes the failure path: it moves an entry whose
	// retry budget MarkEventFailed already exhausted into the dead_lettered
	// terminal state, and records the dead-letter topic the event was written to
	// together with the marshaled failure metadata.
	//
	// The two halves are separate methods because they know different things.
	// MarkEventFailed knows only that a publish attempt failed and whether the
	// budget is spent, so it can say no more than "failed". Only the dead-letter
	// publisher knows whether the event actually reached its `<topic>.dlt`
	// sibling, and only it holds the resolved topic name and the composed
	// metadata — deriving either inside the repository would duplicate the
	// topic-naming rules and the metadata schema in a second place, and the two
	// copies would drift.
	//
	// This method is what makes the dead-letter surface work at all: dlt_topic and
	// failure_metadata are what the dead-letter inventory displays, and
	// dead_lettered is the state a replay requires before it will re-publish. Left
	// uncalled, those columns stay NULL and no event is ever replayable.
	// claimToken is the one MarkEventFailed returned on its exhaustion arm, or the
	// original claim token when a non-retryable failure dead-letters a row directly.
	// Requiring it is what stops two workers each writing the event to the
	// dead-letter topic: only one holds the token, so only one gets past this
	// transition, and the caller that fails it knows not to have published. Publish
	// to the dead-letter topic FIRST and record it here second, so a row is never
	// marked dead-lettered without a message behind it.
	MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage) error

	// ClaimEventForReplay atomically moves a dead-lettered row to replaying and
	// returns it with a fresh claim token, so a replay is a CLAIM rather than a read
	// followed by a check.
	//
	// It exists because the read-then-check form let two concurrent replays of one
	// event both see a dead_lettered row, both pass the check, and both publish — an
	// operator clicking twice, or two operators triaging the same backlog, putting
	// two copies on the topic. Only the caller whose update actually changed a row
	// proceeds to publish.
	//
	// An unknown event_id is a not-found; a row that exists but is not dead-lettered
	// is a conflict naming the state it is actually in, because replaying an
	// already-dispatched event and replaying a still-pending one are different
	// operator mistakes that deserve different answers.
	ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error)

	// ReleaseEventReplay returns a replaying row to dead_lettered, recording
	// replayErr in last_error when it is non-empty.
	//
	// It is the rollback half of ClaimEventForReplay, and the reason a failed replay
	// does not cost an event its replayability: without it, a replay that claimed a
	// row and then failed to publish would strand it in replaying, outside the
	// relay's claimable set and outside the dead-letter inventory, where nothing
	// would ever pick it up again. Every path out of a replay ends in either
	// MarkEventDispatched or this.
	ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error

	MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error // Marks the legacy webhook leg dispatched for a claimed row, so a republished row cannot double-enqueue it

	// Dead-letter and reporting reads
	GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error)               // Retrieves an entry by its business event_id UUID, for replay
	ListDeadLetteredEvents(ctx context.Context, limit, offset int) ([]model.EventOutbox, error) // Pages the dead-letter inventory for the dead-letter API

	// CountEventOutboxByStatus returns a status-keyed count of every row in
	// blnk.event_outbox.
	//
	// IT IS NOT UNUSED — do not delete it. It exists for one named purpose: the
	// daily zero-loss reconciliation, which passes when the dispatched plus
	// dead-lettered counts equal the sum of the main-topic and dead-letter-topic
	// end offsets reported by the broker. Three things consume it: the event
	// statistics endpoint, the reconciliation runbook in the Kafka operations
	// documentation, and the pending-backlog gauge exported to the metrics
	// pipeline. A grep for callers inside this package alone will find none,
	// which is exactly the trap this comment exists to prevent.
	//
	// The map is keyed by the model.EventOutboxStatus* values, and a status with
	// no rows is absent from the map rather than present with a zero — callers
	// must read it with the two-value form or accept the zero value.
	CountEventOutboxByStatus(ctx context.Context) (map[string]int64, error)

	// PurgeTerminalEventsBefore deletes at most limit TERMINAL rows whose
	// occurrence predates cutoff, and returns how many it removed. A caller sweeps
	// in a loop until fewer than limit come back.
	//
	// It is the retention primitive behind the outbox's data-minimisation contract,
	// and it is needed because of WHAT THIS TABLE HOLDS: payload is the webhook body
	// verbatim, so a transaction event carries amounts and balance identifiers and
	// an identity event carries names, email addresses, phone numbers, postal
	// addresses and dates of birth. Retained indefinitely, the delivery buffer
	// becomes an unbounded secondary copy of the ledger's most sensitive data with
	// none of the access controls the primary tables have around them.
	//
	// Only dispatched and dead_lettered rows are eligible. failed is deliberately
	// excluded even though its retry budget is spent: its dead-letter write is still
	// owed, so this table is the only copy of that event in existence.
	PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// eventSubscriber defines methods for the Kafka subscriber registry:
// blnk.event_subscribers. A row records one subscriber's identity together with
// the values that describe its access.
//
// # WHAT IS AND IS NOT AN ACCESS BOUNDARY (SEC-01)
//
// The ENFORCED boundary is exactly two things, because these are the two the broker
// evaluates on every request: the ACL bindings on the topics the subscriber is
// authorised for, and the ACL binding on its consumer group, both granted to the
// Kafka principal on the row. Provisioning translates the row into one SASL/SCRAM
// credential and those bindings; there are no per-tenant topics, so topic-level and
// group-level ACLs are the whole of the enforcement.
//
// partition_key_prefix is NOT a boundary and must never be described or relied upon
// as one. Kafka authorises reads at topic and group granularity — there is no ACL
// operation that restricts a principal to a subset of a topic's partitions or to
// records bearing a particular key, so a principal that may read a topic may read
// EVERY record in it regardless of what this column says. The column is an ADVISORY
// CONSUMER-SIDE FILTER: a hint a well-behaved subscriber may use to discard records
// it does not care about. Treating it as isolation would mean believing two
// subscribers on one topic cannot see each other's events, which is false, and
// would make that belief the basis of a tenancy decision.
//
// # NO METHOD HERE MAY ACCEPT OR RETURN A PLAINTEXT SECRET
//
// This is a hard constraint on what may be declared in this contract, not a
// guideline, and it is why the credential method below takes a reference rather
// than a password.
//
// The SASL secret is generated during provisioning, returned to the caller
// EXACTLY ONCE by the subscriber service, and persisted nowhere: it cannot be
// recovered afterwards, only replaced by issuing a new one. Only a
// non-reversible reference and the issuance instant are stored — the same posture
// as blnk.api_keys, where the key column holds a bcrypt hash and the raw key is
// never stored. RecordSubscriberCredential therefore takes a credentialReference
// that the CALLER has already derived, and no reader returns anything a caller
// could authenticate with.
//
// Do not add a method that takes or hands back a password, secret, SASL password,
// token, or any encrypted variant of one. Such a method would also be
// unimplementable: blnk.event_subscribers deliberately has NO column capable of
// holding a plaintext or reversibly-encrypted secret, so there is nothing for it
// to write to or read from. The prohibition lives in the schema precisely because
// a column that exists eventually gets written to, and removing a secret column
// that has already shipped and been populated is an incident rather than a
// migration.
//
// # Reads and error shape
//
// Single-entity reads return (*model.EventSubscriber, error) rather than a value
// and a boolean, so an implementation can distinguish "no such subscriber" from
// "the query failed" by returning an apierror-wrapped not-found instead of
// leaking a bare sql.ErrNoRows to the API layer.
type eventSubscriber interface {
	// Registry CRUD
	CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) // Registers a subscriber and returns the stored row
	GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)              // Retrieves a subscriber by its business subscriber_id
	ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error)                 // Pages the registry
	UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error                           // Updates a subscriber's access model and legacy webhook URL
	DeleteEventSubscriber(ctx context.Context, subscriberID string) error                                         // Removes a subscriber from the registry

	// TakeEventSubscriber removes a subscriber and RETURNS the row it removed,
	// so the caller holds the principal and authorised topics that broker-side
	// revocation needs — the very values a plain delete destroys unreported.
	//
	// This is the deletion to use whenever the broker side must also be
	// deprovisioned. Read-then-delete-then-revoke leaves a window in which the
	// row can change between the read and the delete, so what gets revoked may
	// not be the boundary that was actually in force; returning the deleted row
	// closes it.
	TakeEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)

	// RecordSubscriberCredential persists the outcome of a credential issuance:
	// a NON-REVERSIBLE reference to the credential and the instant it was
	// issued, which together are the complete stored record of an issuance. A
	// reissue overwrites both.
	//
	// credentialReference is already derived by the caller and MUST NOT be a
	// password or anything from which one can be recovered — see the prohibition
	// in this interface's documentation. A NULL credential reference on a row
	// means no credential has ever been issued to that subscriber, which is a
	// legitimate "registered, not yet provisioned" state.
	RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error

	// RecordSubscriberCredentialIfUnchanged persists an issuance only while the
	// subscriber still holds the reference the caller observed before it
	// provisioned at the broker. Pass expected == nil to require that no
	// credential has ever been issued.
	//
	// Issuance is not idempotent: each call mints a new secret and the broker
	// keeps only the last one written, so two concurrent issuances end with one
	// usable secret and two successful-looking responses. This write is how the
	// loser finds out — it matches no row and returns a CONFLICT, which a handler
	// turns into "your issuance was superseded" rather than handing back a
	// password that authenticates against nothing.
	//
	// It does not make issuance atomic across Blnk and the broker; nothing at
	// this layer can. It makes the divergence DETECTABLE while both outcomes are
	// still visible.
	RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time) error

	// ClearSubscriberCredential erases the credential reference and issuance
	// instant, returning the subscriber to the "registered, not yet
	// provisioned" state. It is the compensating half of
	// RecordSubscriberCredential.
	//
	// Broker-side deletion of the SCRAM credential is what actually ends
	// access; this is what stops the registry from continuing to claim a
	// credential that no longer exists, which every reader of the registry —
	// operator, migration report, reconciliation — would otherwise believe.
	// Revoke at the broker FIRST and clear here second, so a failure between
	// the two over-reports access rather than hiding it.
	//
	// Clearing a subscriber that holds no credential succeeds: the desired end
	// state is already true. A missing subscriber is still an error.
	ClearSubscriberCredential(ctx context.Context, subscriberID string) error

	// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber
	// has completed its move from legacy HTTP webhook delivery to Kafka
	// consumption. A NULL migrated_at means NOT YET MIGRATED, which is exactly
	// what migration-progress reporting counts during the dual-delivery window.
	MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error

	// PurgeMigratedSubscriberWebhookURLs erases the legacy webhook URL of every
	// subscriber whose migration completed strictly before the cut-off,
	// returning how many rows were purged.
	//
	// webhook_url exists solely to give an already-webhooked subscriber
	// somewhere to be migrated FROM. Once migrated it holds a third-party
	// endpoint with no remaining purpose — retention without a reason, and a
	// destination that becomes a request Blnk makes the moment any sender is
	// wired to it. migrated_at is deliberately KEPT: it is an audit fact the
	// migration report reads, not third-party data.
	//
	// The cut-off is the caller's, so the retention period stays a policy
	// decision. A zero cut-off is refused rather than read as "purge
	// everything".
	PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error)
}

// chain defines the hash-chain (tamper-evidence) operations.
type chain interface {
	ChainPendingTransactions(ctx context.Context, cutoff time.Time, batchSize int) (int, error)                     // Seals the next batch of unchained transactions
	GetChainState(ctx context.Context) (*model.ChainState, error)                                                   // Returns the global chain bookmark
	GetChainedTransactionsAfter(ctx context.Context, afterSeq int64, limit int) ([]model.ChainedTransaction, error) // Pages chained transactions in chain order
	CountUnchainedTransactions(ctx context.Context, cutoff time.Time) (int64, error)                                // Counts the chainer backlog
}
