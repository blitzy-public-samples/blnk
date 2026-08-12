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
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"math/big"
	"time"
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
	// balanceMonitorHandoff and bulkTransactionBatch serve the last two event families to
	// be brought under the requirement, and they are no longer the same kind of mechanism.
	balanceMonitorHandoff // Interface for balance monitor handoff operations
	bulkTransactionBatch  // Interface for bulk transaction batch coordinator operations
}

// transaction defines methods for handling transactions.
type transaction interface {
	// RecordTransaction records a new transaction, and — when the caller supplies an event
	// row — commits that row in the SAME database transaction as the insert. The rejection
	// path uses that arm so a REJECTED status and the transaction.rejected event
	// announcing it can no longer be separated by a crash. The tail is variadic so the
	// frozen transaction-queue callers keep compiling; see the note on the atomic writers
	// below.
	RecordTransaction(cxt context.Context, txn *model.Transaction, eventOutbox ...*model.EventOutbox) (*model.Transaction, error)
	RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) // Records a transaction with balance updates atomically
	// The three atomic writers below take their event outbox rows as a VARIADIC parameter,
	// and that is a deliberate, load-bearing choice rather than a stylistic one. DO NOT
	// "tidy" it into a positional parameter.
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

// LedgerEventCapture builds the event outbox row for a newly created ledger, and is
// called by CreateLedger with the FINALISED ledger — after the identifier and the
// creation timestamp have been assigned and before anything is written.
type LedgerEventCapture func(ledger model.Ledger) (*model.EventOutbox, error)

// BalanceEventCapture builds the event outbox row for a newly created balance. See
// LedgerEventCapture for why the call is inverted.
type BalanceEventCapture func(balance model.Balance) (*model.EventOutbox, error)

// IdentityEventCapture builds the event outbox row for a newly created identity. See
// LedgerEventCapture for why the call is inverted.
type IdentityEventCapture func(identity model.Identity) (*model.EventOutbox, error)

// ledger defines methods for handling ledgers.
type ledger interface {
	// CreateLedger creates a new ledger. When the caller supplies an EventPreparer, the
	// ledger.created event row is built from the CREATED ledger and inserted in the same
	// database transaction as the ledger itself, which is the requirement for this
	// producer: the event's aggregate id and payload both depend on the id this method
	// mints, so the row cannot be prepared before the call and a callback is what makes
	// atomicity possible. The tail is variadic so every pre-existing caller compiles
	// unchanged.
	CreateLedger(ledger model.Ledger, prepareEvent ...EventPreparer[model.Ledger]) (model.Ledger, error)
	GetAllLedgers(limit, offset int) ([]model.Ledger, error) // Retrieves all ledgers (legacy)
	GetLedgerByID(id string) (*model.Ledger, error)          // Retrieves a ledger by ID
	UpdateLedger(id, name string) (*model.Ledger, error)     // Updates a ledger's name

	// Advanced filtering methods
	GetAllLedgersWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Ledger, error)                                              // Retrieves ledgers with advanced filtering
	GetAllLedgersWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Ledger, *int64, error) // Retrieves ledgers with filtering, sorting, and count
}

// balance defines methods for handling balances.
type balance interface {
	// CreateBalance creates a new balance. When the caller supplies an EventPreparer, the
	// balance.created event row is built from the CREATED balance and inserted in the same
	// database transaction as the balance itself. Nothing is captured on the idempotent
	// indicator-conflict path, where no balance is created; see the method's own
	// documentation.
	CreateBalance(balance model.Balance, prepareEvent ...EventPreparer[model.Balance]) (model.Balance, error)
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
	// CreateIdentity creates a new identity. When the caller supplies an EventPreparer,
	// the identity.created event row is built from the CREATED identity and inserted in
	// the same database transaction as the identity itself. The tail is variadic so every
	// pre-existing caller compiles unchanged.
	CreateIdentity(identity model.Identity, prepareEvent ...EventPreparer[model.Identity]) (model.Identity, error)
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

// eventOutbox defines methods for the Kafka event-publishing transactional outbox:
type eventOutbox interface {
	// Insert methods for atomic event capture
	InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error // Inserts an event outbox entry within an existing transaction
	InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error                 // Inserts an event outbox entry directly, outside any ledger transaction

	// The three entity writers that commit an event WITH their mutation are declared on
	// the ledger, identity and balance interfaces above, as a variadic EventPreparer
	// tail. Each opens one transaction, writes the entity and its event row, and commits
	// both or neither; supplied no preparer, each falls back to the plain single INSERT,
	// which is the no-op-when-unconfigured contract inherited from SendWebhook.

	// Relay state machine: claim a batch, then drive each row to a terminal state.
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) // Claims pending entries FIFO for publishing, one row per partition key, taking a lease and stamping a claim token
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error               // Marks a claimed entry dispatched after the broker acknowledges the publish, persisting the coordinate the broker assigned

	// MarkEventPermanentlyFailed records an attempt that failed PERMANENTLY, so the row
	// becomes failed on this attempt whatever budget remained and the dead-letter write is
	// owed at once.
	MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// MarkEventFailed's terminal parameter is the CALLER'S VERDICT that no further attempt
	// can succeed, and it is one input to the same in-SQL decision the attempt arithmetic
	// feeds. It carries the publisher's transient-or-permanent classification into the
	// row, so a permanently unpublishable event exhausts at once rather than spending its
	// whole backoff schedule rediscovering that.
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, terminal bool, deadLetterLease time.Duration) (model.EventFailureOutcome, error) // Records a failed attempt against a claimed entry, schedules its next due instant, and reports whether the budget is now spent — exhausting immediately when the caller declares the failure permanent

	// MarkEventDeadLettered completes the failure path: it moves an entry whose retry
	// budget MarkEventFailed already exhausted into the dead_lettered terminal state, and
	// records the dead-letter topic the event was written to together with the marshaled
	// failure metadata.
	MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage, record model.BrokerRecord) error

	// ClaimEventForReplay atomically moves a dead-lettered row to replaying and returns it
	// with a fresh claim token, so a replay is a CLAIM rather than a read followed by a
	// check.
	ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error)

	// ReleaseEventReplay returns a replaying row to dead_lettered, recording replayErr in
	// last_error when it is non-empty.
	ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error

	// RenewEventOutboxLease extends the lease on every row still in flight under one claim
	// token and reports how many it extended.
	RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error)

	// ClaimFailedEventOutboxForDeadLetter claims rows whose retry budget is spent and
	// whose dead-letter write has NOT succeeded (status failed, dlt_topic NULL), so the
	// preservation can be attempted again.
	ClaimFailedEventOutboxForDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error // Marks the legacy webhook leg dispatched for a claimed row, so a republished row cannot double-enqueue it
	// ClaimPendingWebhookDeliveries and MarkWebhookDispatched are the two halves of the
	// legacy leg of the dual-delivery window, and BOTH ARE DELETED at the webhook sunset
	// together with webhooks.go and the relay branch that calls them. Every other method
	// in this interface outlives it.
	ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) // Claims rows in any Kafka end state whose legacy webhook leg is still owed, taking a lease and stamping a claim token without altering status

	// MarkEventLegacyWebhookAttempted records one FAILED legacy enqueue against a row
	// whose Kafka leg has already finished, and reports whether the legacy budget is now
	// spent. DELETED at the webhook sunset with the rest of the legacy leg.
	MarkEventLegacyWebhookAttempted(ctx context.Context, id int64, claimToken string, retryAfter time.Duration) (model.EventWebhookOutcome, error) // Records a failed legacy webhook enqueue against a row whose Kafka leg has finished, without altering that leg's state, and reports whether the legacy budget is spent

	// MarkEventWebhookPending records a Kafka leg that is COMPLETE alongside a legacy
	// webhook leg that is still OWED, and reports which arm the in-SQL decision took:
	MarkEventWebhookPending(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, record model.BrokerRecord) (model.EventWebhookOutcome, error)

	// Dead-letter and reporting reads
	GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) // Retrieves an entry by its business event_id UUID, for replay

	// ListDeadLetterInventory pages the dead-letter inventory behind the dead-letter API,
	// with every filter applied in SQL and the page bounded by a KEYSET CURSOR.
	ListDeadLetterInventory(ctx context.Context, query model.DeadLetterInventoryQuery) (model.DeadLetterInventoryPage, error)

	// CountDeadLetterInventory counts the entries a listing query matches, ignoring its
	// page, so a total and the page it accompanies are driven from one predicate.
	CountDeadLetterInventory(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// ListAndCountDeadLetterInventory answers both of the above from ONE SNAPSHOT, and is
	// what a listing that asked for a total goes through.
	ListAndCountDeadLetterInventory(
		ctx context.Context,
		query model.DeadLetterInventoryQuery,
	) (model.DeadLetterInventoryPage, int64, error)

	// ListDeadLetteredEvents pages the inventory as FULL rows, newest occurrence first,
	// with every narrowing applied in SQL. It is what the dead-letter service and the
	// replay path read; ListDeadLetterInventory is the narrow projection the operator
	// listing pages.
	ListDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) ([]model.EventOutbox, error)

	// CountDeadLetteredEvents counts what the SAME narrowing the listing used matches,
	// ignoring the page, so the total and the page describe one set.
	CountDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// CountEventSubscribers counts the whole registry, which is what the listing's
	// include_count option answers with.
	CountEventSubscribers(ctx context.Context) (int64, error)

	// OldestDeadLetterAgeByTopic reports, per dead-letter topic, the age instant of the
	// oldest outstanding entry and how many are outstanding.
	OldestDeadLetterAgeByTopic(ctx context.Context, deadLetterSuffix string) ([]model.DeadLetterTopicAge, error)

	// CountUnresolvedEventOutbox returns a status-keyed count of every row that has NOT
	// reached its terminal dispatched state, and reads no dispatched row at all.
	CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error)

	// CountEventOutboxByStatus returns a status-keyed count of every row in
	// blnk.event_outbox, INCLUDING the dispatched history inside the window.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)

	// AuditEventRecordsInIntervals is the OUTBOX SIDE of the zero-loss reconciliation, and
	// it is what makes that reconciliation able to prove anything at all.
	AuditEventRecordsInIntervals(ctx context.Context, intervals []model.PartitionOffsetInterval) (model.EventRecordIntervalAudit, error)

	// AuditEventRecordCoordinates reports, per (topic, partition), how many terminal rows
	// claim a broker record and what the extreme claimed offsets are.
	AuditEventRecordCoordinates(ctx context.Context) (model.EventRecordCoordinateAudit, error)

	// SumPurgedTerminalEvents reports what retention has DELETED from the outbox.
	SumPurgedTerminalEvents(ctx context.Context) (model.EventOutboxPurgeTotals, error)

	// ListUndrainedEventTopics groups every row that still owes a publish by the
	// DESTINATION TOPIC recorded on it, so a caller can tell whether any stored
	// destination lies outside the namespaces this deployment currently owns.
	ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error)

	// PurgeTerminalEventsBefore deletes at most limit TERMINAL rows whose occurrence
	// predates cutoff, and returns how many it removed. A caller sweeps in a loop until
	// fewer than limit come back.
	PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)

	// ExistingEventIDs answers "which of these events are already durable?" in ONE round
	// trip, for the post-commit capture path's idempotency check.
	//
	// Parameters:
	//   - ctx context.Context: the request context.
	//   - eventIDs []string: the ids to test. Empty is answered without a query.
	//
	// Returns:
	//   - map[string]struct{}: exactly the supplied ids that already have a row.
	//   - error: a read failure. The set is then empty and the caller must NOT read that
	//     as "none exist" — see the implementation's own note.
	ExistingEventIDs(ctx context.Context, eventIDs []string) (map[string]struct{}, error)
}

// eventSubscriber defines methods for the Kafka subscriber registry:
type eventSubscriber interface {
	// Registry CRUD
	CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) // Registers a subscriber and returns the stored row
	GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)              // Retrieves a subscriber by its business subscriber_id
	// ListEventSubscribers pages the registry newest first, resuming from a KEYSET CURSOR
	// rather than an offset.
	ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error)

	// ListAndCountEventSubscribers pages the registry and counts it from ONE SNAPSHOT, and
	// is what a listing that asked for a total goes through. Two reads on two connections
	// see two registries, so a subscriber registered between them makes the total describe
	// a set the page is not a slice of.
	ListAndCountEventSubscribers(
		ctx context.Context,
		query model.SubscriberPageQuery,
	) (model.SubscriberPage, int64, error)
	DeleteEventSubscriber(ctx context.Context, subscriberID string) error // Removes a subscriber from the registry

	// UpdateEventSubscriber replaces a subscriber's mutable columns — its access model and
	// its legacy webhook URL — under the caller's provisioning claim, and ONLY while the
	// row is not tombstoned for deregistration.
	UpdateEventSubscriber(
		ctx context.Context,
		subscriber *model.EventSubscriber,
		fenceToken string,
	) (*model.EventSubscriber, error)

	// TakeEventSubscriber removes a subscriber and RETURNS the row it removed, so the
	// caller holds the principal and authorised topics that broker-side revocation needs —
	// the very values a plain delete destroys unreported.
	TakeEventSubscriber(ctx context.Context, subscriberID string, claimToken string) (*model.EventSubscriber, error)

	// RecordSubscriberCredential persists the outcome of a credential issuance: a
	// NON-REVERSIBLE reference to the credential and the instant it was issued, which
	// together are the complete stored record of an issuance. A reissue overwrites both.
	RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error

	// RecordSubscriberCredentialIfUnchanged persists an issuance only while the subscriber
	// still holds the reference the caller observed before it provisioned at the broker.
	RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time, claimToken string) error

	// ClearSubscriberCredential erases the credential reference and issuance instant,
	// returning the subscriber to the "registered, not yet provisioned" state. It is the
	// compensating half of RecordSubscriberCredential.
	ClearSubscriberCredential(ctx context.Context, subscriberID, fenceToken string) error

	// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber has
	// completed its move from legacy HTTP webhook delivery to Kafka consumption. A NULL
	// migrated_at means NOT YET MIGRATED, which is exactly what migration-progress
	// reporting counts during the dual-delivery window.
	MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error

	// CompleteSubscriberWebhookMigration forgets the legacy endpoint AND stamps
	// migrated_at in ONE statement, and is the write the cutover endpoint uses.
	CompleteSubscriberWebhookMigration(ctx context.Context, subscriberID string, migratedAt time.Time) (*model.EventSubscriber, error)

	// ClearSubscriberWebhookURL forgets one subscriber's recorded legacy endpoint WITHOUT
	// claiming it migrated, which is the erasure a retention obligation asks for rather
	// than a migration transition.
	ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error

	// RecordSubscriberWebhookURL records a legacy endpoint and clears migrated_at in one
	// statement, so a row can never be migrated and still carry a live URL.
	RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error

	// RecordSubscriberGrantReconcilePending marks a subscriber as owing a broker-side
	// grant reconciliation.
	RecordSubscriberGrantReconcilePending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// ClearSubscriberGrantReconcilePending discharges the grant-reconciliation obligation,
	// and resets the settlement counters only when nothing else remains owed.
	ClearSubscriberGrantReconcilePending(ctx context.Context, subscriberID, fenceToken string) error

	// RecordSubscriberCredentialCleanupPending marks a subscriber as owing a credential
	// cleanup — a SCRAM credential may exist that Blnk intended to destroy, or the row may
	// name one that no longer works.
	RecordSubscriberCredentialCleanupPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// CountSubscriberSettlementObligations reports how many subscribers owe broker-side
	// reconciliation, split by kind, plus the oldest instant.
	CountSubscriberSettlementObligations(ctx context.Context) (model.SubscriberSettlementBacklog, error)

	// GetSubscriberSettlementObligation reads what ONE subscriber currently owes.
	GetSubscriberSettlementObligation(ctx context.Context, subscriberID string) (model.SubscriberSettlementObligation, error)

	// ListSubscriberSettlementObligations returns the subscribers owing broker-side work,
	// oldest attempt first.
	ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error)

	// MarkSubscriberSettlementAttempt records that a settlement pass tried a row and what
	// happened.
	MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error

	// PurgeMigratedSubscriberWebhookURLs erases the legacy webhook URL of every subscriber
	// whose migration completed strictly before the cut-off, returning how many rows were
	// purged.
	PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error)

	// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row,
	// so deregistration can revoke at the broker BEFORE the row that names the principal
	// is deleted.
	MarkSubscriberRevocationPending(ctx context.Context, subscriberID string, pendingAt time.Time, claimToken string) (*model.EventSubscriber, error)

	// CountSubscriberRevocationsPending reports how many subscribers still owe a
	// broker-side credential revocation and when the oldest obligation was recorded.
	CountSubscriberRevocationsPending(ctx context.Context) (model.SubscriberRevocationBacklog, error)

	// CountSubscriberAccessResidue reports how much broker-side access is UNACCOUNTED FOR:
	CountSubscriberAccessResidue(ctx context.Context) (model.SubscriberAccessResidue, error)

	// MarkSubscriberCredentialOrphaned records that a credential exists at the broker
	// which Blnk could neither record nor revoke, making the exposure durable and
	// countable instead of a log line.
	MarkSubscriberCredentialOrphaned(ctx context.Context, subscriberID string, orphanedAt time.Time) error

	// MarkSubscriberRevocationFailed records that the MOST RECENT revocation attempt was
	// refused by the broker. Unlike the pending tombstone it does not keep the first
	// instant — every new attempt clears it — so "pending set, failed set" means the
	// broker refused just now, while "pending set, failed NULL" means the deregistration
	// is in flight or awaiting deletion.
	MarkSubscriberRevocationFailed(ctx context.Context, subscriberID string, failedAt time.Time) error

	// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation
	// and returns the token the claim is held under. A live claim held by anybody else is
	// reported as a CONFLICT.
	ClaimSubscriberForProvisioning(ctx context.Context, subscriberID string, lease time.Duration) (string, error)

	// RenewSubscriberProvisioningFence extends a claim the caller still holds, without
	// rotating the token.

	// ReleaseSubscriberProvisioningFence clears a claim the caller still holds, so a retry
	// after a fast failure does not have to wait out the whole lease. A claim that is no
	// longer the caller's is reported as a conflict rather than swallowed: callers release
	// in a deferred cleanup and log the outcome, and a fence lost mid-operation is exactly
	// the condition worth knowing about.
	ReleaseSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string) error

	// RenewSubscriberProvisioningFence extends a live claim the caller still holds.
	RenewSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string, lease time.Duration) error
}

// chain defines the hash-chain (tamper-evidence) operations.
type chain interface {
	ChainPendingTransactions(ctx context.Context, cutoff time.Time, batchSize int) (int, error)                     // Seals the next batch of unchained transactions
	GetChainState(ctx context.Context) (*model.ChainState, error)                                                   // Returns the global chain bookmark
	GetChainedTransactionsAfter(ctx context.Context, afterSeq int64, limit int) ([]model.ChainedTransaction, error) // Pages chained transactions in chain order
	CountUnchainedTransactions(ctx context.Context, cutoff time.Time) (int64, error)                                // Counts the chainer backlog
}

// balanceMonitorHandoff defines persistence for blnk.balance_monitor_handoff, the row
// that carries a committed movement's monitor evaluation — and every input that
// evaluation depends on — until it has been performed.
type balanceMonitorHandoff interface {
	// ClaimPendingBalanceMonitorHandoffs leases a FIFO batch for evaluation, skipping rows
	// another processor holds. A claim is a lease, so a processor that dies has its rows
	// re-claimed once it expires.
	ClaimPendingBalanceMonitorHandoffs(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.BalanceMonitorHandoff, error)
	// CompleteBalanceMonitorHandoffWithEvents writes the alerts and the completion in ONE
	// transaction. An empty event list is the common case and still completes the row:
	CompleteBalanceMonitorHandoffWithEvents(ctx context.Context, handoffID string, events []*model.EventOutbox) error
	// MarkBalanceMonitorHandoffFailed returns the row to pending while attempts remain and
	// marks it failed once the budget is spent, leaving the record of an evaluation that
	// never happened rather than deleting it.
	MarkBalanceMonitorHandoffFailed(ctx context.Context, handoffID, reason string, permanent bool) error
	// CountBalanceMonitorHandoffByStatus answers the question the event outbox cannot:
	CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error)
}

// bulkTransactionBatch defines persistence for blnk.bulk_transaction_batches, the
// durable coordinator record for an asynchronous bulk batch.
type bulkTransactionBatch interface {
	// InsertBulkTransactionBatch records that a batch began, BEFORE any member transaction
	// runs. Idempotent on the batch id so a retried request does not fail a batch because
	// its coordinator was already recorded.
	InsertBulkTransactionBatch(ctx context.Context, batch *model.BulkTransactionBatch) error
	// FinalizeBulkTransactionBatchWithEvent performs the terminal transition and the
	// outcome event insert in ONE transaction. It reports false when it finds the batch
	// already finalised with the same outcome, which is what makes a retry after a lost
	// acknowledgement stop instead of recording a second event for one outcome.
	FinalizeBulkTransactionBatchWithEvent(ctx context.Context, batchID string, outcome *model.BulkTransactionBatch, event *model.EventOutbox) (bool, error)
	// CountUnfinalizedBulkTransactionBatches counts batches that began and never reported
	// an outcome, past a grace period so batches still legitimately running are not
	// reported as stuck. This is the one window the coordinator cannot close, so it is
	// made countable rather than left as an absence nobody can query.
	CountUnfinalizedBulkTransactionBatches(ctx context.Context, olderThan time.Duration) (int64, *time.Time, error)
}
