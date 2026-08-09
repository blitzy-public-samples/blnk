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
	// balanceMonitorHandoff and bulkTransactionBatch are the two mechanisms that bring
	// the last two event families under requirement R-2. Neither event could be captured
	// inside the mutation that produced it — a monitor alert does not exist until its
	// balance is committed, and a bulk summary belongs to no single transaction — so each
	// gets a durable INTENT that IS written atomically, plus an atomic completion.
	balanceMonitorHandoff // Interface for balance monitor handoff operations
	bulkTransactionBatch  // Interface for bulk transaction batch coordinator operations
}

// transaction defines methods for handling transactions.
type transaction interface {
	// RecordTransaction records a new transaction, and — when the caller supplies an event
	// row — commits that row in the SAME database transaction as the insert. The rejection
	// path uses that arm so a REJECTED status and the transaction.rejected event announcing
	// it can no longer be separated by a crash. The tail is variadic so the frozen
	// transaction-queue callers keep compiling; see the note on the atomic writers below.
	RecordTransaction(cxt context.Context, txn *model.Transaction, eventOutbox ...*model.EventOutbox) (*model.Transaction, error)
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

// LedgerEventCapture builds the event outbox row for a newly created ledger, and is
// called by CreateLedger with the FINALISED ledger — after the identifier and the
// creation timestamp have been assigned and before anything is written.
//
// # Why a callback rather than a prepared row
//
// The three creation writers below assign the entity's identity themselves:
// CreateLedger generates the ledger id, CreateBalance the balance id, CreateIdentity
// the identity id, and all three stamp created_at. The event payload is that
// finalised entity — it is the exact object the HTTP webhook used to carry — so a row
// prepared by the caller BEFORE the write would name an entity with no identifier and
// no timestamp, and the payload would no longer match what a subscriber has always
// received.
//
// Inverting the call is what resolves that: the writer assigns identity, hands the
// finalised entity back through this function, and inserts the returned row inside the
// same SQL transaction as the entity itself. The event and the mutation then commit or
// roll back together, which is requirement R-2 at its narrowest.
//
// A nil return with a nil error means "capture nothing", which is how a deployment
// with no transport configured keeps the single-statement path it has always used.
// An error aborts the whole write, because an event that cannot be built is a defect
// rather than a transient condition, and committing the entity without it would lose
// the event permanently.
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
	// database transaction as the ledger itself, which is requirement R-2 for this producer:
	// the event's aggregate id and payload both depend on the id this method mints, so the
	// row cannot be prepared before the call and a callback is what makes atomicity
	// possible. The tail is variadic so every pre-existing caller compiles unchanged.
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
	// database transaction as the balance itself (requirement R-2). Nothing is captured on
	// the idempotent indicator-conflict path, where no balance is created; see the method's
	// own documentation. The tail is variadic so every pre-existing caller compiles unchanged.
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
	// CreateIdentity creates a new identity. When the caller supplies an EventPreparer, the
	// identity.created event row is built from the CREATED identity and inserted in the same
	// database transaction as the identity itself (requirement R-2). The tail is variadic so
	// every pre-existing caller compiles unchanged.
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
// # The two methods with a limited lifetime
//
// MarkWebhookDispatched serves the 30-day window during which Kafka publishing
// and legacy HTTP webhook delivery run side by side FROM THE SAME CLAIMED ROW —
// which is what makes the two transports carry byte-identical payloads
// structurally rather than by careful coding. The relay calls it once it has
// enqueued the legacy task, so a row republished to Kafka after a crash does not
// enqueue a second webhook.
//
// MarkEventWebhookPending serves the same window from the other direction: it
// records that the Kafka leg is complete while the legacy leg is still owed. It
// exists because the two legs used to share one terminal state, so a Kafka
// publish that succeeded marked the row dispatched even when the webhook enqueue
// beside it had failed — and dispatched is outside the claim predicate, so that
// webhook was never retried and never delivered.
//
// These two are the only members of this contract the webhook sunset makes
// redundant; every other method outlives the sunset.
type eventOutbox interface {
	// Insert methods for atomic event capture
	InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error // Inserts an event outbox entry within an existing transaction
	InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error                 // Inserts an event outbox entry directly, outside any ledger transaction

	// The three entity writers that COMMIT AN EVENT WITH THEIR MUTATION.
	//
	// Ledgers, identities and balances are each created by a single INSERT on the
	// pooled connection, so unlike a transaction there is no existing transaction
	// for an event to join. These variants open one, write the entity and its event
	// row together, and commit both or neither — which is what extends requirement
	// R-2's guarantee to the three creation events (ledger.created,
	// identity.created and balance.created) instead of leaving them to a
	// post-commit goroutine that a crash, a cancellation or a failed insert loses
	// permanently.
	//
	// Each takes AT MOST ONE event row as a variadic tail, and each drops nil
	// entries and falls back to the plain single INSERT when none is supplied. That
	// is the no-op-when-unconfigured contract inherited from SendWebhook:
	// PrepareEventOutbox returns nil with no brokers configured, so a caller passes
	// its result through unconditionally and gets the original behaviour.
	//
	// The unprefixed CreateLedger, CreateIdentity and CreateBalance above are
	// unchanged and share the same INSERT implementation, so neither the SQL nor the
	// error mapping can drift between the two shapes.

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
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) // Claims pending entries FIFO for publishing, one row per partition key, taking a lease and stamping a claim token
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error               // Marks a claimed entry dispatched after the broker acknowledges the publish, persisting the coordinate the broker assigned

	// MarkEventPermanentlyFailed records an attempt that failed PERMANENTLY, so the row
	// becomes failed on this attempt whatever budget remained and the dead-letter write
	// is owed at once.
	//
	// It is separate from MarkEventFailed because the decision is taken somewhere else.
	// MarkEventFailed asks the DATABASE whether the budget is spent, which is what stops
	// two racing instances both concluding they were the last attempt; this one carries a
	// verdict the PUBLISHER reached about the broker's answer — an unauthorised principal,
	// a destination outside the topic catalogue, bytes that will never parse, a message
	// over the size limit. Spending four more attempts on any of those establishes
	// nothing and delays the operator's sight of the event by the whole backoff schedule.
	//
	// It retains the claim token AND THE LEASE for the same reason MarkEventFailed's
	// exhaustion arm does: the dead-letter write and the transition that records it are
	// still owed, and only the worker holding the token may perform them. The lease is
	// what makes the token's exclusivity hold — ClaimFailedEventOutboxForDeadLetter
	// selects on the lease alone, so a released lease lets it stamp a fresh token over
	// the top and two workers write the same event to the same .dlt topic.
	//
	// deadLetterLease is how long that ownership lasts. Callers pass their own claim lock
	// duration, so the hand-off window matches the window every other claim uses; a
	// non-positive value is normalised rather than honoured.
	MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// MarkEventFailed's terminal parameter is the CALLER'S VERDICT that no further
	// attempt can succeed, and it is one input to the same in-SQL decision the attempt
	// arithmetic feeds. The publisher already classifies a failure as transient or
	// permanent; discarding that here returned a permanently unpublishable event to
	// pending to spend its whole backoff schedule rediscovering it, which delayed
	// preservation by the length of the schedule and reported a stuck event as a busy
	// one throughout.
	//
	// deadLetterLease applies to the EXHAUSTION ARM ONLY, where it holds the row for the
	// dead-letter hand-off; the retry arm releases the lease so the row becomes claimable
	// again at its next due instant. Releasing it on both arms is what let the repair
	// claim reclaim an exhausted row while its first dead-letter write was in flight.
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, terminal bool, deadLetterLease time.Duration) (model.EventFailureOutcome, error) // Records a failed attempt against a claimed entry, schedules its next due instant, and reports whether the budget is now spent — exhausting immediately when the caller declares the failure permanent

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
	MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage, record model.BrokerRecord) error

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
	//
	// It also takes over a replaying row whose LEASE HAS EXPIRED. Admitting only
	// dead_lettered left a row stranded by a crash — or by a release that failed on an
	// already-cancelled request context — permanently unreplayable, with the lease
	// written down and nothing ever reading it.
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

	// RenewEventOutboxLease extends the lease on every row still in flight under one
	// claim token and reports how many it extended.
	//
	// It is what makes a large batch safe under a short lease: the fixed lease could
	// expire while the relay still held and still intended to publish a row, so a second
	// instance claimed and published it and the first published it again. Every terminal
	// transition clears the token, so the statement's reach is exactly the unfinished set
	// and no id list is needed. A zero count is the ordinary end of a batch, not an error.
	RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error)

	// ClaimFailedEventOutboxForDeadLetter claims rows whose retry budget is spent and
	// whose dead-letter write has NOT succeeded (status failed, dlt_topic NULL), so the
	// preservation can be attempted again.
	//
	// Without it such a row was a dead end in three directions at once — outside the
	// ordinary claim predicate, refused by replay, and abandoned by the worker that held
	// its token — while being the only copy of the event in existence. The status stays
	// failed so the row remains in the dead-letter inventory throughout and can complete
	// through MarkEventDeadLettered, which accepts failed as a prior state.
	//
	// IT IS THE ONLY REPAIR CLAIM. A second implementation, ClaimEventsOwedDeadLetter,
	// existed alongside it with different status, budget and pacing semantics and no
	// interface entry, no caller, no mock and no test; it was removed, because two repair
	// designs where one is unreachable is one design and one liability — the unreachable
	// copy attracts the fixes and the live one keeps the bugs.
	//
	// It becomes eligible for a row only once that row's LEASE HAS EXPIRED, which is what
	// the terminal transitions' retained lease relies on: the worker that spent the last
	// attempt owns the hand-off for the lease duration, and this pass exists for the case
	// where that worker died inside it.
	ClaimFailedEventOutboxForDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error // Marks the legacy webhook leg dispatched for a claimed row, so a republished row cannot double-enqueue it
	// ClaimPendingWebhookDeliveries and MarkWebhookDispatched are the two halves of
	// the legacy leg of the dual-delivery window, and BOTH ARE DELETED at the
	// webhook sunset together with webhooks.go and the relay branch that calls
	// them. Every other method in this interface outlives it.
	//
	// The claim exists because the legacy enqueue is deliberately allowed to fail
	// without failing the Kafka publish — a webhook receiver being down must not
	// consume a Kafka retry attempt — and the Kafka publish then drives the row to
	// its terminal state, past everything the main claim predicate looks at. Without
	// an independent way back to that row the outstanding webhook was lost silently,
	// for precisely the subscribers that have not migrated yet.
	//
	// It admits ALL THREE Kafka end states — dispatched, failed and dead_lettered —
	// because the relay enqueues the webhook BEFORE it publishes, so when both legs fail
	// on the attempt that spends the retry budget the row goes terminal on the failure
	// path with its webhook still owed. Restricting the set to dispatched discarded that
	// webhook in the one circumstance where the webhook is the only transport that might
	// still work: the broker being unreachable is why the Kafka leg failed.
	//
	// It does NOT change the row's status, which is the property that makes it safe:
	// returning a dispatched row to processing would republish it to Kafka, and reviving a
	// dead-lettered one would hand a terminal event back to the publisher.
	ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) // Claims rows in any Kafka end state whose legacy webhook leg is still owed, taking a lease and stamping a claim token without altering status

	// MarkEventLegacyWebhookAttempted records one FAILED legacy enqueue against a row
	// whose Kafka leg has already finished, and reports whether the legacy budget is now
	// spent. DELETED at the webhook sunset with the rest of the legacy leg.
	//
	// It is the counterpart to MarkEventWebhookPending for the rows that one cannot
	// serve. MarkEventWebhookPending may move the status, because it only ever handles a
	// row whose Kafka publish SUCCEEDED. This one handles a row in any Kafka end state and
	// therefore must leave the status exactly as it is: marking a dead-lettered row
	// dispatched would assert a delivery that never happened in the column the zero-loss
	// audit reads, and moving it to webhook_pending would hand a terminal event back to
	// the publisher. The row's re-claimability comes from the legacy leg's own columns.
	//
	// last_error is NOT overwritten either. On a failed or dead-lettered row it holds the
	// KAFKA failure reason, which the dead-letter metadata and the API projection read;
	// replacing it with a transient queue error would destroy the diagnosis of the failure
	// that actually stranded the event.
	MarkEventLegacyWebhookAttempted(ctx context.Context, id int64, claimToken string, retryAfter time.Duration) (model.EventWebhookOutcome, error) // Records a failed legacy webhook enqueue against a row whose Kafka leg has finished, without altering that leg's state, and reports whether the legacy budget is spent

	// MarkEventWebhookPending records a Kafka leg that is COMPLETE alongside a
	// legacy webhook leg that is still OWED, and reports which arm the in-SQL
	// decision took: another webhook attempt (webhook_pending, claimable again
	// after retryAfter) or the legacy leg abandoned (dispatched, terminal on the
	// strength of the Kafka delivery alone).
	//
	// It increments webhook_attempts and never attempts, because a webhook
	// receiver being down must not consume a Kafka retry attempt — that would let
	// the deprecated transport dead-letter events on the new one.
	//
	// SUNSET: removed with MarkWebhookDispatched and the rest of the legacy leg.
	MarkEventWebhookPending(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, record model.BrokerRecord) (model.EventWebhookOutcome, error)

	// Dead-letter and reporting reads
	GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) // Retrieves an entry by its business event_id UUID, for replay

	// ListDeadLetterInventory pages the dead-letter inventory behind the dead-letter
	// API, with every filter applied in SQL and the page bounded by a KEYSET CURSOR.
	//
	// Two properties are load-bearing and both replaced something that was not
	// (PERF-P06, PERF-P08). The projection is NARROW — it reports the size of the
	// event body and never its bytes — so the cost of a page is set by the number of
	// items in it rather than by how large the events were; reading whole rows to
	// build a listing moved gibibytes to answer a question about kilobytes. And the
	// page resumes from a POSITION rather than a depth, so every page costs the same
	// and concurrent inserts cannot make a paging caller repeat or skip rows.
	//
	// A caller that needs an event's bytes is replaying it, and replay reads the full
	// row through its own claim.
	ListDeadLetterInventory(ctx context.Context, query model.DeadLetterInventoryQuery) (model.DeadLetterInventoryPage, error)

	// CountDeadLetterInventory counts the entries a listing query matches, ignoring its
	// page, so a total and the page it accompanies are driven from one predicate.
	CountDeadLetterInventory(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// ListDeadLetteredEvents pages the inventory as FULL rows, newest occurrence first, with
	// every narrowing applied in SQL. It is what the dead-letter service and the replay path
	// read; ListDeadLetterInventory is the narrow projection the operator listing pages.
	ListDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) ([]model.EventOutbox, error)

	// CountDeadLetteredEvents counts what the SAME narrowing the listing used matches,
	// ignoring the page, so the total and the page describe one set.
	CountDeadLetteredEvents(ctx context.Context, query model.DeadLetterQuery) (int64, error)

	// MarkEventDeadLetterResolved records an operator's account that a dead-lettered
	// event needs no further action, which is the ONLY thing that makes such a row
	// eligible for the retention purge.
	//
	// It is a conditional write requiring the dead_lettered state and an unset
	// resolution, so a row whose `<topic>.dlt` write is still owed is refused and an
	// already-resolved row is refused rather than re-stamped over its audit trail.
	MarkEventDeadLetterResolved(ctx context.Context, eventID string, note string, at time.Time) (*model.EventOutbox, error)

	// CountEventSubscribers counts the whole registry, which is what the listing's
	// include_count option answers with.
	CountEventSubscribers(ctx context.Context) (int64, error)

	// OldestDeadLetterAgeByTopic reports, per dead-letter topic, the age instant of the
	// oldest outstanding entry and how many are outstanding.
	//
	// It is what the dead-letter age gauge and the 15-minute alert are computed from, and
	// it is ONE grouped aggregate over an index (PERF-P07). The gauge previously counted
	// the whole inventory, derived a tail offset from that count and read up to 5,000 whole
	// rows from it on every collection interval — and past its cap reported a LOWER BOUND,
	// which an alert cannot fire on when the true age crosses the threshold and the bound
	// does not. This reading is exact.
	//
	// deadLetterSuffix names the `.dlt` sibling of a row whose dead-letter write has not
	// happened yet, so those rows appear on the same series rather than silently missing.
	OldestDeadLetterAgeByTopic(ctx context.Context, deadLetterSuffix string) ([]model.DeadLetterTopicAge, error)

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
	//
	// since BOUNDS THE DISPATCHED COUNT AND NOTHING ELSE (PERF-P04). Every other
	// status is counted exactly and in full however short the window, because those
	// populations are the ones an operator acts on and any of them can legitimately
	// be older than any window — a pending row stuck for three days must not vanish
	// from a one-day reading of the backlog. Only dispatched, the single population
	// that grows without bound, is restricted to the window; over the whole history
	// it is a sequential scan of a table that gains 43.2 million rows a day.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)

	// AuditEventRecordsInIntervals is the OUTBOX SIDE of the zero-loss
	// reconciliation, and it is what makes that reconciliation able to prove
	// anything at all.
	//
	// CountEventOutboxByStatus alone cannot. Comparing its terminal counts against
	// the broker's cumulative end offsets compares two populations that share no
	// baseline, no time window and no topic incarnation: outbox pruning shrinks one
	// side, Kafka retention deletes records the other side still counts, recreating
	// a topic resets it to zero, and foreign traffic on a shared topic inflates it
	// by an unknown amount. A surplus of redeliveries is then arithmetically
	// indistinguishable from a surplus masking an equal number of losses — ten lost
	// events plus ten redeliveries produce exactly the totals of a healthy
	// pipeline.
	//
	// This method replaces that arithmetic with a BOUNDED MAPPING. Given the
	// measured [first_offset, end_offset) window of each partition, it places every
	// row that claims a record — every row whose Kafka leg completed, plus every
	// dead-lettered row — into exactly one bucket: corroborated inside a window,
	// naming no record at all, naming an unmeasured topic or partition, aged out
	// below the window by retention, or AT OR ABOVE the log end, which is only
	// possible if the partition was truncated or the topic recreated. Nothing else
	// on the topic can affect the answer, so foreign traffic is never attributed to
	// Blnk, and ReconcileAgainstOutbox refuses a green verdict while any row is
	// uncorroborated.
	//
	// The published set is deliberately WIDER than the terminal statuses. A
	// webhook_pending row has been published to Kafka — its Kafka leg completed;
	// what is outstanding is the deprecated HTTP leg — so excluding it would leave
	// its record unaccounted for on the outbox side, loosening the reconciliation
	// during exactly the window it matters most.
	AuditEventRecordsInIntervals(ctx context.Context, intervals []model.PartitionOffsetInterval) (model.EventRecordIntervalAudit, error)

	// AuditEventRecordCoordinates reports, per (topic, partition), how many terminal
	// rows claim a broker record and what the extreme claimed offsets are.
	//
	// It is what turns the reconciliation from an inference into a CHECK.
	// AuditTerminalEventRecords establishes that every row names a record; this
	// establishes whether the records they name exist, by giving the verdict
	// coordinates it can compare against the broker's live per-partition bounds. A
	// claim at or beyond a partition's end offset names a record the log does not
	// contain, which is direct evidence rather than something deduced from totals;
	// a claim below the first retained offset has aged out and can no longer be
	// verified, which the verdict must report rather than assume away.
	//
	// Extremes rather than every offset: the two comparisons that matter are against
	// the largest and smallest claim, so one row per partition answers the question
	// at any table size.
	AuditEventRecordCoordinates(ctx context.Context) (model.EventRecordCoordinateAudit, error)

	// SumPurgedTerminalEvents reports what retention has DELETED from the outbox.
	//
	// Without it the reconciliation compares quantities that measure different
	// intervals. A broker end offset counts every record ever appended and never
	// falls; the outbox side counts rows that still exist. From the first purge
	// onward the broker counts all of history and the outbox a suffix of it, and
	// because the comparison already expects a surplus, that difference is
	// indistinguishable from expected overhead — so it can offset a genuine
	// shortfall exactly and yield a confident "no loss detected" while events are
	// missing.
	//
	// The totals restore a matched baseline. Its Recorded field distinguishes
	// "nothing has been purged" from "we cannot tell", because only the first
	// supports a conclusive verdict.
	SumPurgedTerminalEvents(ctx context.Context) (model.EventOutboxPurgeTotals, error)

	// ListUndrainedEventTopics groups every row that still owes a publish by the
	// DESTINATION TOPIC recorded on it, so a caller can tell whether any stored
	// destination lies outside the namespaces this deployment currently owns.
	//
	// It exists for one failure that no other query can see. A row records its
	// fully-resolved topic at insert time, so changing KAFKA_TOPIC_PREFIX leaves
	// committed rows naming the previous generation's topics. Those rows remain
	// publishable only while the old prefix is declared in
	// KAFKA_HISTORICAL_TOPIC_PREFIXES; declare nothing and they are stranded — the
	// publisher refuses the topic, the rows stay claimable for ever, and every
	// status count still reads as ordinary pending and dead-lettered work. Grouping
	// by topic is what turns that into a name an operator can act on.
	//
	// UNDRAINED covers two kinds of row, and both are required. Rows that have never
	// been dispatched — pending, processing, failed, replaying — each owe the relay a
	// publish to that exact topic. Dead-lettered rows are terminal for delivery but
	// REPLAYABLE, and a replay publishes to the ORIGINAL topic, so a prefix carrying
	// them is a prefix whose replays would be refused. Dispatched rows owe nothing.
	// webhook_pending is excluded too: its Kafka leg is complete and only the
	// deprecated HTTP leg is outstanding, which names no topic.
	//
	// The result is bounded by the number of DISTINCT topics in the table, which is
	// the category count times the number of prefixes that have ever been
	// configured — small enough to return whole rather than paginate.
	ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error)

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

	// ExistingEventIDs answers "which of these events are already durable?" in ONE
	// round trip, for the post-commit capture path's idempotency check.
	//
	// It exists because that path retries: a capture attempt that failed after the
	// commit is repeated, and repeating it must not insert a second row for an event
	// that landed the first time. Asking per event would make the check cost one
	// query per event in a batch, which is the shape that turns a safety check into a
	// throughput problem, so the whole set is asked at once.
	//
	// An EMPTY request issues no query and returns an empty set, because "none of
	// nothing exists" needs no database to establish.
	//
	// Parameters:
	//   - ctx context.Context: the request context.
	//   - eventIDs []string: the ids to test. Empty is answered without a query.
	//
	// Returns:
	//   - map[string]struct{}: exactly the supplied ids that already have a row.
	//   - error: a read failure. The set is then empty and the caller must NOT read
	//     that as "none exist" — see the implementation's own note.
	ExistingEventIDs(ctx context.Context, eventIDs []string) (map[string]struct{}, error)
}

// eventSubscriber defines methods for the Kafka subscriber registry:
// blnk.event_subscribers. A row records one subscriber's identity together with
// the values that describe its access.
//
// # WHAT IS AND IS NOT AN ACCESS BOUNDARY (SEC-01)
//
// The BROKER-ENFORCED boundary is exactly two things, because these are the two the
// broker evaluates on every request: the ACL bindings on the topics the subscriber is
// authorised for, and the ACL binding on its consumer group, both granted to the
// Kafka principal on the row. Provisioning translates the row into one SASL/SCRAM
// credential and those bindings; there are no per-tenant topics, so topic-level and
// group-level ACLs are the whole of the enforcement.
//
// partition_key_prefix is the REQUESTED KEY SCOPE, and it is a boundary the broker
// cannot evaluate: Kafka authorises reads at topic and group granularity, with no ACL
// operation that restricts a principal to a subset of a topic's partitions or to
// records bearing a particular key, so a principal that may read a topic may read
// EVERY record in it regardless of what this column says.
//
// So a non-empty value in that column is DELIVERED rather than bound: the credential
// endpoint returns it to the subscriber inside the same object that states the broker
// does not enforce it, and the subscriber's consumer is what discards records outside
// it. Calling the column an advisory filter, which this contract once did, was the
// dangerous framing, and refusing to issue for such a row — which it did next — was the
// over-correction: it withheld the credential instead of implementing the scope, so a
// key-scoped subscriber could not consume at all. What a reader of this row needs is the
// enforcement POINT, and that is what the column comment in the schema now states.
//
// The grant this contract DOES describe is reconciled rather than accumulated: every
// issuance and every authorization change deletes the bindings the current
// authorized_topics no longer implies, so narrowing the column narrows the boundary.
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
	// ListEventSubscribers pages the registry newest first, resuming from a KEYSET
	// CURSOR rather than an offset (PERF-P08, PERF-P22).
	//
	// Both of its callers need what the cursor gives them. The management API gets a
	// page whose cost is independent of its depth and which cannot repeat or skip a
	// row when a subscriber is registered mid-pagination. The consumer-lag collector
	// gets the ability to RESUME: it measures a bounded number of subscribers per
	// tick, and reading from the top every time measured the newest ones repeatedly
	// while never reaching the rest of the registry, so older subscribers had no lag
	// series at all and the lag alert could not fire for them.
	ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error)
	DeleteEventSubscriber(ctx context.Context, subscriberID string) error // Removes a subscriber from the registry

	// UpdateEventSubscriber replaces a subscriber's mutable columns — its access
	// model and its legacy webhook URL — under the caller's provisioning claim,
	// and ONLY while the row is not tombstoned for deregistration.
	//
	// Both conditions close a fail-OPEN path. Without the claim, an operation whose
	// lease had expired could still overwrite the authorization a new owner had just
	// reconciled with the broker, leaving the registry describing one boundary while
	// Kafka enforced another — and reporting success. Without the tombstone check, an
	// update could WIDEN authorized_topics on a subscriber whose revocation was
	// already in flight, and the caller's subsequent grant step would re-create
	// bindings for a principal that was supposed to be losing them.
	//
	// A miss is reported as a CONFLICT naming which condition failed, never as a
	// not-found: "your claim expired" and "this subscriber does not exist" call for
	// opposite responses.
	UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber, fenceToken string) error

	// TakeEventSubscriber removes a subscriber and RETURNS the row it removed,
	// so the caller holds the principal and authorised topics that broker-side
	// revocation needs — the very values a plain delete destroys unreported.
	//
	// This is the deletion to use whenever the broker side must also be
	// deprovisioned. Read-then-delete-then-revoke leaves a window in which the
	// row can change between the read and the delete, so what gets revoked may
	// not be the boundary that was actually in force; returning the deleted row
	// closes it.
	//
	// The deletion is conditional on claimToken still naming a live provisioning
	// claim. Deleting under a lapsed lease would strand whatever a concurrent
	// operation had just created on the broker, with no registry row left to name
	// the principal that has to be revoked.
	TakeEventSubscriber(ctx context.Context, subscriberID string, claimToken string) (*model.EventSubscriber, error)

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
	//
	// claimToken adds the second half of that detection. The credential
	// comparison catches a concurrent issuance that already recorded a different
	// reference; the fence condition catches this operation no longer being
	// entitled to write at all, which is the case that arises when a slow
	// broker round trip outlives the lease. A lost claim is reported distinctly
	// from a superseded credential, because only the former obliges the caller to
	// compensate for the principal it has already created.
	RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time, claimToken string) error

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
	//
	// It is conditional on the caller's provisioning claim. Clearing runs at the end
	// of a fenced operation, and under an expired lease an unconditional clear would
	// blank the record a NEWER issuance had just written — leaving the registry
	// reporting "not yet provisioned" for a subscriber holding a working credential.
	// That is the one direction this write must never move in, because it
	// UNDER-reports access and so nothing downstream has any reason to look at it.
	ClearSubscriberCredential(ctx context.Context, subscriberID, fenceToken string) error

	// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber
	// has completed its move from legacy HTTP webhook delivery to Kafka
	// consumption. A NULL migrated_at means NOT YET MIGRATED, which is exactly
	// what migration-progress reporting counts during the dual-delivery window.
	//
	// Prefer CompleteSubscriberWebhookMigration wherever a URL is being retired at
	// the same time. This stamps migrated_at ALONE, so calling it on a row that
	// still holds a webhook_url writes a row asserting the opposite of itself —
	// which the schema now refuses outright.
	MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error

	// CompleteSubscriberWebhookMigration forgets the legacy endpoint AND stamps
	// migrated_at in ONE statement, and is the write the cutover endpoint uses.
	//
	// Done as two writes — clear, then stamp — a failure in between left the row
	// absent from BOTH sides of the migration report: no webhook_url, so nothing
	// still to migrate FROM, and no migrated_at, so not counted as migrated. That
	// under-reports progress for the rest of the dual-run window, and nothing
	// prompts the repeat that would fix it, because the row no longer carries the
	// URL that would identify it as owing one.
	//
	// It is deliberately NOT fenced and NOT predicated on the revocation
	// tombstone: neither column has a broker counterpart, and the erasure of
	// third-party data must not sit behind another operation's state — which is
	// also why PurgeMigratedSubscriberWebhookURLs carries no such predicate.
	CompleteSubscriberWebhookMigration(ctx context.Context, subscriberID string, migratedAt time.Time) (*model.EventSubscriber, error)

	// ClearSubscriberWebhookURL forgets one subscriber's recorded legacy endpoint WITHOUT
	// claiming it migrated, which is the erasure a retention obligation asks for rather
	// than a migration transition.
	ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error

	// RecordSubscriberWebhookURL records a legacy endpoint and clears migrated_at in one
	// statement, so a row can never be migrated and still carry a live URL.
	RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error

	// RecordSubscriberGrantReconcilePending marks a subscriber as owing a
	// broker-side grant reconciliation.
	//
	// It is written BEFORE an authorization change touches the broker, not after
	// a failure, because the failure that matters most — the process
	// disappearing mid-change — cannot write anything. Recording the intent
	// first makes the obligation survive any outcome, including no outcome.
	//
	// Idempotent, and KEEPS THE FIRST INSTANT, so the obligation's age is the
	// age of the divergence rather than of the last retry.
	//
	// Fenced, like every other mutation here: an operation whose lease expired
	// must not raise an obligation about work a newer owner is now doing.
	RecordSubscriberGrantReconcilePending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// ClearSubscriberGrantReconcilePending discharges the grant-reconciliation
	// obligation, and resets the settlement counters only when nothing else
	// remains owed.
	//
	// The reset is conditional because the counters belong to whatever is still
	// outstanding: a row that also owes a credential cleanup keeps its history.
	ClearSubscriberGrantReconcilePending(ctx context.Context, subscriberID, fenceToken string) error

	// RecordSubscriberCredentialCleanupPending marks a subscriber as owing a
	// credential cleanup — a SCRAM credential may exist that Blnk intended to
	// destroy, or the row may name one that no longer works.
	//
	// Both failure paths that leave those states behind raise it: a compensation
	// that itself failed, and a confirmed compensation whose registry clear-up
	// then failed. Before this existed, each was a log line.
	//
	// Idempotent and first-instant-preserving, for the same reason as the grant
	// marker.
	RecordSubscriberCredentialCleanupPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// CountSubscriberSettlementObligations reports how many subscribers owe
	// broker-side reconciliation, split by kind, plus the oldest instant.
	//
	// An AGGREGATE rather than a scan, so the cost of observing the backlog does
	// not grow with it. Outstanding is counted with an OR rather than summed,
	// because one subscriber can owe both obligations.
	CountSubscriberSettlementObligations(ctx context.Context) (model.SubscriberSettlementBacklog, error)

	// GetSubscriberSettlementObligation reads what ONE subscriber currently
	// owes.
	//
	// The flags are RE-READ rather than carried from the scan because a
	// successful re-issuance discharges the credential-cleanup obligation in
	// between — provisioning upserts the principal's credential, replacing the
	// orphan the obligation was about — and acting on the stale flag would revoke
	// a credential that had just been issued.
	GetSubscriberSettlementObligation(ctx context.Context, subscriberID string) (model.SubscriberSettlementObligation, error)

	// ListSubscriberSettlementObligations returns the subscribers owing
	// broker-side work, oldest attempt first.
	//
	// UNFENCED, deliberately: the settlement worker owns no subscriber, and a
	// scan requiring a claim could never find the obligations left by an owner
	// that died holding one. The worker takes its own claim before it acts; this
	// only decides where to look.
	//
	// notBefore paces the retries — settlement talks to the broker that just
	// failed, so rows attempted more recently than the bound are skipped. A row
	// never attempted is always eligible. A zero bound returns everything owed.
	ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error)

	// MarkSubscriberSettlementAttempt records that a settlement pass tried a row
	// and what happened.
	//
	// UNFENCED for the same reason as the scan, and because an attempt that
	// could not be recorded would never pace the next one — turning the retry
	// interval into a busy loop against a broker that is already failing.
	//
	// It does NOT discharge either obligation: clearing is a separate,
	// deliberate write, so a pass that logged its attempt and then crashed
	// cannot be mistaken for one that succeeded. A row that no longer exists is
	// not an error, because a deregistered subscriber owes nothing.
	MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error

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

	// MarkSubscriberRevocationPending stamps the revocation tombstone and returns
	// the row, so deregistration can revoke at the broker BEFORE the row that
	// names the principal is deleted.
	//
	// Deleting first — which is what TakeEventSubscriber alone amounted to —
	// destroyed the principal and topic list revocation needs whenever that
	// revocation then failed, leaving live broker access that nothing in Blnk
	// could see. A tombstoned row is a durable to-do item instead: retrying the
	// deregistration finds it and finishes the job.
	//
	// It is idempotent and KEEPS THE FIRST INSTANT, because the value an operator
	// needs is how long the revocation has been outstanding.
	//
	// The marking is conditional on claimToken still naming a live provisioning
	// claim. The mark is the opening move of a deregistration and the rest of that
	// sequence belongs to the same caller, so announcing a revocation another
	// operation now owns would leave a live subscriber advertising an obligation
	// nobody is discharging.
	MarkSubscriberRevocationPending(ctx context.Context, subscriberID string, pendingAt time.Time, claimToken string) (*model.EventSubscriber, error)

	// CountSubscriberRevocationsPending reports how many subscribers still owe a
	// broker-side credential revocation and when the oldest obligation was recorded.
	//
	// It is an AGGREGATE rather than a listing, matching CountEventOutboxByStatus: the
	// cost of observing a backlog must not grow with the backlog, and the two gauges it
	// feeds — plus the alert on the age of the oldest — need a number, not the rows.
	// Without it those gauges were declared, initialised and never recorded, so their
	// alert rule read as healthy and could never fire.
	CountSubscriberRevocationsPending(ctx context.Context) (model.SubscriberRevocationBacklog, error)

	// CountSubscriberAccessResidue reports how much broker-side access is
	// UNACCOUNTED FOR: SCRAM credentials that outlived their registry record, and
	// revocations the broker refused.
	//
	// It is separate from the revocation backlog because an ORPHANED CREDENTIAL is
	// created by a failed ISSUANCE, which never stamps revocation_pending_at — so
	// that backlog and the alert built on it were structurally unable to see an
	// orphan, and the exposure's only representation was a log line. The refused
	// revocation is a subset of the pending rows, separated because a pending row
	// whose attempt actually failed needs the broker's authorization or reach fixed
	// before any retry can work, while one that merely started needs only the retry.
	CountSubscriberAccessResidue(ctx context.Context) (model.SubscriberAccessResidue, error)

	// MarkSubscriberCredentialOrphaned records that a credential exists at the
	// broker which Blnk could neither record nor revoke, making the exposure durable
	// and countable instead of a log line.
	//
	// IT IS DELIBERATELY UNFENCED. It is reached only when an issuance has already
	// failed AND its compensation has also failed, and one of the ways that happens
	// is the provisioning claim lapsing — so conditioning the marker on the claim
	// would leave the exposure unrecorded in exactly the case that produced it. It is
	// safe unfenced because it can only ADD a warning: it never grants, revokes or
	// changes an authorization, and both settlement paths clear it.
	//
	// It is idempotent and keeps the FIRST instant, so the age it reports is the age
	// of the exposure rather than of the last re-observation. A row that has since
	// been deleted is not an error.
	MarkSubscriberCredentialOrphaned(ctx context.Context, subscriberID string, orphanedAt time.Time) error

	// MarkSubscriberRevocationFailed records that the MOST RECENT revocation attempt
	// was refused by the broker. Unlike the pending tombstone it does not keep the
	// first instant — every new attempt clears it — so "pending set, failed set"
	// means the broker refused just now, while "pending set, failed NULL" means the
	// deregistration is in flight or awaiting deletion.
	//
	// Unfenced for the same reason as MarkSubscriberCredentialOrphaned. A row that has
	// since been deleted is not an error.
	MarkSubscriberRevocationFailed(ctx context.Context, subscriberID string, failedAt time.Time) error

	// ClaimSubscriberForProvisioning fences a subscriber for one issuance or
	// revocation and returns the token the claim is held under. A live claim held
	// by anybody else is reported as a CONFLICT.
	//
	// Kafka stores one SCRAM credential per principal, so two overlapping
	// issuances leave the broker holding one password while this table may hold a
	// reference derived from the other — and the caller holding the recorded one
	// cannot authenticate, with no way to find out. The conditional credential
	// write detects the database half of that race; it cannot decide which
	// password the BROKER kept. So the broker is only ever touched under this
	// claim, and the second caller is refused before it generates a secret.
	//
	// The claim is LEASED and the token is rotated on every claim, so a process
	// killed mid-issuance does not fence the subscriber for ever and a caller
	// whose lease expired cannot complete or release a claim somebody else now
	// holds.
	ClaimSubscriberForProvisioning(ctx context.Context, subscriberID string, lease time.Duration) (string, error)

	// RenewSubscriberProvisioningFence extends a claim the caller still holds,
	// without rotating the token.
	//
	// The lease has to satisfy two demands that pull against each other: it must
	// outlast a legitimate operation, or that operation loses its claim while
	// still working and two callers proceed at once; and it must be SHORT, because
	// a process killed while holding a claim fences the subscriber until the lease
	// expires and every second of that is a refused retry for a caller who did
	// nothing wrong. Renewal resolves the tension — the lease stays short, and a
	// caller that is still making progress says so.
	//
	// It cannot resurrect a lapsed claim. The renewal is conditional on the token
	// still being present AND the lease not yet expired, so once a claim has
	// lapsed the only way forward is a fresh claim, which the next caller may
	// already hold. A refused renewal is therefore the earliest possible notice
	// that exclusivity is gone, which is precisely when compensation is still
	// cheap.

	// ReleaseSubscriberProvisioningFence clears a claim the caller still holds, so
	// a retry after a fast failure does not have to wait out the whole lease. A
	// claim that is no longer the caller's is reported as a conflict rather than
	// swallowed: callers release in a deferred cleanup and log the outcome, and a
	// fence lost mid-operation is exactly the condition worth knowing about.
	ReleaseSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string) error

	// RenewSubscriberProvisioningFence extends a live claim the caller still holds.
	//
	// The lease has to be SHORT, because it is also the recovery time after a crash.
	// But the work done under it is a variable-length sequence of Kafka administrative
	// round trips — reconciling an access model prunes stale bindings and then grants
	// new ones — so no single fixed lease can be both short enough to recover quickly
	// and long enough for the worst case. Renewal separates the two: a caller that is
	// still making progress says so and gets one more lease of headroom.
	//
	// It is CONDITIONAL and it does not re-claim. A successful renewal is proof that
	// the caller still owns the subscriber at that instant, which is why it is called
	// immediately before each broker round trip; a refused one means the caller must
	// ABANDON the operation, because continuing is exactly how a stale owner comes to
	// overwrite the state a new owner has established. Reclaiming would defeat the
	// fence outright by letting two callers believe they held exclusive access.
	RenewSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string, lease time.Duration) error
}

// chain defines the hash-chain (tamper-evidence) operations.
type chain interface {
	ChainPendingTransactions(ctx context.Context, cutoff time.Time, batchSize int) (int, error)                     // Seals the next batch of unchained transactions
	GetChainState(ctx context.Context) (*model.ChainState, error)                                                   // Returns the global chain bookmark
	GetChainedTransactionsAfter(ctx context.Context, afterSeq int64, limit int) ([]model.ChainedTransaction, error) // Pages chained transactions in chain order
	CountUnchainedTransactions(ctx context.Context, cutoff time.Time) (int64, error)                                // Counts the chainer backlog
}

// balanceMonitorHandoff defines persistence for blnk.balance_monitor_handoff, the
// durable intent that a balance moved and its monitors have not been evaluated yet.
//
// # Why this exists as its own contract
//
// `balance.monitor` was the one event type that could not honour requirement R-2. A
// monitor fires because a CONDITION was met on a balance a transaction has ALREADY
// committed, so at the instant the alert exists there is no open transaction to enrol
// it in. Retrying the capture narrowed the window; it could not close it, because a
// process that dies between the commit and the insert loses the alert outright and
// leaves nothing to replay.
//
// The handoff splits the problem in two. The INTENT to evaluate is written inside the
// balance's own transaction — see recordBalanceMonitorHandoffs, called by the atomic
// writers — so a committed movement always carries its pending evaluation. The RESULT of
// that evaluation, zero or more event rows, is then written in one transaction with the
// handoff's transition to completed. Nothing in the sequence can lose an alert: a crash
// leaves a claimable handoff, not a missing event.
//
// # There is no in-transaction insert method here, and that is deliberate
//
// The insert is an unexported package function rather than an interface method. The
// writers call it directly, and keeping it off the interface means no caller outside
// this package can write a handoff OUTSIDE a ledger transaction — which would look like
// it worked and would reintroduce exactly the window the handoff exists to close.
type balanceMonitorHandoff interface {
	// ClaimPendingBalanceMonitorHandoffs leases a FIFO batch for evaluation, skipping
	// rows another processor holds. A claim is a lease, so a processor that dies has its
	// rows re-claimed once it expires.
	ClaimPendingBalanceMonitorHandoffs(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.BalanceMonitorHandoff, error)
	// CompleteBalanceMonitorHandoffWithEvents writes the alerts and the completion in ONE
	// transaction. An empty event list is the common case and still completes the row:
	// "evaluated, nothing fired" is a result, and recording it is what distinguishes it
	// from "never evaluated".
	CompleteBalanceMonitorHandoffWithEvents(ctx context.Context, handoffID string, events []*model.EventOutbox) error
	// MarkBalanceMonitorHandoffFailed returns the row to pending while attempts remain
	// and marks it failed once the budget is spent, leaving the record of an evaluation
	// that never happened rather than deleting it.
	MarkBalanceMonitorHandoffFailed(ctx context.Context, handoffID, reason string, permanent bool) error
	// CountBalanceMonitorHandoffByStatus answers the question the event outbox cannot:
	// whether a movement's monitors were evaluated at all.
	CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error)
}

// bulkTransactionBatch defines persistence for blnk.bulk_transaction_batches, the
// durable coordinator record for an asynchronous bulk batch.
//
// # Why this exists as its own contract
//
// `bulk_transaction.<status>` was the second event that could not honour R-2, for a
// different reason. A bulk request executes one transaction at a time, each under its own
// database transaction, so when the batch's OUTCOME becomes known every mutation it
// describes has already committed separately and there is no row the summary could be
// atomic with. The outcome lived only in the local variables of the goroutine that
// computed it, and a failed capture destroyed it permanently and unreconstructably.
//
// The coordinator gives the outcome somewhere durable to be, and the finalising
// transaction writes the outcome and the event together. Two states become unreachable:
// a recorded outcome with no event, and an event describing a batch still called
// in-progress.
type bulkTransactionBatch interface {
	// InsertBulkTransactionBatch records that a batch began, BEFORE any member
	// transaction runs. Idempotent on the batch id so a retried request does not fail a
	// batch because its coordinator was already recorded.
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
