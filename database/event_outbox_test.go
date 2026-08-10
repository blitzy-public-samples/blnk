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

// event_outbox_test.go is the repository test suite for blnk.event_outbox.
//
// # Why this file is structured in two tiers
//
// The suite deliberately runs at two levels, because each proves something the
// other physically cannot:
//
//   - TIER 1 — go-sqlmock. Deterministic assertions about query SHAPE, argument
//     binding, the mark transitions, error mapping, and — most importantly —
//     transaction participation. It runs everywhere with no infrastructure, so it
//     is what guards the invariants on every developer machine and in CI.
//   - TIER 2 — a real PostgreSQL instance, every test suffixed _RealDB. FOR UPDATE
//     SKIP LOCKED, lease expiry and FIFO ordering under concurrent claimers have no
//     meaning at all under a mock: sqlmock replays scripted expectations and has no
//     locking semantics, no planner and no clock. These tests skip cleanly through
//     openRealTestDB when no database is reachable, so the default suite stays
//     green without infrastructure.
//
// # The invariants this file exists to defend
//
// Four of them would survive every naive happy-path test while being completely
// broken, which is precisely why each gets an explicit, commented assertion:
//
//  1. The event row is inserted INSIDE the caller's ledger transaction. Move the
//     insert below tx.Commit() and every ordering test, every count test and every
//     round-trip test still passes — while the transactional-outbox guarantee is
//     gone. TestInsertEventOutboxInTx_ParticipatesInCallerTransaction and its
//     _RealDB counterpart are the only things that catch it.
//  2. The claim orders by occurred_at, NOT created_at. The neighbouring lineage
//     claim orders by created_at, so a copy-paste "fix" in that direction is the
//     single most likely regression here, and it would leave the whole suite green
//     apart from TestClaimPendingEventOutbox_FifoByOccurredAt_RealDB.
//  3. FOR UPDATE SKIP LOCKED is still in the claim query. Without it concurrent
//     relay instances block each other or claim the same row twice.
//  4. Payload bytes are bound through untransformed. A re-marshal or a
//     map[string]interface{} round trip inside the repository would break the
//     dual-delivery and replay guarantees, and a test comparing UNMARSHALLED values
//     would not notice, because key order is exactly what a re-marshal changes.
//
// # One note on payload byte equality
//
// The body is stored twice: payload as JSONB, which PostgreSQL normalises, and
// payload_raw as BYTEA, which returns the producer's exact bytes. event_outbox.go
// documents the split at length. Reads project payload_raw.
//
// The consequence for this file is a hard rule, and both tiers assert the SAME thing:
//
//   - TIER 1 asserts BYTE identity of the value BOUND to the statement, before any
//     database has touched it — the only place the repository itself could corrupt the
//     bytes.
//   - TIER 2 asserts BYTE identity of the value READ BACK, because payload_raw returns
//     what was written. Asserting mere JSON equivalence here would pass just as well
//     against a projection that had gone back to the JSONB column, which is exactly the
//     regression the second column exists to prevent.
package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSettlementLease is the exclusive window the repository tests hand to the terminal
// transitions as their settlement lease.
//
// It matches defaultEventRelayLockDuration, which is what the relay actually passes, so these
// tests exercise the plumbed value rather than the zero-value fallback. A test that needs the
// lease to have EXPIRED manipulates locked_until directly instead of shortening this, because
// the point of the lease is that it is held for a duration a live worker plausibly needs.
const testSettlementLease = 30 * time.Second

// ---------------------------------------------------------------------------
// Shared scaffolding
//
// Every helper below is named so it cannot collide with the identifiers this
// package's other test files already declare — openRealTestDB, eqFilter,
// mockCache, newMockCache, quiesceOutbox and newOutboxEntry are all defined
// elsewhere in package database and are reused, never redeclared.
// ---------------------------------------------------------------------------

// testDeadLetterHandoffLease is the lease every terminal transition in these tests holds
// the row under while its dead-letter write is owed.
//
// It is deliberately LONGER than any single test spends between the terminal transition and
// the assertion that follows it, because a lease that lapsed mid-test would let
// ClaimFailedEventOutboxForDeadLetter reclaim the row and the assertion would then be
// measuring the repair pass rather than the transition under test.
//
// It is also deliberately not the production default: passing the default here would let a
// caller that forgot to plumb its lock duration through pass zero and still see the correct
// value applied by normalizeDeadLetterHandoffLease, so the tests could not tell a supplied
// lease from a normalised one. A distinct value makes the argument observable.
const testDeadLetterHandoffLease = 45 * time.Second

// requireAPIError asserts that err is an apierror.APIError carrying the expected code.
//
// It deliberately returns NOTHING. An earlier version handed the decoded APIError back
// "in case a caller wants to assert on the message", which no caller ever did — and
// because apierror.APIError satisfies the error interface, every call site then looked to
// errcheck like a discarded error return. Dropping the result removes that noise at its
// root and makes the contract honest: this is an assertion, not a getter.
//
// The assertion goes through errors.As and the typed Code rather than through a
// message-string comparison, for two reasons. A message comparison passes for the
// wrong reason the moment two different failures happen to share wording, and it
// breaks on a purely cosmetic rewording that changes no behaviour. The Code is the
// contract the API layer actually maps to an HTTP status.
//
// errors.As rather than a bare type assertion, because two callers in
// transaction.go wrap the repository's error with fmt.Errorf("...: %w", err); a
// type assertion would fail there for a reason that has nothing to do with the
// behaviour under test. Note that apierror.APIError is a VALUE type with a value
// receiver on Error(), so the target is &apierror.APIError{} and never a
// **APIError.
func requireAPIError(t *testing.T, err error, want apierror.ErrorCode) {
	t.Helper()
	require.Error(t, err, "expected an error carrying apierror code %s", want)

	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr),
		"expected an apierror.APIError (possibly wrapped), got %T: %v", err, err)
	assert.Equal(t, want, apiErr.Code, "unexpected apierror code (message was %q)", apiErr.Message)
}

// errorDetailText renders an apierror's Details so a test can assert on the diagnostic the
// caller receives, not only on the code and message.
//
// APIError.Error() formats "CODE: message" and deliberately omits Details, so a marker phrase
// carried in the wrapped cause — the way subscriberFenceLost carries the one blnk's
// subscriberFenceWasLost matches on — is invisible to a plain err.Error() assertion. A test that
// only checked the message would pass whether or not the marker survived, which is the opposite
// of what it is for.
//
// Parameters:
//   - err error: the error to inspect. Anything that is not an APIError, and any APIError with
//     no details, renders as the empty string.
//
// Returns:
//   - string: the rendered details.
func errorDetailText(err error) string {
	var apiErr apierror.APIError
	if !errors.As(err, &apiErr) || apiErr.Details == nil {
		return ""
	}

	if detail, ok := apiErr.Details.(error); ok {
		return detail.Error()
	}

	return fmt.Sprint(apiErr.Details)
}

// capturedArg is a sqlmock.Argument that records the value bound in its position
// and then matches unconditionally.
//
// It exists because sqlmock's WithArgs compares with reflect.DeepEqual and reports
// only "argument N does not match", which for a JSON payload or a timestamp is
// close to useless. Capturing the value lets the assertion live in the test body,
// where it can produce a diff that names the actual bytes.
type capturedArg struct {
	into *driver.Value
}

// captureArg returns an Argument that stores the bound value into the supplied
// pointer. It always matches, so the assertion is the test's responsibility.
func captureArg(into *driver.Value) sqlmock.Argument {
	return capturedArg{into: into}
}

// Match implements sqlmock.Argument.
func (c capturedArg) Match(v driver.Value) bool {
	*c.into = v
	return true
}

// newCapturingSQLMock returns a sqlmock whose query matcher RECORDS the exact SQL
// text of every statement the code under test issues, in order, and then matches
// unconditionally.
//
// This is what makes the convention-enforcement assertions possible. sqlmock's
// default matcher treats the expected string as a regular expression and tells you
// only whether it matched, so asserting "the claim query still contains FOR UPDATE
// SKIP LOCKED" through it would mean encoding the property as a regex and reading a
// match failure as evidence — indirect, and hostile to debug. Capturing the literal
// text lets the test assert on the statement the database would really have
// received and print it verbatim when the assertion fails.
//
// Matching unconditionally is safe: sqlmock still enforces expectation ORDER, still
// distinguishes a Query from an Exec from a Begin, and still compares bound
// arguments, so every other guarantee of the mock is intact.
func newCapturingSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *[]string) {
	t.Helper()

	captured := &[]string{}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(
		sqlmock.QueryMatcherFunc(func(_, actualSQL string) error {
			*captured = append(*captured, actualSQL)
			return nil
		}),
	))
	require.NoError(t, err, "failed to create capturing sqlmock")
	t.Cleanup(func() { _ = db.Close() })

	return db, mock, captured
}

// newSQLMock returns a sqlmock using the package's default regexp matcher, wired to
// close itself on cleanup. It is the shape every other test file in this package
// uses; it is wrapped only so the Close is never forgotten.
func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err, "failed to create sqlmock")
	t.Cleanup(func() { _ = db.Close() })

	return db, mock
}

// eventOutboxProjectedColumns derives the projected column names FROM
// eventOutboxColumns rather than restating them.
//
// Deriving them is the point. A column added to the projection but not to
// scanEventOutbox is a scan-order bug no compiler catches, and a hand-maintained
// duplicate of the list here would hide exactly that: the mocked rows would keep
// agreeing with the test's copy while disagreeing with the real query. Reading the
// constant means the mock's row shape is always the query's row shape.
func eventOutboxProjectedColumns() []string {
	raw := strings.Split(eventOutboxColumns, ",")
	columns := make([]string, 0, len(raw))
	for _, name := range raw {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	return columns
}

// eventOutboxRow renders one model.EventOutbox as the driver values a real
// projection of eventOutboxColumns would produce, in that exact order.
//
// The nullable columns are rendered as SQL NULL when their Go zero value means
// "unset", which is what lets a single builder drive both the fully-populated and
// the all-NULL scan tests. Integers are rendered as int64 and the two JSONB columns
// as []byte because that is what a real driver hands back; using a Go int or a
// string here would let a scan pass under the mock that fails against PostgreSQL.
func eventOutboxRow(e model.EventOutbox) []driver.Value {
	nullTime := func(ts *time.Time) driver.Value {
		if ts == nil {
			return nil
		}
		return *ts
	}
	nullText := func(s string) driver.Value {
		if s == "" {
			return nil
		}
		return s
	}

	var failureMetadata driver.Value
	if len(e.FailureMetadata) > 0 {
		failureMetadata = []byte(e.FailureMetadata)
	}

	return []driver.Value{
		e.ID,
		e.EventID,
		e.EventType,
		e.AggregateID,
		e.PartitionKey,
		// ledger_id is NULLABLE and NULL is the documented "this event has no ledger"
		// value, so an empty Go string is rendered as SQL NULL rather than as ''. The
		// column's own CHECK constraint refuses '', so rendering it would exercise a
		// row PostgreSQL would never store.
		nullText(e.LedgerID),
		e.Topic,
		int64(e.SchemaVersion),
		[]byte(e.Payload),
		// event_raw is NOT NULL on the table, so it is rendered as plain bytes. A fixture
		// that rendered NULL here would exercise a row PostgreSQL refuses, and one that
		// left it empty would make the scanned row fall back to composing its envelope
		// instead of reading the stored one.
		e.EventRaw,
		e.OccurredAt,
		e.Status,
		int64(e.Attempts),
		int64(e.MaxAttempts),
		// next_attempt_at is NOT NULL on the table with a NOW() default, so it always has
		// a value and is rendered as a plain instant rather than through nullTime.
		e.NextAttemptAt,
		nullText(e.LastError),
		nullTime(e.FirstAttemptedAt),
		nullTime(e.LastAttemptedAt),
		nullTime(e.DispatchedAt),
		nullTime(e.LockedUntil),
		nullText(e.ClaimToken),
		// The three per-leg delivery columns, in projection order. Both booleans are NOT NULL
		// with a FALSE default and legacy_attempts is NOT NULL with a 0 default, so all three
		// are rendered as plain values rather than through the null helpers.
		e.WebhookDispatched,
		// kafka_dispatched_at is nullable and its NULL is meaningful: "the Kafka leg of
		// this row is not done". SUNSET: this column goes with the legacy leg.
		nullTime(e.KafkaDispatchedAt),
		int64(e.WebhookAttempts),
		// The BROKER COORDINATE is rendered all-or-nothing, mirroring the CHECK
		// constraint that refuses a partial triple on the table. All three NULL means
		// "no write to this row has been acknowledged yet" — the state the audit counts
		// as unconfirmed — so a fixture that left, say, kafka_topic populated while the
		// offset was NULL would exercise a row PostgreSQL would never store.
		//
		// The partition and offset are rendered as int64 because that is what a real
		// driver hands back for INTEGER and BIGINT; the scanner reads them through
		// sql.NullInt64 and sql.NullInt32-free int64 destinations, so a Go int here
		// would let a scan pass under the mock that fails against PostgreSQL.
		nullText(e.KafkaTopic),
		nullInt64(func() *int64 {
			if e.KafkaPartition == nil {
				return nil
			}
			p := int64(*e.KafkaPartition)
			return &p
		}()),
		nullInt64(e.KafkaOffset),
		nullText(e.DLTTopic),
		failureMetadata,
		// The two TRACE-CONTEXT columns, both nullable, and NULL is the common case: an event
		// captured with no active trace records neither. Rendered through nullText for the
		// reason ledger_id is — the columns' CHECK constraints bound the length and the
		// service never writes '', so a fixture rendering an empty string would exercise a
		// row nothing produces.
		nullText(e.Traceparent),
		nullText(e.Tracestate),
	}
}

// nullInt64 renders an optional integer as the driver value a nullable INTEGER or
// BIGINT column produces: SQL NULL when absent, an int64 when present.
//
// Absence has to be distinguishable from zero here, which is exactly why the model
// carries pointers: partition 0 and offset 0 are both REAL broker coordinates — the
// first message on the first partition of a fresh topic has precisely that record —
// so rendering an absent value as 0 would make the audit count a row as confirmed
// that the broker never acknowledged.
func nullInt64(v *int64) driver.Value {
	if v == nil {
		return nil
	}
	return *v
}

// indexOfProjectedColumn resolves a column's POSITION in the real projection, so a
// test that needs to corrupt one cell can address it by name rather than by counting.
//
// Counting by hand is what makes such a test rot: it silently addresses a different
// column the moment the projection changes, and the test then passes for the wrong
// reason.
func indexOfProjectedColumn(t *testing.T, name string) int {
	t.Helper()

	for i, column := range eventOutboxProjectedColumns() {
		if column == name {
			return i
		}
	}

	require.FailNowf(t, "column not projected", "%q is not in eventOutboxColumns", name)
	return -1
}

// eventOutboxInsertColumnNames splits the real insert column list, so a test can
// address a bound value by COLUMN NAME instead of by counting placeholders.
func eventOutboxInsertColumnNames() []string {
	raw := strings.Split(eventOutboxInsertColumns, ",")
	columns := make([]string, 0, len(raw))
	for _, name := range raw {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	return columns
}

// insertArgMatchers builds the positional argument matchers for one inserted row —
// exactly eventOutboxInsertValueCount of them — capturing the value bound to the named
// column and accepting anything elsewhere.
//
// It is derived from eventOutboxInsertColumns rather than written out, and that is the
// point: a hand-written list of AnyArg() matchers has to be re-counted by every author
// who adds a column, and the failure mode when they forget is an unhelpful
// "arguments do not match" a long way from the column that changed. Addressing the
// value by name means these tests keep asserting the same thing as the schema grows.
func insertArgMatchers(t *testing.T, column string, into *driver.Value) []driver.Value {
	t.Helper()

	columns := eventOutboxInsertColumnNames()
	require.Len(t, columns, eventOutboxInsertValueCount,
		"the insert column list and the bound value count must agree, or the placeholder arithmetic is wrong")

	target := -1
	matchers := make([]driver.Value, 0, len(columns))
	for i, name := range columns {
		if name == column {
			target = i
			matchers = append(matchers, captureArg(into))
			continue
		}
		matchers = append(matchers, sqlmock.AnyArg())
	}
	require.NotEqual(t, -1, target, "%q is not an inserted column", column)

	return matchers
}

// newEventOutboxRows builds a mocked result set over the real projected column
// list, one row per supplied entry.
func newEventOutboxRows(entries ...model.EventOutbox) *sqlmock.Rows {
	rows := sqlmock.NewRows(eventOutboxProjectedColumns())
	for _, entry := range entries {
		rows.AddRow(eventOutboxRow(entry)...)
	}
	return rows
}

// deadLetterInventoryProjectedColumns derives the NARROW inventory column list from
// deadLetterInventoryColumns, for the same reason eventOutboxProjectedColumns derives the wide
// one: a hand-kept copy would keep agreeing with itself while the query moved.
//
// The last entry is an expression with an alias — octet_length(payload_raw) AS payload_bytes —
// so the alias is taken as the column name, which is what a driver reports for it.
func deadLetterInventoryProjectedColumns() []string {
	raw := strings.Split(deadLetterInventoryColumns, ",")
	columns := make([]string, 0, len(raw))
	for _, name := range raw {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if index := strings.LastIndex(strings.ToUpper(trimmed), " AS "); index != -1 {
			trimmed = strings.TrimSpace(trimmed[index+4:])
		}
		columns = append(columns, trimmed)
	}

	return columns
}

// deadLetterInventoryRow renders one entry as the driver values the narrow projection produces,
// in that exact order.
//
// Nullable columns render as SQL NULL when their Go zero value means "unset", and payload_bytes
// renders as int64 because octet_length returns an integer PostgreSQL hands back as one — a Go
// int here would let a scan pass under the mock that fails against the real driver.
func deadLetterInventoryRow(e model.DeadLetterInventoryEntry) []driver.Value {
	nullTime := func(ts *time.Time) driver.Value {
		if ts == nil {
			return nil
		}

		return *ts
	}
	nullText := func(s string) driver.Value {
		if s == "" {
			return nil
		}

		return s
	}

	var failureMetadata driver.Value
	if len(e.FailureMetadata) > 0 {
		failureMetadata = []byte(e.FailureMetadata)
	}

	return []driver.Value{
		e.ID,
		e.EventID,
		e.EventType,
		e.AggregateID,
		e.PartitionKey,
		nullText(e.LedgerID),
		e.Topic,
		int64(e.SchemaVersion),
		e.OccurredAt,
		e.Status,
		int64(e.Attempts),
		nullText(e.LastError),
		nullTime(e.FirstAttemptedAt),
		nullTime(e.LastAttemptedAt),
		nullText(e.DLTTopic),
		failureMetadata,
		int64(e.PayloadBytes),
	}
}

// newDeadLetterInventoryRows builds a mocked narrow result set, one row per entry.
func newDeadLetterInventoryRows(entries ...model.DeadLetterInventoryEntry) *sqlmock.Rows {
	rows := sqlmock.NewRows(deadLetterInventoryProjectedColumns())
	for _, entry := range entries {
		rows.AddRow(deadLetterInventoryRow(entry)...)
	}

	return rows
}

// deadLetterInventoryEntryOf projects a stored row the way the narrow statement projects it:
// every column the inventory reads, the body's SIZE, and not the body (PERF-P06).
func deadLetterInventoryEntryOf(e model.EventOutbox) model.DeadLetterInventoryEntry {
	return model.DeadLetterInventoryEntry{
		ID:               e.ID,
		EventID:          e.EventID,
		EventType:        e.EventType,
		AggregateID:      e.AggregateID,
		PartitionKey:     e.PartitionKey,
		LedgerID:         e.LedgerID,
		Topic:            e.Topic,
		SchemaVersion:    e.SchemaVersion,
		OccurredAt:       e.OccurredAt,
		Status:           e.Status,
		Attempts:         e.Attempts,
		LastError:        e.LastError,
		FirstAttemptedAt: e.FirstAttemptedAt,
		LastAttemptedAt:  e.LastAttemptedAt,
		DLTTopic:         e.DLTTopic,
		FailureMetadata:  e.FailureMetadata,
		PayloadBytes:     len(e.Payload),
	}
}

// deadLetterEventIDsOf projects the event ids out of an inventory page.
//
// Membership over a projection is asserted through this helper and assert.Contains rather than
// through a bespoke containsDeadLetteredEventID predicate: one idiom, and a failure names the
// ids that WERE found instead of reporting a bare false.
func deadLetterEventIDsOf(page model.DeadLetterInventoryPage) []string {
	ids := make([]string, 0, len(page.Entries))
	for _, entry := range page.Entries {
		ids = append(ids, entry.EventID)
	}

	return ids
}

// walkDeadLetterInventory pages the whole inventory by CURSOR and returns every entry.
//
// A cursor walk rather than an offset one (PERF-P08), and it is what a real-database test has
// to do now: a shared database holds other runs' dead-lettered rows, so this run's fixtures are
// not guaranteed to be on page one. maxPages bounds it so a runaway walk fails the test rather
// than the suite.
func walkDeadLetterInventory(
	t *testing.T,
	ds Datasource,
	ctx context.Context,
	pageSize int,
) []model.DeadLetterInventoryEntry {
	t.Helper()

	const maxPages = 200

	all := make([]model.DeadLetterInventoryEntry, 0, pageSize)

	var cursor *model.DeadLetterCursor
	for page := range maxPages {
		got, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit:  pageSize,
			Cursor: cursor,
		})
		require.NoError(t, err, "paging the dead-letter inventory (page %d)", page)

		all = append(all, got.Entries...)

		if !got.HasMore || got.NextCursor == nil {
			return all
		}

		cursor = got.NextCursor
	}

	t.Fatalf("the dead-letter inventory did not terminate within %d pages of %d", maxPages, pageSize)

	return all
}

// dbTimestamp truncates an instant to the resolution PostgreSQL actually stores.
//
// TIMESTAMP WITH TIME ZONE holds MICROSECONDS, while Go's time.Now() carries
// nanoseconds. A fixture stamped with nanosecond precision therefore comes back from
// the database rounded, and an equality assertion against the pre-insert value fails
// by a few hundred nanoseconds — a difference that says nothing about the code and
// everything about the column type.
//
// Truncating at the fixture rather than loosening the assertion is the right fix,
// because occurred_at equality genuinely matters: the FIFO claim orders by it, and a
// tolerance-based comparison would also accept a value the database had mangled.
func dbTimestamp(ts time.Time) time.Time {
	return ts.UTC().Truncate(time.Microsecond)
}

// newEventOutboxFixture builds a valid, fully-populated entry ready to insert.
//
// markerPrefix is carried in aggregate_id, partition_key and ledger_id so that the
// real-database tier can find, quiesce and retire exactly its own fixtures in a
// shared database. The payload is deliberately shaped like the legacy webhook body
// — the two-key {"event": ..., "data": ...} object that requirement R-2 preserves
// verbatim — rather than an arbitrary JSON document, so the tests exercise the real
// thing.
//
// event_id is a BARE canonical UUID and carries no marker, because the persistence
// layer now requires canonical UUID form: event_id is the subscriber's idempotency
// key, and two spellings of one UUID are two distinct keys to a consumer
// deduplicating on the string. The fixtures find themselves by aggregate_id instead.
func newEventOutboxFixture(markerPrefix string) *model.EventOutbox {
	return &model.EventOutbox{
		EventID:     uuid.NewString(),
		EventType:   "transaction.applied",
		AggregateID: markerPrefix + "agg",
		// UNIQUE PER FIXTURE, deliberately, AND IN BOTH COLUMNS. At most one row per
		// EFFECTIVE key is claimable at any instant — that is what preserves
		// per-aggregate ordering — so fixtures sharing a key would drain one at a time
		// and every test that claims a batch would see a single row.
		//
		// The effective key is the LEDGER wherever a row has one and the partition key
		// only where it does not (model.EffectivePartitionKey, rendered as SQL by
		// eventOutboxEffectiveKeySQL). So a unique partition_key over a SHARED ledger —
		// which is what this fixture used to build — produces rows that are independent
		// on paper and one aggregate in fact. Every real-database test that claims a
		// batch of them then saw exactly one row.
		//
		// Tests that are ABOUT same-key behaviour call shareEventOutboxKey, which sets
		// both columns; every other test wants independent rows and gets them here.
		PartitionKey:  markerPrefix + "key-" + uuid.NewString(),
		LedgerID:      markerPrefix + "ldg-" + uuid.NewString(),
		Topic:         "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload: json.RawMessage(
			`{"event":"transaction.applied","data":{"transaction_id":"` + markerPrefix + `txn","status":"APPLIED"}}`),
		OccurredAt:  dbTimestamp(time.Now()),
		MaxAttempts: defaultEventMaxAttempts,
	}
}

// shareEventOutboxKey makes a fixture publish under a given Kafka message key, which means
// setting BOTH key columns.
//
// A test asserting same-key behaviour is asserting something about the key the PUBLISHER uses,
// and that key is the EFFECTIVE one: the ledger wherever the row has one, the stored partition
// key only where it does not. Setting partition_key alone therefore does not make two rows share
// a key at all — their distinct ledgers still separate them — and the assertion would be made
// about a condition the fixtures never created. Setting the ledger alone would work today and
// would stop working the moment a fixture stopped carrying one.
//
// Both, so the fixture states the intent unambiguously and holds under either resolution.
//
// Parameters:
//   - entry *model.EventOutbox: the fixture to re-key.
//   - key string: the key both columns take. Non-blank, because partition_key has a not-blank
//     CHECK and a blank ledger falls through rather than grouping.
func shareEventOutboxKey(entry *model.EventOutbox, key string) {
	entry.PartitionKey = key
	entry.LedgerID = key
}

// insertedEventOutboxRows is the single-row insert's RETURNING id result.
func insertedEventOutboxRows(id int64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id"}).AddRow(id)
}

// eventIDsOf projects the event ids out of a claimed batch, which is what the
// ordering and disjointness assertions compare.
func eventIDsOf(entries []model.EventOutbox) []string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.EventID)
	}
	return ids
}

// containsEventID reports whether a claimed batch includes the given event id.
func containsEventID(entries []model.EventOutbox, eventID string) bool {
	for _, entry := range entries {
		if entry.EventID == eventID {
			return true
		}
	}
	return false
}

// ===========================================================================
// TIER 1 — the projection contract
// ===========================================================================

// TestEventOutboxColumns_ProjectsEveryScannedColumnAndNoOthers pins the projection
// that the claim, the single-row fetch and the dead-letter listing all share.
//
// scanEventOutbox consumes these columns POSITIONALLY. Adding a column to the
// constant without adding a matching destination to the scanner — or reordering them
// — produces mis-assigned field values at runtime and not one compiler error, so the
// count and the order are asserted explicitly rather than left implicit.
//
// created_at is asserted ABSENT on purpose: the column exists on the table for
// forensics, model.EventOutbox has no field for it, and nothing orders by it,
// because all ordering here is by occurred_at.
func TestEventOutboxColumns_ProjectsEveryScannedColumnAndNoOthers(t *testing.T) {
	columns := eventOutboxProjectedColumns()

	want := []string{
		"id", "event_id", "event_type", "aggregate_id", "partition_key", "ledger_id", "topic",
		// payload_raw and NOT payload: the BYTEA column carries the producer's exact
		// bytes, the JSONB column carries PostgreSQL's normalised re-rendering of them,
		// and every reader of the body — the Kafka publish, the legacy webhook leg and a
		// dead-letter replay — must get the former.
		// event_raw follows payload_raw because the two are read together and answer
		// different questions: payload_raw is the legacy webhook body, which the HTTP leg
		// posts, and event_raw is the whole canonical envelope, which is what goes on a
		// Kafka topic. It is projected because every publish, dead-letter write and replay
		// takes the stored value rather than rebuilding the envelope from these columns —
		// a row read without it would silently fall back to composition and lose the byte
		// guarantee for events captured under a different serialiser.
		"schema_version", "payload_raw", "event_raw", "occurred_at",
		// next_attempt_at is projected because model.EventOutbox carries it and a caller
		// deciding whether a row is due reads it. It sits between max_attempts and
		// last_error, matching both the scanner's destination order and the column order
		// in the migration.
		"status", "attempts", "max_attempts", "next_attempt_at", "last_error",
		"first_attempted_at", "last_attempted_at", "dispatched_at", "locked_until", "claim_token",
		// kafka_dispatched_at and webhook_attempts are the DUAL-DELIVERY pair, and both are
		// projected because the relay reads them on every claimed row: the first tells it the
		// Kafka leg is already done — so the re-claim delivers only the outstanding webhook and
		// publishes nothing — and the second is the legacy leg's own budget, kept separate so a
		// webhook receiver being down cannot spend a Kafka retry attempt.
		//
		// SUNSET: both go with the legacy transport, along with webhook_dispatched above.
		"webhook_dispatched", "kafka_dispatched_at", "webhook_attempts",
		// The BROKER COORDINATE is the triple the broker assigned to the accepted
		// message, and it is projected because it is the only thing that makes the
		// zero-loss reconciliation auditable: a terminal row that cannot name its
		// (topic, partition, offset) is a row whose delivery nobody can confirm, and
		// the audit counts exactly those. It is written all-or-nothing, so it must be
		// read all-or-nothing too, which is why the three sit adjacent.
		"kafka_topic", "kafka_partition", "kafka_offset",
		"dlt_topic", "failure_metadata",
		// THE CAPTURED TRACE CONTEXT, projected because the PUBLISH path reads it: the relay
		// links its producer span to the trace of the request that captured the event, and a
		// claimed row read without these two arrives with no memory of where it came from, so
		// every publish span would start a trace of its own. They sit last, matching both the
		// scanner's destination order and the order the ALTER TABLE in sql/1781249137.sql adds
		// them in.
		"traceparent", "tracestate",
	}
	assert.Equal(t, want, columns,
		"the projected column list must match scanEventOutbox's destination order exactly")
	assert.NotContains(t, columns, "created_at",
		"created_at is deliberately not projected: model.EventOutbox has no field for it and all ordering is by occurred_at")
}

// TestEventOutboxInsertColumns_MatchesBoundValueCount keeps the placeholder
// arithmetic in the batch insert tied to the column list.
//
// insertEventOutboxChunkInTx generates $N placeholders from
// eventOutboxInsertValueCount. If a column were added to eventOutboxInsertColumns
// without bumping that constant, the generated statement would bind the wrong number
// of values per row and every batch insert would fail at runtime with a syntax
// error, in a code path only the bulk writer exercises.
func TestEventOutboxInsertColumns_MatchesBoundValueCount(t *testing.T) {
	columns := strings.Split(eventOutboxInsertColumns, ",")
	assert.Len(t, columns, eventOutboxInsertValueCount,
		"eventOutboxInsertValueCount must equal the number of inserted columns")

	fixture := newEventOutboxFixture("count-")
	assert.Len(t, eventOutboxInsertArgs(fixture), eventOutboxInsertValueCount,
		"eventOutboxInsertArgs must bind exactly one value per inserted column")
}

// ===========================================================================
// TIER 1 — ClaimPendingEventOutbox: query shape and FIFO ordering (V-6)
// ===========================================================================

// TestClaimPendingEventOutbox_QueryContainsSkipLockedAndOccurredAtOrdering is the
// convention-enforcement test, and it is here to make a documented convention
// ENFORCEABLE rather than aspirational.
//
// Two properties of the claim statement carry guarantees that nothing else in this
// suite can see, and both are one careless edit away from being lost:
//
//   - FOR UPDATE SKIP LOCKED is what lets several relay instances claim DISJOINT
//     batches without blocking one another. Replace it with a plain FOR UPDATE and
//     concurrent relays serialise behind each other; drop the row lock entirely and
//     two relays claim the same row and publish the event twice. Neither failure is
//     visible to a single-threaded test.
//   - ORDER BY occurred_at is what carries acceptance criterion V-6, per-aggregate
//     ordering. The neighbouring lineage claim in database/lineage.go orders by
//     created_at, so "fixing" this query to match its sibling is the single most
//     likely regression in this file — and it would leave every unit test in the
//     suite green, because occurred_at and created_at almost always agree in a test
//     that inserts rows in order.
//
// The assertion is made against the SQL the method ACTUALLY ISSUES, captured from
// the driver, rather than against the constant alone: asserting only on the constant
// would still pass if the method were changed to issue some other statement.
func TestClaimPendingEventOutbox_QueryContainsSkipLockedAndOccurredAtOrdering(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(newEventOutboxRows())

	_, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, *captured, 1, "the claim must be a single statement, not a multi-round-trip read")

	issued := (*captured)[0]

	assert.Contains(t, issued, "FOR UPDATE SKIP LOCKED",
		"the claim must keep FOR UPDATE SKIP LOCKED: it is what makes concurrent relay claims disjoint without blocking")
	assert.Contains(t, issued, "ORDER BY occurred_at",
		"the claim must order by occurred_at (NOT created_at, which the lineage claim uses): this ordering carries the per-aggregate ordering guarantee")
	assert.NotContains(t, issued, "ORDER BY created_at",
		"ordering by created_at would silently break per-aggregate ordering while leaving every other test green")

	// THE ORD-01 PREDICATE. FOR UPDATE SKIP LOCKED stops two relays claiming the same
	// row; it does NOT stop one relay skipping an earlier locked row and claiming a
	// LATER row of the same message key. Kafka preserves append order rather than
	// occurred_at, so that reordering reaches subscribers with nothing anywhere to show
	// it. The NOT EXISTS clause is what makes at most one row per key claimable, and it
	// is asserted here because it is invisible in behaviour until two relays run.
	assert.Contains(t, issued, "NOT EXISTS",
		"the claim must exclude a row whose message key already has an earlier row in flight; SKIP LOCKED alone permits same-key reordering")

	// SCOPED BY THE EFFECTIVE KEY, which is the key the publisher hashes — the ledger
	// wherever a row has one, the stored partition key only where it does not. Scoping it
	// wider would serialise unrelated aggregates and destroy throughput; scoping it to the
	// partition_key COLUMN was the previous shape and was NARROWER than the Kafka
	// partition, which is worse than either (PERF-C02): two same-ledger rows with divergent
	// stored keys went unserialised while both hashed to one partition.
	assert.Contains(t, issued, eventOutboxEffectiveKeySQL("earlier")+" = "+eventOutboxEffectiveKeySQL("candidate"),
		"the exclusion must compare the EFFECTIVE key on both sides, so the value the database serialises on is the value Kafka partitions on")
	assert.NotContains(t, issued, "earlier.partition_key = candidate.partition_key",
		"the superseded column comparison must not return")
	assert.Contains(t, issued, "(earlier.occurred_at, earlier.id) < (candidate.occurred_at, candidate.id)",
		"the exclusion must compare occurrence THEN id, matching the claim ordering exactly, or two rows sharing an instant could both be claimed")
	assert.Contains(t, issued, "earlier.status IN ('pending', 'processing')",
		"only pending and processing rows may block their key: a permanently failed or dead-lettered row must not stall its aggregate forever")

	// The remaining predicates the relay's correctness rests on.
	assert.Contains(t, issued, "candidate.attempts < candidate.max_attempts",
		"rows that have spent their retry budget must stay out of the claimable set, or an exhausted row is retried forever")
	assert.Contains(t, issued, "locked_until",
		"the lease predicate is what makes a crashed relay's in-flight rows reclaimable rather than stranded")
	assert.Contains(t, issued, "COALESCE(first_attempted_at, NOW())",
		"first_attempted_at must be pinned to the FIRST claim: it bounds the retry window the dead-letter metadata reports")
	assert.Contains(t, issued, "blnk.event_outbox",
		"the claim must target blnk.event_outbox and never the lineage outbox: they are separate tables served by separate relays")
	assert.NotContains(t, issued, "lineage_outbox",
		"blnk.event_outbox and blnk.lineage_outbox are never joined or merged")

	// UPDATE ... RETURNING does not preserve the inner ORDER BY, so the outer sort
	// over the CTE is what guarantees FIFO WITHIN a batch. It is not redundant and
	// must not be collapsed.
	assert.Contains(t, issued, "ORDER BY candidate.occurred_at ASC, candidate.id ASC",
		"the inner selection must order by occurrence then id, so rows sharing an instant are still claimed in the order they were recorded")
	assert.Contains(t, issued, "SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC",
		"the outer re-sort over the CTE is what guarantees FIFO WITHIN a batch; UPDATE ... RETURNING does not preserve the inner ORDER BY, so it is not redundant and must not be collapsed")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_BindsProcessingStatusLockIntervalAndBatchSize pins the
// three bound values, in order.
//
// The lock duration is bound as its Go duration string ("30s"), which PostgreSQL
// parses directly as an interval through the $2::interval cast. It is asserted
// because the alternative — formatting an interval into the statement text — is both
// an injection surface and a locale hazard, and because binding a raw
// time.Duration would send a nanosecond count that PostgreSQL reads as microseconds.
func TestClaimPendingEventOutbox_BindsProcessingStatusLockIntervalAndBatchSize(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	var claimToken driver.Value
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs(model.EventOutboxStatusProcessing, "30s", captureArg(&claimToken), 100).
		WillReturnRows(newEventOutboxRows())

	_, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the claim must bind exactly (processing, lock interval string, claim token, batch size) in that order")

	token, ok := claimToken.(string)
	require.True(t, ok, "the claim token must be bound as text")
	assert.True(t, model.IsCanonicalUUID(token),
		"the claim token must be a freshly generated UUID; a predictable or reused token would let a stale worker's transitions still match")
}

// TestClaimPendingEventOutbox_NonPositiveBatchSizeIsRejected locks in the guard.
//
// A zero LIMIT claims nothing and raises no error, which is INDISTINGUISHABLE from
// an empty backlog: the relay would report itself healthy while publishing nothing
// at all, and the only symptom would be a silently growing outbox. The
// implementation therefore rejects it as a bad request rather than passing it
// through, and both the zero and the negative case are asserted because a guard
// written as `== 0` would let the negative through.
func TestClaimPendingEventOutbox_NonPositiveBatchSizeIsRejected(t *testing.T) {
	for _, batchSize := range []int{0, -1, -100} {
		t.Run(fmt.Sprintf("batch size %d", batchSize), func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			entries, err := ds.ClaimPendingEventOutbox(context.Background(), batchSize, 30*time.Second)

			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrBadRequest)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"a rejected batch size must not reach the database at all")
		})
	}
}

// TestClaimPendingEventOutbox_NonPositiveLockDurationFallsBackToDefault pins the
// deliberately DIFFERENT treatment of a bad lease.
//
// A non-positive lease is normalised rather than rejected, and the asymmetry with
// batchSize above is the interesting part. A lease that expires the instant it is
// taken is a correctness problem — a second relay instance could reclaim and
// republish a row that is still being published — but failing the poll outright
// would STOP the relay rather than protect it. Falling back to the lease the outbox
// relays already use keeps the relay running and safe.
func TestClaimPendingEventOutbox_NonPositiveLockDurationFallsBackToDefault(t *testing.T) {
	for _, lockDuration := range []time.Duration{0, -time.Second} {
		t.Run(lockDuration.String(), func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
				WithArgs(model.EventOutboxStatusProcessing, defaultEventClaimLease.String(),
					sqlmock.AnyArg(), 25).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ClaimPendingEventOutbox(context.Background(), 25, lockDuration)
			require.NoError(t, err, "a non-positive lease must be normalised, not rejected: failing the poll would stop the relay")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestClaimPendingEventOutbox_PreservesDriverRowOrderVerbatim asserts that the
// collect path does not re-sort in Go.
//
// This is the honest, provable half of the ordering guarantee under a mock. sqlmock
// has no planner, so it cannot demonstrate that SQL returns rows in occurred_at
// order — that is what TestClaimPendingEventOutbox_FifoByOccurredAt_RealDB is for.
// What it CAN demonstrate is the property that makes the SQL's ORDER BY the single
// source of truth: the repository hands back exactly the order the driver produced.
//
// The rows are scripted in DESCENDING occurred_at order, deliberately contradicting
// the statement's ascending ORDER BY, and the result must come back in that same
// contradictory order. A Go-side sort here would look like a helpful belt-and-braces
// addition while actually being harmful: it would MASK a broken SQL ORDER BY, and
// the real-database FIFO test would then be the only thing standing between the code
// and a silent V-6 regression.
func TestClaimPendingEventOutbox_PreservesDriverRowOrderVerbatim(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	newest := model.EventOutbox{
		ID: 3, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg_1",
		PartitionKey: "agg_1", LedgerID: "ldg_1", Topic: "blnk.transactions", SchemaVersion: model.SchemaVersionV1,
		Payload:    json.RawMessage(`{"event":"transaction.applied","data":{}}`),
		OccurredAt: base.Add(2 * time.Minute), Status: model.EventOutboxStatusProcessing,
		MaxAttempts: 5,
	}
	middle := newest
	middle.ID, middle.EventID, middle.OccurredAt = 2, uuid.NewString(), base.Add(time.Minute)
	oldest := newest
	oldest.ID, oldest.EventID, oldest.OccurredAt = 1, uuid.NewString(), base

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnRows(newEventOutboxRows(newest, middle, oldest))

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 10, 30*time.Second)
	require.NoError(t, err)

	assert.Equal(t, []string{newest.EventID, middle.EventID, oldest.EventID}, eventIDsOf(entries),
		"the claim must return the driver's row order verbatim; a Go-side sort would mask a broken SQL ORDER BY")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_ScansEveryColumnIntoItsField walks a fully-populated
// row through the scanner and checks every field.
//
// It is the positional-scan safety net. Two columns of the same SQL type swapped in
// either the projection or the scanner produce no error whatsoever — just two fields
// holding each other's values — and the only way to catch that is to give every
// field a DISTINCT value and assert each one individually.
func TestClaimPendingEventOutbox_ScansEveryColumnIntoItsField(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	occurredAt := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	firstAttempt := occurredAt.Add(time.Second)
	lastAttempt := occurredAt.Add(31 * time.Second)
	dispatchedAt := occurredAt.Add(32 * time.Second)
	lockedUntil := occurredAt.Add(time.Minute)

	want := model.EventOutbox{
		ID:                99,
		EventID:           "3f8c1b2a-5d6e-4f70-8a91-b2c3d4e5f607",
		EventType:         "balance.monitor",
		AggregateID:       "bln_agg",
		PartitionKey:      "key_scan",
		LedgerID:          "ldg_scan",
		Topic:             "blnk.balances",
		SchemaVersion:     model.SchemaVersionV1,
		Payload:           json.RawMessage(`{"event":"balance.monitor","data":{"balance_id":"bln_agg"}}`),
		OccurredAt:        occurredAt,
		Status:            model.EventOutboxStatusFailed,
		Attempts:          4,
		MaxAttempts:       5,
		LastError:         "broken pipe",
		FirstAttemptedAt:  &firstAttempt,
		LastAttemptedAt:   &lastAttempt,
		DispatchedAt:      &dispatchedAt,
		LockedUntil:       &lockedUntil,
		WebhookDispatched: true,
		// The broker coordinate is populated with PARTITION 0 AND OFFSET 0 on purpose.
		// Those are the record of the very first message on the first partition of a
		// fresh topic — entirely real values — and they are exactly the pair a scanner
		// that tested truthiness instead of NULL-ness would drop. Asserting a non-nil
		// pointer holding zero is therefore a stronger claim than any non-zero pair.
		KafkaTopic:      "blnk.balances",
		KafkaPartition:  intPtr(0),
		KafkaOffset:     int64Ptr(0),
		DLTTopic:        "blnk.balances.dlt",
		FailureMetadata: json.RawMessage(`{"original_topic":"blnk.balances","attempt_count":5}`),
	}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(newEventOutboxRows(want))

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	got := entries[0]

	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.EventID, got.EventID)
	assert.Equal(t, want.EventType, got.EventType)
	assert.Equal(t, want.AggregateID, got.AggregateID)
	assert.Equal(t, want.LedgerID, got.LedgerID)
	assert.Equal(t, want.Topic, got.Topic)
	assert.Equal(t, want.SchemaVersion, got.SchemaVersion)
	assert.JSONEq(t, string(want.Payload), string(got.Payload))
	assert.True(t, want.OccurredAt.Equal(got.OccurredAt), "occurred_at %s != %s", want.OccurredAt, got.OccurredAt)
	assert.Equal(t, want.Status, got.Status)
	assert.Equal(t, want.Attempts, got.Attempts)
	assert.Equal(t, want.MaxAttempts, got.MaxAttempts)
	assert.Equal(t, want.LastError, got.LastError)
	require.NotNil(t, got.FirstAttemptedAt)
	assert.True(t, firstAttempt.Equal(*got.FirstAttemptedAt))
	require.NotNil(t, got.LastAttemptedAt)
	assert.True(t, lastAttempt.Equal(*got.LastAttemptedAt))
	require.NotNil(t, got.DispatchedAt)
	assert.True(t, dispatchedAt.Equal(*got.DispatchedAt))
	require.NotNil(t, got.LockedUntil)
	assert.True(t, lockedUntil.Equal(*got.LockedUntil))
	assert.True(t, got.WebhookDispatched)
	assert.Equal(t, want.KafkaTopic, got.KafkaTopic)
	require.NotNil(t, got.KafkaPartition, "partition 0 is a real partition and must scan to a non-nil pointer, not nil")
	assert.Equal(t, 0, *got.KafkaPartition)
	require.NotNil(t, got.KafkaOffset, "offset 0 is a real offset and must scan to a non-nil pointer, not nil")
	assert.Equal(t, int64(0), *got.KafkaOffset)
	record, named := got.BrokerRecord()
	assert.True(t, named,
		"a row carrying a complete triple must report a confirmed record: the zero-loss audit counts exactly these")
	assert.Equal(t, "blnk.balances/0@0", record.String(),
		"the accessor must reassemble the coordinate the driver handed back, zeroes included")
	assert.Equal(t, want.DLTTopic, got.DLTTopic)
	assert.JSONEq(t, string(want.FailureMetadata), string(got.FailureMetadata))

	assert.NoError(t, mock.ExpectationsWereMet())
}

// intPtr and int64Ptr keep the coordinate fixtures readable. The model carries
// pointers precisely so absence is distinguishable from zero, which means a literal
// cannot express a present zero without one of these.
func intPtr(v int) *int { return &v }

func int64Ptr(v int64) *int64 { return &v }

// TestClaimPendingEventOutbox_NullableColumnsScanCleanly is where a missing sql.Null*
// wrapper surfaces.
//
// Every nullable column is scripted as SQL NULL. Without the wrappers this fails
// outright with "converting NULL to string is unsupported"; with them, the
// normalisation has to be right too — nullable text becomes the empty string, and a
// nullable timestamp becomes a NIL POINTER rather than a zero time.Time.
//
// That last distinction is not cosmetic. A nil FirstAttemptedAt means "never
// attempted", which is what model.EventOutbox documents. A zero time.Time would
// report the year 1 instead, and the dead-letter age arithmetic behind the
// blnk.dlt.oldest_message_age_seconds gauge would compute an age of roughly two
// thousand years and fire its 15-minute alert on every never-attempted row.
//
// failure_metadata is asserted nil rather than the four bytes "null", which is what
// a []byte scan gives and a json.RawMessage scan of a JSON null would not.
func TestClaimPendingEventOutbox_NullableColumnsScanCleanly(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	bare := model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "ledger.created", AggregateID: "ldg_1",
		PartitionKey: "ldg_1", LedgerID: "", Topic: "blnk.system", SchemaVersion: model.SchemaVersionV1,
		Payload:     json.RawMessage(`{"event":"ledger.created","data":{}}`),
		OccurredAt:  time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC),
		Status:      model.EventOutboxStatusPending,
		MaxAttempts: 5,
		// LastError, DLTTopic, FailureMetadata and all four timestamps left unset,
		// so eventOutboxRow renders each of them as SQL NULL.
	}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(newEventOutboxRows(bare))

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 1, 30*time.Second)
	require.NoError(t, err, "NULL nullable columns must scan cleanly")
	require.Len(t, entries, 1)
	got := entries[0]

	assert.Empty(t, got.LastError, "a NULL last_error must normalise to the empty string")
	assert.Empty(t, got.DLTTopic, "a NULL dlt_topic must normalise to the empty string")
	assert.Nil(t, got.FailureMetadata, "a NULL failure_metadata must stay a nil RawMessage, never the bytes \"null\"")
	assert.Nil(t, got.FirstAttemptedAt, "a NULL first_attempted_at must be nil, not a zero time: nil means \"never attempted\"")
	assert.Nil(t, got.LastAttemptedAt)
	assert.Nil(t, got.DispatchedAt)
	assert.Nil(t, got.LockedUntil, "a NULL locked_until must be nil: an unleased row is claimable")
	assert.Empty(t, got.LedgerID, "an empty ledger_id is the documented \"no ledger\" value and must survive the scan")
	assert.Nil(t, got.KafkaPartition, "a NULL kafka_partition must be nil, never 0: partition 0 is a real partition")
	assert.Nil(t, got.KafkaOffset, "a NULL kafka_offset must be nil, never 0: offset 0 is a real offset")
	_, named := got.BrokerRecord()
	assert.False(t, named,
		"a row with no coordinate must report an unconfirmed record: that is what the zero-loss audit counts as unaudited")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_APartialBrokerCoordinateScansAsAbsent covers the read
// side of the all-or-nothing rule.
//
// The table's CHECK constraint refuses a partial triple, so PostgreSQL should never
// hand one back — but the scanner still tests all three columns together, and this is
// what proves it. A scanner that assigned each column independently would produce a
// row naming a topic with no partition and no offset, and BrokerRecord would then have
// to decide what that means at every call site instead of once here.
//
// The consequence of getting it wrong is not a crash but a WRONG AUDIT: a half-record
// looks like evidence of a delivery nobody can actually look up, which is precisely
// the masking the reconciliation verdict exists to refuse.
func TestClaimPendingEventOutbox_APartialBrokerCoordinateScansAsAbsent(t *testing.T) {
	partials := map[string][]driver.Value{
		"topic without partition or offset": {"blnk.transactions", nil, nil},
		"topic and partition without offset": {
			"blnk.transactions", int64(4), nil,
		},
		"partition and offset without topic": {
			nil, int64(4), int64(91),
		},
	}

	for name, coordinate := range partials {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			values := eventOutboxRow(*newEventOutboxFixture("partial-"))
			topicAt := indexOfProjectedColumn(t, "kafka_topic")
			values[topicAt] = coordinate[0]
			values[topicAt+1] = coordinate[1]
			values[topicAt+2] = coordinate[2]

			rows := sqlmock.NewRows(eventOutboxProjectedColumns()).AddRow(values...)
			mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(rows)

			entries, err := ds.ClaimPendingEventOutbox(context.Background(), 1, 30*time.Second)
			require.NoError(t, err, "a partial coordinate must scan cleanly, not error")
			require.Len(t, entries, 1)

			assert.Empty(t, entries[0].KafkaTopic,
				"an incomplete coordinate must leave the topic empty rather than half-report a record")
			assert.Nil(t, entries[0].KafkaPartition)
			assert.Nil(t, entries[0].KafkaOffset)
			_, named := entries[0].BrokerRecord()
			assert.False(t, named,
				"an incomplete coordinate is not evidence of a delivery: it must read as unconfirmed")

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestClaimPendingEventOutbox_EmptyBacklogReturnsNoError distinguishes "nothing to
// do" from a failure. The relay polls once per second, so this is by far the most
// frequently executed path in the whole file, and it must be silent.
func TestClaimPendingEventOutbox_EmptyBacklogReturnsNoError(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(newEventOutboxRows())

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_QueryErrorIsWrapped asserts the driver failure is
// converted to a typed internal error rather than leaking a raw driver error to the
// relay.
func TestClaimPendingEventOutbox_QueryErrorIsWrapped(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnError(sql.ErrConnDone)

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)

	assert.Nil(t, entries)
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_ScanErrorIsWrapped drives the per-row scan failure
// branch by returning a value the scanner cannot decode.
//
// A scan failure must abandon the whole batch rather than return a partial one. A
// partially-returned batch would leave the unreturned rows leased in the processing
// state with nobody publishing them, stranded until the lease expired.
func TestClaimPendingEventOutbox_ScanErrorIsWrapped(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	// occurred_at is a timestamp; a non-numeric string cannot convert to time.Time.
	// The row is built from the real fixture builder and then has its occurred_at cell
	// corrupted by POSITION, so the column count can never drift away from the
	// projection the way a hand-written value list does.
	valid := model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1, Payload: json.RawMessage(`{}`),
		OccurredAt: time.Now().UTC(), Status: model.EventOutboxStatusProcessing, MaxAttempts: 5,
	}
	cells := eventOutboxRow(valid)
	occurredAtIndex := indexOfProjectedColumn(t, "occurred_at")
	cells[occurredAtIndex] = "not-a-timestamp"

	broken := sqlmock.NewRows(eventOutboxProjectedColumns()).AddRow(cells...)
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(broken)

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)

	assert.Nil(t, entries, "a scan failure must abandon the batch, never return a partial one")
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimPendingEventOutbox_RowIterationErrorIsWrapped covers the rows.Err()
// branch — a connection that drops midway through streaming a result set, which is
// distinct from both a failed query and a failed scan and is easy to leave unchecked.
func TestClaimPendingEventOutbox_RowIterationErrorIsWrapped(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	entry := model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions", SchemaVersion: model.SchemaVersionV1,
		Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		Status: model.EventOutboxStatusProcessing, MaxAttempts: 5,
	}
	rows := newEventOutboxRows(entry).RowError(0, sql.ErrConnDone)
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(rows)

	entries, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 30*time.Second)

	assert.Nil(t, entries)
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ===========================================================================
// TIER 1 — the mark transitions
// ===========================================================================

// TestMarkEventDispatched_SetsDispatchedStatusStampsTimestampAndClearsLease pins the
// success terminal transition.
//
// Three things happen in ONE statement, and all three matter:
//
//   - status becomes dispatched, which is what removes the row from the claimable
//     set, because the claim predicate admits only pending and processing rows.
//   - dispatched_at is stamped. The column is dispatched_at and NOT processed_at:
//     the lineage outbox uses processed_at and this table deliberately does not, so
//     a copy-paste from that sibling would target a column that does not exist here.
//   - locked_until is cleared, releasing the lease.
//
// It is additionally CONDITIONAL on the claim token and on the prior state, which is
// what stops a worker whose lease expired from marking a row dispatched that another
// instance has since taken and moved on.
func TestMarkEventDispatched_SetsDispatchedStatusStampsTimestampAndClearsLease(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec("").
		WithArgs(model.EventOutboxStatusDispatched, int64(42), "tok-42",
			model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying,
			// $6-$8: the broker coordinate, NULL here because this case has none.
			// TestMarkEventDispatched_PersistsTheBrokerCoordinate covers the populated form.
			nil, nil, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.MarkEventDispatched(context.Background(), 42, "tok-42", model.BrokerRecord{}))
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "UPDATE blnk.event_outbox")
	assert.Contains(t, issued, "claim_token = $3",
		"the transition must be conditional on the claim token, or a zombie worker can overwrite newer state")
	assert.Contains(t, issued, "claim_token = NULL",
		"a terminal state releases the claim")
	assert.Contains(t, issued, "dispatched_at = NOW()",
		"the dispatch instant must be stamped; the column is dispatched_at, not the lineage outbox's processed_at")
	assert.NotContains(t, issued, "processed_at",
		"processed_at belongs to blnk.lineage_outbox and does not exist on blnk.event_outbox")
	assert.Contains(t, issued, "locked_until = NULL",
		"the lease must be released on the terminal transition")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_ChoosesRetryOrExhaustionInSQL exercises BOTH arms of the CASE
// and the boundary between them.
//
// The status is decided inside the UPDATE rather than in Go, and that is
// load-bearing: deciding it in SQL keeps the read of attempts and the write of
// status atomic, so two relay instances racing on one row cannot both conclude they
// were the last attempt and both dead-letter it.
//
// Both arms are asserted because the boundary — attempts + 1 >= max_attempts — is a
// classic off-by-one site, and requirement R-4 fixes the retry budget at 5 attempts.
// Written as `>` instead of `>=` the row gets a sixth attempt; written as
// `attempts >= max_attempts` it gets only four. Neither mistake is visible from a
// single-arm test.
//
// NOTE ON THE STATE MACHINE, because it is easy to get backwards: the exhaustion arm
// sets FAILED, not dead_lettered. Those are two distinct states — failed means "we
// gave up publishing", dead_lettered means "and the event is now preserved on its
// <topic>.dlt sibling, from which it can be replayed". Only the dead-letter publisher
// knows whether that second step actually happened, so only MarkEventDeadLettered
// may declare it. ListDeadLetterInventory covers both literals precisely so that an
// event stranded between them is still visible to an operator.
func TestMarkEventFailed_ChoosesRetryOrExhaustionInSQL(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusFailed,        // $1 — the exhaustion arm
			model.EventOutboxStatusPending,       // $2 — the retry arm
			"broker unavailable",                 // $3 — the reason
			"2s",                                 // $4 — the backoff the caller computed
			int64(7),                             // $5 — the row
			"tok-7",                              // $6 — the claim token
			model.EventOutboxStatusProcessing,    // $7 — the required prior state
			false,                                // $8 — the caller's permanent-failure verdict
			eventDeadLetterHandoffLease.String(), // $9 — the lease the exhaustion arm keeps
		).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7", "broker unavailable", 2*time.Second, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN $1 ELSE $2 END",
		"the retry-versus-exhaustion decision must be made in SQL, atomically with the attempt increment, "+
			"and it must read BOTH inputs: the caller's permanent-failure verdict and the attempt budget")
	assert.Contains(t, issued, "attempts = attempts + 1",
		"the attempt counter must be incremented by the same statement that reads it")
	assert.Contains(t, issued, "locked_until = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN NOW() + $9::interval ELSE NULL END",
		"the lease must follow the claim token: released on the retry arm so the row is picked up "+
			"again as soon as next_attempt_at is due, and RENEWED on the exhaustion arm so the "+
			"dead-letter hand-off the retained token authorises cannot be stolen by another instance")
	assert.Contains(t, issued, "RETURNING status, attempts",
		"the decision the UPDATE made must be returned, not re-read: a second read reintroduces the race the in-SQL decision removes")
	assert.Contains(t, issued, "claim_token = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN claim_token ELSE NULL END",
		"the retry arm must release the claim so the row can be re-claimed, and the exhaustion arm must retain it so only this worker may dead-letter")
	assert.Contains(t, issued, "next_attempt_at = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts",
		"the due instant must be advanced on the retry arm only: a terminal row is not waiting for anything, "+
			"and a future due instant on one would read as though a retry were still coming")
	assert.Contains(t, issued, "NOW() + $4::interval",
		"the backoff must come from the caller's bound interval, not from arithmetic frozen into this statement — "+
			"the schedule is RELAY_RETRY_* configuration")

	assert.Equal(t, model.EventOutboxStatusPending, outcome.Status)
	assert.Equal(t, 1, outcome.Attempts)
	assert.False(t, outcome.Exhausted, "one failure against a five-attempt budget is not exhaustion")
	assert.Empty(t, outcome.ClaimToken,
		"the retry arm releases the claim, so there is no token to hand on")

	assert.NoError(t, mock.ExpectationsWereMet(),
		"the exhaustion arm must bind failed and the retry arm pending, in that order")
}

// TestMarkEventFailed_ExhaustionReportsTheHandOffToken is the exhaustion arm's
// outcome, and it pins the hand-off that makes concurrent dead-lettering impossible.
//
// When the budget is spent the row still owes one step — the dead-letter write and the
// transition that records it — and the claim token is RETAINED so that only the worker
// which spent the last attempt can take it. Returning that token here is what lets the
// caller perform the hand-off without knowing which arm keeps it.
func TestMarkEventFailed_ExhaustionReportsTheHandOffToken(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(5)))

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7", "final attempt failed", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)

	assert.True(t, outcome.Exhausted, "five of five attempts is exhaustion")
	assert.Equal(t, model.EventOutboxStatusFailed, outcome.Status,
		"exhaustion produces failed, never dead_lettered: only the dead-letter publisher may declare that")
	assert.Equal(t, "tok-7", outcome.ClaimToken,
		"the exhaustion arm must hand the token on, or nothing can perform the dead-letter transition")
	assert.False(t, outcome.Terminal,
		"a row that spent its budget on transient failures did not exhaust because the caller called the "+
			"failure permanent, and the two causes must stay distinguishable in the log")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_ATerminalVerdictExhaustsOnTheFirstAttempt is the other input to the
// in-SQL decision, and the reason it is an input at all.
//
// The publisher classifies every failure. Some are PERMANENT by construction: an envelope over
// the size ceiling, bytes that are not valid JSON, a destination outside the topic namespace
// Blnk owns. It reports those as terminal and no retry can change the answer — but the durable
// state used to disagree, because this statement read only the attempt count. The row went back
// to pending with four attempts left and spent the whole 31-second backoff schedule
// rediscovering what the publisher already knew, which delayed preservation by that long, spent
// four attempts of relay throughput on a message that can never be published, and reported a
// permanently stuck event as a busy one in the status counter throughout.
//
// The verdict is bound as $8 and ORed into all three arms, so status, the due instant and the
// retained token agree. The token in particular matters: the caller needs it to perform the
// dead-letter hand-off, and it is retained here on attempt ONE.
func TestMarkEventFailed_ATerminalVerdictExhaustsOnTheFirstAttempt(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusFailed,
			model.EventOutboxStatusPending,
			"the serialised event is over the size ceiling",
			"0s",
			int64(7),
			"tok-7",
			model.EventOutboxStatusProcessing,
			true,                                 // $8 — the caller's PERMANENT verdict
			eventDeadLetterHandoffLease.String(), // $9 — the lease the exhaustion arm keeps
		).
		// The database resolves the CASE, so the returned row is what a real PostgreSQL would
		// have produced for a terminal verdict on attempt one: failed, with the count at 1.
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(1)))

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7",
		"the serialised event is over the size ceiling", 0, true, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.Len(t, *captured, 1)

	assert.Contains(t, (*captured)[0], "$8::boolean OR attempts + 1 >= max_attempts",
		"the verdict must be BOUND and read by the CASE, never applied by a Go-side branch: deciding "+
			"outside the statement is what lets two instances racing on one row both conclude they were last")
	assert.NotContains(t, (*captured)[0], "max_attempts = ",
		"the budget itself must not be rewritten to force exhaustion; that would corrupt the row's own history")
	assert.Contains(t, (*captured)[0],
		"locked_until = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN NOW() + $9::interval ELSE NULL END",
		"the terminal verdict retains the claim token, so the SAME condition must retain the lease: a "+
			"token whose lease has been released is advisory, and the dead-letter repair claim adopts the "+
			"row while this worker's dead-letter write is still in flight")

	assert.True(t, outcome.Exhausted,
		"a permanent failure is exhaustion on whichever attempt it happened")
	assert.True(t, outcome.Terminal,
		"and the reason must be reported, or 'attempt 1 of 5, exhausted' reads as a bookkeeping defect")
	assert.Equal(t, 1, outcome.Attempts,
		"the attempt count must report the truth — one attempt was made — rather than being inflated to the budget")
	assert.Equal(t, "tok-7", outcome.ClaimToken,
		"the token must be handed on for the dead-letter write, exactly as on the budget-driven arm")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_LostClaimIsAConflictAndNotASilentSuccess is the regression guard
// on the behaviour that made stale workers dangerous.
//
// A transition matching no row means the lease expired and another instance owns the
// row now. The old code logged a warning and returned nil, so the zombie worker
// carried on as though it had recorded its attempt — which is exactly how the retry
// budget got double-spent and how newer state got overwritten. It must be a conflict.
func TestMarkEventFailed_LostClaimIsAConflictAndNotASilentSuccess(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnError(sql.ErrNoRows)

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "stale-token", "boom", 0, false, testDeadLetterHandoffLease)
	require.Error(t, err, "a lost claim must NOT be reported as success")
	assert.Equal(t, model.EventFailureOutcome{}, outcome,
		"no decision was made, so no decision may be reported")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"a lost claim is a conflict: nothing is broken, the caller simply no longer owns the row")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestEventOutboxTransitions_RefuseABlankClaimToken proves every conditional
// transition names the omission instead of matching a row nobody holds.
//
// A blank token could only ever match a row whose claim_token is NULL — a pending or
// terminal row that is not the caller's to move. Passing it through would either match
// nothing and be reported as a lost claim, which misleads, or match the wrong row.
func TestEventOutboxTransitions_RefuseABlankClaimToken(t *testing.T) {
	transitions := map[string]func(ctx context.Context, ds Datasource) error{
		"MarkEventDispatched": func(ctx context.Context, ds Datasource) error {
			return ds.MarkEventDispatched(ctx, 1, "   ", model.BrokerRecord{})
		},
		"MarkEventFailed": func(ctx context.Context, ds Datasource) error {
			_, err := ds.MarkEventFailed(ctx, 1, "", "boom", 0, false, testDeadLetterHandoffLease)
			return err
		},
		"MarkEventPermanentlyFailed": func(ctx context.Context, ds Datasource) error {
			_, err := ds.MarkEventPermanentlyFailed(ctx, 1, " ", "boom", testDeadLetterHandoffLease)
			return err
		},
		"MarkEventDeadLettered": func(ctx context.Context, ds Datasource) error {
			return ds.MarkEventDeadLettered(ctx, 1, "", "blnk.transactions.dlt", json.RawMessage(`{}`), model.BrokerRecord{})
		},
		"MarkWebhookDispatched": func(ctx context.Context, ds Datasource) error {
			return ds.MarkWebhookDispatched(ctx, 1, "")
		},
		"ReleaseEventReplay": func(ctx context.Context, ds Datasource) error {
			return ds.ReleaseEventReplay(ctx, 1, "", "gave up")
		},
	}

	for name, call := range transitions {
		t.Run(name, func(t *testing.T) {
			// No statement is expected: the guard must reject before touching the
			// database, so an unexpected query would fail ExpectationsWereMet.
			db, mock := newSQLMock(t)
			err := call(context.Background(), Datasource{Conn: db})

			require.Error(t, err, "a blank claim token must be refused, not passed through")
			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"the guard must run before any statement is issued")
		})
	}
}

// TestMarkEventFailed_BelowMaxAttemptsReturnsToPending is arm one, asserted through
// the bound values.
//
// The retry arm binds model.EventOutboxStatusPending — and specifically pending
// rather than processing, because only pending and processing rows are claimable and
// leaving it processing while clearing the lease would work by accident. Pending is
// the state the claim predicate and the partial index on pending rows are both
// written against.
func TestMarkEventFailed_BelowMaxAttemptsReturnsToPending(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	var retryArm driver.Value
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs(sqlmock.AnyArg(), captureArg(&retryArm), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "transient", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)

	assert.Equal(t, model.EventOutboxStatusPending, retryArm,
		"a failure within budget must return the row to pending so the claim predicate and the pending partial index both pick it up")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_AtMaxAttemptsBecomesFailedNotDeadLettered is arm two, and it
// pins the distinction the whole dead-letter surface rests on.
//
// Exhausting the retry budget produces FAILED. Declaring dead_lettered here would be
// a lie: at this point the event has NOT been written to any dead-letter topic, and
// dead_lettered is the state a replay checks for before re-publishing. A row marked
// dead_lettered with a NULL dlt_topic and NULL failure_metadata is unreplayable and
// shows an operator nothing, which is strictly worse than being honestly failed.
func TestMarkEventFailed_AtMaxAttemptsBecomesFailedNotDeadLettered(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	var exhaustionArm driver.Value
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs(captureArg(&exhaustionArm), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(5)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "final attempt failed", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)

	assert.Equal(t, model.EventOutboxStatusFailed, exhaustionArm,
		"exhausting the retry budget must produce failed; only MarkEventDeadLettered may declare dead_lettered, because only the dead-letter publisher knows the event reached its .dlt sibling")
	assert.NotEqual(t, model.EventOutboxStatusDeadLettered, exhaustionArm,
		"a row marked dead_lettered without a dlt_topic or failure metadata is unreplayable and shows an operator nothing")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_StampsFirstAttemptedAtOnceAndLastAttemptedAtEveryTime pins the
// two timestamps that bound the retry window.
//
// first_attempted_at goes through COALESCE so it survives as the FIRST attempt across
// every retry; last_attempted_at is set unconditionally so it tracks the most recent
// one. Together they are two of the five fields model.FailureMetadata requires
// (original topic, error reason, attempt count, first-attempted-at,
// last-attempted-at), and they are what distinguishes a momentary broker blip from a
// sustained outage during dead-letter triage.
//
// If first_attempted_at were assigned rather than COALESCEd, the window would collapse
// to the final attempt on every dead-lettered event and the metadata would be
// worthless — while remaining perfectly non-NULL, so nothing would look broken.
func TestMarkEventFailed_StampsFirstAttemptedAtOnceAndLastAttemptedAtEveryTime(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
		AddRow(model.EventOutboxStatusPending, int64(1)))
	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "boom", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "first_attempted_at = COALESCE(first_attempted_at, NOW())",
		"first_attempted_at must be pinned to the first attempt; assigning it would collapse the retry window reported in the dead-letter metadata")
	assert.Contains(t, issued, "last_attempted_at = NOW()",
		"last_attempted_at must move with every attempt so the retry window has an upper bound")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventFailed_RecordsErrorMessageAsABoundParameter asserts the reason is
// stored, and stored as a PARAMETER.
//
// Parameterisation is the assertion that matters. The message is a broker error
// string that this code never generated and cannot constrain, so interpolating it
// into the statement text would be an injection surface reachable from anything that
// can make a publish fail. The message chosen here would terminate the statement and
// start another if it were interpolated.
func TestMarkEventFailed_RecordsErrorMessageAsABoundParameter(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	hostile := "'; DROP TABLE blnk.event_outbox; --"
	mock.ExpectQuery("").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), hostile, sqlmock.AnyArg(), int64(7), "tok",
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", hostile, 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.Len(t, *captured, 1)

	assert.Contains(t, (*captured)[0], "last_error = $3",
		"the failure reason must be bound, never interpolated: it is an uncontrolled broker string")
	assert.NotContains(t, (*captured)[0], "DROP TABLE",
		"the failure reason must not reach the statement text")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventDeadLettered_RecordsTerminalStateTopicAndMetadata pins the second half
// of the failure path — the step that actually makes an event replayable.
//
// dlt_topic and failure_metadata are what the dead-letter inventory displays and what
// an operator triages from, and dead_lettered is the state a replay requires before
// it will re-publish. Left uncalled, those columns stay NULL and no event is ever
// replayable, so requirement R-5's replay surface would have nothing to work with.
func TestMarkEventDeadLettered_RecordsTerminalStateTopicAndMetadata(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	metadata := json.RawMessage(
		`{"original_topic":"blnk.transactions","error_reason":"broken pipe","attempt_count":5}`)

	mock.ExpectExec("").
		WithArgs(
			model.EventOutboxStatusDeadLettered,
			"blnk.transactions.dlt",
			[]byte(metadata),
			int64(11),
			"tok-11",
			model.EventOutboxStatusFailed,
			model.EventOutboxStatusProcessing,
			// $8-$10: the dead-letter coordinate, absent in this case.
			nil, nil, nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.MarkEventDeadLettered(
		context.Background(), 11, "tok-11", "blnk.transactions.dlt", metadata, model.BrokerRecord{}))
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "dlt_topic = $2")
	assert.Contains(t, issued, "failure_metadata = $3")
	assert.Contains(t, issued, "locked_until = NULL")
	assert.Contains(t, issued, "claim_token = $5",
		"only the worker holding the claim may dead-letter, or two workers each put a copy on the .dlt topic")
	assert.Contains(t, issued, "status IN ($6, $7)",
		"an already dead-lettered row must not be dead-lettered a second time")

	assert.NoError(t, mock.ExpectationsWereMet(),
		"the dead-letter transition must bind dead_lettered, the resolved .dlt topic and the metadata bytes")
}

// TestMarkEventDeadLettered_EmptyTopicAndMetadataBindAsNull keeps "not recorded"
// distinguishable from "recorded as empty".
//
// An empty string in dlt_topic would read as a topic named "", and an empty
// failure_metadata document would read as metadata that was genuinely captured and
// happened to be blank. Both would make the dead-letter inventory lie about what it
// knows, and a NULL check is the only way a reader can tell the difference.
func TestMarkEventDeadLettered_EmptyTopicAndMetadataBindAsNull(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs(model.EventOutboxStatusDeadLettered, nil, nil, int64(11), "tok-11",
			model.EventOutboxStatusFailed, model.EventOutboxStatusProcessing,
			nil, nil, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.MarkEventDeadLettered(context.Background(), 11, "tok-11", "", nil, model.BrokerRecord{}))
	assert.NoError(t, mock.ExpectationsWereMet(),
		"an empty topic or metadata must be stored as SQL NULL so \"not recorded\" stays distinct from \"recorded as empty\"")
}

// TestMarkWebhookDispatched_SetsTheDualDeliveryFlag pins the dual-delivery marker.
//
// During the 30-day window the relay publishes to Kafka AND enqueues the legacy
// webhook task FROM THE SAME CLAIMED ROW — which is what makes the two transports
// carry byte-identical payloads structurally rather than by careful coding. This flag
// is what makes the legacy leg individually idempotent: a row republished to Kafka
// after a relay crash must not enqueue a second webhook.
//
// This method, the webhook_dispatched column and the relay branch that calls it are
// all removed at the webhook sunset, together with webhooks.go. Every other method in
// this file outlives it, so this is the one test here that is EXPECTED to be deleted
// rather than maintained.
func TestMarkWebhookDispatched_SetsTheDualDeliveryFlag(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec("").
		WithArgs(int64(13), "tok-13").
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.MarkWebhookDispatched(context.Background(), 13, "tok-13"))
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "webhook_dispatched = TRUE")
	assert.Contains(t, issued, "claim_token = $2",
		"the marker must be conditional on the claim, or a zombie worker can record a webhook that was never sent")
	assert.NotContains(t, issued, "status =",
		"the dual-delivery marker must not disturb the relay state machine: the Kafka leg owns status")
	assert.NotContains(t, issued, "locked_until",
		"the dual-delivery marker must not touch the lease")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventWebhookPending_ChoosesRetryOrAbandonInSQL exercises BOTH arms of the
// legacy leg's own budget and the boundary between them.
//
// # What this transition is for
//
// The two legs of the dual-delivery window used to share one terminal state. A row
// whose Kafka publish succeeded was marked dispatched even when the webhook enqueue
// beside it had failed — and dispatched is outside the claim predicate, so that
// webhook was never retried and never delivered. This transition is the fix: it
// records the Kafka leg in its OWN column and leaves the row claimable for the
// webhook alone.
//
// Four things are asserted about the statement, and each of them is a distinct way to
// get this wrong:
//
//   - webhook_attempts, not attempts, is incremented. Spending a KAFKA retry attempt
//     on a webhook receiver being down would let the deprecated transport
//     dead-letter events on the transport replacing it.
//   - kafka_dispatched_at is stamped through COALESCE, so the FIRST acknowledgement
//     instant survives every subsequent webhook retry. Overwriting it would report
//     the last bookkeeping write as the moment the broker accepted the message.
//   - dispatched_at is stamped on the ABANDON arm only. Stamping it on the retry arm
//     would say the row is finished while a delivery is still owed.
//   - the arm is chosen in SQL. Deciding it in Go would let two relay instances
//     working one row both conclude they spent the last webhook attempt.
//
// SUNSET: this test goes with the transition it covers.
func TestMarkEventWebhookPending_ChoosesRetryOrAbandonInSQL(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			"redis unavailable",                   // $1 — the reason, stored in last_error
			model.EventOutboxStatusDispatched,     // $2 — the abandon arm
			model.EventOutboxStatusWebhookPending, // $3 — the retry arm
			"1s",                                  // $4 — the backoff the caller computed
			int64(9),                              // $5 — the row
			"tok-9",                               // $6 — the claim token
			model.EventOutboxStatusProcessing,     // $7 — the ordinary prior state
			model.EventOutboxStatusWebhookPending, // $8 — and an idempotent re-call
			nil, nil, nil,                         // $9-$11 — the coordinate, absent on this pass
		).
		WillReturnRows(sqlmock.NewRows([]string{"status", "webhook_attempts"}).
			AddRow(model.EventOutboxStatusWebhookPending, int64(1)))

	outcome, err := ds.MarkEventWebhookPending(context.Background(), 9, "tok-9", "redis unavailable", time.Second, model.BrokerRecord{})
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "webhook_attempts = webhook_attempts + 1",
		"the LEGACY leg's own counter must be incremented")
	assert.NotContains(t, issued, "attempts = attempts + 1",
		"and the KAFKA budget must be untouched: a webhook receiver being down must never spend a "+
			"Kafka retry attempt, or the deprecated transport can dead-letter events on the new one")
	assert.Contains(t, issued, "WHEN webhook_attempts + 1 >= max_attempts THEN $2",
		"the ABANDON arm must be chosen in SQL when the increment spends the budget, atomically with "+
			"the increment itself, or two instances working one row both conclude they were last")
	assert.Contains(t, issued, "ELSE $3",
		"and the retry arm otherwise")
	assert.Contains(t, issued, "kafka_dispatched_at = COALESCE(kafka_dispatched_at, NOW())",
		"the Kafka leg must be recorded separately from dispatched_at, and the FIRST acknowledgement kept")
	assert.Contains(t, issued, "dispatched_at = CASE",
		"dispatched_at belongs to the abandon arm only: a row with a webhook still owed is not finished")
	assert.Contains(t, issued, "NOW() + $4::interval",
		"the backoff must be the caller's bound interval, not arithmetic frozen into this statement")
	assert.Contains(t, issued, "locked_until = NULL",
		"the lease must be released on both arms, or the row is not claimable for its webhook")
	assert.Contains(t, issued, "claim_token = NULL",
		"and so must the token: unlike the dead-letter hand-off, nothing is owed to THIS worker")
	assert.Contains(t, issued, "RETURNING status, webhook_attempts",
		"the decision the UPDATE made must be returned, not re-read")

	assert.Equal(t, model.EventOutboxStatusWebhookPending, outcome.Status)
	assert.Equal(t, 1, outcome.WebhookAttempts)
	assert.False(t, outcome.Abandoned, "one failed enqueue against a five-attempt budget is not exhaustion")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventWebhookPending_AbandonArmIsReportedFromTheStatusTheDatabaseChose pins the
// terminal arm.
//
// Abandoned is DERIVED from the returned status rather than computed from the attempt
// count, so the caller's verdict and the row's state cannot disagree — which they would
// if the arithmetic were repeated in Go against a possibly different max_attempts.
//
// SUNSET: goes with the transition it covers.
func TestMarkEventWebhookPending_AbandonArmIsReportedFromTheStatusTheDatabaseChose(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"status", "webhook_attempts"}).
			AddRow(model.EventOutboxStatusDispatched, int64(5)))

	outcome, err := ds.MarkEventWebhookPending(context.Background(), 9, "tok-9", "still unreachable", 0, model.BrokerRecord{})
	require.NoError(t, err)

	assert.True(t, outcome.Abandoned,
		"five of five enqueue attempts spends the legacy budget, so no further webhook is attempted")
	assert.Equal(t, model.EventOutboxStatusDispatched, outcome.Status,
		"and the row becomes terminal on the strength of its Kafka delivery, which did happen")
	assert.Equal(t, 5, outcome.WebhookAttempts)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventWebhookPending_LostClaimIsAConflict asserts a transition that matched no
// row is reported rather than swallowed.
//
// No row matching means the lease expired and another instance owns this row now.
// Returning nil would tell the caller its outstanding webhook was recorded when nothing
// was written, and the row would carry on with a webhook owed and no record of it.
//
// SUNSET: goes with the transition it covers.
func TestMarkEventWebhookPending_LostClaimIsAConflict(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnError(sql.ErrNoRows)

	outcome, err := ds.MarkEventWebhookPending(context.Background(), 9, "tok-9", "unreachable", time.Second, model.BrokerRecord{})

	requireAPIError(t, err, apierror.ErrConflict)
	assert.Empty(t, outcome.Status, "a conflict must not report a state the row is not in")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventWebhookPending_RequiresTheClaimToken asserts the transition refuses to run
// without the token the claim issued.
//
// Every conditional transition matches on (id, claim_token), so an empty token could
// only ever match a row nobody holds. Naming the omission is the only honest outcome.
//
// SUNSET: goes with the transition it covers.
func TestMarkEventWebhookPending_RequiresTheClaimToken(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	for name, token := range map[string]string{"empty": "", "whitespace": "   "} {
		t.Run(name, func(t *testing.T) {
			outcome, err := ds.MarkEventWebhookPending(context.Background(), 9, token, "unreachable", time.Second, model.BrokerRecord{})

			requireAPIError(t, err, apierror.ErrBadRequest)
			assert.Empty(t, outcome.Status)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet(),
		"no statement may be issued for a transition that cannot be authorised")
}

// TestMarkEventWebhookPending_NegativeBackoffIsTreatedAsDueImmediately mirrors
// MarkEventFailed's handling: a caller that computed a negative delay meant "no delay",
// and reaching into the past would be indistinguishable from it while looking deliberate.
//
// SUNSET: goes with the transition it covers.
func TestMarkEventWebhookPending_NegativeBackoffIsTreatedAsDueImmediately(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs(
			"unreachable",
			model.EventOutboxStatusDispatched,
			model.EventOutboxStatusWebhookPending,
			"0s",
			int64(9),
			"tok-9",
			model.EventOutboxStatusProcessing,
			model.EventOutboxStatusWebhookPending,
			nil, nil, nil,
		).
		WillReturnRows(sqlmock.NewRows([]string{"status", "webhook_attempts"}).
			AddRow(model.EventOutboxStatusWebhookPending, int64(1)))

	_, err := ds.MarkEventWebhookPending(context.Background(), 9, "tok-9", "unreachable", -5*time.Second, model.BrokerRecord{})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"a negative delay must be bound as 0s, never as a negative interval")
}

// TestRenewEventOutboxLease_ExtendsEveryRowStillInFlightUnderOneToken pins the statement the
// relay's heartbeat issues.
//
// # The defect it exists for
//
// The lease is taken once, when the batch is claimed, and a batch can legitimately outlive it:
// a hundred rows published eight at a time run in thirteen waves, and one wave can occupy the
// writer's whole produce timeout. Rows in the later waves therefore had their lease expire
// BEFORE their publish was attempted, while the relay still held them — so a second instance
// claimed and published exactly those rows, this one published them again, and every transition
// this one attempted failed as a lost claim. Duplicates on the topic, and no error anywhere.
//
// # Why the token alone addresses the batch
//
// Every terminal and near-terminal transition CLEARS claim_token, so a row that has been
// dispatched, dead-lettered, returned to pending or moved to webhook_pending is already outside
// this statement's reach. What is still under the token is exactly what is still in flight —
// which is why no id list is passed and why the absence of one is asserted rather than assumed.
//
// # Why it is not in eventTransitionTable
//
// The shared table drives the wrapped-error, lost-claim and undeterminable-count cases for every
// CONDITIONAL TRANSITION. This is not one: a renewal that extends nothing is the ordinary end of
// a batch, not a lost claim, so the table's zero-rows-is-a-conflict case would be wrong here.
// The three cases are therefore written out below with the semantics this method actually has.
func TestRenewEventOutboxLease_ExtendsEveryRowStillInFlightUnderOneToken(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec("").
		WithArgs(
			"45s",                             // $1 — the lease, as an interval string
			"tok-batch",                       // $2 — the token that IS the batch
			model.EventOutboxStatusProcessing, // $3 — the only state a lease may be extended in
		).
		WillReturnResult(sqlmock.NewResult(0, 7))

	renewed, err := ds.RenewEventOutboxLease(context.Background(), "tok-batch", 45*time.Second)
	require.NoError(t, err)
	require.Len(t, *captured, 1, "the renewal must be a single statement per heartbeat, not one per row")

	issued := (*captured)[0]

	assert.Contains(t, issued, "locked_until = NOW() + $1::interval",
		"the lease must be extended from NOW in the database's own clock: computing the expiry in Go "+
			"would extend it by however far the two clocks differ")
	assert.Contains(t, issued, "WHERE claim_token = $2",
		"the renewal must be scoped to the claim token, which is what makes it this batch's lease and "+
			"nobody else's")
	assert.Contains(t, issued, "status = $3",
		"and to the processing state: a row in any other state is finished or owned by somebody else, "+
			"and extending a lease on it would assert a hold this instance no longer has")
	assert.NotContains(t, issued, "id IN",
		"no id list may be threaded through: every terminal transition clears claim_token, so the token "+
			"already selects exactly the rows still in flight")
	assert.NotContains(t, issued, "attempts",
		"a renewal must not touch the retry budget: holding a lease longer is not an attempt, and "+
			"spending one here would dead-letter healthy events under a slow broker")
	assert.Contains(t, issued, "blnk.event_outbox",
		"the renewal must target blnk.event_outbox and never the lineage outbox")

	assert.Equal(t, int64(7), renewed,
		"the count must be reported so the caller can retire the heartbeat when nothing is left to hold")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRenewEventOutboxLease_RenewingNothingIsNotAnError pins the semantics that make the
// heartbeat self-retiring.
//
// Zero renewals means every row of the batch has reached a terminal state, which is how a batch
// ordinarily ends. Reporting it as a lost claim — the right answer for every conditional
// transition — would log an error on every successful batch and give the caller nothing to
// distinguish a finished batch from a real failure.
func TestRenewEventOutboxLease_RenewingNothingIsNotAnError(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	renewed, err := ds.RenewEventOutboxLease(context.Background(), "tok-finished", time.Minute)

	require.NoError(t, err, "a batch whose rows have all finished must not be reported as a failure")
	assert.Zero(t, renewed, "and the count must say so, so the caller can stop renewing")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRenewEventOutboxLease_RequiresTheClaimToken asserts the guard.
//
// Without a token the statement's WHERE clause degenerates to "every row nobody holds", which
// matches nothing on a healthy table and would be indistinguishable from a finished batch.
// Naming the omission is the only honest outcome.
func TestRenewEventOutboxLease_RequiresTheClaimToken(t *testing.T) {
	for name, token := range map[string]string{"empty": "", "whitespace": "  \t "} {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			renewed, err := ds.RenewEventOutboxLease(context.Background(), token, time.Minute)

			requireAPIError(t, err, apierror.ErrBadRequest)
			assert.Zero(t, renewed)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"a renewal that cannot be authorised must not reach the database")
		})
	}
}

// TestRenewEventOutboxLease_NonPositiveLeaseFallsBackToTheDefault mirrors the claim's treatment
// of a bad lease, and for the same reason.
//
// A renewal to an instant already past is worse than no renewal at all: it would look like a
// heartbeat while leaving every row of the batch immediately claimable by another instance.
// Normalising keeps the batch protected; failing would stop the heartbeat entirely.
func TestRenewEventOutboxLease_NonPositiveLeaseFallsBackToTheDefault(t *testing.T) {
	for _, lease := range []time.Duration{0, -time.Second} {
		t.Run(lease.String(), func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
				WithArgs(defaultEventClaimLease.String(), "tok", model.EventOutboxStatusProcessing).
				WillReturnResult(sqlmock.NewResult(0, 3))

			renewed, err := ds.RenewEventOutboxLease(context.Background(), "tok", lease)

			require.NoError(t, err)
			assert.Equal(t, int64(3), renewed)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"a non-positive lease must be bound as the default interval, never as a past instant")
		})
	}
}

// TestRenewEventOutboxLease_FailureBranchesAreReportedHonestly covers the two ways the renewal
// can go wrong, which are deliberately reported differently.
//
// A driver error is a failure: the lease may not have been extended, so the caller must be told
// and must log it. An UNCOUNTABLE result is not: the statement succeeded, and the count is used
// only to decide whether to keep renewing, so one uncounted round is harmless and reporting it
// as an error would abort a heartbeat over a driver's bookkeeping.
func TestRenewEventOutboxLease_FailureBranchesAreReportedHonestly(t *testing.T) {
	t.Run("a driver error is wrapped", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
			WillReturnError(errors.New("connection reset by peer"))

		renewed, err := ds.RenewEventOutboxLease(context.Background(), "tok", time.Minute)

		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.Zero(t, renewed)
		assert.NotContains(t, err.Error(), "connection reset by peer",
			"the driver's own text must not travel to a caller; it is logged instead")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("an uncountable result is not an error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
			WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected unavailable")))

		renewed, err := ds.RenewEventOutboxLease(context.Background(), "tok", time.Minute)

		require.NoError(t, err,
			"the renewal itself succeeded: a driver that cannot count must not abort the heartbeat")
		assert.Zero(t, renewed)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestClaimFailedEventOutboxForDeadLetter_ReachesRowsNoOtherClaimCan pins the predicate that
// ends the dead-letter limbo.
//
// # The limbo
//
// A row whose retry budget is spent and whose dead-letter WRITE failed sits at 'failed' with
// dlt_topic still NULL, and that is a dead end in three directions at once: the ordinary claim
// admits only attempts < max_attempts, replay accepts only 'dead_lettered', and the worker that
// held the token has moved on. The event exists only as that row, listed in the dead-letter
// inventory and acted on by nothing.
//
// # The two properties that make the repair work
//
// The status is deliberately NOT changed — the row stays in the inventory for the whole repair,
// and MarkEventDeadLettered accepts 'failed' as a prior state, so a recovered row completes
// through exactly the transition the first attempt would have used. And the claim is NOT
// attempt-bounded, because the dead-letter topic IS the last resort: abandoning the write would
// delete the only copy of the event. Both are asserted here because both are invisible in the
// method's signature.
func TestClaimFailedEventOutboxForDeadLetter_ReachesRowsNoOtherClaimCan(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(newEventOutboxRows())

	_, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, *captured, 1, "the repair claim must be a single statement")

	issued := (*captured)[0]

	assert.Contains(t, issued, "candidate.status = 'failed'",
		"the repair must claim exactly the rows the ordinary claim excludes")
	assert.Contains(t, issued, "candidate.dlt_topic IS NULL",
		"and only the UNPRESERVED ones: dlt_topic is set by the same statement that records "+
			"dead_lettered, so its absence on a failed row means the message never reached a topic")
	assert.NotContains(t, issued, "SET status",
		"the status must stay 'failed': it keeps the row in the dead-letter inventory for the whole "+
			"repair, and MarkEventDeadLettered accepts 'failed' as a prior state precisely so the "+
			"recovered row can finish through the ordinary transition")
	assert.NotContains(t, issued, "attempts <",
		"the repair must NOT be attempt-bounded: the dead-letter topic is the last resort, so giving "+
			"up on the write would delete the only copy of the event. Frequency and the 15-minute "+
			"dead-letter age alert are what bound it")
	assert.Contains(t, issued, "claim_token = $2",
		"a FRESH token must be stamped: the original belonged to a worker that may no longer exist, "+
			"and every transition is conditional on the token its caller holds")
	assert.Contains(t, issued, "locked_until = NOW() + $1::interval",
		"the lease is what stops several instances repairing one row at once, and what makes the "+
			"retry frequency-bounded")
	assert.Contains(t, issued, "FOR UPDATE SKIP LOCKED",
		"several relay instances must be able to repair disjoint subsets, exactly as they claim")
	assert.Contains(t, issued, "ORDER BY candidate.occurred_at ASC, candidate.id ASC",
		"the oldest unpreserved event is the most urgent, and the ordering must match the rest of the file")
	assert.Contains(t, issued, "SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC",
		"UPDATE ... RETURNING does not preserve the inner ORDER BY, so the outer re-sort is not redundant")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimFailedEventOutboxForDeadLetter_BindsLeaseTokenAndBatchSize pins the bound values and
// the freshness of the token.
func TestClaimFailedEventOutboxForDeadLetter_BindsLeaseTokenAndBatchSize(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	var claimToken driver.Value
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WithArgs("30s", captureArg(&claimToken), 20).
		WillReturnRows(newEventOutboxRows())

	_, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, 30*time.Second)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the repair claim must bind exactly (lease interval string, claim token, batch size) in that order")

	token, ok := claimToken.(string)
	require.True(t, ok, "the claim token must be bound as text")
	assert.True(t, model.IsCanonicalUUID(token),
		"the token must be freshly generated: a predictable or reused one would let the worker whose "+
			"dead-letter write already failed still move the row")
}

// TestClaimFailedEventOutboxForDeadLetter_GuardsMatchTheOrdinaryClaim asserts the two guards
// behave exactly as the ordinary claim's do, because the asymmetry between them is deliberate
// and easy to get backwards.
//
// A non-positive batch is REJECTED: a zero LIMIT claims nothing and raises nothing, which is
// indistinguishable from an empty repair backlog, so a broken caller would look healthy while
// events stayed stranded. A non-positive lease is NORMALISED: an expired-on-arrival lease is a
// correctness problem, but failing the poll would stop the repair altogether.
func TestClaimFailedEventOutboxForDeadLetter_GuardsMatchTheOrdinaryClaim(t *testing.T) {
	for _, batchSize := range []int{0, -1} {
		t.Run(fmt.Sprintf("batch size %d is rejected", batchSize), func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			entries, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), batchSize, time.Minute)

			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrBadRequest)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"a rejected batch size must not reach the database at all")
		})
	}

	for _, lease := range []time.Duration{0, -time.Second} {
		t.Run(fmt.Sprintf("lease %s is normalised", lease), func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
				WithArgs(defaultEventClaimLease.String(), sqlmock.AnyArg(), 20).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, lease)

			require.NoError(t, err,
				"a non-positive lease must be normalised, not rejected: failing the poll would stop the repair")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestClaimFailedEventOutboxForDeadLetter_ErrorsAreWrapped covers the read's three failure
// branches, so a repair that cannot run is reported rather than read as an empty backlog.
//
// That distinction is the whole point: "nothing needs repair" is the normal state, so a silently
// swallowed error here would be indistinguishable from health while stranded events accumulated.
func TestClaimFailedEventOutboxForDeadLetter_ErrorsAreWrapped(t *testing.T) {
	t.Run("the query fails", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
			WillReturnError(errors.New("deadlock detected"))

		entries, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, time.Minute)

		assert.Nil(t, entries)
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NotContains(t, err.Error(), "deadlock detected",
			"the driver's own text must not travel to a caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a row cannot be scanned", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		malformed := sqlmock.NewRows(eventOutboxProjectedColumns())
		values := make([]driver.Value, len(eventOutboxProjectedColumns()))
		values[0] = "not-an-int64" // id
		malformed.AddRow(values...)

		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnRows(malformed)

		entries, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, time.Minute)

		assert.Nil(t, entries)
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("iteration fails part-way", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		stranded := model.EventOutbox{
			ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
			PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions",
			SchemaVersion: model.SchemaVersionV1, Payload: json.RawMessage(`{}`),
			OccurredAt: time.Now().UTC(), Status: model.EventOutboxStatusFailed, MaxAttempts: 5,
		}

		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
			WillReturnRows(newEventOutboxRows(stranded).RowError(0, sql.ErrConnDone))

		entries, err := ds.ClaimFailedEventOutboxForDeadLetter(context.Background(), 20, time.Minute)

		assert.Nil(t, entries)
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// eventTransitionTable is every conditional transition, invoked with a held claim
// token, reduced to a plain error-returning closure.
//
// One table drives the wrapped-error, lost-claim and undeterminable-count cases below,
// which is what keeps a newly added transition from silently arriving without any of
// the three. MarkEventFailed's outcome is discarded here on purpose: these cases are
// about the failure branches, and the outcome is asserted where the decision is.
func eventTransitionTable() map[string]func(context.Context, Datasource) error {
	return map[string]func(context.Context, Datasource) error{
		"MarkEventDispatched": func(ctx context.Context, ds Datasource) error {
			return ds.MarkEventDispatched(ctx, 1, "tok", model.BrokerRecord{})
		},
		"MarkEventDeadLettered": func(ctx context.Context, ds Datasource) error {
			return ds.MarkEventDeadLettered(ctx, 1, "tok", "blnk.transactions.dlt", json.RawMessage(`{}`), model.BrokerRecord{})
		},
		"MarkWebhookDispatched": func(ctx context.Context, ds Datasource) error {
			return ds.MarkWebhookDispatched(ctx, 1, "tok")
		},
		"ReleaseEventReplay": func(ctx context.Context, ds Datasource) error {
			return ds.ReleaseEventReplay(ctx, 1, "tok", "gave up")
		},
	}
}

// TestMarkEventTransitions_DatabaseErrorIsWrapped covers the failure branch of every
// transition in one table, so none of them can be left without one.
func TestMarkEventTransitions_DatabaseErrorIsWrapped(t *testing.T) {
	for name, invoke := range eventTransitionTable() {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnError(sql.ErrConnDone)

			requireAPIError(t, invoke(context.Background(), ds), apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestMarkEventTransitions_ZeroRowsAffectedIsALostClaimConflict is the REGRESSION
// GUARD on the most consequential behaviour change in this file, and it replaces a
// test that asserted the opposite.
//
// The previous behaviour — and the previous test — treated an UPDATE that matched no
// row as benign: log a warning, return nil. The reasoning was that a concurrent relay
// whose lease expired may legitimately have moved the row on, and that failing the
// caller would turn a benign race into a spurious retry.
//
// That reasoning had the consequence backwards. Returning nil tells the caller its
// transition SUCCEEDED, so a worker that had lost its claim went on to act as though
// it owned the row: it recorded a terminal state over the top of the current holder's
// work, it recorded a failed attempt against a claim it no longer held — spending the
// retry budget at twice the intended rate — and it could move a row back out of a
// terminal state entirely. The spurious retry the old behaviour avoided is a much
// smaller cost than any of those.
//
// A lost claim is now a CONFLICT: nothing is broken, the caller simply no longer owns
// what it is trying to change, and the correct response is to stop working on the row.
func TestMarkEventTransitions_ZeroRowsAffectedIsALostClaimConflict(t *testing.T) {
	for name, invoke := range eventTransitionTable() {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
				WillReturnResult(sqlmock.NewResult(0, 0))

			err := invoke(context.Background(), ds)
			require.Error(t, err,
				"an UPDATE matching no row means the claim was lost; reporting success lets a zombie worker overwrite the current holder's state")

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrConflict, apiErr.Code,
				"a lost claim is a conflict, not a server fault")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestMarkEventTransitions_UndeterminableRowsAffectedIsNotAnError covers the last
// branch of the bookkeeping helper: a driver that cannot report RowsAffected.
//
// A completed UPDATE must not be turned into a failure by its own instrumentation.
// The transition already succeeded; only the diagnostic is unavailable. This is the
// one case that still resolves to success, and it does so because the UPDATE itself
// did not error — which is different in kind from an UPDATE that matched nothing.
func TestMarkEventTransitions_UndeterminableRowsAffectedIsNotAnError(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected not supported")))

	assert.NoError(t, ds.MarkEventDispatched(context.Background(), 5, "tok", model.BrokerRecord{}),
		"a driver that cannot report RowsAffected must not fail an UPDATE that already succeeded")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRequireEventOutboxRowAffected_ToleratesANilResult guards the defensive nil check
// directly.
//
// database/sql guarantees a non-nil Result alongside a nil error, so this should be
// unreachable — but a transition that has already SUCCEEDED must never be turned into
// a failure by its own bookkeeping, and the cost of being sure is one assertion.
func TestRequireEventOutboxRowAffected_ToleratesANilResult(t *testing.T) {
	assert.NoError(t, requireEventOutboxRowAffected(nil, 1, model.EventOutboxStatusDispatched),
		"bookkeeping must never fail a transition that already succeeded")
}

// ===========================================================================
// TIER 1 — the replay claim, and the concurrency it removes
// ===========================================================================

// TestClaimEventForReplay_TransitionsDeadLetteredToReplayingAndIssuesAToken pins the
// transition that makes concurrent replay safe.
//
// The precondition lives INSIDE the update. Two concurrent replays of one event
// therefore cannot both proceed: exactly one changes a row and receives a token, and
// the other is refused before it can publish. The read-then-check form this replaced
// let both pass and both publish, so an operator double-clicking put two copies of the
// event on the topic.
func TestClaimEventForReplay_TransitionsDeadLetteredToReplayingAndIssuesAToken(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	stored := model.EventOutbox{
		ID: 21, EventID: "b1f0c2d3-4e5f-4061-8273-8495a6b7c8d9", EventType: "transaction.applied",
		AggregateID: "agg", PartitionKey: "agg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(`{"event":"transaction.applied","data":{}}`),
		OccurredAt:    time.Now().UTC(), Status: model.EventOutboxStatusReplaying,
		MaxAttempts: 5, DLTTopic: "blnk.transactions.dlt", ClaimToken: "issued-token",
	}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnRows(newEventOutboxRows(stored))

	row, err := ds.ClaimEventForReplay(context.Background(), stored.EventID, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "status = $1", "the row must be moved into the replaying state by the claim itself")
	assert.Contains(t, issued, "claim_token = $2", "the claim must issue a token, or nothing can complete the replay")
	assert.Contains(t, issued, "WHERE event_id = $4",
		"the precondition must be part of the UPDATE, not a separate read")
	assert.Contains(t, issued, "status = $5",
		"a dead-lettered row is claimable")
	assert.Contains(t, issued, "status = $1 AND (locked_until IS NULL OR locked_until < NOW())",
		"and so is a replaying row whose LEASE HAS EXPIRED. Admitting dead_lettered alone left a "+
			"row stranded by a crash — or by a release that failed on an already-cancelled request "+
			"context — permanently unreplayable, with the lease written down and nothing reading it")
	assert.Contains(t, issued, "RETURNING", "the claimed row must come back from the same statement")

	assert.Equal(t, "issued-token", row.ClaimToken,
		"the token must reach the caller; without it no follow-up transition can be authorised")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestClaimEventForReplay_DiscriminatesMissingFromWrongState keeps two different
// operator mistakes apart.
//
// A wrong id and an event that is not dead-lettered are not the same problem. Told
// "not found" for an event they had already replayed, an operator goes looking for a
// typo instead of realising the replay had already happened — so the wrong-state case
// must be a conflict whose message names the status the row is actually in.
func TestClaimEventForReplay_DiscriminatesMissingFromWrongState(t *testing.T) {
	t.Run("no such event", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnError(sql.ErrNoRows)

		_, err := ds.ClaimEventForReplay(context.Background(), "missing", time.Minute)
		requireAPIError(t, err, apierror.ErrNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("present but already dispatched", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		existing := model.EventOutbox{
			ID: 7, EventID: "c2e1d3f4-5061-4172-8384-95a6b7c8d9e0", EventType: "transaction.applied",
			AggregateID: "agg", PartitionKey: "agg", Topic: "blnk.transactions",
			SchemaVersion: model.SchemaVersionV1,
			Payload:       json.RawMessage(`{"event":"transaction.applied","data":{}}`),
			OccurredAt:    time.Now().UTC(), Status: model.EventOutboxStatusDispatched, MaxAttempts: 5,
		}

		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnRows(newEventOutboxRows(existing))

		_, err := ds.ClaimEventForReplay(context.Background(), existing.EventID, time.Minute)
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), model.EventOutboxStatusDispatched,
			"the message must name the status the row is actually in, or an operator cannot tell which mistake they made")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestReleaseEventReplay_ReturnsTheRowToDeadLetteredSoItStaysReplayable pins the
// rollback half of the replay claim.
//
// Without it, a replay that claimed a row and then failed to publish would strand the
// row in replaying — outside the relay's claimable set AND outside the dead-letter
// inventory — where nothing would ever pick it up again. A failed replay must not cost
// an event its replayability.
func TestReleaseEventReplay_ReturnsTheRowToDeadLetteredSoItStaysReplayable(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectExec("").
		WithArgs(model.EventOutboxStatusDeadLettered, "broker refused", int64(21), "tok-21",
			model.EventOutboxStatusReplaying).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.ReleaseEventReplay(context.Background(), 21, "tok-21", "broker refused"))
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "status = $1", "the row must go back to dead_lettered, not stay in replaying")
	assert.Contains(t, issued, "last_error = COALESCE(NULLIF($2, ''), last_error)",
		"a blank reason must leave the original publish failure in place rather than blanking it")
	assert.Contains(t, issued, "claim_token = NULL", "the replay claim must be released")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ===========================================================================
// TIER 1 — retention
// ===========================================================================

// TestPurgeTerminalEventsBefore_DeletesOnlyTerminalRowsInABoundedSlice pins the
// retention primitive, and the safety property that makes it usable at all.
//
// WHAT THIS TABLE HOLDS is why retention exists: payload is the webhook body verbatim,
// so an identity event carries names, email addresses, phone numbers, postal addresses
// and dates of birth, and a transaction event carries amounts and balance identifiers.
// Kept forever, the delivery buffer becomes an unbounded second copy of the ledger's
// most sensitive data.
//
// The safety property is that only ELIGIBLE rows are deleted, and SEC-08 narrowed what
// eligible means to ONE state. A pending, processing, replaying or failed row is still owed a
// delivery attempt — failed is the subtle one: its retry budget is spent but its dead-letter
// write is not done, so this table is the ONLY copy of that event in existence.
//
// A DEAD-LETTERED row is the newly subtle one, and the reason this test changed. It is
// terminal — Blnk will not try again — and it used to be purged on that basis alone. But it
// is the record of an event NO SUBSCRIBER EVER RECEIVED: the only inventory triage reads,
// the only thing a replay can be driven from, and the only place the failure metadata
// explaining the loss exists. Deleting it on an age timer destroyed all of that
// unrecoverably, oldest first — the failure most likely to have been forgotten rather than
// handled. So it is NEVER eligible: a dead-lettered row leaves the inventory by being
// REPLAYED, which makes it dispatched, and the predicate below is what enforces that.
func TestPurgeTerminalEventsBefore_DeletesOnlyTerminalRowsInABoundedSlice(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// A QUERY rather than an exec, because the statement now RETURNS the deleted rows so it
	// can both count them and record them. See the assertions on the purge log below.
	mock.ExpectQuery(regexp.QuoteMeta("DELETE FROM blnk.event_outbox")).
		WithArgs(model.EventOutboxStatusDispatched, cutoff, 250).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(3)))

	purged, err := ds.PurgeTerminalEventsBefore(context.Background(), cutoff, 250)
	require.NoError(t, err)
	assert.Equal(t, int64(3), purged, "the caller sweeps in a loop and needs the true count to know when to stop")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "status = $1", "the eligible state must be bound, not interpolated")
	assert.NotContains(t, issued, model.EventOutboxStatusDeadLettered,
		"a dead-lettered row must never be reachable by this delete: purging one destroys the "+
			"only record that a ledger event went undelivered")
	assert.Contains(t, issued, "occurred_at < $2")
	assert.Contains(t, issued, "LIMIT $3",
		"the delete must be bounded, or one sweep blocks the relay and bloats the WAL in a single transaction")

	// THE DELETION AND ITS RECORD ARE ONE STATEMENT, and that is the assertion that matters
	// most here. A deleted row leaves no trace, so "how many terminal rows has retention
	// removed" is unanswerable unless the purge records it — and the daily reconciliation
	// cannot reach a sound verdict without that number, because a broker end offset counts
	// every record ever written and never falls. Two statements would leave a window in which
	// a crash keeps the deletion and loses the record, after which the reconciliation is
	// permanently and silently wrong by that batch's size with no way to discover it.
	assert.Contains(t, issued, "INSERT INTO blnk.event_outbox_purge_log",
		"the purge must record what it removed, in the same statement that removes it")
	assert.Contains(t, issued, "RETURNING occurred_at, kafka_offset",
		"the counts must be computed from the DELETED ROWS themselves, not from a second query "+
			"whose idea of the batch could differ")
	assert.Contains(t, issued, "COUNT(kafka_offset)",
		"the confirmed subset is counted separately because it restores a different baseline: "+
			"the row-to-record mapping rather than the terminal total")
	assert.Contains(t, issued, "HAVING COUNT(*) > 0",
		"a sweep that removed nothing must write no log row, or the batches that matter are "+
			"buried in records of the steady state")
	assert.Contains(t, issued, "MIN(occurred_at), MAX(occurred_at)",
		"the purged WINDOW is recorded, so an operator can tell whether a measurement window "+
			"overlaps a purge")

	assert.Equal(t, []string{model.EventOutboxStatusDispatched, model.EventOutboxStatusDeadLettered},
		model.TerminalEventOutboxStatuses(),
		"only dispatched and dead_lettered are terminal")
	assert.NotContains(t, model.TerminalEventOutboxStatuses(), model.EventOutboxStatusFailed,
		"a failed row still owes its dead-letter write, so this table is the only copy of that event and it must never be purged")
	assert.NotContains(t, model.TerminalEventOutboxStatuses(), model.EventOutboxStatusPending)
	assert.NotContains(t, model.TerminalEventOutboxStatuses(), model.EventOutboxStatusProcessing)
	assert.NotContains(t, model.TerminalEventOutboxStatuses(), model.EventOutboxStatusReplaying)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestEventOutboxRetention_GoAndSQLAgreeOnEligibility is the SEC-08 anti-drift guard.
//
// The purge predicate lives in SQL and model.EventOutbox.IsPurgeableByRetention states the
// same rule in Go, so a caller, a test and the runbook can evaluate eligibility without a
// database. Two expressions of one rule drift, and this drift would be SILENT and would go
// wrong in whichever direction the change was made: a looser Go rule tells an operator a row
// will be deleted that will not, and a looser SQL rule deletes evidence the Go rule promised
// to keep.
//
// So the table below is the rule, and both are checked against it.
func TestEventOutboxRetention_GoAndSQLAgreeOnEligibility(t *testing.T) {
	cases := []struct {
		name      string
		row       model.EventOutbox
		purgeable bool
		why       string
	}{
		{
			name:      "a dispatched row is purgeable on age alone",
			row:       model.EventOutbox{Status: model.EventOutboxStatusDispatched},
			purgeable: true,
			why:       "the event reached the broker and a subscriber has had it; the row is a receipt",
		},
		{
			name:      "a dead-lettered row is never purgeable",
			row:       model.EventOutbox{Status: model.EventOutboxStatusDeadLettered},
			purgeable: false,
			why: "it is the only record that a ledger event went undelivered, and deleting it " +
				"destroys the failure metadata and the bytes a replay is driven from. It leaves " +
				"the inventory by being replayed, which makes it dispatched",
		},
		{
			name:      "a failed row is never purgeable",
			row:       model.EventOutbox{Status: model.EventOutboxStatusFailed},
			purgeable: false,
			why:       "its dead-letter write is still owed, so this table is the only copy in existence",
		},
		{
			name:      "a pending row is not purgeable",
			row:       model.EventOutbox{Status: model.EventOutboxStatusPending},
			purgeable: false,
			why:       "a delivery attempt is still owed",
		},
		{
			name:      "a replaying row is not purgeable",
			row:       model.EventOutbox{Status: model.EventOutboxStatusReplaying},
			purgeable: false,
			why:       "a publish is in flight",
		},
	}

	// The SQL rule, rendered once from the statement the repository issues so the two
	// halves of this test cannot be checked against different predicates.
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	// A QUERY, not an exec: the delete RETURNS the rows it removed so the same statement can
	// count them and write the purge-log record from them.
	mock.ExpectQuery(regexp.QuoteMeta("DELETE FROM blnk.event_outbox")).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

	_, err := ds.PurgeTerminalEventsBefore(context.Background(),
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 100)
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	statement := (*captured)[0]

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.purgeable, tc.row.IsPurgeableByRetention(), tc.why)
		})
	}

	// And the SQL says the same thing: ONE arm, naming the one state a receipt is in.
	//
	// Compared on the NORMALISED statement rather than through whereClauseOf, because the
	// purge nests a bounded SELECT inside the DELETE and therefore carries two WHERE
	// keywords — the eligibility rule is the inner one.
	normalized := strings.Join(strings.Fields(statement), " ")
	assert.Contains(t, normalized,
		"WHERE status = $1 AND occurred_at < $2",
		"the SQL rule must be the Go rule: one age-governed arm, and it admits dispatched rows only")
	assert.NotContains(t, normalized, "status = ANY(",
		"a status SET is what conflated the two populations; eligibility is one state")
	assert.NotContains(t, normalized, "resolved_at",
		"the resolution column is gone: it created a state a broker-acknowledged replay could "+
			"not commit from, and retention needs no second column to be safe")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestPurgeTerminalEventsBefore_RefusesAZeroCutoffAndClampsTheSlice covers the two
// argument guards.
//
// A zero cutoff reads as "delete everything older than the year 1", which deletes
// nothing — far more likely an unset field than an intention, and silently doing
// nothing would hide a retention job that looks like it is running. A non-positive or
// oversized limit is clamped rather than refused, because the caller's intent is clear
// and the bound exists to protect the relay rather than the caller.
func TestPurgeTerminalEventsBefore_RefusesAZeroCutoffAndClampsTheSlice(t *testing.T) {
	t.Run("zero cutoff is refused before any statement", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		_, err := ds.PurgeTerminalEventsBefore(context.Background(), time.Time{}, 100)
		requireAPIError(t, err, apierror.ErrBadRequest)
		assert.NoError(t, mock.ExpectationsWereMet(), "the guard must run before any statement is issued")
	})

	t.Run("limits are clamped into range", func(t *testing.T) {
		for name, tc := range map[string]struct{ given, want int }{
			"zero":      {0, defaultEventPurgeBatchSize},
			"negative":  {-5, defaultEventPurgeBatchSize},
			"oversized": {maxEventPurgeBatchSize * 10, maxEventPurgeBatchSize},
			"in range":  {42, 42},
		} {
			t.Run(name, func(t *testing.T) {
				db, mock := newSQLMock(t)
				ds := Datasource{Conn: db}

				var boundLimit driver.Value
				// Three arguments: the one eligible state, the cutoff and the bound.
				mock.ExpectQuery(regexp.QuoteMeta("DELETE FROM blnk.event_outbox")).
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), captureArg(&boundLimit)).
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

				_, err := ds.PurgeTerminalEventsBefore(context.Background(),
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), tc.given)
				require.NoError(t, err)
				assert.EqualValues(t, tc.want, boundLimit)
				assert.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})
}

// ===========================================================================
// TIER 1 — the inserts, and THE PROOF OF REQUIREMENT R-2
// ===========================================================================

// TestInsertEventOutboxInTx_ParticipatesInCallerTransaction is the single most
// important test in this file.
//
// Requirement R-2 demands that the event row be written INSIDE THE SAME DATABASE
// TRANSACTION as the ledger mutation that produced it. That is the entire
// transactional-outbox guarantee: roll the transaction back and the event is gone with
// it, commit and neither can exist without the other. There is no window in which a
// balance moved but its event was lost, and none in which an event describes work that
// was rolled back.
//
// AN IMPLEMENTATION THAT INSERTS AFTER tx.Commit() PASSES EVERYTHING ELSE. Every
// happy-path test, every ordering test, every count test, every round-trip test — all
// green, while the guarantee is completely gone. There is no functional symptom until
// a process dies at exactly the wrong moment in production and a ledger mutation is
// left with no event, or an event is published for a mutation that never committed.
// This assertion is the only thing in the suite that catches it.
//
// The mechanism: the statement is expected BETWEEN ExpectBegin and ExpectRollback,
// with NO ExpectCommit scripted at all. sqlmock enforces expectation ORDER, so an
// insert issued on the pooled connection rather than on the transaction, or issued
// after the transaction closed, cannot satisfy this script.
func TestInsertEventOutboxInTx_ParticipatesInCallerTransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(insertedEventOutboxRows(1))
	mock.ExpectRollback()
	// Deliberately NO mock.ExpectCommit(): the insert must be enrolled in the
	// caller's transaction, and this test rolls that transaction back.

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	require.NoError(t, err)

	entry := newEventOutboxFixture("r2-")
	require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
	require.NoError(t, tx.Rollback())

	assert.NoError(t, mock.ExpectationsWereMet(),
		"the event insert must be issued on the caller's transaction, between BEGIN and ROLLBACK, with no commit of its own")
}

// TestInsertEventOutboxInTx_BackfillsGeneratedID asserts the RETURNING id is written
// back onto the supplied entry.
//
// The caller needs it after the commit: the relay and the dual-delivery leg both work
// from the in-memory row, and every subsequent transition — MarkEventDispatched,
// MarkEventFailed, MarkEventDeadLettered — keys on this surrogate id. Leaving it zero
// would make every one of them silently address row 0.
func TestInsertEventOutboxInTx_BackfillsGeneratedID(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(insertedEventOutboxRows(4242))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	entry := newEventOutboxFixture("id-")
	require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
	require.NoError(t, tx.Commit())

	assert.Equal(t, int64(4242), entry.ID,
		"the generated id must be written back: every later transition keys on it")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordTransactionWithBalancesAndOutbox_EnrolsEventInTheLedgerTransaction proves
// R-2 through the real atomic writer rather than through the repository method in
// isolation.
//
// Both halves matter, and they are asserted as two sub-cases:
//
//   - The COMMIT path pins the ORDERING. sqlmock matches expectations in order, so
//     scripting the event insert after the transaction insert and before the commit
//     asserts exactly that placement. Move the insert below tx.Commit() in
//     transaction.go and this sub-case fails, because the commit would arrive while
//     the insert expectation was still outstanding.
//   - The FAILURE path pins the ATOMICITY. A failing event insert must roll the whole
//     ledger mutation back — the balance updates and the transaction row included —
//     and must never commit. That is the direction of the guarantee that actually
//     protects the ledger: it is not enough that the event follows the mutation, the
//     mutation must also be abandoned if the event cannot be recorded.
func TestRecordTransactionWithBalancesAndOutbox_EnrolsEventInTheLedgerTransaction(t *testing.T) {
	newLedgerFixtures := func() (*model.Transaction, *model.Balance, *model.Balance) {
		now := time.Now().UTC()
		txn := &model.Transaction{
			TransactionID: "txn_r2", Source: "bln_source", Reference: "ref_r2",
			AmountString: "10.00", PreciseAmount: big.NewInt(1000), Precision: 100,
			Currency: "USD", Destination: "bln_dest", Description: "r2",
			Status: "APPLIED", CreatedAt: now, Hash: "hash_r2", EffectiveDate: &now,
		}
		newBalance := func(id string) *model.Balance {
			return &model.Balance{
				BalanceID: id, Balance: big.NewInt(1000), CreditBalance: big.NewInt(500),
				DebitBalance: big.NewInt(500), InflightBalance: big.NewInt(0),
				InflightCreditBalance: big.NewInt(0), InflightDebitBalance: big.NewInt(0),
				Currency: "USD", Version: 1,
			}
		}
		return txn, newBalance("bln_source"), newBalance("bln_dest")
	}

	t.Run("the event insert sits after the ledger writes and before the commit", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newLedgerFixtures()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO blnk.transactions").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(insertedEventOutboxRows(7))
		mock.ExpectCommit()

		entry := newEventOutboxFixture("r2-commit-")
		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil, entry)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, int64(7), entry.ID)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"the event insert must be issued inside the ledger transaction, after the ledger writes and before the commit")
	})

	t.Run("a failing event insert rolls the ledger mutation back and never commits", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newLedgerFixtures()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO blnk.transactions").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()
		// Deliberately NO mock.ExpectCommit(): if the event cannot be recorded, the
		// balance updates and the transaction row must be abandoned with it.

		entry := newEventOutboxFixture("r2-rollback-")
		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil, entry)

		require.Error(t, err, "a failed event insert must fail the whole ledger mutation")
		assert.Nil(t, result)
		// transaction.go wraps this with fmt.Errorf("...: %w", err), which is exactly
		// why the assertion unwraps with errors.As instead of a type assertion.
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"the ledger transaction must roll back and must never commit when the event insert fails")
	})

	t.Run("nil event entries are skipped rather than rejected", func(t *testing.T) {
		// A producer that assembles its slice conditionally must not have to compact
		// it, matching insertLineageOutboxesInTx's nil tolerance.
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newLedgerFixtures()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO blnk.transactions").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil, nil, nil)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"nil event entries must issue no statement at all")
	})
}

// TestRecordTransactionWithBalancesAndOutbox_CommitsAMonitorAlertWithTheMovement is the
// repository half of finding F-03's critical case.
//
// A balance.monitor alert describes a threshold the movement being persisted has just crossed, so
// it is mutation-derived and R-2 applies. The producer prepares it before the write; this asserts
// the writer accepts it ALONGSIDE the transaction's own event and commits both in the same
// database transaction.
//
// # The cardinality rule this exercises, and why it had to be refined rather than relaxed
//
// resolveEventOutboxes refuses more mutation-DESCRIBING rows than there are transactions, because
// two transaction events for one transaction is an undetectable duplicate publication. Counted
// over EVERY row, that rule would also refuse an alert riding along with its trigger — so the
// count is taken over the non-repeatable rows only, which is exactly the class the rule protects.
// The three sub-cases below pin both halves: the alert is accepted, and a genuine duplicate is
// still refused.
func TestRecordTransactionWithBalancesAndOutbox_CommitsAMonitorAlertWithTheMovement(t *testing.T) {
	newFixtures := func() (*model.Transaction, *model.Balance, *model.Balance) {
		now := time.Now().UTC()
		txn := &model.Transaction{
			TransactionID: "txn_f03", Source: "bln_source_f03", Reference: "ref_f03",
			AmountString: "10.00", PreciseAmount: big.NewInt(1000), Precision: 100,
			Currency: "USD", Destination: "bln_dest_f03", Description: "f03",
			Status: "APPLIED", CreatedAt: now, Hash: "hash_f03", EffectiveDate: &now,
		}
		newBalance := func(id string) *model.Balance {
			return &model.Balance{
				BalanceID: id, Balance: big.NewInt(1000), CreditBalance: big.NewInt(500),
				DebitBalance: big.NewInt(500), InflightBalance: big.NewInt(0),
				InflightCreditBalance: big.NewInt(0), InflightDebitBalance: big.NewInt(0),
				Currency: "USD", Version: 1,
			}
		}

		return txn, newBalance("bln_source_f03"), newBalance("bln_dest_f03")
	}

	monitorAlert := func(prefix string) *model.EventOutbox {
		alert := newEventOutboxFixture(prefix)
		alert.EventType = model.EventTypeBalanceMonitor
		alert.Topic = "blnk.balances"

		return alert
	}

	transactionEvent := func(prefix string) *model.EventOutbox {
		event := newEventOutboxFixture(prefix)
		event.EventType = "transaction.applied"
		event.Topic = "blnk.transactions"

		return event
	}

	t.Run("the alert is inserted inside the same transaction as the movement", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newFixtures()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO blnk.transactions").WillReturnResult(sqlmock.NewResult(1, 1))
		// TWO event inserts, both before the commit. The single-transaction writer inserts row
		// by row, so each is its own statement and each must land inside the transaction.
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(insertedEventOutboxRows(21))
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(insertedEventOutboxRows(22))
		mock.ExpectCommit()

		event := transactionEvent("f03-event-")
		alert := monitorAlert("f03-alert-")

		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil, event, alert)

		require.NoError(t, err,
			"a movement and the threshold alert it triggers must be committable together; refusing "+
				"the pair would force the alert back outside the transaction and reopen the loss window")
		require.NotNil(t, result)
		assert.Equal(t, int64(21), event.ID)
		assert.Equal(t, int64(22), alert.ID)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"both event inserts must be issued between BEGIN and COMMIT")
	})

	t.Run("a failing alert insert rolls the movement back and never commits", func(t *testing.T) {
		// The direction that makes it a guarantee. A crossing whose alert cannot be recorded
		// must not commit: the alternative is a balance past its threshold with no alert
		// anywhere and nothing able to discover afterwards that one was due.
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newFixtures()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE blnk.balances SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO blnk.transactions").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(insertedEventOutboxRows(23))
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()
		// Deliberately NO ExpectCommit.

		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil,
			transactionEvent("f03-event-fail-"), monitorAlert("f03-alert-fail-"))

		require.Error(t, err,
			"an alert that cannot be recorded must fail the movement that triggered it")
		assert.Nil(t, result)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"the movement must roll back and must never commit when its alert insert fails")
	})

	t.Run("two transaction events for one transaction are still refused", func(t *testing.T) {
		// The invariant must have been NARROWED, not removed. Two mutation-describing rows for
		// one transaction is a duplicate publication that nothing can detect afterwards, and it
		// must still be refused before any statement is issued.
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}
		txn, source, destination := newFixtures()
		// No expectations at all: the refusal must precede BEGIN.

		result, err := ds.RecordTransactionWithBalancesAndOutbox(
			context.Background(), txn, source, destination, nil,
			transactionEvent("f03-dup-a-"), transactionEvent("f03-dup-b-"))

		require.Error(t, err,
			"two events describing one transaction must be refused; counting only the repeatable "+
				"rows out of the cardinality check must not have disabled the check itself")
		assert.Nil(t, result)
		assert.NoError(t, mock.ExpectationsWereMet(),
			"the refusal must happen before any statement is issued, so no transaction is opened")
	})
}

// TestRecordTransactionsWithBalancesAndOutboxes_ForwardsEventOutboxes guards a
// specific, INVISIBLE bug.
//
// RecordTransactionsWithBalancesAndOutboxes is a one-line delegation to
// RecordTransactionsWithBalanceSetAndOutboxes. Because the event outboxes are a
// VARIADIC parameter, an implementation that forgets to forward them —
// `...Outboxes(ctx, txns, balances, outboxes)` instead of
// `...Outboxes(ctx, txns, balances, outboxes, eventOutboxes...)` — COMPILES CLEANLY,
// passes type checking, and silently drops every event produced by the bulk path.
// Nothing else would notice: the transactions still commit, the balances still move,
// and the only symptom is that bulk transaction events never reach Kafka.
//
// The script is deliberately minimal. Passing no transactions, no balances and no
// lineage outboxes exploits the empty-input short circuits in updateBalanceSet,
// recordTransactionsInTx and insertLineageOutboxesInTx, so the ONLY statement between
// BEGIN and COMMIT is the event insert. If the variadic is dropped, no statement is
// issued and the outstanding expectation fails the test.
func TestRecordTransactionsWithBalancesAndOutboxes_ForwardsEventOutboxes(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	entry := newEventOutboxFixture("fwd-")

	mock.ExpectBegin()
	// RETURNING comes back keyed by event_id rather than by position, because
	// RETURNING makes no promise about row order, so the scripted row must carry the
	// real event id for the id back-fill to find its entry.
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).AddRow(int64(77), entry.EventID))
	mock.ExpectCommit()

	result, err := ds.RecordTransactionsWithBalancesAndOutboxes(
		context.Background(), nil, nil, nil, nil, entry)

	require.NoError(t, err)
	assert.Empty(t, result, "no transactions were supplied, so none come back")
	assert.Equal(t, int64(77), entry.ID,
		"the forwarded entry must come back with its generated id")
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the bulk delegation must forward eventOutboxes...; dropping the variadic compiles cleanly and silently discards every bulk-path event")
}

// TestInsertEventOutboxInTx_BindsPendingStatusNotCallerStatus asserts the initial
// state belongs to this layer.
//
// The caller does not get to choose it — the same rule InsertLineageOutboxInTx
// establishes. Binding it explicitly means a caller that sets Status to processing,
// dispatched or dead_lettered cannot smuggle a row into the middle of the relay state
// machine, where it would either be published twice or never published at all.
//
// The in-memory struct is asserted too, because the relay and the dual-delivery leg
// read the entry back after the insert; leaving the struct disagreeing with the stored
// row would be a trap.
func TestInsertEventOutboxInTx_BindsPendingStatusNotCallerStatus(t *testing.T) {
	for _, smuggled := range []string{
		model.EventOutboxStatusProcessing,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusFailed,
		model.EventOutboxStatusDeadLettered,
	} {
		t.Run("caller set "+smuggled, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}
			ctx := context.Background()

			var boundStatus driver.Value
			mock.ExpectBegin()
			mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
				WithArgs(insertArgMatchers(t, "status", &boundStatus)...).
				WillReturnRows(insertedEventOutboxRows(1))
			mock.ExpectCommit()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)

			entry := newEventOutboxFixture("status-")
			entry.Status = smuggled
			require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
			require.NoError(t, tx.Commit())

			assert.Equal(t, model.EventOutboxStatusPending, boundStatus,
				"the initial status belongs to the repository, not the caller: a smuggled status would drop the row into the middle of the relay state machine")
			assert.Equal(t, model.EventOutboxStatusPending, entry.Status,
				"the in-memory entry must agree with the stored row; the relay reads it back after the insert")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestInsertEventOutboxInTx_PayloadBytesPassThroughUnmodified compares RAW BYTES, and
// that is the entire point of the test.
//
// The payload column holds the marshaled legacy webhook object — the exact bytes
// today's HTTP webhook body carries. Acceptance criteria V-8 (dual-delivery byte
// equality) and V-9 (byte-for-byte replay) are both written against those bytes. Any
// re-marshal, map[string]interface{} round trip or JSON compaction inside the
// repository would break both, and A TEST COMPARING UNMARSHALLED VALUES WOULD NOT
// NOTICE, because key order and whitespace are precisely what a re-marshal changes and
// precisely what an unmarshalled comparison discards.
//
// The fixture therefore uses deliberately NON-CANONICAL key order and spacing: "event"
// after "data", and padding around the separators. A single json.Marshal round trip
// anywhere in the insert path reorders and compacts this, and the byte comparison
// fails immediately.
//
// This assertion belongs at the BIND SITE and nowhere else. The column is JSONB, which
// is a parsed representation, so PostgreSQL itself sorts keys and renormalises
// whitespace on write — see the _RealDB counterpart, which asserts JSON EQUIVALENCE
// for exactly that reason. The bind site is the only place the repository could corrupt
// the bytes, so it is the only place byte equality is both meaningful and achievable.
func TestInsertEventOutboxInTx_PayloadBytesPassThroughUnmodified(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	nonCanonical := json.RawMessage(
		`{  "data" : { "zebra" : 1, "alpha" : 2 } ,  "event" : "transaction.applied"  }`)

	var boundPayload driver.Value
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WithArgs(insertArgMatchers(t, "payload", &boundPayload)...).
		WillReturnRows(insertedEventOutboxRows(1))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	entry := newEventOutboxFixture("bytes-")
	entry.Payload = nonCanonical
	require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
	require.NoError(t, tx.Commit())

	bound, ok := boundPayload.([]byte)
	require.True(t, ok, "the payload must be bound as raw bytes, got %T", boundPayload)
	assert.Equal(t, []byte(nonCanonical), bound,
		"the payload bytes must reach the statement byte-identical: key order and spacing are exactly what V-8 and V-9 assert on, and exactly what a re-marshal destroys")
	assert.Equal(t, string(nonCanonical), string(entry.Payload),
		"the insert must not rewrite the caller's payload in place either")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutboxInTx_AppliesDocumentedDefaults pins normalisation.
//
// Each default prevents a specific silent failure rather than being a convenience:
//
//   - A zero max_attempts would make the row both un-retryable AND UN-CLAIMABLE,
//     because the claim predicate requires attempts < max_attempts. The row would
//     never publish and never appear in the dead-letter inventory either — invisible
//     in both directions.
//   - A zero schema_version reaches subscribers on the wire, where it reads as an
//     unknown schema.
//   - A zero occurred_at would sort to the front of every FIFO claim forever, ahead of
//     every real event.
func TestInsertEventOutboxInTx_AppliesDocumentedDefaults(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(insertedEventOutboxRows(1))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	entry := newEventOutboxFixture("defaults-")
	entry.MaxAttempts = 0
	entry.SchemaVersion = 0
	entry.OccurredAt = time.Time{}

	before := time.Now().Add(-time.Second)
	require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
	require.NoError(t, tx.Commit())

	assert.Equal(t, defaultEventMaxAttempts, entry.MaxAttempts,
		"a zero retry budget would make the row both un-retryable and un-claimable, so it would never publish and never be dead-lettered either")
	assert.Equal(t, model.SchemaVersionV1, entry.SchemaVersion,
		"a zero schema version reaches subscribers on the wire as an unknown schema")
	assert.False(t, entry.OccurredAt.IsZero(),
		"a zero occurred_at would sort ahead of every real event in every FIFO claim, forever")
	assert.True(t, entry.OccurredAt.After(before), "occurred_at must default to now")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutbox_ValidationRejectsUnusableEntries covers EVERY field guard, on
// all three insert entry points.
//
// # Why the persistence layer validates at all
//
// A stored row is a COMMITMENT: the relay will try to publish it, retry it, spend its
// budget on it and finally preserve it on a dead-letter topic. A row that could never
// have been published is therefore not a harmless bad value — it is a guaranteed
// dead-letter entry that an operator has to triage, and the cause will by then be far
// away from the code that caused it. Each guard converts that into a typed bad request
// raised BEFORE any statement runs, which matters most inside a caller's ledger
// transaction where an opaque driver error surfaces nowhere near its origin.
//
// The cases, and the specific failure each prevents:
//
//   - A nil entry would panic on dereference.
//   - A non-UUID or non-canonical event_id is a SUBSCRIBER IDEMPOTENCY KEY a consumer
//     cannot deduplicate on reliably; the braced and unhyphenated spellings are refused
//     for the same reason, because two spellings of one UUID are two distinct keys. An
//     empty one is worse: the first row succeeds and every later one collides on
//     event_outbox_event_id_uidx, so the failure lands on an unrelated later write.
//   - A blank event_type is unroutable and unfilterable; a blank aggregate_id leaves
//     consumers nothing to group by; a blank partition_key makes Kafka scatter the event
//     round-robin and silently destroys the per-aggregate ordering guarantee.
//   - A topic outside the Blnk-owned namespace would have the relay lazily create a
//     writer and PUBLISH TO A DESTINATION BLNK DOES NOT OWN, driven by stored data.
//   - An unsupported schema_version reaches subscribers on the wire as an unknown
//     envelope shape.
//   - An absent payload has nothing to publish; invalid JSON would be rejected by the
//     JSONB column with a driver error instead of a field name; and an oversize payload
//     is accepted here only to be rejected by the broker on every attempt until it
//     dead-letters.
//
// Every case also asserts that NO statement reached the database, which is the half
// that proves the guard runs first rather than merely running.
func TestInsertEventOutbox_ValidationRejectsUnusableEntries(t *testing.T) {
	invalid := func(mutate func(*model.EventOutbox)) *model.EventOutbox {
		e := newEventOutboxFixture("invalid-")
		mutate(e)
		return e
	}

	cases := map[string]*model.EventOutbox{
		"nil entry": nil,

		"empty event id":         invalid(func(e *model.EventOutbox) { e.EventID = "" }),
		"non-uuid event id":      invalid(func(e *model.EventOutbox) { e.EventID = "evt_1" }),
		"braced uuid":            invalid(func(e *model.EventOutbox) { e.EventID = "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}" }),
		"unhyphenated uuid":      invalid(func(e *model.EventOutbox) { e.EventID = "6ba7b8109dad11d180b400c04fd430c8" }),
		"urn prefixed uuid":      invalid(func(e *model.EventOutbox) { e.EventID = "urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8" }),
		"uuid with whitespace":   invalid(func(e *model.EventOutbox) { e.EventID = " 6ba7b810-9dad-11d1-80b4-00c04fd430c8" }),
		"blank event type":       invalid(func(e *model.EventOutbox) { e.EventType = "   " }),
		"blank aggregate id":     invalid(func(e *model.EventOutbox) { e.AggregateID = "" }),
		"blank partition key":    invalid(func(e *model.EventOutbox) { e.PartitionKey = "\t " }),
		"blank topic":            invalid(func(e *model.EventOutbox) { e.Topic = "" }),
		"foreign topic":          invalid(func(e *model.EventOutbox) { e.Topic = "attacker.transactions" }),
		"unknown category topic": invalid(func(e *model.EventOutbox) { e.Topic = "blnk.payroll" }),
		"bare topic":             invalid(func(e *model.EventOutbox) { e.Topic = "transactions" }),
		"topic with whitespace":  invalid(func(e *model.EventOutbox) { e.Topic = " blnk.transactions" }),
		"unsupported version":    invalid(func(e *model.EventOutbox) { e.SchemaVersion = model.SchemaVersionV1 + 1 }),

		"nil payload":   invalid(func(e *model.EventOutbox) { e.Payload = nil }),
		"empty payload": invalid(func(e *model.EventOutbox) { e.Payload = json.RawMessage{} }),
		"invalid json":  invalid(func(e *model.EventOutbox) { e.Payload = json.RawMessage(`{"event":`) }),
		"oversize payload": invalid(func(e *model.EventOutbox) {
			// One byte past the limit, so the boundary itself is exercised rather than
			// something comfortably over it.
			filler := strings.Repeat("x", model.MaxEventMessageBytes)
			e.Payload = json.RawMessage(`{"event":"x","data":"` + filler + `"}`)
		}),
	}

	for name, entry := range cases {
		t.Run(name+" via InsertEventOutbox", func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			requireAPIError(t, ds.InsertEventOutbox(context.Background(), entry), apierror.ErrBadRequest)
			assert.NoError(t, mock.ExpectationsWereMet(),
				"validation must reject the entry before any statement runs")
		})

		t.Run(name+" via InsertEventOutboxInTx", func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}
			ctx := context.Background()

			mock.ExpectBegin()
			mock.ExpectRollback()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			requireAPIError(t, ds.InsertEventOutboxInTx(ctx, tx, entry), apierror.ErrBadRequest)
			require.NoError(t, tx.Rollback())

			assert.NoError(t, mock.ExpectationsWereMet(),
				"validation must fail inside the caller's transaction without issuing a statement")
		})

		t.Run(name+" via insertEventOutboxesInTx", func(t *testing.T) {
			if entry == nil {
				// The batch helper SKIPS nil elements rather than rejecting them, so a
				// nil entry is covered by TestInsertEventOutboxesInTx_SkipsNilEntries
				// instead. Asserting rejection here would contradict that contract.
				t.Skip("nil elements are skipped by the batch helper, not rejected")
			}

			db, mock := newSQLMock(t)
			ctx := context.Background()

			mock.ExpectBegin()
			mock.ExpectRollback()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			requireAPIError(t, insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{entry}), apierror.ErrBadRequest)
			require.NoError(t, tx.Rollback())

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestInsertEventOutboxInTx_DuplicateEventIDReturnsWrappedConflict pins the
// discrimination that makes retry safe.
//
// event_id carries event_outbox_event_id_uidx, which is what makes "one event is
// recorded once" a SCHEMA-ENFORCED invariant rather than merely an intention. A
// duplicate insert is therefore a CONFLICT and not a server fault: a caller retrying a
// mutation whose event was already captured has to be able to recognise that and carry
// on. Collapsing it into a 500 would turn an idempotent retry into an outage.
//
// The test also asserts the method does not PANIC. wrapEventOutboxInsertError reaches
// into *pq.Error to read Code.Name(), and a nil-safety mistake there would surface as
// a panic inside a ledger transaction — the worst possible place for one.
func TestInsertEventOutboxInTx_DuplicateEventIDReturnsWrappedConflict(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(&pq.Error{
			Code:       "23505",
			Message:    `duplicate key value violates unique constraint "event_outbox_event_id_uidx"`,
			Constraint: "event_outbox_event_id_uidx",
		})
	mock.ExpectRollback()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	entry := newEventOutboxFixture("dup-")

	var insertErr error
	require.NotPanics(t, func() {
		insertErr = ds.InsertEventOutboxInTx(ctx, tx, entry)
	}, "a driver error must never panic inside a caller's ledger transaction")
	require.NoError(t, tx.Rollback())

	requireAPIError(t, insertErr, apierror.ErrConflict)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutbox_DriverErrorsMapToTypedCodes covers the remaining arms of the
// error discrimination in one table, including the non-pq fallback.
//
// The distinction is what lets a caller act: a conflict is retry-safe, a bad request is
// the caller's fault and will never succeed on retry, and an internal error may.
// Collapsing them all into one code would make every failure look permanent or every
// failure look transient, and both are wrong.
func TestInsertEventOutbox_DriverErrorsMapToTypedCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want apierror.ErrorCode
	}{
		// The unique violation reaches ErrConflict HERE because the fixture's stored row
		// cannot be re-read — the mock expects only the insert — so nothing about "the event is
		// already recorded" is established and the original failure stands. That fall-through
		// is itself the contract: a duplicate is only forgiven when the stored row is proven
		// identical. The forgiving path has its own tests below.
		{"unique violation whose stored row cannot be read", &pq.Error{Code: "23505"}, apierror.ErrConflict},
		{"foreign key violation", &pq.Error{Code: "23503"}, apierror.ErrBadRequest},
		{"not null violation", &pq.Error{Code: "23502"}, apierror.ErrBadRequest},
		{"other postgres error", &pq.Error{Code: "42P01"}, apierror.ErrInternalServer},
		{"non-postgres error", sql.ErrConnDone, apierror.ErrInternalServer},
		{
			"wrapped unique violation",
			fmt.Errorf("driver said: %w", &pq.Error{Code: "23505"}),
			apierror.ErrConflict,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).WillReturnError(tc.err)

			entry := newEventOutboxFixture("map-")
			requireAPIError(t, ds.InsertEventOutbox(context.Background(), entry), tc.want)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestInsertEventOutbox_AnIdenticalEventAlreadyRecordedIsIdempotentSuccess is the
// non-transactional path's idempotency contract.
//
// # Why a duplicate here is SUCCESS
//
// event_outbox_event_id_uidx exists to make "one event is recorded once" an invariant the
// database enforces. When it refuses an insert it is reporting that the invariant HOLDS: the
// event is durable, exactly once, and every guarantee built on it is intact. Reporting that as
// a failure inverted the constraint's meaning — the caller was told the event was lost, an
// ERROR line in the log said so, and a retrying caller (the bulk-outcome capture retries three
// times) kept re-attempting a write that could only ever fail again before finally reporting an
// outcome that was sitting in the table all along.
//
// # The surrogate id must be ADOPTED, not left at zero
//
// A first-time insert returns the row fully identified. An idempotent success that left ID at 0
// would hand the caller a row the dead-letter path explicitly refuses — "a row with no database
// id cannot be dead-lettered" — so the two outcomes have to be indistinguishable to the caller.
func TestInsertEventOutbox_AnIdenticalEventAlreadyRecordedIsIdempotentSuccess(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	// Normalised up front so the fixture carries the SAME canonical envelope the insert will
	// bind, which is what the comparison is made on. The insert's own normalisation is
	// idempotent, so calling it here changes nothing about what is sent.
	entry := newEventOutboxFixture("idem-")
	require.NoError(t, prepareEventOutboxEntry(entry))
	require.NotEmpty(t, entry.EventRaw, "the fixture must carry its canonical envelope")

	// The row the database already holds: the same event, under the id a previous insert was
	// given.
	stored := *entry
	stored.ID = 4242
	stored.Status = model.EventOutboxStatusPending

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(&pq.Error{
			Code:       "23505",
			Message:    `duplicate key value violates unique constraint "event_outbox_event_id_uidx"`,
			Constraint: "event_outbox_event_id_uidx",
		})
	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(entry.EventID).
		WillReturnRows(newEventOutboxRows(stored))

	require.NoError(t, ds.InsertEventOutbox(ctx, entry),
		"an identical event already recorded is the unique index working, not the event being lost")
	assert.Equal(t, int64(4242), entry.ID,
		"the stored row's id must be adopted, or the caller holds a row nothing downstream accepts")

	assert.NoError(t, mock.ExpectationsWereMet(),
		"the duplicate must be resolved by RE-READING the stored row, not assumed")
}

// TestInsertEventOutbox_ADifferentEventUnderTheSameIDStaysAConflict is the boundary of that
// forgiveness, and the case worth failing over.
//
// Two DIFFERENT events sharing one event_id means an id has been reused. Swallowing the second
// would lose it for real — silently, reported as success — so the comparison is made on the
// canonical envelope bytes and anything that does not match exactly stays a conflict.
func TestInsertEventOutbox_ADifferentEventUnderTheSameIDStaysAConflict(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}
	ctx := context.Background()

	entry := newEventOutboxFixture("collide-")
	require.NoError(t, prepareEventOutboxEntry(entry))

	// Same id, DIFFERENT event. Only the stored envelope differs, which is the narrowest
	// possible difference and therefore the sharpest test of the comparison.
	stored := *entry
	stored.ID = 99
	stored.EventRaw = append([]byte(nil), entry.EventRaw...)
	stored.EventRaw[len(stored.EventRaw)-1] = ' '
	require.NotEqual(t, string(entry.EventRaw), string(stored.EventRaw))

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnError(&pq.Error{Code: "23505", Constraint: "event_outbox_event_id_uidx"})
	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(entry.EventID).
		WillReturnRows(newEventOutboxRows(stored))

	requireAPIError(t, ds.InsertEventOutbox(ctx, entry), apierror.ErrConflict)
	assert.Zero(t, entry.ID,
		"a genuine collision must not adopt the other event's id")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutbox_UsesTheConnectionNotATransaction pins the standalone variant.
//
// It exists for events that have NO accompanying ledger mutation to be atomic with —
// internal error notifications, and ledger and identity creation, none of which go
// through the atomic transaction writers. Those events still belong in the outbox so
// they get the same durable retry, dead-letter and replay treatment as every other
// event; there is simply no wider transaction to enrol them in.
//
// No ExpectBegin is scripted, so an implementation that opened its own transaction
// here would fail. That would not be harmlessly redundant: it would create a second
// transaction with its own commit point, and a caller who believed this method was
// atomic with something else would be wrong.
func TestInsertEventOutbox_UsesTheConnectionNotATransaction(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	// No ExpectBegin and no ExpectCommit: this path must not open a transaction.
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(insertedEventOutboxRows(31))

	entry := newEventOutboxFixture("standalone-")
	require.NoError(t, ds.InsertEventOutbox(context.Background(), entry))

	assert.Equal(t, int64(31), entry.ID)
	assert.Equal(t, model.EventOutboxStatusPending, entry.Status)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"the standalone insert must run on the connection and must not open a transaction of its own")
}

// ===========================================================================
// TIER 1 — insertEventOutboxesInTx, the batch path used by the bulk writers
// ===========================================================================

// TestInsertEventOutboxesInTx_EmptySliceIsNoOp asserts a zero-length batch issues NO
// statement.
//
// This is what lets the event rows be threaded through the atomic writers as a
// variadic: a caller that captures no events passes nothing, and this must be a
// no-op rather than an error. Every existing caller of the three atomic writers relies
// on it — that is the whole reason the parameter is variadic instead of a required
// slice — so an error here would break the frozen transaction-coalescing pipeline.
func TestInsertEventOutboxesInTx_EmptySliceIsNoOp(t *testing.T) {
	for name, entries := range map[string][]*model.EventOutbox{
		"nil slice":   nil,
		"empty slice": {},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ctx := context.Background()

			mock.ExpectBegin()
			mock.ExpectCommit()
			// No query expectation at all: an empty batch must reach PostgreSQL with
			// nothing, because a VALUES list containing no tuples is a syntax error.

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, insertEventOutboxesInTx(ctx, tx, entries))
			require.NoError(t, tx.Commit())

			assert.NoError(t, mock.ExpectationsWereMet(),
				"an empty batch must issue no statement; a VALUES list with no tuples is a syntax error")
		})
	}
}

// TestInsertEventOutboxesInTx_SkipsNilEntries mirrors insertLineageOutboxesInTx's nil
// tolerance, so a producer that assembles its slice conditionally does not have to
// compact it.
//
// The all-nil case is the interesting one: after compaction there is nothing left, so
// the helper must return before building a statement. Building one anyway would emit
// `VALUES` with no tuples and fail with a syntax error — and only on the specific input
// where every element happened to be nil.
func TestInsertEventOutboxesInTx_SkipsNilEntries(t *testing.T) {
	t.Run("mixed nil and real entries insert only the real ones", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ctx := context.Background()

		first := newEventOutboxFixture("skip-a-")
		second := newEventOutboxFixture("skip-b-")

		// One statement only: the nil elements must be compacted away rather than
		// binding NULL tuples that the NOT NULL columns would reject.
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
				AddRow(int64(1), first.EventID).
				AddRow(int64(2), second.EventID))
		mock.ExpectCommit()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{nil, first, nil, second, nil}))
		require.NoError(t, tx.Commit())

		assert.Equal(t, int64(1), first.ID)
		assert.Equal(t, int64(2), second.ID)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("an all-nil batch issues no statement", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ctx := context.Background()

		mock.ExpectBegin()
		mock.ExpectCommit()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{nil, nil, nil}))
		require.NoError(t, tx.Commit())

		assert.NoError(t, mock.ExpectationsWereMet(),
			"a batch that is empty AFTER compaction must also issue no statement")
	})
}

// TestInsertEventOutboxesInTx_BuildsOneMultiRowStatementWithBoundPlaceholders pins the
// statement construction.
//
// One multi-row INSERT rather than a loop of single inserts, because the coalescing
// path can present a large batch inside an already-open ledger transaction, where every
// extra round trip lengthens the window during which that transaction holds its balance
// row locks.
//
// The placeholder numbering is asserted because it is the ONLY string formatting in the
// statement. It is generated from eventOutboxInsertValueCount, never from caller data,
// and the assertion checks the second row's tuple starts at $11 — which is what proves
// the arithmetic strides by the column count rather than restarting or overlapping.
func TestInsertEventOutboxesInTx_BuildsOneMultiRowStatementWithBoundPlaceholders(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ctx := context.Background()

	first := newEventOutboxFixture("multi-a-")
	second := newEventOutboxFixture("multi-b-")

	mock.ExpectBegin()
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
		AddRow(int64(1), first.EventID).
		AddRow(int64(2), second.EventID))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{first, second}))
	require.NoError(t, tx.Commit())

	require.Len(t, *captured, 1, "a batch must be ONE statement, not one per row: extra round trips lengthen the ledger transaction's lock window")
	issued := (*captured)[0]

	assert.Contains(t, issued, "INSERT INTO blnk.event_outbox")
	// The expected tuples are DERIVED from eventOutboxInsertValueCount rather than
	// written out, so adding a column changes the assertion automatically and the test
	// keeps proving the same property: the numbering strides by the real column count.
	tupleAt := func(base int) string {
		placeholders := make([]string, 0, eventOutboxInsertValueCount)
		for offset := 0; offset < eventOutboxInsertValueCount; offset++ {
			placeholders = append(placeholders, fmt.Sprintf("$%d", base+offset))
		}
		return "(" + strings.Join(placeholders, ",") + ")"
	}

	assert.Contains(t, issued, tupleAt(1), "the first row's placeholders must start at $1")
	assert.Contains(t, issued, tupleAt(1+eventOutboxInsertValueCount),
		"the second row's placeholders must stride by the column count, proving the numbering is derived from eventOutboxInsertValueCount")
	assert.Contains(t, issued, "RETURNING id, event_id",
		"the batch must return event_id alongside id: RETURNING makes no promise about row order, so ids are matched by event id")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutboxesInTx_BackfillsGeneratedIDsByEventID asserts the ids are matched
// BY EVENT ID and not by position.
//
// PostgreSQL makes no promise about RETURNING row order, so a positional back-fill is a
// latent bug that happens to work whenever the server returns insertion order — which
// is most of the time. The scripted rows here come back DELIBERATELY REVERSED, so a
// positional implementation assigns each entry the other one's id and this test fails
// while a same-order script would have let it pass.
func TestInsertEventOutboxesInTx_BackfillsGeneratedIDsByEventID(t *testing.T) {
	db, mock := newSQLMock(t)
	ctx := context.Background()

	first := newEventOutboxFixture("backfill-a-")
	second := newEventOutboxFixture("backfill-b-")

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
			AddRow(int64(200), second.EventID). // reversed on purpose
			AddRow(int64(100), first.EventID))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{first, second}))
	require.NoError(t, tx.Commit())

	assert.Equal(t, int64(100), first.ID,
		"ids must be matched by event_id: RETURNING makes no promise about row order, so a positional back-fill assigns the wrong ids")
	assert.Equal(t, int64(200), second.ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestInsertEventOutboxesInTx_PartialInsertIsAnError guards against a LOST EVENT.
//
// A short RETURNING count means at least one event was silently not persisted. If the
// helper returned nil, the surrounding ledger transaction would commit and that event
// would be gone forever — a balance moved with no event announcing it, which is exactly
// the failure the transactional outbox exists to make impossible. Returning an error
// rolls the transaction back instead, so the mutation is retried WITH its event.
//
// This is the write-side half of acceptance criterion V-2, zero message loss.
func TestInsertEventOutboxesInTx_PartialInsertIsAnError(t *testing.T) {
	db, mock := newSQLMock(t)
	ctx := context.Background()

	first := newEventOutboxFixture("partial-a-")
	second := newEventOutboxFixture("partial-b-")

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
			AddRow(int64(1), first.EventID)) // one row short
	mock.ExpectRollback()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{first, second})
	require.NoError(t, tx.Rollback())

	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NoError(t, mock.ExpectationsWereMet(),
		"a short insert must fail so the surrounding ledger transaction rolls back rather than committing a mutation whose event was lost")
}

// TestInsertEventOutboxesInTx_QueryAndScanErrorsAreWrapped covers the batch path's three
// remaining failure branches: the statement itself, a per-row scan, and mid-stream row
// iteration. All three must fail the batch so the ledger transaction rolls back.
func TestInsertEventOutboxesInTx_QueryAndScanErrorsAreWrapped(t *testing.T) {
	entry := newEventOutboxFixture("batcherr-")

	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
					AddRow("not-a-bigserial", entry.EventID))
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
					AddRow(int64(1), entry.EventID).
					RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ctx := context.Background()

			mock.ExpectBegin()
			script(mock)
			mock.ExpectRollback()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			batchErr := insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{entry})
			require.NoError(t, tx.Rollback())

			requireAPIError(t, batchErr, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestInsertEventOutboxesInTx_ChunksBeyondTheStatementLimit asserts the batch is split.
//
// This is a HARD REQUIREMENT rather than tuning. PostgreSQL's extended protocol accepts
// at most 65535 bound parameters per statement; at ten parameters per row a single
// statement tops out around 6553 rows, and the transaction-coalescing path is allowed to
// present up to 10000 transactions in one batch. An unbounded batch would therefore fail
// outright on a large enough coalesced write and roll the whole ledger transaction back
// with it.
//
// Every chunk runs inside the CALLER'S transaction, so the batch remains all-or-nothing
// even though it is several statements — which is why the chunking does not weaken the
// atomicity guarantee.
func TestInsertEventOutboxesInTx_ChunksBeyondTheStatementLimit(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ctx := context.Background()

	total := maxEventOutboxInsertRows + 5
	entries := make([]*model.EventOutbox, 0, total)
	for i := 0; i < total; i++ {
		entries = append(entries, newEventOutboxFixture(fmt.Sprintf("chunk-%d-", i)))
	}

	chunkRows := func(chunk []*model.EventOutbox) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"id", "event_id"})
		for i, entry := range chunk {
			rows.AddRow(int64(i+1), entry.EventID)
		}
		return rows
	}

	mock.ExpectBegin()
	mock.ExpectQuery("").WillReturnRows(chunkRows(entries[:maxEventOutboxInsertRows]))
	mock.ExpectQuery("").WillReturnRows(chunkRows(entries[maxEventOutboxInsertRows:]))
	mock.ExpectCommit()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, insertEventOutboxesInTx(ctx, tx, entries))
	require.NoError(t, tx.Commit())

	require.Len(t, *captured, 2,
		"a batch larger than maxEventOutboxInsertRows must be split; PostgreSQL accepts at most 65535 bound parameters per statement")
	for i, issued := range *captured {
		assert.LessOrEqual(t, strings.Count(issued, "("), maxEventOutboxInsertRows+2,
			"chunk %d must stay within the row bound", i)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ===========================================================================
// TIER 1 — the query methods
// ===========================================================================

// TestGetEventByID_KeysOnTheBusinessEventID asserts the lookup targets event_id and not
// the surrogate id.
//
// event_id is what the dead-letter replay route carries and what a subscriber quotes
// when it asks for an event to be resent; the BIGSERIAL id is never exposed outside this
// layer. Keying on id would make the replay endpoint unusable — and it would still
// compile and still return rows, just the wrong ones, since both columns are addressable
// by number.
func TestGetEventByID_KeysOnTheBusinessEventID(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	want := model.EventOutbox{
		ID: 5, EventID: "evt_lookup", EventType: "transaction.applied", AggregateID: "agg",
		LedgerID: "ldg", Topic: "blnk.transactions", SchemaVersion: model.SchemaVersionV1,
		Payload:    json.RawMessage(`{"event":"transaction.applied","data":{}}`),
		OccurredAt: time.Now().UTC(), Status: model.EventOutboxStatusDeadLettered,
		Attempts: 5, MaxAttempts: 5,
	}

	mock.ExpectQuery("").WithArgs("evt_lookup").WillReturnRows(newEventOutboxRows(want))

	got, err := ds.GetEventByID(context.Background(), "evt_lookup")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "evt_lookup", got.EventID)
	assert.Equal(t, int64(5), got.ID)

	require.Len(t, *captured, 1)
	assert.Contains(t, (*captured)[0], "WHERE event_id = $1",
		"the lookup must key on the business event_id: the surrogate id is never exposed outside this layer")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetEventByID_NotFoundReturnsTypedAPIError pins a DELIBERATE DIVERGENCE from
// GetOutboxByTransactionID, which returns (nil, nil) for a missing row.
//
// The replay handler has to tell "no such event" apart from "found it" in order to answer
// correctly, and a nil-nil result forces every caller to re-derive that distinction and
// invites the nil dereference that follows from forgetting to. A typed not-found error is
// what the API layer maps to its event-not-found response.
//
// Both the not-nil error AND the nil entry are asserted, because returning a populated
// zero-value entry alongside the error would be just as dangerous.
func TestGetEventByID_NotFoundReturnsTypedAPIError(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs("evt_missing").
		WillReturnError(sql.ErrNoRows)

	got, err := ds.GetEventByID(context.Background(), "evt_missing")

	assert.Nil(t, got, "a missing row must not come back as a zero-value entry")
	requireAPIError(t, err, apierror.ErrNotFound)
	assert.NotErrorIs(t, err, sql.ErrNoRows,
		"the driver sentinel must not leak to callers; the typed code is what the API layer maps")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetEventByID_ScanErrorIsWrappedAsInternal keeps a genuine fault distinguishable
// from a missing row. Collapsing the two would make a broken column mapping look like a
// bad event id and send an operator hunting for the wrong problem.
func TestGetEventByID_ScanErrorIsWrappedAsInternal(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs("evt_broken").
		WillReturnError(sql.ErrConnDone)

	got, err := ds.GetEventByID(context.Background(), "evt_broken")

	assert.Nil(t, got)
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListDeadLetterInventory_CoversBothTerminalFailureStates asserts the filter includes
// failed AS WELL AS dead_lettered, and that every predicate is applied in SQL.
//
// A row becomes failed the moment its retry budget is spent, and dead_lettered only once
// the event has additionally been written to its <topic>.dlt sibling. Listing only the
// latter would HIDE events that exhausted their retries but whose dead-letter publication
// itself failed — which are exactly the events an operator most needs to see, because they
// are the ones sitting nowhere at all. The partial index idx_event_outbox_failed covers
// both literals for this reason.
func TestListDeadLetterInventory_CoversBothTerminalFailureStates(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	deadLettered := deadLetterInventoryEntryOf(model.EventOutbox{
		ID: 2, EventID: "evt_dlt", EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "key", LedgerID: "ldg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(`{"event":"transaction.applied","data":{}}`),
		OccurredAt:    dbTimestamp(time.Now()),
		Status:        model.EventOutboxStatusDeadLettered, Attempts: 5,
		DLTTopic: "blnk.transactions.dlt",
	})
	stranded := deadLettered
	stranded.ID, stranded.EventID, stranded.Status, stranded.DLTTopic =
		1, "evt_stranded", model.EventOutboxStatusFailed, ""

	// The population comes from model.DeadLetterInventoryStatuses as a single array
	// parameter, which is what lets the predicate builder serve the page, the count and
	// the age gauge from one rendering — see deadLetterInventoryPredicate.
	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
			"", "", "", nil, int64(0), nil, nil, defaultDeadLetterPageSize+1,
		).
		WillReturnRows(newDeadLetterInventoryRows(deadLettered, stranded))

	page, err := ds.ListDeadLetterInventory(context.Background(), model.DeadLetterInventoryQuery{})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt_dlt", "evt_stranded"}, deadLetterEventIDsOf(page),
		"an event stranded in failed, whose dead-letter publication itself failed, must still be visible to an operator")
	assert.False(t, page.HasMore, "two rows against a page of fifty leaves nothing to follow")
	assert.Nil(t, page.NextCursor)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "WHERE status IN ($1, $2)")
	assert.Contains(t, issued, "ORDER BY occurred_at DESC, id DESC",
		"triage starts from the most recent failures, and id breaks ties so paging cannot show or skip a row twice")

	// PERF-P06: the predicates live in the statement. Asserted on the SQL because a fixture
	// small enough to read cannot distinguish "the database filtered" from "the application
	// filtered what the database returned" — and that distinction was the finding.
	assert.Contains(t, issued, "($3 = '' OR status = $3)")
	assert.Contains(t, issued, "($4 = '' OR event_type = $4)")
	assert.Contains(t, issued, "($5 = '' OR topic = $5)")

	// PERF-P08: a keyset, not an offset.
	assert.Contains(t, issued, "(occurred_at, id) < ($6::timestamptz, $7::bigint)")
	assert.NotContains(t, issued, "OFFSET",
		"an offset's cost grows with its depth and the depth was the caller's to choose")

	// The occurrence window is applied in SQL too, and is bound as a nullable pair so that an
	// unbounded end means "no bound" rather than "the zero instant".
	assert.Contains(t, issued, "($8::timestamptz IS NULL OR occurred_at >= $8::timestamptz)")
	assert.Contains(t, issued, "($9::timestamptz IS NULL OR occurred_at <= $9::timestamptz)")
	assert.Contains(t, issued, "LIMIT $10")
	assert.NotContains(t, issued, "resolved_at",
		"the resolution narrowing is gone with the endpoint that wrote it: every entry in this "+
			"inventory is outstanding, and a replay is what takes one out")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListDeadLetterInventory_ReadsNeitherBodyColumn is the PERF-P06 guard on the projection.
//
// payload_raw and event_raw are the same event body twice over, up to 768 KiB each, and the
// listing shows neither: it reports the body's SIZE, computed inside PostgreSQL. Reading whole
// rows to build a page of at most a few hundred small items moved gibibytes to answer a
// question about kilobytes.
//
// Asserted on the statement text rather than on the returned entries, because a projection that
// selected the bodies and discarded them in Go would satisfy every assertion about the entries
// while costing exactly what the finding was about.
func TestListDeadLetterInventory_ReadsNeitherBodyColumn(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(newDeadLetterInventoryRows())

	_, err := ds.ListDeadLetterInventory(context.Background(), model.DeadLetterInventoryQuery{})
	require.NoError(t, err)

	require.Len(t, *captured, 1)
	projection := (*captured)[0]
	projection = projection[:strings.Index(projection, "FROM blnk.event_outbox")]

	assert.NotContains(t, projection, "event_raw",
		"the canonical envelope is the body; a listing has no use for it")
	assert.Contains(t, projection, "octet_length(payload_raw) AS payload_bytes",
		"the SIZE is what a triage view needs, and octet_length computes it without moving the bytes")
	assert.NotRegexp(t, `(^|[\s,(])payload_raw([\s,)]|$)`, strings.ReplaceAll(projection, "octet_length(payload_raw)", ""),
		"payload_raw must appear ONLY inside octet_length")

	// The model cannot carry a body either, which is what stops the projection widening back.
	entryType := reflect.TypeOf(model.DeadLetterInventoryEntry{})
	for _, field := range []string{"Payload", "PayloadRaw", "EventRaw"} {
		_, present := entryType.FieldByName(field)
		assert.False(t, present, "model.DeadLetterInventoryEntry must have no %s field", field)
	}

	assert.NoError(t, mock.ExpectationsWereMet())

	// The two literals the unfiltered predicate admits come from ONE list, so a state
	// added to the inventory cannot reach the query without reaching the count and the
	// status-filter validation as well.
	assert.Equal(t,
		[]string{model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed},
		model.DeadLetterInventoryStatuses(),
		"the inventory's composition is published through one list")
}

// TestListDeadLetteredEvents_AppliesEveryNarrowingInSQL is the whole point of the filter
// contract, and the defect it closes was invisible to the client.
//
// The narrowing used to be applied ABOVE this layer, in the service, by paging the
// inventory and testing rows in Go up to a fixed scan ceiling. A page that reached the
// ceiling was returned looking exactly like a complete one — only a log line said
// otherwise — and an exact total was impossible, so include_count had to be refused for
// any event-type or topic filter. Both were properties of where the filtering happened.
//
// The proof is the SQL: every filter must appear as a predicate, positionally aligned with
// its argument, and LIMIT/OFFSET must bind AFTER them so the page applies to the filtered
// set.
func TestListDeadLetteredEvents_AppliesEveryNarrowingInSQL(t *testing.T) {
	from := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	to := from.Add(20 * time.Minute)

	cases := []struct {
		name          string
		query         model.DeadLetterQuery
		wantPredicate []string
		wantArgs      []driver.Value
		wantPage      string
	}{
		{
			name:          "unfiltered admits both terminal states",
			query:         model.DeadLetterQuery{Limit: 10},
			wantPredicate: []string{"WHERE status IN ($1, $2)"},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, 10, 0,
			},
			wantPage: "LIMIT $3 OFFSET $4",
		},
		{
			name:  "a status filter narrows to one state by equality",
			query: model.DeadLetterQuery{Status: model.EventOutboxStatusFailed, Limit: 10},
			// Equality rather than a one-element IN, so the same partial index is used
			// either way.
			wantPredicate: []string{"WHERE status = $1"},
			wantArgs:      []driver.Value{model.EventOutboxStatusFailed, 10, 0},
			wantPage:      "LIMIT $2 OFFSET $3",
		},
		{
			name:  "an event type filter is a predicate, not a post-filter",
			query: model.DeadLetterQuery{EventType: "transaction.applied", Limit: 10},
			wantPredicate: []string{
				"WHERE status IN ($1, $2)", "event_type = $3",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"transaction.applied", 10, 0,
			},
			wantPage: "LIMIT $4 OFFSET $5",
		},
		{
			name:  "a topic filter matches the ORIGINAL topic column",
			query: model.DeadLetterQuery{Topic: "blnk.transactions", Limit: 10},
			wantPredicate: []string{
				"WHERE status IN ($1, $2)", "topic = $3",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"blnk.transactions", 10, 0,
			},
			wantPage: "LIMIT $4 OFFSET $5",
		},
		{
			name:  "the occurrence window bounds occurred_at inclusively at both ends",
			query: model.DeadLetterQuery{OccurredFrom: from, OccurredTo: to, Limit: 10},
			wantPredicate: []string{
				"WHERE status IN ($1, $2)", "occurred_at >= $3", "occurred_at <= $4",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				from, to, 10, 0,
			},
			wantPage: "LIMIT $5 OFFSET $6",
		},
		{
			name: "every filter combines, and the page binds after all of them",
			query: model.DeadLetterQuery{
				Status:       model.EventOutboxStatusDeadLettered,
				EventType:    "transaction.applied",
				Topic:        "blnk.transactions",
				OccurredFrom: from,
				OccurredTo:   to,
				Limit:        25,
				Offset:       75,
			},
			wantPredicate: []string{
				"WHERE status = $1", "event_type = $2", "topic = $3",
				"occurred_at >= $4", "occurred_at <= $5",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, "transaction.applied",
				"blnk.transactions", from, to, 25, 75,
			},
			wantPage: "LIMIT $6 OFFSET $7",
		},
		{
			name: "surrounding whitespace on a filter is ignored",
			query: model.DeadLetterQuery{
				EventType: "  transaction.applied  ",
				Topic:     "  blnk.transactions  ",
				Status:    "  " + model.EventOutboxStatusFailed + "  ",
				Limit:     10,
			},
			wantPredicate: []string{
				"WHERE status = $1", "event_type = $2", "topic = $3",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusFailed, "transaction.applied",
				"blnk.transactions", 10, 0,
			},
			wantPage: "LIMIT $4 OFFSET $5",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, captured := newCapturingSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WithArgs(tc.wantArgs...).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEvents(context.Background(), tc.query)
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet(),
				"every filter must reach SQL as a bound argument")

			require.Len(t, *captured, 1)
			issued := (*captured)[0]
			for _, predicate := range tc.wantPredicate {
				assert.Contains(t, issued, predicate)
			}
			assert.Contains(t, issued, tc.wantPage,
				"the page must bind after the filters, so the offset is an offset into the FILTERED set")
			assert.Contains(t, issued, "ORDER BY occurred_at DESC, id DESC",
				"ordering is fixed regardless of the narrowing, so paging stays stable")
		})
	}
}

// TestCountDeadLetteredEvents_CountsTheSamePredicateAsThePage is what makes include_count
// answerable for a filtered request at all.
//
// The total used to come from a whole-table per-status aggregate that knew nothing about
// the event-type or topic filter, so the endpoint refused include_count rather than report
// a total describing a different set than the page. A total over a different predicate is
// worse than no total: a client pages until it has seen total_count rows and either loops
// forever or stops early, with nothing in the response to say which.
//
// The page is deliberately IGNORED. The total is a property of the filter, not of the
// window into it — honouring Limit here would make total_count never exceed the page size,
// which is precisely the number a caller asks for it in order not to have to guess.
func TestCountDeadLetteredEvents_CountsTheSamePredicateAsThePage(t *testing.T) {
	from := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	to := from.Add(20 * time.Minute)

	query := model.DeadLetterQuery{
		EventType:    "transaction.applied",
		Topic:        "blnk.transactions",
		OccurredFrom: from,
		OccurredTo:   to,
		// Both are set and both must be absent from the emitted SQL.
		Limit:  25,
		Offset: 75,
	}

	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
			"transaction.applied", "blnk.transactions", from, to,
		).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(412)))

	total, err := ds.CountDeadLetteredEvents(context.Background(), query)
	require.NoError(t, err)
	assert.Equal(t, int64(412), total)
	require.NoError(t, mock.ExpectationsWereMet(),
		"the count must bind exactly the page's own filters and nothing else")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "COUNT(*)")
	assert.Contains(t, issued, "WHERE status IN ($1, $2)")
	assert.Contains(t, issued, "event_type = $3")
	assert.Contains(t, issued, "topic = $4")
	assert.Contains(t, issued, "occurred_at >= $5")
	assert.Contains(t, issued, "occurred_at <= $6")
	assert.NotContains(t, issued, "LIMIT",
		"the total is a property of the filter, not of the window into it")
	assert.NotContains(t, issued, "OFFSET")
	assert.NotContains(t, issued, "ORDER BY",
		"ordering a count is wasted work the planner should never be asked for")

	t.Run("a failure is wrapped as an internal error", func(t *testing.T) {
		failing, failMock := newSQLMock(t)
		failingDS := Datasource{Conn: failing}

		failMock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnError(sql.ErrConnDone)

		total, err := failingDS.CountDeadLetteredEvents(context.Background(), model.DeadLetterQuery{})
		assert.Zero(t, total)
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, failMock.ExpectationsWereMet())
	})
}

// TestListDeadLetterInventory_NormalisesTheLimitAndPassesTheCursorThrough asserts a malformed
// page request degrades to a sane one rather than scanning the table or failing.
//
// The upper cap is the load-bearing half: without it a caller could turn a triage
// endpoint into a full table scan of an outbox that grows without bound, which is a
// denial-of-service surface reachable from an authenticated operator endpoint.
//
// The cursor is passed through VERBATIM (PERF-P08). There is nothing to clamp about a position,
// and the bound limit is limit+1 in every case — the probe row that answers "is there another
// page" without a COUNT.
func TestListDeadLetterInventory_NormalisesTheLimitAndPassesTheCursorThrough(t *testing.T) {
	resumeAt := dbTimestamp(time.Now().Add(-time.Hour))

	cases := []struct {
		name      string
		query     model.DeadLetterInventoryQuery
		wantLimit int
		wantTime  driver.Value
		wantID    int64
	}{
		{"zero limit takes the default", model.DeadLetterInventoryQuery{}, defaultDeadLetterPageSize, nil, 0},
		{
			"negative limit takes the default",
			model.DeadLetterInventoryQuery{Limit: -10}, defaultDeadLetterPageSize, nil, 0,
		},
		{
			"oversized limit is capped",
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize + 1}, maxDeadLetterPageSize, nil, 0,
		},
		{
			"limit at the cap is preserved",
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize}, maxDeadLetterPageSize, nil, 0,
		},
		{
			"a cursor is bound as the pair it is",
			model.DeadLetterInventoryQuery{
				Limit:  25,
				Cursor: &model.DeadLetterCursor{OccurredAt: resumeAt, ID: 8171},
			},
			25, resumeAt, 8171,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WithArgs(
					model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
					"", "", "", tc.wantTime, tc.wantID, nil, nil, tc.wantLimit+1,
				).
				WillReturnRows(newDeadLetterInventoryRows())

			_, err := ds.ListDeadLetterInventory(context.Background(), tc.query)
			require.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetterInventory_BindsTheFiltersItWasGiven asserts each filter reaches the
// statement in its own placeholder, and that an absent filter binds the empty string the
// statement's `($n = ” OR ...)` arm is written for.
func TestListDeadLetterInventory_BindsTheFiltersItWasGiven(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
			model.EventOutboxStatusFailed, "transaction.applied", "blnk.transactions",
			nil, int64(0), nil, nil, 11,
		).
		WillReturnRows(newDeadLetterInventoryRows())

	_, err := ds.ListDeadLetterInventory(context.Background(), model.DeadLetterInventoryQuery{
		Limit:     10,
		Status:    model.EventOutboxStatusFailed,
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListDeadLetterInventory_ReadsAProbeRowToAnswerHasMore pins the limit+1 contract.
//
// The extra row is READ and not RETURNED, and the cursor comes from the last row that WAS
// returned. Taking it from the probe instead would skip that row on the next page — a silent
// gap in an inventory an operator is triaging from.
func TestListDeadLetterInventory_ReadsAProbeRowToAnswerHasMore(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	base := time.Now().UTC().Truncate(time.Microsecond)
	entries := make([]model.DeadLetterInventoryEntry, 0, 3)
	for i := range 3 {
		entries = append(entries, deadLetterInventoryEntryOf(model.EventOutbox{
			ID: int64(30 - i), EventID: fmt.Sprintf("evt_%d", i), EventType: "transaction.applied",
			AggregateID: "agg", PartitionKey: "key", Topic: "blnk.transactions",
			SchemaVersion: model.SchemaVersionV1, Payload: json.RawMessage(`{}`),
			OccurredAt: base.Add(-time.Duration(i) * time.Minute),
			Status:     model.EventOutboxStatusDeadLettered,
			DLTTopic:   "blnk.transactions.dlt",
		}))
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
			"", "", "", nil, int64(0), nil, nil, 3,
		).
		WillReturnRows(newDeadLetterInventoryRows(entries...))

	page, err := ds.ListDeadLetterInventory(context.Background(), model.DeadLetterInventoryQuery{Limit: 2})
	require.NoError(t, err)

	assert.Equal(t, []string{"evt_0", "evt_1"}, deadLetterEventIDsOf(page),
		"the probe row must be read and discarded, never returned")
	assert.True(t, page.HasMore, "its existence is the answer")
	require.NotNil(t, page.NextCursor)
	assert.Equal(t, entries[1].OccurredAt, page.NextCursor.OccurredAt,
		"the cursor must name the LAST RETURNED row, so the next page resumes exactly where this one stopped")
	assert.Equal(t, entries[1].ID, page.NextCursor.ID)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListDeadLetterInventory_ErrorsAreWrapped covers the statement, scan and row
// iteration branches of the listing.
func TestListDeadLetterInventory_ErrorsAreWrapped(t *testing.T) {
	valid := deadLetterInventoryEntryOf(model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(`{}`), OccurredAt: dbTimestamp(time.Now()),
		Status: model.EventOutboxStatusDeadLettered,
	})

	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			// Built from the real row builder with occurred_at corrupted BY POSITION,
			// so the cell count can never drift away from the projection.
			columns := deadLetterInventoryProjectedColumns()
			cells := deadLetterInventoryRow(valid)
			cells[slices.Index(columns, "occurred_at")] = "not-a-timestamp"

			broken := sqlmock.NewRows(columns).AddRow(cells...)
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnRows(broken)
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(newDeadLetterInventoryRows(valid).RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			script(mock)

			entries, err := ds.ListDeadLetteredEvents(context.Background(), model.DeadLetterQuery{Limit: 10})
			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetteredEventsFiltered_PushesEveryPredicateIntoSQL is the assertion that the
// narrowing reaches the DATABASE.
//
// It is the whole point of the method. The service used to filter above this layer: it read
// unfiltered pages, applied the predicates in Go and stopped at a fixed row budget, so a
// filtered page silently omitted every match past the budget and answered 200 with no
// indication that it had stopped looking. A predicate that is not in this WHERE clause is
// a predicate that has to be applied somewhere it cannot be applied correctly.
//
// Three properties are asserted rather than one, because each is separately breakable:
//
//   - Every set filter appears as a BOUND PARAMETER, never as interpolated text. The
//     values are operator-supplied, so an interpolated topic is an injection.
//   - A status filter REPLACES the two-state default rather than intersecting with it,
//     which is what makes narrowing to `failed` alone expressible at all.
//   - The page placeholders are numbered AFTER the predicates, so adding a filter cannot
//     silently rebind the limit to a topic string.
func TestListDeadLetteredEventsFiltered_PushesEveryPredicateIntoSQL(t *testing.T) {
	cases := []struct {
		name      string
		filter    model.DeadLetterFilter
		wantWhere string
		wantPage  string
		wantArgs  []driver.Value
	}{
		{
			name:      "no filter keeps the two-state default and the unfiltered binding",
			filter:    model.DeadLetterFilter{},
			wantWhere: "WHERE status IN ($1, $2)",
			wantPage:  "LIMIT $3 OFFSET $4",
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, 25, 5,
			},
		},
		{
			name:      "an event type is bound, alongside the default states",
			filter:    model.DeadLetterFilter{EventType: "transaction.applied"},
			wantWhere: "WHERE status IN ($1, $2) AND event_type = $3",
			wantPage:  "LIMIT $4 OFFSET $5",
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"transaction.applied", 25, 5,
			},
		},
		{
			name:      "a topic is bound against the row's own topic column",
			filter:    model.DeadLetterFilter{Topic: "blnk.transactions"},
			wantWhere: "WHERE status IN ($1, $2) AND topic = $3",
			wantPage:  "LIMIT $4 OFFSET $5",
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"blnk.transactions", 25, 5,
			},
		},
		{
			name:      "a status filter replaces the default rather than adding to it",
			filter:    model.DeadLetterFilter{Status: model.EventOutboxStatusFailed},
			wantWhere: "WHERE status = $1",
			wantPage:  "LIMIT $2 OFFSET $3",
			wantArgs:  []driver.Value{model.EventOutboxStatusFailed, 25, 5},
		},
		{
			name: "every filter together, in the documented order",
			filter: model.DeadLetterFilter{
				EventType: "balance.monitor",
				Topic:     "blnk.balances",
				Status:    model.EventOutboxStatusDeadLettered,
			},
			wantWhere: "WHERE status = $1 AND event_type = $2 AND topic = $3",
			wantPage:  "LIMIT $4 OFFSET $5",
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, "balance.monitor", "blnk.balances", 25, 5,
			},
		},
		{
			name: "surrounding whitespace is trimmed before binding",
			filter: model.DeadLetterFilter{
				EventType: "  identity.created  ",
				Topic:     "\tblnk.identities\n",
			},
			wantWhere: "WHERE status IN ($1, $2) AND event_type = $3 AND topic = $4",
			wantPage:  "LIMIT $5 OFFSET $6",
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"identity.created", "blnk.identities", 25, 5,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, captured := newCapturingSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery("").WithArgs(tc.wantArgs...).WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEventsFiltered(context.Background(), tc.filter, 25, 5)
			require.NoError(t, err)

			require.Len(t, *captured, 1, "one query serves the request; a walk would issue several")
			issued := (*captured)[0]
			assert.Contains(t, issued, tc.wantWhere,
				"the predicates must be in SQL, or they are being applied above the database on a page that was already truncated")
			assert.Contains(t, issued, tc.wantPage,
				"the page placeholders follow the predicates, so adding a filter cannot rebind the limit")
			assert.Contains(t, issued, "ORDER BY occurred_at DESC, id DESC",
				"a filtered page must be ordered identically to an unfiltered one, or paging is unstable across the two")
			assert.NotContains(t, issued, tc.filter.Topic+"'",
				"filter values are bound, never interpolated: an interpolated topic is an injection surface")

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetteredEventsFiltered_NormalisesPaginationLikeTheUnfilteredListing keeps the
// two entry points interchangeable.
//
// A caller that adds a filter to a working request must not also get a different page. The
// upper cap is the load-bearing half here as it is there: without it an authenticated
// operator endpoint becomes a full table scan of an outbox that grows without bound.
func TestListDeadLetteredEventsFiltered_NormalisesPaginationLikeTheUnfilteredListing(t *testing.T) {
	cases := []struct {
		name                  string
		limit, offset         int
		wantLimit, wantOffset int
	}{
		{"zero limit takes the default", 0, 0, defaultDeadLetterPageSize, 0},
		{"negative limit takes the default", -10, 0, defaultDeadLetterPageSize, 0},
		{"oversized limit is capped", maxDeadLetterPageSize + 1, 0, maxDeadLetterPageSize, 0},
		{"limit at the cap is preserved", maxDeadLetterPageSize, 0, maxDeadLetterPageSize, 0},
		{"negative offset is clamped to zero", 10, -5, 10, 0},
		{"a valid page is passed through unchanged", 25, 75, 25, 75},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WithArgs(
					model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
					"transaction.applied", tc.wantLimit, tc.wantOffset,
				).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEventsFiltered(context.Background(),
				model.DeadLetterFilter{EventType: "transaction.applied"}, tc.limit, tc.offset)
			require.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetteredEventsFiltered_ErrorsAreWrapped covers the statement, scan and row
// iteration branches, and asserts no driver text escapes into the caller's message.
func TestListDeadLetteredEventsFiltered_ErrorsAreWrapped(t *testing.T) {
	valid := model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		Status: model.EventOutboxStatusDeadLettered, MaxAttempts: 5,
	}

	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			cells := eventOutboxRow(valid)
			cells[indexOfProjectedColumn(t, "occurred_at")] = "not-a-timestamp"

			broken := sqlmock.NewRows(eventOutboxProjectedColumns()).AddRow(cells...)
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnRows(broken)
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(newEventOutboxRows(valid).RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			script(mock)

			entries, err := ds.ListDeadLetteredEventsFiltered(context.Background(),
				model.DeadLetterFilter{Topic: "blnk.transactions"}, 10, 0)
			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCountDeadLetteredEvents_CountsTheSameSetTheListingPages is the invariant that makes a
// reported total trustworthy.
//
// The count and the listing must narrow IDENTICALLY. A total taken with a different
// predicate than the page it is reported beside is worse than no total: a paging client
// either stops early, believing it has seen everything, or loops forever asking for a page
// past the end. Both methods build their WHERE clause from the one shared clause builder,
// and this test asserts the resulting predicate and bindings are the same rather than
// merely similar.
func TestCountDeadLetteredEvents_CountsTheSameSetTheListingPages(t *testing.T) {
	filters := []model.DeadLetterFilter{
		{},
		{EventType: "transaction.applied"},
		{Topic: "blnk.transactions"},
		{Status: model.EventOutboxStatusFailed},
		{
			EventType: "balance.monitor",
			Topic:     "blnk.balances",
			Status:    model.EventOutboxStatusDeadLettered,
		},
	}

	// whereOf extracts the predicate from a captured statement, so the two queries are
	// compared on the clause they share rather than on the projections they do not.
	whereOf := func(t *testing.T, statement string) string {
		t.Helper()

		start := strings.Index(statement, "WHERE ")
		require.GreaterOrEqual(t, start, 0, "every dead-letter query is filtered")

		clause := statement[start:]
		if end := strings.Index(clause, "ORDER BY"); end >= 0 {
			clause = clause[:end]
		}
		if end := strings.Index(clause, "LIMIT"); end >= 0 {
			clause = clause[:end]
		}

		return strings.Join(strings.Fields(clause), " ")
	}

	for _, filter := range filters {
		t.Run(fmt.Sprintf("%s|%s|%s", filter.Status, filter.EventType, filter.Topic), func(t *testing.T) {
			listDB, listMock, listCaptured := newCapturingSQLMock(t)
			listMock.ExpectQuery("").WillReturnRows(newEventOutboxRows())
			_, listErr := Datasource{Conn: listDB}.ListDeadLetteredEventsFiltered(
				context.Background(), filter, 10, 0)
			require.NoError(t, listErr)

			countDB, countMock, countCaptured := newCapturingSQLMock(t)
			countMock.ExpectQuery("").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(7)))
			total, countErr := Datasource{Conn: countDB}.CountDeadLetteredEvents(
				context.Background(), filter)
			require.NoError(t, countErr)
			assert.Equal(t, int64(7), total)

			require.Len(t, *listCaptured, 1)
			require.Len(t, *countCaptured, 1)
			assert.Equal(t,
				whereOf(t, (*listCaptured)[0]),
				whereOf(t, (*countCaptured)[0]),
				"the count must select the same rows the page came from, or the total describes a different set")
			assert.Contains(t, (*countCaptured)[0], "SELECT COUNT(*)",
				"a count is an aggregate, not a page whose length is reported as a total")
			assert.NotContains(t, (*countCaptured)[0], "LIMIT",
				"a count of the set cannot be bounded by a page, or it reports the page size")
		})
	}
}

// TestCountDeadLetteredEvents_ErrorsAreWrapped keeps a counting failure typed and
// driver-silent: the count feeds an operator-facing total, and a raw driver string in that
// response would disclose the schema.
func TestCountDeadLetteredEvents_ErrorsAreWrapped(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)

	total, err := ds.CountDeadLetteredEvents(context.Background(), model.DeadLetterFilter{})

	assert.Zero(t, total, "a failed count must not be reported as an empty inventory")
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.NotContains(t, err.Error(), sql.ErrConnDone.Error(),
		"driver text must not reach an operator-facing response")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCountEventOutboxByStatus_ReturnsPerStatusCounts pins the method that acceptance
// criterion V-2 is measured with.
//
// IT IS NOT UNUSED — a grep for callers inside this package finds none, which is exactly
// the trap this test and its comment exist to prevent. Three things consume it:
//
//   - the event statistics endpoint, GET /events/stats;
//   - the DAILY ZERO-LOSS RECONCILIATION in the Kafka operations runbook, which passes
//     when the dispatched plus dead-lettered counts equal the sum of the main-topic and
//     dead-letter-topic end offsets reported by the broker;
//   - the blnk.outbox.pending backlog gauge exported to the metrics pipeline.
//
// Delete it as dead code and criterion V-2 becomes unmeasurable.
func TestCountEventOutboxByStatus_ReturnsPerStatusCounts(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(
		sqlmock.NewRows([]string{"status", "count"}).
			AddRow(model.EventOutboxStatusPending, int64(3)).
			AddRow(model.EventOutboxStatusProcessing, int64(1)).
			AddRow(model.EventOutboxStatusDispatched, int64(1204)).
			AddRow(model.EventOutboxStatusFailed, int64(2)).
			AddRow(model.EventOutboxStatusDeadLettered, int64(7)),
	)

	window := time.Now().UTC().Add(-6 * time.Hour)

	counts, err := ds.CountEventOutboxByStatus(context.Background(), window)
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{
		model.EventOutboxStatusPending:      3,
		model.EventOutboxStatusProcessing:   1,
		model.EventOutboxStatusDispatched:   1204,
		model.EventOutboxStatusFailed:       2,
		model.EventOutboxStatusDeadLettered: 7,
	}, counts)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "GROUP BY status",
		"the reconciliation needs one row per status, aggregated in the database rather than by counting rows in Go")

	// PERF-P04: the count is TWO-ARMED, and which arm is windowed is the substance of the
	// fix. Asserted on the statement because a small fixture cannot tell a bounded count from
	// an unbounded one — both return the same rows.
	assert.Contains(t, issued, "UNION ALL",
		"one arm counts everything outstanding exactly, the other bounds the one unbounded population")
	assert.Contains(t, issued, "WHERE status <> $1",
		"every non-dispatched status must be counted EXACTLY and in full, however short the window: "+
			"a row pending for three days must still appear in the backlog it is the point of")
	assert.Contains(t, issued, "WHERE status = $1 AND occurred_at >= $2",
		"dispatched is the only population that grows without bound, so it is the only one windowed")

	// The reconciliation arithmetic the runbook actually performs, asserted here so the
	// two terminal states cannot drift out of the sum.
	assert.Equal(t, int64(1211),
		counts[model.EventOutboxStatusDispatched]+counts[model.EventOutboxStatusDeadLettered],
		"dispatched + dead_lettered is what the daily reconciliation compares against the broker's end offsets")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCountUnresolvedEventOutbox_CountsTheInventoryAndNoHistory is the PERF-M05 guard on the
// repository side.
//
// # What the two-armed count cost the callers that only ever read one arm
//
// Two callers run this aggregate on a timer: the metrics collector recomputes the backlog and
// repair gauges every fifteen seconds, and the dead-letter service counts the rows awaiting a
// dead-letter write. Both read ONLY non-dispatched statuses, and both said so in a comment while
// calling the two-armed query with a twenty-four-hour window — so every call additionally
// performed an exact COUNT of a day of dispatched rows, 43.2 million index entries at the target
// rate, and discarded the answer. Fifteen seconds apart, that is a scan that never stops.
//
// # The assertions are on the STATEMENT, and they have to be
//
// A fixture holding five rows cannot tell a one-armed query from a two-armed one: both return the
// same rows. The substance of the fix is the ABSENCE of the second arm and of the window, which is
// visible only in the SQL — and it is exactly what a well-meaning future edit would undo by
// "reusing" the fuller query with a wide window.
//
// The predicate must also be spelled `status <> $1`, character for character as the other query's
// first arm spells it, so both resolve to the same partial index. A paraphrase — an IN list of the
// known non-dispatched statuses — would read identically, plan as a sequential scan, and silently
// stop counting any status added to the state machine later, which the column deliberately allows.
func TestCountUnresolvedEventOutbox_CountsTheInventoryAndNoHistory(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(
		sqlmock.NewRows([]string{"status", "count"}).
			AddRow(model.EventOutboxStatusPending, int64(3)).
			AddRow(model.EventOutboxStatusProcessing, int64(1)).
			AddRow(model.EventOutboxStatusWebhookPending, int64(5)).
			AddRow(model.EventOutboxStatusFailed, int64(2)).
			AddRow(model.EventOutboxStatusDeadLettered, int64(7)),
	)

	counts, err := ds.CountUnresolvedEventOutbox(context.Background())
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{
		model.EventOutboxStatusPending:        3,
		model.EventOutboxStatusProcessing:     1,
		model.EventOutboxStatusWebhookPending: 5,
		model.EventOutboxStatusFailed:         2,
		model.EventOutboxStatusDeadLettered:   7,
	}, counts)

	// The four figures the routine callers actually publish, all of them from this ONE reading:
	// the backlog gauge is pending plus processing, and the two repair backlogs are `failed`
	// and `webhook_pending` (PERF-M06).
	assert.Equal(t, int64(4), counts[model.EventOutboxStatusPending]+
		counts[model.EventOutboxStatusProcessing],
		"the backlog gauge comes out of this reading")
	assert.Equal(t, int64(2), counts[model.EventOutboxStatusFailed],
		"and so does the dead-letter repair backlog")
	assert.Equal(t, int64(5), counts[model.EventOutboxStatusWebhookPending],
		"and the legacy-webhook repair backlog")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "GROUP BY status",
		"one row per status, aggregated in the database rather than by counting rows in Go")
	assert.Contains(t, issued, "WHERE status <> $1",
		"the predicate must be spelled exactly as the two-armed query's first arm spells it, so "+
			"both resolve to the same partial index; a paraphrased IN list would plan as a "+
			"sequential scan and would stop counting any status added to the state machine later")

	assert.NotContains(t, issued, "UNION ALL",
		"THE SECOND ARM MUST BE ABSENT. This is the whole substance of PERF-M05: a routine caller "+
			"must not pay for an exact count of the one unbounded population — 43.2 million index "+
			"entries a day at the target rate — to read a backlog it computes from four other "+
			"statuses")
	assert.NotContains(t, issued, "occurred_at",
		"and there must be no window at all: a window could only HIDE rows this reading exists to "+
			"surface, and a row pending for three days must still appear in the backlog")
	assert.NotContains(t, issued, "$2",
		"one parameter, the dispatched status to exclude, and nothing else")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCountUnresolvedEventOutbox_ErrorsAreWrapped covers the statement, scan and row-iteration
// failures on the lightweight aggregate.
//
// It shares its scan loop with CountEventOutboxByStatus, and the shared loop is what makes these
// cases worth stating twice: the one property both must have is that a mid-iteration failure is
// REPORTED rather than yielding the partially-drained map, because a zero-loss reconciliation
// compared against a short total reports loss that has not happened.
func TestCountUnresolvedEventOutbox_ErrorsAreWrapped(t *testing.T) {
	for name, arrange := range map[string]func(sqlmock.Sqlmock){
		"the statement fails": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("").WillReturnError(errors.New("pq: permission denied"))
		},
		"a row cannot be scanned": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("").WillReturnRows(
				sqlmock.NewRows([]string{"status", "count"}).
					AddRow(model.EventOutboxStatusPending, "not-a-number"),
			)
		},
		"iteration fails partway": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery("").WillReturnRows(
				sqlmock.NewRows([]string{"status", "count"}).
					AddRow(model.EventOutboxStatusPending, int64(3)).
					RowError(0, errors.New("connection reset by peer")),
			)
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, _ := newCapturingSQLMock(t)
			ds := Datasource{Conn: db}
			arrange(mock)

			counts, err := ds.CountUnresolvedEventOutbox(context.Background())

			require.Error(t, err)
			assert.Nil(t, counts,
				"a failed reading must return NO map: a partially-drained one would be read as a "+
					"complete census and would understate every backlog it feeds")
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCountEventOutboxByStatus_NormalisesTheWindow pins what a caller cannot ask for.
//
// A zero instant means "no preference" and a future one is a caller error; both become one
// default window before now. Correcting rather than failing is deliberate — a zero instant
// bound verbatim would ask PostgreSQL for every dispatched row since year one, which is the
// whole-history scan this window exists to prevent, and a future instant would report zero
// dispatched rows and read as total message loss.
func TestCountEventOutboxByStatus_NormalisesTheWindow(t *testing.T) {
	cases := []struct {
		name  string
		given time.Time
	}{
		{"a zero instant takes the default window", time.Time{}},
		{"a future instant takes the default window", time.Now().UTC().Add(time.Hour)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			before := time.Now().UTC()

			var bound time.Time
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WithArgs(model.EventOutboxStatusDispatched, sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"status", "row_count"}))

			_, err := ds.CountEventOutboxByStatus(context.Background(), tc.given)
			require.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())

			// The instant itself is checked through the normaliser, which is the one place
			// the rule lives; sqlmock's AnyArg cannot assert a range.
			bound = normalizeEventCountWindow(tc.given)
			assert.WithinDuration(t, before.Add(-defaultEventCountWindow), bound, time.Minute,
				"a window nobody chose must be exactly one default window wide")
			assert.True(t, bound.Before(time.Now().UTC()),
				"and must be in the past, or the dispatched count reads as total loss")
		})
	}

	t.Run("an explicit past instant is honoured", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		chosen := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Microsecond)

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WithArgs(model.EventOutboxStatusDispatched, chosen).
			WillReturnRows(sqlmock.NewRows([]string{"status", "row_count"}))

		_, err := ds.CountEventOutboxByStatus(context.Background(), chosen)
		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestCountEventOutboxByStatus_AbsentStatusMeansAbsentKey pins the semantics a caller can
// get catastrophically wrong.
//
// GROUP BY produces rows only for statuses that EXIST, so a status with no rows is ABSENT
// from the map rather than present with a zero. Callers must read it with the two-value
// form or knowingly accept Go's zero value — which is what the pending-backlog gauge does
// when nothing is pending, and which is correct there.
//
// It is asserted explicitly because misreading a MISSING key as a genuine zero is
// precisely how the daily reconciliation would report success while events were missing:
// if the dispatched key were absent because of a query fault, dispatched would read as
// zero, and zero plus zero equalling a zero offset sum looks like a clean reconciliation.
//
// The map is also asserted NON-NIL on an empty table, so a caller can range over the
// result without a nil check.
func TestCountEventOutboxByStatus_AbsentStatusMeansAbsentKey(t *testing.T) {
	t.Run("an empty table yields an empty, non-nil map", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"status", "count"}))

		counts, err := ds.CountEventOutboxByStatus(context.Background(), time.Time{})
		require.NoError(t, err)
		require.NotNil(t, counts, "the map must never be nil on success so callers can range over it without a nil check")
		assert.Empty(t, counts)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a status with no rows is absent, not zero", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).
				AddRow(model.EventOutboxStatusDispatched, int64(10)))

		counts, err := ds.CountEventOutboxByStatus(context.Background(), time.Time{})
		require.NoError(t, err)

		_, present := counts[model.EventOutboxStatusPending]
		assert.False(t, present,
			"GROUP BY emits no row for a status with no rows, so the key is absent; reading a missing key as a genuine zero is how a reconciliation reports success while events are missing")
		assert.Zero(t, counts[model.EventOutboxStatusPending],
			"Go's zero value is nevertheless the correct reading for the pending-backlog gauge")
		assert.Equal(t, int64(10), counts[model.EventOutboxStatusDispatched])
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("an unknown status is carried through verbatim", func(t *testing.T) {
		// The column is deliberately unconstrained TEXT so the state machine can be
		// extended without a migration. An unknown literal must therefore surface rather
		// than being dropped, or a future state would silently vanish from the
		// reconciliation.
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).AddRow("quarantined", int64(4)))

		counts, err := ds.CountEventOutboxByStatus(context.Background(), time.Time{})
		require.NoError(t, err)
		assert.Equal(t, int64(4), counts["quarantined"],
			"an unrecognised status must surface rather than be dropped: the column permits extension without a migration")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestCountEventOutboxByStatus_ErrorsAreWrapped covers the statement, scan and row
// iteration branches of the count.
func TestCountEventOutboxByStatus_ErrorsAreWrapped(t *testing.T) {
	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).
					AddRow(model.EventOutboxStatusPending, "not-a-number"))
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).
					AddRow(model.EventOutboxStatusPending, int64(1)).
					RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			script(mock)

			counts, err := ds.CountEventOutboxByStatus(context.Background(), time.Time{})
			assert.Nil(t, counts)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestEventOutboxReads_DriverCloseFailureSurfacesThroughRowsErr documents a subtle and
// easily-misread interaction between the deferred close and the rows.Err() check.
//
// Each read in event_outbox.go closes its result set in a defer that LOGS rather than
// returns, and the reasoning is sound: for ClaimPendingEventOutbox the batch has already
// been claimed by the time the rows are closed, so the rows are LEASED IN THE DATABASE,
// and failing there would make the relay discard a batch it already owns and leave those
// events un-published until the lease expired.
//
// It is tempting to read that defer as meaning "a close failure can never fail a read".
// IT DOES NOT, and this test is what stops the next reader believing it. database/sql
// folds a DRIVER-reported close failure into (*sql.Rows).Err(), and rows.Err() IS checked
// and returned. So a driver that fails on close does produce a typed internal error — by
// way of the Err() branch rather than the defer.
//
// The distinction is worth pinning down, because the two behaviours are correct for
// different reasons and a refactor could easily collapse them:
//
//   - The rows.Err() check must stay. It is the only thing that catches a connection
//     dropping MIDWAY through streaming a result set, which would otherwise silently
//     yield a short batch — a lost event on the claim path.
//   - The deferred close must stay non-failing, because by the time it runs the caller
//     already has a complete, usable result and the only remaining question is
//     bookkeeping.
//
// One consequence worth stating plainly: the defer's logging branch is NOT reachable
// through sqlmock, because database/sql has already performed and recorded the underlying
// close by the time the defer runs. Those few statements are defensive and are left
// deliberately uncovered rather than covered by a test asserting something untrue.
func TestEventOutboxReads_DriverCloseFailureSurfacesThroughRowsErr(t *testing.T) {
	closeErr := errors.New("connection reset while closing rows")

	entry := model.EventOutbox{
		ID: 1, EventID: "evt_close", EventType: "transaction.applied", AggregateID: "agg",
		LedgerID: "ldg", Topic: "blnk.transactions", SchemaVersion: model.SchemaVersionV1,
		Payload:    json.RawMessage(`{"event":"transaction.applied","data":{}}`),
		OccurredAt: time.Now().UTC(), Status: model.EventOutboxStatusDeadLettered,
		Attempts: 5, MaxAttempts: 5,
	}

	t.Run("ClaimPendingEventOutbox", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		claimed := entry
		claimed.Status = model.EventOutboxStatusProcessing
		mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
			WillReturnRows(newEventOutboxRows(claimed).CloseError(closeErr))

		entries, err := ds.ClaimPendingEventOutbox(context.Background(), 10, 30*time.Second)

		assert.Nil(t, entries,
			"a batch the driver could not vouch for must not be handed back as though it were complete")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ListDeadLetterInventory", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(newDeadLetterInventoryRows(deadLetterInventoryEntryOf(entry)).CloseError(closeErr))

		entries, err := ds.ListDeadLetteredEvents(context.Background(), model.DeadLetterQuery{Limit: 10})
		assert.Nil(t, entries)
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CountEventOutboxByStatus", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"status", "count"}).
				AddRow(model.EventOutboxStatusDispatched, int64(9)).
				CloseError(closeErr))

		counts, err := ds.CountEventOutboxByStatus(context.Background(), time.Time{})
		assert.Nil(t, counts,
			"a partial count must never reach the zero-loss reconciliation: it would under-report while looking like a clean result")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("insertEventOutboxesInTx fails the batch so the ledger transaction rolls back", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ctx := context.Background()

		pending := newEventOutboxFixture("close-")
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO blnk.event_outbox")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "event_id"}).
				AddRow(int64(55), pending.EventID).
				CloseError(closeErr))
		mock.ExpectRollback()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		batchErr := insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{pending})
		require.NoError(t, tx.Rollback())

		// On the WRITE path this conservative outcome is the right one: if the driver
		// cannot vouch for the RETURNING stream, the safe assumption is that not every
		// event was recorded, and rolling the ledger mutation back beats committing one
		// whose event may be missing.
		requireAPIError(t, batchErr, apierror.ErrInternalServer)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// ===========================================================================
// TIER 1 — the subscriber registry's secret-handling posture
//
// This lives here rather than in a file of its own because the property is
// structural and belongs beside the other repository-layer invariants.
// ===========================================================================

// credentialBearingIdentifier reports whether a name looks like it carries a
// recoverable credential.
//
// "reference", "hash" and "digest" are deliberately NOT on this list: a non-reversible
// reference to a credential is exactly what the registry is supposed to store, so
// matching those would flag the correct design as a violation.
func credentialBearingIdentifier(name string) bool {
	lowered := strings.ToLower(name)
	for _, forbidden := range []string{"password", "passwd", "secret", "plaintext", "cleartext"} {
		if strings.Contains(lowered, forbidden) {
			return true
		}
	}
	return false
}

// TestEventSubscriberRepository_NoMethodReturnsPlaintextSecret asserts the
// secret-handling posture MECHANICALLY rather than by eyeballing the source.
//
// The posture: a SASL password is issued exactly once by the service layer that owns
// issuance, and this repository persists only a NON-REVERSIBLE REFERENCE plus the
// issuance instant. It mirrors the existing API-key posture, in which keys are hashed
// and the raw key is never stored. The registry must therefore have no way — through
// any parameter, any return value, or any field of any returned struct — to carry a
// recoverable credential.
//
// Three independent structural checks, because the property can be broken in three
// different places:
//
//  1. REFLECTION over the eventSubscriber interface's method set, walking every
//     parameter and return type transitively into struct fields. This is the check that
//     survives refactoring: it does not care what the source looks like, only what the
//     types can carry.
//  2. AST PARSING of database/event_subscriber.go's import block, to confirm no
//     password-hashing library is imported here. Hashing and derivation belong to the
//     caller that owns issuance; importing one in the repository would signal that this
//     layer handles raw credentials, which is the design mistake being prevented. The
//     imports are parsed rather than grepped ON PURPOSE — the file's own documentation
//     MENTIONS golang.org/x/crypto/bcrypt in prose, so a text search returns a
//     false positive and would make this test pass or fail for the wrong reason.
//  3. Exported method signatures on Datasource whose names begin with the subscriber
//     prefixes, checked for credential-bearing PARAMETER names in the source, since a
//     parameter named `password` would be a design regression even if its type were a
//     harmless string.
//
// A source-level assertion is legitimate here: the property being protected is
// structural, and structural properties are best defended structurally.
func TestEventSubscriberRepository_NoMethodReturnsPlaintextSecret(t *testing.T) {
	subscriberRepo := reflect.TypeOf((*eventSubscriber)(nil)).Elem()
	require.Positive(t, subscriberRepo.NumMethod(), "the eventSubscriber interface must declare methods")

	// (1) Reflection over every parameter and return type in the method set.
	t.Run("no method signature can carry a recoverable credential", func(t *testing.T) {
		visited := map[reflect.Type]bool{}

		var walk func(t *testing.T, typ reflect.Type, path string)
		walk = func(t *testing.T, typ reflect.Type, path string) {
			for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice ||
				typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
				typ = typ.Elem()
			}
			if typ.Kind() != reflect.Struct || visited[typ] {
				return
			}
			visited[typ] = true

			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				fieldPath := path + "." + field.Name
				assert.False(t, credentialBearingIdentifier(field.Name),
					"%s looks like it carries a recoverable credential; the registry may store only a non-reversible reference", fieldPath)
				assert.False(t, credentialBearingIdentifier(field.Tag.Get("json")),
					"%s has a credential-bearing json tag, so a credential would be serialised over the wire", fieldPath)
				walk(t, field.Type, fieldPath)
			}
		}

		for i := 0; i < subscriberRepo.NumMethod(); i++ {
			method := subscriberRepo.Method(i)
			assert.False(t, credentialBearingIdentifier(method.Name),
				"%s is named as though it handles a raw credential", method.Name)

			signature := method.Type
			for in := 0; in < signature.NumIn(); in++ {
				walk(t, signature.In(in), fmt.Sprintf("%s param %d", method.Name, in))
			}
			for out := 0; out < signature.NumOut(); out++ {
				walk(t, signature.Out(out), fmt.Sprintf("%s result %d", method.Name, out))
			}
		}

		// The reference and its issuance timestamp together are the complete stored
		// record of an issuance, so their absence would mean the posture had been
		// abandoned rather than tightened.
		subscriber := reflect.TypeOf(model.EventSubscriber{})
		_, hasReference := subscriber.FieldByName("CredentialReference")
		assert.True(t, hasReference, "the registry must keep a non-reversible credential reference")
		_, hasIssuedAt := subscriber.FieldByName("CredentialIssuedAt")
		assert.True(t, hasIssuedAt, "the registry must record when a credential was issued")
	})

	// (2) The import block, parsed rather than grepped.
	t.Run("the repository imports no password-hashing library", func(t *testing.T) {
		const source = "event_subscriber.go"

		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, source, nil, parser.ImportsOnly)
		require.NoError(t, err, "failed to parse %s", source)

		var imported []string
		for _, spec := range parsed.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			imported = append(imported, path)
		}
		require.NotEmpty(t, imported)

		for _, path := range imported {
			for _, hashing := range []string{"bcrypt", "scrypt", "argon2", "pbkdf2", "sasl/scram"} {
				assert.NotContains(t, path, hashing,
					"%s imports %s; hashing and derivation belong to the caller that owns issuance, and importing one here signals that this layer handles raw credentials", source, path)
			}
		}
	})

	// (3) Parameter names on the exported subscriber methods.
	t.Run("no exported subscriber method takes a credential-named parameter", func(t *testing.T) {
		const source = "event_subscriber.go"

		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, source, nil, 0)
		require.NoError(t, err, "failed to parse %s", source)

		inspected := 0
		for _, decl := range parsed.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok || funcDecl.Recv == nil || !funcDecl.Name.IsExported() {
				continue
			}
			inspected++

			assert.False(t, credentialBearingIdentifier(funcDecl.Name.Name),
				"%s is named as though it handles a raw credential", funcDecl.Name.Name)

			for _, param := range funcDecl.Type.Params.List {
				for _, name := range param.Names {
					assert.False(t, credentialBearingIdentifier(name.String()),
						"%s takes a parameter named %q; the repository must receive an already-derived, non-reversible reference",
						funcDecl.Name.Name, name.String())
				}
			}
		}
		assert.Positive(t, inspected, "no exported methods were inspected, so this assertion proved nothing")
	})
}

// ===========================================================================
// TIER 2 — the real-database tier
//
// Every test below carries a _RealDB suffix and opens its connection through
// openRealTestDB, which skips the test when no database is reachable so that the
// default suite stays green without infrastructure.
//
// These tests prove what a mock physically cannot: FOR UPDATE SKIP LOCKED under real
// contention, FIFO ordering decided by the planner rather than by a script, lease
// expiry against the database clock, and the query plan itself.
// ===========================================================================

// quiesceEventOutbox RETIRES every UNRELATED row that is still in a claim-visible state,
// so that this test's claim assertions see only its own fixtures, and registers a cleanup
// that retires this test's fixtures the same way.
//
// It is not optional hygiene — without it these tests are FLAKY AND POLLUTING. The test
// database is shared and rows persist between runs, so an unquiesced claim picks up
// whatever another test or an earlier run left behind and the assertions become
// non-deterministic. The cleanup half matters just as much: fixtures left claimable would
// be claimed by a later run's assertions, or chewed by a relay started elsewhere.
//
// # WHY UNRELATED ROWS ARE RETIRED RATHER THAN PARKED, AND UNCONDITIONALLY
//
// This used to park them instead — set locked_until an hour into the future, and only for
// rows whose lease was already NULL or lapsed. That was measurably not enough: running the
// database package repeatedly failed roughly one run in three or four, and the failing test
// rotated between every assertion in this file that reads a GLOBAL claim or a GLOBAL status
// count — "row was not claimed", "every fixture must be claimed exactly once ... but has 2",
// "the backlog must drain rather than stall", and status deltas larger than the fixtures
// inserted. A diagnostic run confirmed the shape directly: immediately after the parking
// UPDATE returned without error, a foreign row from an earlier test was still sitting in the
// claimable set.
//
// Parking is the weaker instrument for two structural reasons, and retiring is immune to
// both:
//
//   - A LEASE IS A TIME WINDOW AND A STATUS IS NOT. Parking asserts nothing about the row
//     after the hour, and it silently declines to touch a row whose lease has not yet
//     lapsed — the exact rows a crashed or interrupted earlier test leaves behind. Moving
//     the row to a terminal status removes it from the claim by the one predicate the claim
//     cannot ignore.
//   - THE CLAIM READS MORE THAN THE LEASE. It orders the whole claimable set by occurred_at
//     and takes the first batch, and it excludes a row when an EARLIER row shares its
//     EFFECTIVE key — its ledger where it has one, its stored partition key where it does
//     not — and is still pending or processing. A foreign row that survives parking
//     can therefore starve a fixture out of the batch or block its key, and both read as a
//     product defect in the claim rather than as pollution in the table.
//
// Retiring an unrelated row is safe because it is, by construction, a leftover: the test
// that created it has finished, so nothing is still asserting on it. It is exactly what that
// test's own cleanup was supposed to do.
//
// It mirrors quiesceOutbox, which does the same job for blnk.lineage_outbox, with two
// differences that follow from this table's state machine: the claim-visible set here spans
// pending, processing AND replaying rather than pending alone, and the terminal state used
// to retire rows is dispatched rather than completed.
// foreignEventOutboxIndexes returns the indexes present on blnk.event_outbox that NO migration
// in this build creates, sorted by name.
//
// # Why a test needs to know this
//
// The plan assertion in TestClaimPendingEventOutbox_UsesClaimIndex_RealDB is a statement about
// which of THIS BUILD'S indexes the planner chooses. On a shared development database the
// available set can be larger: a database that has had another build's migrations applied keeps
// that build's indexes, the planner costs them alongside this build's, and a cheaper foreign
// index wins. The assertion then fails on a plan this build's schema cannot produce — and
// widening it to accept the foreign index would make it vacuous, because a foreign index need
// not have the partial predicate the guarantee depends on.
//
// # Why the expected set is read from the migration files
//
// Restating the index names in the test would be a second declaration of the schema, and it
// would go stale silently in the direction that matters: a migration that ADDED an index would
// make this function report the new index as foreign and skip the assertion for ever. Reading
// sql/*.sql means the expected set is whatever this build actually creates, by construction.
//
// A name is matched anywhere in a migration, deliberately: an index may be created in one file
// and dropped and recreated in a later one, and every mention is evidence that this build owns
// the name.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - ctx context.Context: bounds the catalogue query.
//   - ds Datasource: the connection to read pg_indexes through.
//
// Returns:
//   - []string: index names present in the database and absent from this build's migrations.
func foreignEventOutboxIndexes(t *testing.T, ctx context.Context, ds Datasource) []string {
	t.Helper()

	// The migration directory sits beside this package, one level up. Located relatively rather
	// than through an environment variable so the check works in any checkout.
	entries, err := os.ReadDir(filepath.Join("..", "sql"))
	require.NoError(t, err, "this build's migration directory must be readable to know which indexes it declares")

	var declared strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		contents, readErr := os.ReadFile(filepath.Join("..", "sql", entry.Name()))
		require.NoErrorf(t, readErr, "migration %s must be readable", entry.Name())

		declared.Write(contents)
		declared.WriteString("\n")
	}

	migrations := declared.String()
	require.NotEmpty(t, migrations, "this build must declare at least one migration")

	rows, err := ds.Conn.QueryContext(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = 'blnk' AND tablename = 'event_outbox'
		ORDER BY indexname
	`)
	require.NoError(t, err, "reading the index catalogue must succeed")
	defer func() { _ = rows.Close() }()

	foreign := make([]string, 0)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))

		// The primary key is named by the table rather than declared as an index, so it is
		// never spelled in a migration and is not foreign.
		if name == "event_outbox_pkey" {
			continue
		}

		if !strings.Contains(migrations, name) {
			foreign = append(foreign, name)
		}
	}
	require.NoError(t, rows.Err())

	return foreign
}

func quiesceEventOutbox(t *testing.T, ds Datasource, markerPrefix string) {
	t.Helper()

	// EXCLUSIVE USE OF THE TABLE FIRST, before anything is retired. Retiring is a whole-table
	// write and the straggler assertion below is a whole-table read, and the root package's live
	// tiers claim from the same table in another PROCESS — so without this the two corrupt each
	// other in both directions. lockEventOutboxTier documents it in full.
	lockEventOutboxTier(t, ds.Conn)

	// Keyed on AGGREGATE_ID and not on event_id. event_id must now be a canonical
	// UUID — it is the subscriber's idempotency key, so the persistence layer refuses
	// any other spelling — which leaves no room for a test marker in it. aggregate_id
	// is unconstrained and carries the marker instead.
	_, err := ds.Conn.Exec(`
		UPDATE blnk.event_outbox
		SET status = 'dispatched', dispatched_at = NOW(), locked_until = NULL, claim_token = NULL
		WHERE status IN ('pending', 'processing', 'replaying')
		  AND aggregate_id NOT LIKE $1
	`, markerPrefix+"%")
	require.NoError(t, err, "failed to quiesce unrelated event outbox rows")

	// Proof rather than hope. A quiesce that silently matched nothing — a mistyped
	// status, a marker predicate that inverted — leaves every claim assertion below
	// reading a shared table, which is the failure this helper exists to prevent and the
	// one that reads as a defect in the claim instead of as pollution here.
	var stragglers int
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT COUNT(*) FROM blnk.event_outbox
		WHERE status IN ('pending', 'processing', 'replaying')
		  AND aggregate_id NOT LIKE $1
	`, markerPrefix+"%").Scan(&stragglers))
	require.Zero(t, stragglers,
		"the event outbox must hold no claim-visible row outside this test's marker once quiesced; "+
			"a straggler starves this test's fixtures out of the claim batch or blocks their partition key")

	// DELETED, not retired. Retiring took the fixtures out of the claim, which is all the
	// assertions in this file need, and left every row in the table for ever: one narrow subset
	// of this tier left 301 rows behind, and four full runs left 5,698. That residue is not
	// inert. It makes every subsequent claim read past it, it is what made a shared database a
	// cross-test hazard, and a retired row records the broker coordinate it was dispatched to
	// under a unique index on (kafka_topic, kafka_partition, kafka_offset) — so after a broker
	// reset, when topic offsets restart at zero, a stale row's coordinate collides with a live
	// publish and the relay cannot mark the new row dispatched at all.
	//
	// Both the aggregate id and the partition key are matched: newEventOutboxFixture derives
	// both from the marker, but a test that is ABOUT same-key behaviour sets the aggregate id
	// itself, and a cleanup that silently stopped covering those rows is the state this replaces.
	t.Cleanup(func() {
		result, cleanupErr := ds.Conn.Exec(`
			DELETE FROM blnk.event_outbox
			WHERE aggregate_id LIKE $1 OR partition_key LIKE $1
		`, markerPrefix+"%")
		if cleanupErr != nil {
			// FAILS the test rather than logging, for the reasons the comment above already
			// gives: a leaked row makes every subsequent claim read past it and its retired
			// broker coordinate collides with a live publish after an offset reset, so the
			// relay cannot mark the NEW row dispatched at all. That is a failure this test
			// caused, and a log line lets it be reported as a pass.
			t.Errorf("failed to remove event outbox fixtures for marker %q: %v",
				markerPrefix, cleanupErr)
		} else if affected, affectedErr := result.RowsAffected(); affectedErr == nil {
			t.Logf("removed %d event outbox fixture rows for marker %q", affected, markerPrefix)
		}

		// Confirmed, not assumed — and checked even when the delete errored, because the
		// question that matters to the next test is whether the table is clean, not whether
		// the statement returned without error.
		var remaining int
		if err := ds.Conn.QueryRow(`
			SELECT count(*) FROM blnk.event_outbox
			WHERE aggregate_id LIKE $1 OR partition_key LIKE $1
		`, markerPrefix+"%").Scan(&remaining); err != nil {
			t.Errorf("could not confirm the event outbox fixtures for marker %q were removed: %v",
				markerPrefix, err)

			return
		}

		assert.Zerof(t, remaining,
			"marker %q left %d fixture rows in blnk.event_outbox. Every later claim in this "+
				"shared table reads past them, and a retired row's (topic, partition, offset) "+
				"coordinate collides with a live publish once broker offsets restart.",
			markerPrefix, remaining)
	})
}

// newRealEventOutboxMarker builds a marker prefix unique to one test run, so concurrent
// runs against the same shared database cannot see one another's fixtures.
func newRealEventOutboxMarker(kind string) string {
	return "evtcov-" + kind + "-" + gofakeit.UUID()[:8] + "-"
}

// insertRealEventOutbox inserts a fixture through the production insert path and returns
// it, so the tests exercise the real code rather than hand-written SQL.
func insertRealEventOutbox(t *testing.T, ds Datasource, entry *model.EventOutbox) *model.EventOutbox {
	t.Helper()
	require.NoError(t, ds.InsertEventOutbox(context.Background(), entry))
	require.NotZero(t, entry.ID, "the insert must return a generated id")
	return entry
}

// isMarkedEventOutbox reports whether a claimed row belongs to this test run.
//
// Matching is on AGGREGATE_ID, not event_id: event_id is now required to be a
// canonical UUID, so there is nowhere in it for a marker to live.
func isMarkedEventOutbox(entry model.EventOutbox, markerPrefix string) bool {
	return strings.HasPrefix(entry.AggregateID, markerPrefix)
}

// claimEventOutboxToken claims a specific pending row and returns the claim token the
// claim issued, which every subsequent transition must present.
//
// It exists because a transition is no longer addressable by row id alone. Threading
// the real token through these tests is not ceremony: it is what makes them exercise
// the same conditional path production takes, so a transition that stopped checking the
// token would fail here rather than pass.
//
// It requires that the row actually be claimed, because a silently unclaimed row would
// turn every following assertion into a test of the failure path.
func claimEventOutboxToken(t *testing.T, ds Datasource, entry *model.EventOutbox) string {
	t.Helper()

	claimed, err := ds.ClaimPendingEventOutbox(context.Background(), 100, 5*time.Minute)
	require.NoError(t, err)

	for _, row := range claimed {
		if row.EventID == entry.EventID {
			require.NotEmpty(t, row.ClaimToken, "a claimed row must carry the token the claim issued")
			return row.ClaimToken
		}
	}

	require.FailNowf(t, "row was not claimed",
		"event %q was expected in the claimed batch; without its token no transition can be authorised", entry.EventID)
	return ""
}

// TestInsertEventOutbox_RoundTripsThroughTheDatabase_RealDB is the baseline: a row
// inserted through the production path reads back with every field intact.
//
// The payload is compared by BYTE EQUALITY, and that is the whole point of the two body
// columns. The read projects payload_raw, which is BYTEA and therefore returns the
// producer's exact bytes; the JSONB payload column, which PostgreSQL normalises on write,
// exists only for SQL-side containment and extraction queries and is never read as the
// body. Asserting equivalence here instead of equality would pass just as well against a
// projection that returned the normalised bytes — which is precisely the defect the second
// column removes, and precisely what acceptance criteria V-8 and V-9 compare.
func TestInsertEventOutbox_RoundTripsThroughTheDatabase_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("roundtrip")
	quiesceEventOutbox(t, ds, marker)

	entry := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, entry)

	got, err := ds.GetEventByID(ctx, entry.EventID)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, entry.ID, got.ID)
	assert.Equal(t, entry.EventType, got.EventType)
	assert.Equal(t, entry.AggregateID, got.AggregateID)
	assert.Equal(t, entry.LedgerID, got.LedgerID)
	assert.Equal(t, entry.Topic, got.Topic)
	assert.Equal(t, model.SchemaVersionV1, got.SchemaVersion)
	assert.Equal(t, model.EventOutboxStatusPending, got.Status, "a fresh row must be pending")
	assert.Zero(t, got.Attempts)
	assert.Equal(t, defaultEventMaxAttempts, got.MaxAttempts)
	assert.False(t, got.WebhookDispatched, "the dual-delivery flag must start false")
	assert.Nil(t, got.FirstAttemptedAt, "a never-claimed row must have no first attempt")
	assert.Nil(t, got.LastAttemptedAt)
	assert.Nil(t, got.DispatchedAt)
	assert.Nil(t, got.LockedUntil, "a fresh row must be unleased and therefore claimable")
	assert.Empty(t, got.LastError)
	assert.Empty(t, got.DLTTopic)
	assert.Nil(t, got.FailureMetadata)
	assert.True(t, entry.OccurredAt.Equal(got.OccurredAt),
		"occurred_at must survive the round trip exactly: the FIFO claim orders by it")

	assert.Equal(t, string(entry.Payload), string(got.Payload),
		"the payload must round-trip BYTE for byte: payload_raw is BYTEA and returns exactly what the producer marshaled, which is what the dual-delivery and replay-fidelity criteria compare")
}

// TestInsertEventOutbox_PreservesAHostilePayloadSpellingByteForByte is the test the
// byte-preserving column exists for.
//
// The fixture payload is close to canonical, so it would round-trip through JSONB looking
// almost right — which is exactly why this second case is needed. Every element of the
// body below is one PostgreSQL's JSONB parser is documented to change:
//
//   - member order that is NOT the order JSONB stores in (it sorts by length, then bytes)
//   - insignificant whitespace, which JSONB renormalises rather than merely strips
//   - a 17-digit integer, beyond float64's exact range
//   - a trailing zero in a decimal, which a re-marshal would drop
//   - exponent notation, which JSONB expands
//   - a duplicate key, which JSONB collapses to its last occurrence
//
// If the read ever goes back to the JSONB column, this test fails on the first of them.
func TestInsertEventOutbox_PreservesAHostilePayloadSpellingByteForByte(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("hostile")
	quiesceEventOutbox(t, ds, marker)

	hostile := `{"event":"transaction.applied","data":{"zzz":1,  "amount":100.50,` +
		`"id":12345678901234567,"ratio":1.0e3,"a":"x","dup":1,"dup":2}}`

	entry := newEventOutboxFixture(marker)
	entry.Payload = json.RawMessage(hostile)
	insertRealEventOutbox(t, ds, entry)

	got, err := ds.GetEventByID(ctx, entry.EventID)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, hostile, string(got.Payload),
		"the stored body must be returned exactly as the producer marshaled it; a single "+
			"reordered member, dropped zero or collapsed duplicate makes a replay differ from "+
			"the original and the two dual-delivery transports differ from each other")

	// And the JSONB projection really is a DIFFERENT rendering, which is what makes the
	// assertion above meaningful rather than vacuous: if both columns returned the same
	// bytes there would be nothing for the second column to fix.
	var normalised string
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		`SELECT payload::text FROM blnk.event_outbox WHERE event_id = $1`, entry.EventID,
	).Scan(&normalised))
	assert.NotEqual(t, hostile, normalised,
		"the JSONB column is expected to normalise this body; if it did not, this test would "+
			"prove nothing about which column is projected")
}

// TestInsertEventOutbox_DuplicateEventIDIsRejectedByTheUniqueIndex_RealDB proves the
// exactly-once WRITE-SIDE guarantee is enforced by the SCHEMA and not merely intended by
// the code.
//
// event_outbox_event_id_uidx is what makes event_id usable as the subscriber idempotency
// key: a consumer can only deduplicate on it if it is genuinely unique in the first place.
func TestInsertEventOutbox_DuplicateEventIDIsRejectedByTheUniqueIndex_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dup")
	quiesceEventOutbox(t, ds, marker)

	first := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, first)

	duplicate := newEventOutboxFixture(marker)
	duplicate.EventID = first.EventID

	requireAPIError(t, ds.InsertEventOutbox(ctx, duplicate), apierror.ErrConflict)
}

// TestInsertEventOutboxInTx_RolledBackWithLedgerTransaction_RealDB is the real-database
// half of the requirement R-2 proof.
//
// The sqlmock counterpart proves the insert is ISSUED on the caller's transaction; this
// proves the consequence that actually matters — after a rollback the row is GENUINELY
// ABSENT from the table, and after a commit it is genuinely present. Together they close
// the guarantee from both ends: an event cannot outlive a rolled-back mutation, and it
// cannot go missing from a committed one.
func TestInsertEventOutboxInTx_RolledBackWithLedgerTransaction_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("r2")
	quiesceEventOutbox(t, ds, marker)

	t.Run("a rolled-back transaction leaves no event row behind", func(t *testing.T) {
		tx, err := ds.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
		require.NoError(t, err)

		entry := newEventOutboxFixture(marker)
		require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
		require.NotZero(t, entry.ID, "the id is assigned inside the transaction")
		require.NoError(t, tx.Rollback())

		got, err := ds.GetEventByID(ctx, entry.EventID)
		assert.Nil(t, got)
		requireAPIError(t, err, apierror.ErrNotFound)

		// Also asserted on the id, because the row must be gone in every respect and
		// not merely unreachable by its business key.
		var count int
		require.NoError(t, ds.Conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM blnk.event_outbox WHERE id = $1", entry.ID).Scan(&count))
		assert.Zero(t, count, "the event row must not survive a rolled-back ledger transaction")
	})

	t.Run("a committed transaction persists the event row", func(t *testing.T) {
		tx, err := ds.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
		require.NoError(t, err)

		entry := newEventOutboxFixture(marker)
		require.NoError(t, ds.InsertEventOutboxInTx(ctx, tx, entry))
		require.NoError(t, tx.Commit())

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, entry.ID, got.ID)
		assert.Equal(t, model.EventOutboxStatusPending, got.Status)
	})
}

// TestClaimPendingEventOutbox_FifoByOccurredAt_RealDB is the ONLY test that can tell
// occurred_at ordering apart from created_at ordering.
//
// The fixtures are inserted so that their occurred_at order DELIBERATELY DISAGREES with
// their insertion order — the row inserted first has the LATEST occurred_at — which is
// exactly the situation the two columns are silently interchangeable in every other test.
// created_at and id both follow insertion order, so:
//
//   - ordering by occurred_at returns them oldest-occurrence first, which is the reverse
//     of the insertion order;
//   - ordering by created_at or id returns them in insertion order.
//
// A copy-paste "fix" toward created_at, matching the neighbouring lineage claim, leaves
// every other test in this suite green and fails only here. That is why this test exists
// and why the disagreement is constructed rather than incidental.
//
// This is acceptance criterion V-6: per-aggregate ordering. All fixtures deliberately
// share one aggregate and one ledger, so they would all be pinned to a single Kafka
// partition and this claim order becomes the order a consumer observes.
func TestClaimPendingEventOutbox_FifoByOccurredAt_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("fifo")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))

	// Inserted newest-occurrence first, so insertion order is the REVERSE of
	// occurrence order.
	type fixture struct {
		label      string
		occurredAt time.Time
	}
	inserted := []fixture{
		{"third", base.Add(3 * time.Minute)},
		{"first", base.Add(1 * time.Minute)},
		{"fourth", base.Add(4 * time.Minute)},
		{"second", base.Add(2 * time.Minute)},
	}

	const sharedKey = "shared-partition-key"

	byLabel := make(map[string]string, len(inserted))
	insertionOrder := make([]string, 0, len(inserted))
	for _, f := range inserted {
		entry := newEventOutboxFixture(marker)
		entry.AggregateID = marker + "shared-aggregate"
		shareEventOutboxKey(entry, marker+sharedKey)
		entry.OccurredAt = f.occurredAt
		insertRealEventOutbox(t, ds, entry)
		byLabel[f.label] = entry.EventID
		insertionOrder = append(insertionOrder, entry.EventID)
	}

	// ONE AT A TIME, and that is the guarantee rather than a limitation of the test.
	// Every fixture shares a partition key, and at most one row per key may be in
	// flight — otherwise a second relay could claim a later row of the same key while
	// an earlier one was still being published, and because Kafka preserves APPEND
	// order rather than occurred_at, a subscriber would observe the aggregate's events
	// out of order. So the batch is drained by claiming, retiring, and claiming again,
	// and the sequence that comes out is the sequence a consumer sees.
	drained := make([]string, 0, len(inserted))
	for len(drained) < len(inserted) {
		claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)

		mine := make([]model.EventOutbox, 0, 1)
		for _, entry := range claimed {
			if isMarkedEventOutbox(entry, marker) {
				mine = append(mine, entry)
			}
		}
		require.Len(t, mine, 1,
			"at most ONE row per partition key may be claimable at a time; more than one in flight is how same-aggregate events get appended out of order")

		entry := mine[0]
		assert.Equal(t, model.EventOutboxStatusProcessing, entry.Status,
			"a claimed row must be moved to processing and leased")
		require.NotNil(t, entry.LockedUntil, "a claimed row must carry a lease")
		require.NotNil(t, entry.FirstAttemptedAt, "the claim must stamp first_attempted_at")
		require.NotNil(t, entry.LastAttemptedAt, "the claim must stamp last_attempted_at")
		require.NotEmpty(t, entry.ClaimToken, "a claimed row must carry the token the claim issued")

		drained = append(drained, entry.EventID)
		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, entry.ClaimToken, model.BrokerRecord{}))
	}

	wantOccurrenceOrder := []string{
		byLabel["first"], byLabel["second"], byLabel["third"], byLabel["fourth"],
	}
	assert.Equal(t, wantOccurrenceOrder, drained,
		"the claim must release rows in occurred_at order; ordering by created_at or id would return insertion order instead and silently break per-aggregate ordering")
	assert.NotEqual(t, insertionOrder, drained,
		"the fixtures were built so occurrence order disagrees with insertion order; if these matched, the test could not distinguish occurred_at from created_at")
}

// TestClaimPendingEventOutbox_OnlyOneRowPerPartitionKeyIsEverInFlight_RealDB is the
// direct proof of the ORD-01 predicate, and it is the test SKIP LOCKED alone cannot pass.
//
// # The failure it guards against
//
// FOR UPDATE SKIP LOCKED makes it impossible for two relays to claim the SAME row. It
// does nothing about two relays claiming two DIFFERENT rows of the same aggregate out of
// order: relay B skips the earlier row relay A holds and claims a LATER row with the same
// key. Kafka preserves append order, not occurred_at, so relay B's message can land
// first and a subscriber observes transaction.applied before transaction.queued for one
// transaction — with no error, no log line, and nothing in the row state to show it.
//
// # What is asserted
//
// Two rows share a key and a third has its own. A first claim may take the earlier
// same-key row and the independent row, and MUST NOT take the later same-key row. Only
// once the earlier row leaves the blocking states does its successor become claimable.
// The independent row proves the predicate is scoped to the key rather than serialising
// the whole table, which would have destroyed throughput while passing the ordering test.
func TestClaimPendingEventOutbox_OnlyOneRowPerPartitionKeyIsEverInFlight_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("onekey")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "one-key"

	earlier := newEventOutboxFixture(marker)
	shareEventOutboxKey(earlier, sharedKey)
	earlier.OccurredAt = base
	insertRealEventOutbox(t, ds, earlier)

	later := newEventOutboxFixture(marker)
	shareEventOutboxKey(later, sharedKey)
	later.OccurredAt = base.Add(time.Minute)
	insertRealEventOutbox(t, ds, later)

	independent := newEventOutboxFixture(marker)
	independent.OccurredAt = base.Add(2 * time.Minute)
	insertRealEventOutbox(t, ds, independent)

	first, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.True(t, containsEventID(first, earlier.EventID),
		"the earliest row of a key must be claimable")
	assert.False(t, containsEventID(first, later.EventID),
		"a LATER row of the same partition key must not be claimable while an earlier one is in flight; this is exactly what SKIP LOCKED alone permits")
	assert.True(t, containsEventID(first, independent.EventID),
		"the exclusion must be scoped to the partition key: serialising unrelated keys would destroy throughput")

	// A second poll, with the earlier row still held, must still refuse its successor —
	// which is the multi-relay case, since a second relay's poll is indistinguishable
	// from a second poll here.
	second, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.False(t, containsEventID(second, later.EventID),
		"a second relay instance must not be able to claim the successor either")

	// A FAILURE within budget returns the earlier row to pending, where it still blocks
	// its key — retries preserve order.
	var earlierToken string
	for _, row := range first {
		if row.EventID == earlier.EventID {
			earlierToken = row.ClaimToken
		}
	}
	require.NotEmpty(t, earlierToken)
	outcome, err := ds.MarkEventFailed(ctx, earlier.ID, earlierToken, "transient", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.False(t, outcome.Exhausted)

	retry, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.True(t, containsEventID(retry, earlier.EventID),
		"a retryable row must be re-claimed")
	assert.False(t, containsEventID(retry, later.EventID),
		"a retrying row must still block its key, or a retry would be appended after its own successor")

	// Retiring the earlier row finally releases the key.
	var retryToken string
	for _, row := range retry {
		if row.EventID == earlier.EventID {
			retryToken = row.ClaimToken
		}
	}
	require.NotEmpty(t, retryToken)
	require.NoError(t, ds.MarkEventDispatched(ctx, earlier.ID, retryToken, model.BrokerRecord{}))

	released, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.True(t, containsEventID(released, later.EventID),
		"once its predecessor reaches a terminal state the successor must become claimable, or the key stalls forever")
}

// TestClaimPendingEventOutbox_AnExhaustedRowDoesNotStallItsKeyForever_RealDB pins the
// deliberate trade inside the ORD-01 predicate.
//
// The blocking set is pending and processing ONLY. A row that has spent its retry budget
// (failed) or been preserved on its dead-letter topic (dead_lettered) does NOT hold its
// key back. Strict ordering would demand that it did — but then a single permanently
// undeliverable event would stall every subsequent event for that aggregate
// indefinitely, which is a worse failure than a gap. That trade is asserted here so it
// cannot be "tightened" into a stall by someone who reads the predicate and assumes the
// narrower state list was an oversight.
func TestClaimPendingEventOutbox_AnExhaustedRowDoesNotStallItsKeyForever_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("nostall")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "stall-key"

	doomed := newEventOutboxFixture(marker)
	shareEventOutboxKey(doomed, sharedKey)
	doomed.OccurredAt = base
	doomed.MaxAttempts = 1
	insertRealEventOutbox(t, ds, doomed)

	successor := newEventOutboxFixture(marker)
	shareEventOutboxKey(successor, sharedKey)
	successor.OccurredAt = base.Add(time.Minute)
	insertRealEventOutbox(t, ds, successor)

	token := claimEventOutboxToken(t, ds, doomed)
	outcome, err := ds.MarkEventFailed(ctx, doomed.ID, token, "permanently undeliverable", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.True(t, outcome.Exhausted, "a one-attempt budget is spent by its first failure")

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.False(t, containsEventID(claimed, doomed.EventID),
		"a failed row is out of the claimable set: it belongs to the dead-letter path")
	assert.True(t, containsEventID(claimed, successor.EventID),
		"a failed predecessor must NOT stall its key: one undeliverable event stalling an aggregate forever is worse than a gap in its sequence")
}

// TestMarkEventWebhookPending_LeavesTheRowClaimableWithoutBlockingItsKey_RealDB is the
// property no mock can give, and the one the two-leg design rests on.
//
// SUNSET: this test goes with the legacy leg.
//
// # Two facts, and they pull in opposite directions
//
// A webhook_pending row must be CLAIMABLE — otherwise its outstanding webhook is never
// enqueued, which is the defect the state exists to fix — and it must NOT BLOCK its
// partition key, because it has already been published to Kafka and its position in the
// partition is fixed. Blocking would let a failing legacy enqueue stall Kafka delivery
// for the whole aggregate: the deprecated transport interfering with the one replacing
// it.
//
// The two facts live in two different SQL predicates — the claim's status list and the
// NOT EXISTS blocking list — backed by two different partial indexes whose WHERE clauses
// must match them. Only a real database evaluates all four together.
//
// The re-claimed row is additionally asserted to carry kafka_dispatched_at, because that
// is what tells the relay to publish nothing: without it, retrying the webhook would put
// a duplicate on the topic.
func TestMarkEventWebhookPending_LeavesTheRowClaimableWithoutBlockingItsKey_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("whpending")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "webhook-pending-key"

	published := newEventOutboxFixture(marker)
	shareEventOutboxKey(published, sharedKey)
	published.OccurredAt = base
	insertRealEventOutbox(t, ds, published)

	successor := newEventOutboxFixture(marker)
	shareEventOutboxKey(successor, sharedKey)
	successor.OccurredAt = base.Add(time.Minute)
	insertRealEventOutbox(t, ds, successor)

	// The Kafka leg succeeds and the legacy enqueue fails: the transition under test.
	token := claimEventOutboxToken(t, ds, published)
	outcome, err := ds.MarkEventWebhookPending(ctx, published.ID, token, "queue unreachable", 0, model.BrokerRecord{})
	require.NoError(t, err)
	require.Equal(t, model.EventOutboxStatusWebhookPending, outcome.Status)
	require.False(t, outcome.Abandoned, "the default budget is not spent by one failure")

	stored, err := ds.GetEventByID(ctx, published.EventID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.NotNil(t, stored.KafkaDispatchedAt,
		"the Kafka leg must be recorded, which is what makes the re-claim publish nothing")
	assert.Nil(t, stored.DispatchedAt,
		"and the row must NOT be dispatched: a delivery is still owed")
	assert.Equal(t, 1, stored.WebhookAttempts)
	assert.Equal(t, 0, stored.Attempts, "no Kafka attempt may have been spent")

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	assert.True(t, containsEventID(claimed, published.EventID),
		"a webhook_pending row MUST be claimable, or its outstanding webhook is never enqueued — "+
			"which is exactly what marking it dispatched used to do")
	assert.True(t, containsEventID(claimed, successor.EventID),
		"and it must NOT block its key: the row is already on its Kafka topic, so holding the key "+
			"would let a failing legacy enqueue stall Kafka delivery for the whole aggregate")

	for _, row := range claimed {
		if row.EventID != published.EventID {
			continue
		}

		assert.NotNil(t, row.KafkaDispatchedAt,
			"the re-claimed row must carry its Kafka acknowledgement, or the relay republishes it")
		assert.Equal(t, 1, row.WebhookAttempts,
			"and its legacy budget, so the next failure is the second attempt rather than the first")
	}
}

// TestRenewEventOutboxLease_HoldsAnInFlightBatchPastItsOriginalLease_RealDB is the renewal proof
// a mock cannot give: sqlmock has no clock, so it can show the statement is issued but never that
// the row is still unclaimable after the lease it was claimed under would have expired.
//
// Four properties are asserted, and each rules out a different way of getting this wrong:
//
//   - The renewal EXTENDS the rows still in flight, so a batch that outlives its own lease keeps
//     it and no second instance can claim those rows out from under it.
//   - It leaves FINISHED rows alone. A dispatched row has had its token cleared, so it is outside
//     the statement's reach — and it must be, or a terminal row would be handed a live lease and
//     appear to an operator as work in progress.
//   - The count reports what was extended, which is what lets the heartbeat retire itself when
//     the batch is done rather than renewing an empty set for ever.
//   - ANOTHER instance's token extends nothing, which is what makes the token an ownership claim
//     rather than a shared handle.
func TestRenewEventOutboxLease_HoldsAnInFlightBatchPastItsOriginalLease_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("renew")
	quiesceEventOutbox(t, ds, marker)

	// A lease short enough to expire during the test, which is what makes "still held" mean
	// something. Production uses thirty seconds; the mechanism is identical, only the period.
	const originalLease = 900 * time.Millisecond

	base := dbTimestamp(time.Now().Add(-time.Hour))

	finished := newEventOutboxFixture(marker)
	finished.OccurredAt = base
	insertRealEventOutbox(t, ds, finished)

	inFlight := make([]*model.EventOutbox, 0, 2)
	for index := 1; index <= 2; index++ {
		row := newEventOutboxFixture(marker)
		row.OccurredAt = base.Add(time.Duration(index) * time.Minute)
		inFlight = append(inFlight, insertRealEventOutbox(t, ds, row))
	}

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, originalLease)
	require.NoError(t, err)

	mine := make(map[string]model.EventOutbox, 3)
	for _, row := range claimed {
		if isMarkedEventOutbox(row, marker) {
			mine[row.EventID] = row
		}
	}
	require.Len(t, mine, 3, "all three fixtures must be claimed in one batch, under one token")

	token := mine[finished.EventID].ClaimToken
	require.NotEmpty(t, token)
	for eventID, row := range mine {
		require.Equalf(t, token, row.ClaimToken,
			"one claim issues ONE token; event %s came back with a different one, which would make the "+
				"heartbeat unable to address the batch", eventID)
	}

	// One row finishes, exactly as the first wave of a real batch does.
	require.NoError(t, ds.MarkEventDispatched(ctx, finished.ID, token, model.BrokerRecord{}))

	renewed, err := ds.RenewEventOutboxLease(ctx, token, 10*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(len(inFlight)), renewed,
		"the renewal must extend exactly the rows still in flight — %d of the %d claimed — so the "+
			"heartbeat can tell a batch still working from one that has finished",
		len(inFlight), len(mine))

	// Past the ORIGINAL lease. Without the renewal every one of these rows would be claimable now.
	time.Sleep(originalLease + 200*time.Millisecond)

	afterExpiry, err := ds.ClaimPendingEventOutbox(ctx, 100, time.Minute)
	require.NoError(t, err)

	for _, row := range inFlight {
		assert.Falsef(t, containsEventID(afterExpiry, row.EventID),
			"event %s must NOT be claimable %s after a %s lease was taken on it: the renewal extended "+
				"it, and a row reclaimed here is published a second time while this instance is still "+
				"publishing it", row.EventID, originalLease+200*time.Millisecond, originalLease)

		stored, storedErr := ds.GetEventByID(ctx, row.EventID)
		require.NoError(t, storedErr)
		require.NotNil(t, stored)
		require.NotNil(t, stored.LockedUntil, "a renewed row must still hold a lease")
		assert.Truef(t, stored.LockedUntil.After(time.Now().Add(5*time.Minute)),
			"event %s must carry the EXTENDED expiry, not the original one: locked_until is %s",
			row.EventID, stored.LockedUntil.UTC().Format(time.RFC3339Nano))
		assert.Equal(t, token, stored.ClaimToken,
			"and it must still be held by the same claim")
	}

	// The finished row was untouched by the renewal.
	settled, err := ds.GetEventByID(ctx, finished.EventID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, model.EventOutboxStatusDispatched, settled.Status)
	assert.Nil(t, settled.LockedUntil,
		"a dispatched row must not be handed a lease by a renewal: it is finished, and a live lease on "+
			"it would read as work in progress")
	assert.Empty(t, settled.ClaimToken)

	// A different instance's token owns nothing here.
	stranger, err := ds.RenewEventOutboxLease(ctx, uuid.NewString(), 10*time.Minute)
	require.NoError(t, err, "an unknown token is not an error; it simply holds nothing")
	assert.Zero(t, stranger,
		"another instance's token must extend nothing: the token is an ownership claim, not a shared handle")
}

// TestClaimFailedEventOutboxForDeadLetter_RecoversAnUnpreservedRowNothingElseCanReach_RealDB is
// the F14 limbo, walked end to end against the real table.
//
// The sequence is the production one: a row spends its retry budget, its dead-letter write fails,
// and it is left at 'failed' with dlt_topic NULL. What is asserted is that this row —
//
//   - is INVISIBLE to the ordinary claim, because that predicate admits only attempts <
//     max_attempts, which is exactly why the repair pass has to exist;
//   - is invisible to the REPAIR claim too while the hand-off lease the exhaustion transition
//     left on it is still live, which is what stops the repair pass racing the worker that owes
//     the dead-letter write (F-28);
//   - IS reachable by the repair claim once that lease lapses, which leaves the status at
//     'failed' and stamps a fresh token;
//   - is protected by its new lease from being repaired twice at once;
//   - can then complete through the ORDINARY MarkEventDeadLettered transition, which accepts
//     'failed' as a prior state precisely so no second code path is needed; and
//   - fences the token the failed worker was holding, so a straggler cannot preserve a row the
//     repair has taken over.
//
// A row that WAS preserved is seeded alongside it and must never be claimed, because a repair
// pass that re-claimed dead-lettered rows would rewrite their dead-letter records for ever.
func TestClaimFailedEventOutboxForDeadLetter_RecoversAnUnpreservedRowNothingElseCanReach_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dltrepair")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))

	// A one-attempt budget: the first failure spends it, which is what routes the row to the
	// dead-letter hand-off without walking the whole production backoff schedule.
	stranded := newEventOutboxFixture(marker)
	stranded.OccurredAt = base
	stranded.MaxAttempts = 1
	insertRealEventOutbox(t, ds, stranded)

	preserved := newEventOutboxFixture(marker)
	preserved.OccurredAt = base.Add(time.Minute)
	preserved.MaxAttempts = 1
	insertRealEventOutbox(t, ds, preserved)

	// Both rows fail their only attempt. They are claimed in ONE batch — their partition keys
	// differ, so nothing serialises them — and the tokens are captured together, because a second
	// claim would find them already leased under the first one.
	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	tokens := make(map[string]string, 2)
	for _, row := range claimed {
		if isMarkedEventOutbox(row, marker) {
			tokens[row.EventID] = row.ClaimToken
		}
	}
	require.Len(t, tokens, 2, "both fixtures must be claimed before either can be failed")

	strandedToken := tokens[stranded.EventID]
	require.NotEmpty(t, strandedToken, "the stranded fixture must come back with the token its claim issued")

	strandedOutcome, err := ds.MarkEventFailed(ctx, stranded.ID, strandedToken, "broker refused the publish", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.True(t, strandedOutcome.Exhausted, "a one-attempt budget must be spent by one failure")
	require.Equal(t, model.EventOutboxStatusFailed, strandedOutcome.Status)

	preservedToken := tokens[preserved.EventID]
	require.NotEmpty(t, preservedToken)

	preservedOutcome, err := ds.MarkEventFailed(ctx, preserved.ID, preservedToken, "broker refused the publish", 0, false, testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.True(t, preservedOutcome.Exhausted)

	// Only the second one's dead-letter write succeeded.
	require.NoError(t, ds.MarkEventDeadLettered(ctx, preserved.ID, preservedOutcome.ClaimToken,
		"blnk.transactions.dlt", json.RawMessage(`{"original_topic":"blnk.transactions"}`), model.BrokerRecord{}))

	// THE LIMBO. The ordinary claim will never take the stranded row again.
	ordinary, err := ds.ClaimPendingEventOutbox(ctx, 100, time.Minute)
	require.NoError(t, err)
	require.False(t, containsEventID(ordinary, stranded.EventID),
		"a row whose retry budget is spent must be outside the ordinary claim: if it were not, this "+
			"test would be exercising the retry path rather than the repair")

	// AND IT IS NOT REACHABLE BY THE REPAIR PASS EITHER, YET. This is the F-28 regression, and it
	// is the assertion the earlier arrangement failed.
	//
	// The exhaustion arm RETAINS the claim token so that only the worker which spent the last
	// attempt may write the dead-letter message and record it. That retention is only worth
	// anything if the row also stays LEASED: this repair claim admits a failed row with no
	// dlt_topic and no live lease, and it stamps a FRESH token over whatever was there. When the
	// exhaustion arm cleared locked_until, a second instance could therefore take the row in the
	// same instant, overwrite the token the owning worker was about to present, and publish the
	// dead-letter message alongside it — two copies of one event on the .dlt topic.
	tooSoon, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
	require.NoError(t, err)
	require.False(t, containsEventID(tooSoon, stranded.EventID),
		"a row whose exhaustion transition has just handed it to the dead-letter writer must stay "+
			"leased: the repair pass reaching it while the owning worker is still publishing is what "+
			"puts two copies of one event on a dead-letter topic")

	// EXPIRING THE LEASE is how the owning worker's death is modelled, and it is the only
	// condition under which the repair pass should see the row. Written directly because there
	// is no API for "that process is gone" — the lease lapsing IS that statement.
	_, err = ds.Conn.ExecContext(ctx,
		`UPDATE blnk.event_outbox SET locked_until = NOW() - INTERVAL '1 second' WHERE id = $1`,
		stranded.ID)
	require.NoError(t, err, "expiring the hand-off lease models the owning worker having died")

	// THE WAY OUT.
	repair, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
	require.NoError(t, err)

	require.True(t, containsEventID(repair, stranded.EventID),
		"once the hand-off lease has expired the repair claim must reach a failed row with no "+
			"dead-letter record; without it the event is the only copy of itself and nothing will ever "+
			"act on it again")
	assert.False(t, containsEventID(repair, preserved.EventID),
		"a row already preserved on its dead-letter topic must NOT be re-claimed: repairing it again "+
			"would rewrite its dead-letter record on every poll for ever")

	var claimedStranded model.EventOutbox
	for _, row := range repair {
		if row.EventID == stranded.EventID {
			claimedStranded = row
		}
	}

	assert.Equal(t, model.EventOutboxStatusFailed, claimedStranded.Status,
		"the repair must LEAVE the status at 'failed': it keeps the row in the dead-letter inventory "+
			"for the whole attempt, and MarkEventDeadLettered accepts 'failed' as a prior state")
	assert.Equal(t, 1, claimedStranded.Attempts,
		"and must not spend or reset the retry budget: the metadata reports the attempts that were made")
	require.NotEmpty(t, claimedStranded.ClaimToken)
	assert.NotEqual(t, strandedToken, claimedStranded.ClaimToken,
		"a FRESH token must be stamped: the original belonged to a worker whose write already failed")

	// The new lease is what makes the retry frequency-bounded rather than a hot loop.
	again, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
	require.NoError(t, err)
	assert.False(t, containsEventID(again, stranded.EventID),
		"a row under a live repair lease must not be claimed again, or several instances write the same "+
			"dead-letter message at once")

	// FENCING: the worker whose dead-letter write failed can no longer finish the row.
	staleErr := ds.MarkEventDeadLettered(ctx, stranded.ID, strandedToken,
		"blnk.transactions.dlt", json.RawMessage(`{"original_topic":"blnk.transactions"}`), model.BrokerRecord{})
	require.Error(t, staleErr,
		"the superseded token must be refused: a straggler must not preserve a row the repair has taken over")

	// AND THE ORDINARY TRANSITION FINISHES IT.
	require.NoError(t, ds.MarkEventDeadLettered(ctx, stranded.ID, claimedStranded.ClaimToken,
		"blnk.transactions.dlt", json.RawMessage(`{"original_topic":"blnk.transactions","error_reason":"broker refused the publish"}`), model.BrokerRecord{}),
		"the repair's own token must be able to record the preservation through the ordinary transition")

	repaired, err := ds.GetEventByID(ctx, stranded.EventID)
	require.NoError(t, err)
	require.NotNil(t, repaired)
	assert.Equal(t, model.EventOutboxStatusDeadLettered, repaired.Status)
	assert.Equal(t, "blnk.transactions.dlt", repaired.DLTTopic)
	assert.Nil(t, repaired.LockedUntil, "a preserved row holds no lease")
	assert.Empty(t, repaired.ClaimToken, "and no token: it is nobody's to hold")

	settled, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
	require.NoError(t, err)
	assert.False(t, containsEventID(settled, stranded.EventID),
		"once preserved, the row must leave the repair backlog for good")
}

// TestClaimPendingEventOutbox_ConcurrentClaimsAreDisjoint_RealDB proves that concurrent relay
// instances never publish the same event twice, and sqlmock cannot give it: a mock has no
// locking semantics at all.
//
// Two properties are asserted, and both are needed:
//
//   - DISJOINTNESS. No row may appear in two workers' batches. Without the row lock entirely,
//     two relay instances can claim the same row and publish the same event twice.
//   - NO STARVATION. Every fixture must be claimed exactly once across the workers, so
//     the whole backlog is drained.
//
// # What this test does NOT prove, and why a second one exists
//
// It does not prove SKIP LOCKED, and it is important not to read it as though it did. The
// claim is a SINGLE autocommit statement, so the row locks it takes are released the instant
// it returns. Two concurrent claimers with SKIP LOCKED removed would therefore BLOCK on each
// other for microseconds, unblock when the other statement committed, re-evaluate the row
// under READ COMMITTED, find it is no longer pending, and move on — arriving at exactly the
// disjoint, fully-drained outcome asserted below. Both assertions here survive deleting SKIP
// LOCKED from the query.
//
// That matters because SKIP LOCKED is doing real work: at 500 events per second across
// several relay instances, a claimer that queues behind another's lock instead of skipping it
// serialises the whole pipeline. Proving it needs a lock that OUTLIVES the claim statement,
// which no self-contained concurrent claim can produce.
// TestClaimPendingEventOutbox_SkipsALockedRowRatherThanBlockingOnIt_RealDB holds exactly such
// a lock and is where that property is established.
func TestClaimPendingEventOutbox_ConcurrentClaimsAreDisjoint_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("conc")
	quiesceEventOutbox(t, ds, marker)

	const total = 20
	const workers = 4

	fixtures := make(map[string]bool, total)
	base := dbTimestamp(time.Now().Add(-time.Hour))
	for i := 0; i < total; i++ {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = base.Add(time.Duration(i) * time.Second)
		insertRealEventOutbox(t, ds, entry)
		fixtures[entry.EventID] = true
	}

	results := make([][]model.EventOutbox, workers)
	errs := make([]error, workers)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for w := 0; w < workers; w++ {
		done.Add(1)
		go func(w int) {
			defer done.Done()
			start.Wait() // line every worker up for maximum contention
			results[w], errs[w] = ds.ClaimPendingEventOutbox(ctx, total/workers, 5*time.Minute)
		}(w)
	}
	start.Done()
	done.Wait()

	for w, err := range errs {
		require.NoError(t, err, "worker %d claim failed", w)
	}

	claimCounts := make(map[string]int, total)
	for w := 0; w < workers; w++ {
		for _, entry := range results[w] {
			if fixtures[entry.EventID] {
				claimCounts[entry.EventID]++
				assert.Equal(t, model.EventOutboxStatusProcessing, entry.Status)
			}
		}
	}

	for eventID, n := range claimCounts {
		assert.Equal(t, 1, n,
			"event %s was claimed by %d workers; FOR UPDATE SKIP LOCKED must keep concurrent claims disjoint", eventID, n)
	}

	// Between them the workers requested exactly `total` rows, every one of which was
	// pending and unleased, so a claim that blocked instead of skipping would leave some
	// unclaimed.
	assert.Len(t, claimCounts, total,
		"every fixture must be claimed exactly once across the workers; a short count means workers blocked on each other rather than skipping locked rows")
}

// TestClaimPendingEventOutbox_SkipsALockedRowRatherThanBlockingOnIt_RealDB is the SKIP LOCKED
// proof, and it is the only test in this file that can be one.
//
// # Why the concurrent-claim test is not enough
//
// The neighbouring disjointness test lines four claimers up against twenty rows and asserts
// that no row is claimed twice and none is left behind. Both hold with SKIP LOCKED deleted.
// The claim is one autocommit statement, so its row locks live only for the duration of that
// statement: a claimer without SKIP LOCKED waits microseconds for the other statement to
// commit, re-evaluates the row it was waiting on, finds it is `processing` now, and skips it
// anyway. The outcome is identical and the property is untested.
//
// # What this test does instead
//
// It holds a row lock that the claim CANNOT outlast. A second connection opens an explicit
// transaction, takes `SELECT ... FOR UPDATE` on the FIFO-earliest claimable row, and keeps it
// open across the claim. Two behaviours then diverge completely:
//
//   - WITH SKIP LOCKED the locked row is passed over, the next candidate is locked instead,
//     and the claim returns promptly with a different row.
//   - WITHOUT IT the claim blocks on the lock for as long as the holder keeps it — here, until
//     the context deadline expires and the claim fails outright.
//
// So the assertion is not a statistical one about contention. It is: the claim answered, it
// answered with the OTHER row, it answered quickly, and the lock was still held when it did.
//
// # And the row is skipped, not excluded
//
// A claim that simply refused to consider the locked row would satisfy all of that while
// stranding it for ever, so the lock is released and the parked row is required to become
// claimable again. That is what makes "skipped" mean deferred rather than dropped — and it is
// the difference between SKIP LOCKED and a bug that silently drains part of the backlog.
func TestClaimPendingEventOutbox_SkipsALockedRowRatherThanBlockingOnIt_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("skiplock")
	quiesceEventOutbox(t, ds, marker)

	// Two rows with DIFFERENT partition keys, which newEventOutboxFixture gives by default and
	// which is load-bearing here: at most one row per partition key is claimable at a time, so
	// two rows sharing a key would leave the second unclaimable for a reason that has nothing to
	// do with locking and the test would pass without proving anything.
	base := dbTimestamp(time.Now().Add(-time.Hour))

	parked := newEventOutboxFixture(marker)
	parked.OccurredAt = base
	insertRealEventOutbox(t, ds, parked)

	successor := newEventOutboxFixture(marker)
	successor.OccurredAt = base.Add(time.Second)
	insertRealEventOutbox(t, ds, successor)

	// The FIFO-earliest row is the one locked, deliberately. It is the row the claim's ORDER BY
	// reaches FIRST, so a claimer that blocks blocks immediately and a claimer that skips has to
	// carry on to a later candidate — the arrangement in which the two behaviours are furthest
	// apart. Locking a later row would let a blocking claimer succeed on the first candidate and
	// prove nothing.
	holder, err := ds.Conn.BeginTx(ctx, nil)
	require.NoError(t, err, "opening the transaction that holds the row lock")

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if rollbackErr := holder.Rollback(); rollbackErr != nil &&
			!errors.Is(rollbackErr, sql.ErrTxDone) {
			logrus.WithError(rollbackErr).Warn("rolling back the SKIP LOCKED holder transaction")
		}
	}
	// Registered before the lock is taken, so a failure anywhere below cannot leave a row locked
	// for the rest of the package's run.
	t.Cleanup(release)

	var lockedID int64
	require.NoError(t,
		holder.QueryRowContext(ctx,
			`SELECT id FROM blnk.event_outbox WHERE id = $1 FOR UPDATE`, parked.ID).Scan(&lockedID),
		"the holder transaction must take the row lock")
	require.Equal(t, parked.ID, lockedID)

	// A DEADLINE IS THE INSTRUMENT. Without SKIP LOCKED the claim waits on the lock above, which
	// is never released inside this window, so the deadline is what converts an indefinite block
	// into a reportable failure instead of a hung test.
	claimCtx, cancelClaim := context.WithTimeout(ctx, 5*time.Second)
	defer cancelClaim()

	started := time.Now()
	claimed, claimErr := ds.ClaimPendingEventOutbox(claimCtx, 1, testSettlementLease)
	elapsed := time.Since(started)

	require.NoErrorf(t, claimErr,
		"the claim must ANSWER while another transaction holds a row lock on the earliest "+
			"candidate. It did not, and after %s the context deadline expired — which is what "+
			"happens when FOR UPDATE SKIP LOCKED loses its SKIP LOCKED: the claimer queues behind "+
			"the lock instead of moving to the next candidate, and at 500 events per second "+
			"across several relay instances that serialises the entire pipeline", elapsed)

	require.Len(t, claimed, 1, "one row was asked for and one claimable row was unlocked")
	assert.Equalf(t, successor.EventID, claimed[0].EventID,
		"the claim must return the SUCCESSOR, not the locked row. Returning the locked row would "+
			"mean the row lock is not being taken at all, and two relay instances would publish "+
			"the same event")
	assert.Equal(t, model.EventOutboxStatusProcessing, claimed[0].Status)

	assert.Lessf(t, elapsed, 2*time.Second,
		"the claim must skip PROMPTLY rather than wait. %s is long enough to be a block that "+
			"happened to be released rather than a row that was passed over", elapsed)

	// THE LOCK WAS STILL HELD WHEN THE CLAIM ANSWERED. Without this the test would also pass
	// against a claimer that blocked and was unblocked by the holder ending early — the exact
	// scenario the elapsed-time bound only makes unlikely.
	var stillLocked int64
	require.NoError(t,
		holder.QueryRowContext(ctx, `SELECT $1::bigint`, parked.ID).Scan(&stillLocked),
		"the holder transaction must still be open, or the claim may simply have waited it out")
	require.Equal(t, parked.ID, stillLocked)

	// SKIPPED MEANS DEFERRED, NOT DROPPED. Release the lock and the parked row must re-enter the
	// claimable set; a claim that had excluded it rather than skipped it would strand the oldest
	// event in the backlog for ever while reporting a healthy drain.
	release()

	reclaimed, err := ds.ClaimPendingEventOutbox(ctx, 5, testSettlementLease)
	require.NoError(t, err)
	assert.Truef(t, containsEventID(reclaimed, parked.EventID),
		"once the lock is released the passed-over row must be claimable again. SKIP LOCKED defers "+
			"a row; it does not remove it from the backlog.\nclaimed: %v", eventIDsOf(reclaimed))
}

// TestClaimPendingEventOutbox_ReclaimsAfterLockExpiry_RealDB is the crash-recovery
// mechanism behind acceptance criterion V-7.
//
// A relay that dies mid-batch leaves its rows in processing with a lease that nobody will
// release. The lease — rather than an in-memory marker — is what makes those rows
// recoverable: once locked_until passes they re-enter the claimable set and another relay
// instance picks them up. Without this, a single relay crash would strand every in-flight
// event permanently, and the outbox would guarantee durability of rows while still losing
// the events.
//
// This is also the one behaviour where this table deliberately differs from
// blnk.lineage_outbox, whose claim admits only pending rows and therefore CANNOT reclaim a
// crashed processor's work — a limitation the lineage test suite documents as a suspected
// bug. The claim here admits pending AND processing, so the recovery actually happens.
//
// The lease is expired on the DATABASE clock rather than by sleeping: it makes the test
// fast and immune to any skew between the test process's clock and the server's.
func TestClaimPendingEventOutbox_ReclaimsAfterLockExpiry_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("expiry")
	quiesceEventOutbox(t, ds, marker)

	entry := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, entry)

	firstClaim, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	require.True(t, containsEventID(firstClaim, entry.EventID), "the fixture must be claimed first time")

	// While the lease holds, the row must not be claimable again.
	secondClaim, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.False(t, containsEventID(secondClaim, entry.EventID),
		"a leased row must not be claimable again; otherwise two relays publish the same event")

	// Simulate a relay that crashed after claiming: the lease expires but the status
	// stays processing, because no transition ever ran.
	_, err = ds.Conn.ExecContext(ctx,
		"UPDATE blnk.event_outbox SET locked_until = NOW() - interval '1 minute' WHERE id = $1", entry.ID)
	require.NoError(t, err)

	var status string
	var lockExpired bool
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		"SELECT status, locked_until < NOW() FROM blnk.event_outbox WHERE id = $1", entry.ID).
		Scan(&status, &lockExpired))
	require.Equal(t, model.EventOutboxStatusProcessing, status,
		"the crashed-relay scenario requires the row to still be in processing")
	require.True(t, lockExpired, "the lease must already be expired on the database clock")

	reclaimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)
	assert.True(t, containsEventID(reclaimed, entry.EventID),
		"an expired lease on a processing row must make it claimable again; this is what stops a relay crash from stranding in-flight events forever")
}

// TestEventOutboxTransitions_AStaleLeaseHolderCannotWrite_RealDB is the STATE-01
// scenario end to end, against real SQL, and it is the test that proves the claim token
// does the job the lease alone could not.
//
// # The scenario, and what used to happen
//
// Relay A claims a row. A stalls — a long GC pause, a paused container, a network
// partition — and its lease expires. Relay B reclaims the row and publishes the event
// successfully. A then wakes up, still holding its own in-memory copy of the row, and
// records the outcome of ITS attempt.
//
// When transitions matched on row id alone, A's write LANDED. Three things followed, and
// every one of them was silent: A could mark the row failed and increment attempts,
// double-spending a budget B had already used; A could mark it dispatched over the top of
// a state B had moved on from; and A could move a terminal row back out of its terminal
// state entirely. Nothing errored, and nothing in the row afterwards recorded that two
// workers had written to it.
//
// The token closes it. A holds the token from ITS claim; B's reclaim issued a new one; so
// every transition A attempts matches no row and is reported as a lost claim. A learns to
// stop, which is the correct behaviour and the one the old code made impossible.
func TestEventOutboxTransitions_AStaleLeaseHolderCannotWrite_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("stale")
	quiesceEventOutbox(t, ds, marker)

	entry := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, entry)

	// Relay A claims and holds a token.
	staleToken := claimEventOutboxToken(t, ds, entry)

	// A stalls; its lease expires on the database clock.
	_, err := ds.Conn.ExecContext(ctx,
		"UPDATE blnk.event_outbox SET locked_until = NOW() - interval '1 minute' WHERE id = $1", entry.ID)
	require.NoError(t, err)

	// Relay B reclaims and gets its OWN token.
	liveToken := claimEventOutboxToken(t, ds, entry)
	require.NotEqual(t, staleToken, liveToken,
		"a reclaim must issue a new token, or the stale holder's token would still authorise writes")

	// Everything relay A now attempts must be refused.
	t.Run("stale dispatch is refused", func(t *testing.T) {
		requireAPIError(t, ds.MarkEventDispatched(ctx, entry.ID, staleToken, model.BrokerRecord{}), apierror.ErrConflict)
	})
	t.Run("stale failure is refused and does not spend the budget", func(t *testing.T) {
		_, failErr := ds.MarkEventFailed(ctx, entry.ID, staleToken, "stale attempt", 0, false, testDeadLetterHandoffLease)
		requireAPIError(t, failErr, apierror.ErrConflict)

		got, getErr := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, getErr)
		assert.Equal(t, 0, got.Attempts,
			"a stale worker must not be able to spend an attempt: that is how a five-attempt budget got consumed at twice the intended rate")
		assert.Equal(t, model.EventOutboxStatusProcessing, got.Status,
			"the row must remain exactly as the live holder left it")
	})
	t.Run("stale dead-letter is refused", func(t *testing.T) {
		requireAPIError(t, ds.MarkEventDeadLettered(ctx, entry.ID, staleToken,
			entry.Topic+".dlt", json.RawMessage(`{"attempt_count":1}`), model.BrokerRecord{}), apierror.ErrConflict)
	})
	t.Run("stale webhook marker is refused", func(t *testing.T) {
		requireAPIError(t, ds.MarkWebhookDispatched(ctx, entry.ID, staleToken), apierror.ErrConflict)

		got, getErr := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, getErr)
		assert.False(t, got.WebhookDispatched,
			"a stale worker must not be able to record a webhook leg the live holder has not sent")
	})

	// And the LIVE holder is unaffected throughout.
	require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, liveToken, model.BrokerRecord{}),
		"the live claim holder must still be able to complete its work")

	final, err := ds.GetEventByID(ctx, entry.EventID)
	require.NoError(t, err)
	assert.Equal(t, model.EventOutboxStatusDispatched, final.Status)
	assert.Equal(t, 0, final.Attempts, "no attempt was ever legitimately recorded against this row")
}

// TestClaimPendingEventOutbox_SkipsRowsAtMaxAttempts_RealDB asserts the retry budget is
// honoured by the claim predicate.
//
// Once attempts reaches max_attempts the row must leave the claimable set, so the relay
// stops retrying it and it is left for the dead-letter path. Without this the row is
// retried forever: it never reaches a terminal state, it never appears in the dead-letter
// inventory, and it occupies claim capacity on every poll for the rest of the table's
// life.
//
// The over-budget case is asserted as well as the exactly-at-budget case, because a
// predicate written as `attempts != max_attempts` would pass the boundary test and let an
// over-budget row through.
func TestClaimPendingEventOutbox_SkipsRowsAtMaxAttempts_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("budget")
	quiesceEventOutbox(t, ds, marker)

	exhausted := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, exhausted)
	_, err := ds.Conn.ExecContext(ctx,
		"UPDATE blnk.event_outbox SET attempts = max_attempts WHERE id = $1", exhausted.ID)
	require.NoError(t, err)

	overBudget := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, overBudget)
	_, err = ds.Conn.ExecContext(ctx,
		"UPDATE blnk.event_outbox SET attempts = max_attempts + 3 WHERE id = $1", overBudget.ID)
	require.NoError(t, err)

	withinBudget := newEventOutboxFixture(marker)
	insertRealEventOutbox(t, ds, withinBudget)
	_, err = ds.Conn.ExecContext(ctx,
		"UPDATE blnk.event_outbox SET attempts = max_attempts - 1 WHERE id = $1", withinBudget.ID)
	require.NoError(t, err)

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	assert.False(t, containsEventID(claimed, exhausted.EventID),
		"a row at max_attempts must not be claimed; it belongs to the dead-letter path, not to another retry")
	assert.False(t, containsEventID(claimed, overBudget.EventID),
		"a row past max_attempts must not be claimed either: the predicate must be attempts < max_attempts, not attempts != max_attempts")
	assert.True(t, containsEventID(claimed, withinBudget.EventID),
		"a row with budget remaining must still be claimed")
}

// TestEventOutboxTransitions_DriveTheStateMachine_RealDB walks the whole state machine
// against a real table, which is what proves the SQL CASE arms and the column writes
// actually land where the mocked assertions say they do.
//
//	pending -> processing (claim) -> pending (failure within budget)
//	                              -> failed (budget exhausted)
//	                              -> dead_lettered (written to the .dlt sibling)
//	pending -> processing (claim) -> dispatched (broker ack)
func TestEventOutboxTransitions_DriveTheStateMachine_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("machine")
	quiesceEventOutbox(t, ds, marker)

	t.Run("dispatch is terminal and releases the lease", func(t *testing.T) {
		entry := newEventOutboxFixture(marker)
		insertRealEventOutbox(t, ds, entry)

		claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		require.True(t, containsEventID(claimed, entry.EventID))

		var token string
		for _, row := range claimed {
			if row.EventID == entry.EventID {
				token = row.ClaimToken
			}
		}
		require.NotEmpty(t, token, "the claim must issue a token, or no transition can be authorised")

		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, token, model.BrokerRecord{}))

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusDispatched, got.Status)
		require.NotNil(t, got.DispatchedAt, "dispatched_at must be stamped")
		assert.Nil(t, got.LockedUntil, "the lease must be released")
		assert.Empty(t, got.ClaimToken, "a terminal state releases the claim token")

		assert.Error(t, ds.MarkEventDispatched(ctx, entry.ID, token, model.BrokerRecord{}),
			"replaying the same transition with the same token must be refused: the row is terminal and the token has been released")

		again, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		assert.False(t, containsEventID(again, entry.EventID),
			"dispatched is terminal: the claim predicate admits only pending and processing rows")
	})

	t.Run("a failure within budget returns the row to pending and it is reclaimed", func(t *testing.T) {
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 3
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "broker unavailable", 0, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		assert.False(t, outcome.Exhausted, "one of three attempts is not exhaustion")
		assert.Equal(t, model.EventOutboxStatusPending, outcome.Status)
		assert.Equal(t, 1, outcome.Attempts)

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusPending, got.Status,
			"a failure within budget must return the row to pending")
		assert.Empty(t, got.ClaimToken,
			"the retry arm must release the claim, or the row cannot be re-claimed")
		assert.Equal(t, 1, got.Attempts)
		assert.Equal(t, "broker unavailable", got.LastError)
		assert.Nil(t, got.LockedUntil, "the lease must be released so the retry is actually picked up")
		require.NotNil(t, got.FirstAttemptedAt)
		require.NotNil(t, got.LastAttemptedAt)

		reclaimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		assert.True(t, containsEventID(reclaimed, entry.EventID), "a retryable row must be reclaimable")
	})

	t.Run("a row inside its backoff window is not claimed until it is due", func(t *testing.T) {
		// This is the DURABLE half of the configured exponential backoff, and it is a
		// property of the claim rather than of the relay. Releasing the lease is not
		// enough: at a 1-second poll interval a released row is retried on the very next
		// lap, so the configured 1s/2s/4s/8s/16s schedule collapses into roughly five
		// seconds of consecutive attempts against a broker that has barely begun to fail.
		//
		// Sleeping the delay in the relay is worse than no backoff at all: the schedule
		// sums to 31 seconds, which OUTLIVES the 30-second lease, so a second instance
		// reclaims the row mid-sleep and publishes the event twice.
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 3
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "broker unavailable", time.Hour, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		require.False(t, outcome.Exhausted)
		require.Equal(t, model.EventOutboxStatusPending, outcome.Status)

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Nil(t, got.LockedUntil,
			"the lease is still released: it answers a different question from the due instant")
		assert.True(t, got.NextAttemptAt.After(time.Now().Add(30*time.Minute)),
			"the due instant must be advanced by the caller's delay, got %s", got.NextAttemptAt)

		notDue, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		assert.False(t, containsEventID(notDue, entry.EventID),
			"a pending, unleased row that is not yet DUE must not be claimed — that is the whole backoff")

		// Bring the due instant into the past exactly as the passage of time would, and the
		// same row becomes claimable with nothing else changed. Without this half the test
		// would pass just as well against a row that had been made permanently unclaimable.
		_, err = ds.Conn.ExecContext(ctx,
			`UPDATE blnk.event_outbox SET next_attempt_at = NOW() - interval '1 second' WHERE id = $1`,
			entry.ID)
		require.NoError(t, err)

		due, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		assert.True(t, containsEventID(due, entry.EventID),
			"once the due instant passes the row must be claimed again")
	})

	t.Run("an exhausted row keeps the due instant it already had", func(t *testing.T) {
		// A row whose budget is spent is not waiting for anything. Advancing its due
		// instant would read as though a retry were still coming, which is exactly the
		// wrong thing to show an operator triaging a dead-letter backlog.
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 1
		insertRealEventOutbox(t, ds, entry)

		before, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)

		token := claimEventOutboxToken(t, ds, entry)

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "gave up", time.Hour, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		require.True(t, outcome.Exhausted, "one of one attempt is exhaustion")

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusFailed, got.Status)
		assert.WithinDuration(t, before.NextAttemptAt, got.NextAttemptAt, time.Second,
			"the exhaustion arm must leave next_attempt_at alone, however large the delay it was handed")
	})

	t.Run("first_attempted_at is pinned across retries while last_attempted_at moves", func(t *testing.T) {
		// This is what bounds the retry window reported in the dead-letter failure
		// metadata, and it is only observable across MORE THAN ONE attempt.
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 5
		insertRealEventOutbox(t, ds, entry)

		firstToken := claimEventOutboxToken(t, ds, entry)
		_, err := ds.MarkEventFailed(ctx, entry.ID, firstToken, "attempt one", 0, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		afterFirst, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		require.NotNil(t, afterFirst.FirstAttemptedAt)
		require.NotNil(t, afterFirst.LastAttemptedAt)

		// A SECOND claim, and therefore a second token. The first token is now stale,
		// which is precisely the state a zombie worker would be in.
		secondToken := claimEventOutboxToken(t, ds, entry)
		require.NotEqual(t, firstToken, secondToken,
			"each claim must issue its own token, or a stale worker's token would still authorise a write")
		assert.Error(t, ds.MarkEventDispatched(ctx, entry.ID, firstToken, model.BrokerRecord{}),
			"the stale token from the previous claim must no longer authorise anything")

		_, err = ds.MarkEventFailed(ctx, entry.ID, secondToken, "attempt two", 0, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		afterSecond, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		require.NotNil(t, afterSecond.FirstAttemptedAt)
		require.NotNil(t, afterSecond.LastAttemptedAt)

		assert.True(t, afterFirst.FirstAttemptedAt.Equal(*afterSecond.FirstAttemptedAt),
			"COALESCE must pin first_attempted_at to the FIRST attempt; assigning it would collapse the retry window the dead-letter metadata reports")
		assert.False(t, afterSecond.LastAttemptedAt.Before(*afterFirst.LastAttemptedAt),
			"last_attempted_at must move forward with every attempt")
		assert.Equal(t, 2, afterSecond.Attempts)
		assert.Equal(t, "attempt two", afterSecond.LastError)
	})

	t.Run("exhausting the budget produces failed, and dead-lettering is the second step", func(t *testing.T) {
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 1
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)

		// attempts 0 + 1 >= max_attempts 1, so the exhaustion arm fires on the first
		// failure. This is the CASE boundary, exercised against real SQL.
		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "final failure", 0, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		assert.True(t, outcome.Exhausted, "one of one attempt is exhaustion")
		assert.Equal(t, token, outcome.ClaimToken,
			"the exhaustion arm must hand the token on so the dead-letter step can be authorised")

		failed, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, token, failed.ClaimToken,
			"the exhaustion arm must RETAIN the claim, so only this worker may dead-letter the row")
		assert.Equal(t, model.EventOutboxStatusFailed, failed.Status,
			"exhausting the budget produces failed; dead_lettered is claimed only once the event reaches its .dlt sibling")
		assert.Equal(t, 1, failed.Attempts)
		assert.Empty(t, failed.DLTTopic, "no dead-letter topic has been recorded yet")
		assert.Nil(t, failed.FailureMetadata)

		// A failed row is out of the claimable set even though it is not yet
		// dead-lettered, so it cannot be retried past its budget.
		claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
		require.NoError(t, err)
		assert.False(t, containsEventID(claimed, entry.EventID))

		// It is nevertheless visible to an operator, which is why the listing covers
		// both terminal failure literals.
		listed, err := ds.ListDeadLetteredEvents(ctx, model.DeadLetterQuery{Limit: maxDeadLetterPageSize})
		require.NoError(t, err)
		assert.True(t, containsEventID(listed, entry.EventID),
			"an event stranded in failed must still appear in the dead-letter inventory; it is the one an operator most needs to see")

		metadata, err := json.Marshal(model.FailureMetadata{
			OriginalTopic:    entry.Topic,
			ErrorReason:      "final failure",
			AttemptCount:     1,
			FirstAttemptedAt: *failed.FirstAttemptedAt,
			LastAttemptedAt:  *failed.LastAttemptedAt,
		})
		require.NoError(t, err)

		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt", metadata, model.BrokerRecord{}))

		deadLettered, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusDeadLettered, deadLettered.Status)
		assert.Equal(t, entry.Topic+".dlt", deadLettered.DLTTopic,
			"the resolved .dlt topic must be stored: a replay sends the event back to its ORIGINAL topic, which the metadata carries")
		require.NotNil(t, deadLettered.FailureMetadata)
		assert.JSONEq(t, string(metadata), string(deadLettered.FailureMetadata),
			"the failure metadata must round-trip: it is what the dead-letter inventory displays and what a replay reads its origin topic from")
		assert.Empty(t, deadLettered.ClaimToken, "a terminal state releases the claim")

		assert.Error(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt", metadata, model.BrokerRecord{}),
			"a second dead-letter of the same row must be refused, or two workers each put a copy on the .dlt topic")
	})

	t.Run("a replay claims the row atomically and can be rolled back", func(t *testing.T) {
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 1
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "gave up", 0, false, testDeadLetterHandoffLease)
		require.NoError(t, err)
		require.True(t, outcome.Exhausted)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, outcome.ClaimToken,
			entry.Topic+".dlt", json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":1}`), model.BrokerRecord{}))

		claimed, err := ds.ClaimEventForReplay(ctx, entry.EventID, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, model.EventOutboxStatusReplaying, claimed.Status)
		require.NotEmpty(t, claimed.ClaimToken, "the replay claim must issue a token")

		// THE STATE GUARD: a row already replaying is not claimable, so a second request
		// arriving after the first has landed finds nothing to claim and cannot publish a
		// duplicate.
		//
		// This is a SERIAL call and it proves a serial property. It says nothing about two
		// requests that arrive together — both could read `dead_lettered` and both transition if
		// the claim were a read followed by a write instead of one conditional UPDATE, and this
		// assertion would hold throughout. That race is proved in
		// TestClaimEventForReplay_ExactlyOneOfTwoSimultaneousClaimsWins_RealDB, which is where
		// the word "concurrent" belongs.
		_, err = ds.ClaimEventForReplay(ctx, entry.EventID, time.Minute)
		requireAPIError(t, err, apierror.ErrConflict)

		// A replaying row is also out of the relay's reach, so the relay cannot publish
		// it underneath the replay.
		relayBatch, err := ds.ClaimPendingEventOutbox(ctx, 100, time.Minute)
		require.NoError(t, err)
		assert.False(t, containsEventID(relayBatch, entry.EventID),
			"a replaying row must be outside the relay's claimable set")

		require.NoError(t, ds.ReleaseEventReplay(ctx, claimed.ID, claimed.ClaimToken, "broker refused"))

		released, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusDeadLettered, released.Status,
			"a failed replay must return the row to dead_lettered, or it loses its replayability")
		assert.Equal(t, "broker refused", released.LastError)
		assert.Empty(t, released.ClaimToken)

		// And it is replayable again, which is the property the rollback exists for.
		reclaimed, err := ds.ClaimEventForReplay(ctx, entry.EventID, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, reclaimed)
		require.NoError(t, ds.MarkEventDispatched(ctx, reclaimed.ID, reclaimed.ClaimToken, model.BrokerRecord{}))

		done, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.Equal(t, model.EventOutboxStatusDispatched, done.Status,
			"a successful replay reaches the same terminal success state an ordinary publish does")
		assert.Equal(t, entry.Topic+".dlt", done.DLTTopic,
			"the dead-letter history must be retained so the record of what went wrong is not erased")
	})

	t.Run("the dual-delivery flag is set independently of the relay state machine", func(t *testing.T) {
		entry := newEventOutboxFixture(marker)
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		require.NoError(t, ds.MarkWebhookDispatched(ctx, entry.ID, token))

		got, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		assert.True(t, got.WebhookDispatched, "the legacy webhook leg must be recorded")
		assert.Equal(t, model.EventOutboxStatusProcessing, got.Status,
			"the dual-delivery marker must not disturb the relay state machine: the row stays as the claim left it")

		require.NoError(t, ds.MarkWebhookDispatched(ctx, entry.ID, token),
			"an honest re-call from the claim holder must be idempotent, not reported as a lost claim")
		assert.Error(t, ds.MarkWebhookDispatched(ctx, entry.ID, "someone-elses-token"),
			"a worker that does not hold the claim must not be able to record a webhook it never sent")
	})
}

// TestClaimEventForReplay_ExactlyOneOfTwoSimultaneousClaimsWins_RealDB is the replay claim's
// concurrency property, and it is the only test that establishes it.
//
// # What the serial assertion could not reach
//
// The replay lifecycle test calls the claim twice in sequence and asserts the second one
// conflicts. That is a real property — a row already replaying is not claimable — but it is a
// property about a row whose transition has ALREADY LANDED, and it holds under an
// implementation with no atomicity whatsoever. A claim written as "read the row, check its
// status in Go, then write" would pass it every time while allowing two simultaneous requests
// to both read `dead_lettered`, both decide they may proceed, and both transition. Two replay
// requests for one stuck event is not a hypothetical: it is a double-click on a triage console,
// or two operators working the same dead-letter backlog, and the consequence is the same event
// published twice onto its original topic.
//
// # The arrangement
//
// Two goroutines are lined up on a barrier and released together, both claiming the SAME
// dead-lettered row. Exactly one must succeed with a token and exactly one must be told the row
// is not available — and the row itself must end up `replaying` under the winner's token, so
// that "one winner" is a fact about the database and not merely about the two return values.
//
// It runs several rounds on fresh rows. One round is logically sufficient — the claim is a
// single conditional UPDATE, so PostgreSQL's row lock decides it — but a single round can be
// won by the goroutines simply not overlapping, and rounds are what make the overlap real
// rather than assumed.
func TestClaimEventForReplay_ExactlyOneOfTwoSimultaneousClaimsWins_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("replayrace")
	quiesceEventOutbox(t, ds, marker)

	// A lease long enough that the expired-replay arm of the claim cannot open: a claim also
	// succeeds on a `replaying` row whose lease has run out, and a short lease here would let
	// the loser win legitimately for a reason that has nothing to do with the race.
	const replayLease = time.Minute
	const rounds = 8

	// deadLetter drives one fresh fixture all the way to dead_lettered, which is the only state
	// a replay may be claimed from.
	deadLetter := func() *model.EventOutbox {
		entry := newEventOutboxFixture(marker)
		entry.MaxAttempts = 1
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "gave up", 0, false,
			testDeadLetterHandoffLease)
		require.NoError(t, err)
		require.True(t, outcome.Exhausted)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, outcome.ClaimToken,
			entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":1}`),
			model.BrokerRecord{}))

		return entry
	}

	for round := range rounds {
		entry := deadLetter()

		type attempt struct {
			row *model.EventOutbox
			err error
		}
		attempts := make([]attempt, 2)

		// A CLOSED CHANNEL rather than a WaitGroup for the release, so both goroutines are
		// unblocked by one operation instead of being woken in sequence. `ready` proves each
		// goroutine reached the barrier before it is opened, which is what makes the overlap a
		// fact rather than a hope.
		release := make(chan struct{})
		ready := make(chan struct{}, len(attempts))

		var finished sync.WaitGroup
		for i := range attempts {
			finished.Add(1)
			go func(i int) {
				defer finished.Done()

				ready <- struct{}{}
				<-release

				attempts[i].row, attempts[i].err = ds.ClaimEventForReplay(ctx, entry.EventID, replayLease)
			}(i)
		}

		for range attempts {
			<-ready
		}
		close(release)
		finished.Wait()

		winners := make([]*model.EventOutbox, 0, len(attempts))
		var conflicts int
		for i, result := range attempts {
			if result.err == nil {
				require.NotNilf(t, result.row, "attempt %d returned neither a row nor an error", i)
				winners = append(winners, result.row)

				continue
			}

			requireAPIError(t, result.err, apierror.ErrConflict)
			conflicts++
			assert.Nilf(t, result.row,
				"attempt %d reported a conflict and must return no row with it: a caller that "+
					"reads the row anyway would publish the event the winner is publishing", i)
		}

		require.Lenf(t, winners, 1,
			"round %d: EXACTLY ONE of two simultaneous replay claims may succeed; %d did. Two "+
				"winners means the claim is a read followed by a write rather than one conditional "+
				"UPDATE, and the same dead-lettered event is republished twice onto its original "+
				"topic", round, len(winners))
		require.Equalf(t, 1, conflicts,
			"round %d: the loser must be told the row is unavailable rather than receiving a "+
				"database error it cannot interpret", round)

		winner := winners[0]
		require.NotEmpty(t, winner.ClaimToken, "the winning claim must issue a token")
		assert.Equal(t, model.EventOutboxStatusReplaying, winner.Status)

		// THE DATABASE IS THE ARBITER. Two return values agreeing is not the property; the row
		// holding exactly one claim is.
		stored, err := ds.GetEventByID(ctx, entry.EventID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equalf(t, model.EventOutboxStatusReplaying, stored.Status,
			"round %d: the row must be replaying once a claim has been granted", round)
		assert.Equalf(t, winner.ClaimToken, stored.ClaimToken,
			"round %d: the stored token must be the WINNER's. A loser that overwrote it would fence "+
				"the winner out of its own replay, and the event would be left stuck in replaying "+
				"with nobody able to finish or release it", round)

		// AND THE LOSER CANNOT ACT. Its own release must be refused, or a caller that treated
		// the conflict as recoverable could roll back a replay that is under way.
		require.Error(t, ds.ReleaseEventReplay(ctx, entry.ID, uuid.NewString(), "loser interference"),
			"a token the row does not hold must not be able to release the replay")

		// Leave the row terminal so it cannot interact with a later round's claim assertions.
		require.NoError(t,
			ds.ReleaseEventReplay(ctx, winner.ID, winner.ClaimToken, "round complete"))
	}
}

// TestCountEventOutboxByStatus_ReconcilesAgainstInsertedRows_RealDB is acceptance
// criterion V-2 measured end to end against a real table.
//
// Known counts are driven into each terminal state and the returned map is checked
// against them, so the arithmetic the daily reconciliation runbook performs — dispatched
// plus dead-lettered against the broker's end offsets — is verified against real GROUP BY
// output rather than a scripted result set.
//
// The counts are compared as DELTAS against a baseline taken before the fixtures are
// inserted, because the table is shared and holds other tests' rows. An absolute
// comparison would be wrong even when the code is right.
func TestCountEventOutboxByStatus_ReconcilesAgainstInsertedRows_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("count")
	quiesceEventOutbox(t, ds, marker)

	// TWO windows, read as a pair (PERF-P04). The narrow one covers only the fixtures this
	// test creates at the current instant; the wide one additionally covers the deliberately
	// AGED fixtures below. Comparing the two deltas is what proves the window bounds the
	// dispatched arm and nothing else — and it is deterministic, which a single reading against
	// an absolute count on a shared table could never be.
	narrowWindow := time.Now().UTC().Add(-time.Hour)
	wideWindow := time.Now().UTC().Add(-6 * time.Hour)

	baseline, err := ds.CountEventOutboxByStatus(ctx, narrowWindow)
	require.NoError(t, err)
	require.NotNil(t, baseline)

	wideBaseline, err := ds.CountEventOutboxByStatus(ctx, wideWindow)
	require.NoError(t, err)

	const (
		wantPending         = 3
		wantDispatched      = 2
		wantDeadLettered    = 1
		wantAgedDispatched  = 2
		wantAgedPending     = 1
		agedOccurrenceShift = 3 * time.Hour
	)

	// ORDER MATTERS HERE, and getting it wrong is subtle. Driving a row to a terminal
	// state requires CLAIMING it first, and a claim takes a whole batch — so any row
	// left pending when the dispatched and dead-lettered fixtures are claimed would be
	// swept into processing along with them and would then be counted in the wrong
	// bucket. The terminal fixtures are therefore created first, and the pending ones
	// last, so nothing claims them.
	aged := func() *model.EventOutbox {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = dbTimestamp(time.Now().Add(-agedOccurrenceShift))

		return entry
	}

	for range wantDispatched {
		entry := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, claimEventOutboxToken(t, ds, entry), model.BrokerRecord{}))
	}
	// Dispatched THREE HOURS AGO: inside the wide window, outside the narrow one. These are
	// the rows whose disappearance from the narrow reading is the fix.
	for range wantAgedDispatched {
		entry := insertRealEventOutbox(t, ds, aged())
		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, claimEventOutboxToken(t, ds, entry), model.BrokerRecord{}))
	}
	for range wantDeadLettered {
		entry := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, claimEventOutboxToken(t, ds, entry),
			entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`), model.BrokerRecord{}))
	}
	for range wantPending {
		insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	}
	// A row that has been PENDING since three hours ago — older than the narrow window, and
	// therefore the single most important row in this test. It is the backlog a gauge exists to
	// show, and windowing the whole count rather than only the dispatched arm would have hidden
	// it precisely when it mattered.
	for range wantAgedPending {
		insertRealEventOutbox(t, ds, aged())
	}

	after, err := ds.CountEventOutboxByStatus(ctx, narrowWindow)
	require.NoError(t, err)

	wideAfter, err := ds.CountEventOutboxByStatus(ctx, wideWindow)
	require.NoError(t, err)

	delta := func(status string) int64 { return after[status] - baseline[status] }
	wideDelta := func(status string) int64 { return wideAfter[status] - wideBaseline[status] }

	// EVERY NON-DISPATCHED STATUS IS EXACT AND COMPLETE, whatever the window says. The aged
	// pending row is counted by both readings, which is the property the backlog gauge rests on.
	assert.Equal(t, int64(wantPending+wantAgedPending), delta(model.EventOutboxStatusPending),
		"pending is counted exactly and in full however short the window: a row pending for three "+
			"hours must still appear in the backlog it is the point of")
	assert.Equal(t, delta(model.EventOutboxStatusPending), wideDelta(model.EventOutboxStatusPending),
		"and the window makes no difference to it at all")
	assert.Equal(t, int64(wantDeadLettered), delta(model.EventOutboxStatusDeadLettered))
	assert.Equal(t, delta(model.EventOutboxStatusDeadLettered), wideDelta(model.EventOutboxStatusDeadLettered),
		"the dead-letter inventory is bounded by operation rather than by time, so it is counted in full as well")

	// DISPATCHED IS THE ONE POPULATION THE WINDOW RESTRICTS, and this is the assertion that
	// says so. The narrow reading sees only the recent fixtures; the wide one additionally sees
	// the aged ones.
	assert.Equal(t, int64(wantDispatched), delta(model.EventOutboxStatusDispatched),
		"a dispatched row older than the window must not be counted: that history is the population "+
			"whose unbounded scan PERF-P04 is about")
	assert.Equal(t, int64(wantDispatched+wantAgedDispatched), wideDelta(model.EventOutboxStatusDispatched),
		"and a wider window must see it again, or the arm is not windowed but truncated")

	assert.Equal(t, int64(wantDispatched+wantAgedDispatched+wantDeadLettered),
		wideDelta(model.EventOutboxStatusDispatched)+wideDelta(model.EventOutboxStatusDeadLettered),
		"dispatched + dead_lettered is the quantity the daily zero-loss reconciliation compares against the broker's main-topic and dead-letter-topic end offsets")

	// Every row inserted here is accounted for in exactly one status, which is the
	// zero-loss property itself: no event may be missing from the totals, and none may
	// be double-counted. Measured over the WIDE window, which is the one that covers every
	// fixture — a reconciliation must be run over a window that contains what it reconciles.
	var totalDelta int64
	for status := range wideAfter {
		totalDelta += wideDelta(status)
	}
	assert.Equal(t, int64(wantPending+wantAgedPending+wantDispatched+wantAgedDispatched+wantDeadLettered), totalDelta,
		"every inserted event must appear in exactly one status: a short total is a lost event and an over-count is a double-counted one")
}

// TestClaimPendingEventOutbox_UsesClaimIndex_RealDB asserts the claim is INDEX-DRIVEN.
//
// This is what makes acceptance criterion V-1's 500 events per second attainable. A
// sequential scan over the outbox works perfectly on a small table and degrades
// continuously as the table grows — the relay simply gets slower every day until it
// cannot keep up, with no error and no failing test anywhere. No unit test can reveal
// that; only the query plan can.
//
// The partial predicate on idx_event_outbox_claim is what confines the scan to the
// CLAIMABLE WORKING SET, keeping dispatched and dead-lettered rows — eventually the
// overwhelming majority of the table — out of the index entirely.
//
// A Sort node above the index scan is EXPECTED and is not a failure. Because the leading
// index key is matched against a two-element IN-list, a btree cannot emit rows already
// ordered by the trailing occurred_at, so the planner legitimately sorts. The migration
// documents this and blnk.lineage_outbox has the same shape. It costs nothing that
// matters, because the partial predicate confines that sort's input to the claimable
// working set rather than the whole table. What must NOT appear is a sequential scan.
func TestClaimPendingEventOutbox_UsesClaimIndex_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("plan")
	quiesceEventOutbox(t, ds, marker)

	// The table must be given its PRODUCTION SHAPE before the plan means anything, and
	// getting this wrong is how a plan assertion becomes theatre.
	//
	// On a nearly-empty table PostgreSQL correctly prefers a sequential scan: one page
	// is cheaper to read than any index lookup, so an assertion made there would fail
	// for a reason that is not a defect. Seeding only CLAIMABLE rows does not help
	// either — it makes the partial index cover the whole table, which is precisely the
	// situation in which it saves nothing.
	//
	// The shape that matters is the steady state the index was designed for: a large
	// cold history of terminal rows, which the partial predicate excludes ENTIRELY, and
	// a small claimable working set. That is when the index is dramatically cheaper than
	// a scan, and it is the shape the relay lives in.
	const (
		coldHistory   = 3000 // dispatched rows the partial index must exclude
		claimableRows = 20   // the working set the relay actually polls
	)

	// Seeded in bulk rather than through InsertEventOutbox: this test is about the query
	// PLAN, not the insert path, and three thousand single-row round trips would cost
	// seconds for no additional coverage.
	// partition_key is DISTINCT per row, and deliberately so: the plan under test
	// includes the earlier-same-key exclusion, and seeding one shared key would give the
	// planner a wildly unrepresentative selectivity estimate for that subquery.
	// event_raw is NOT NULL, so the bulk seed must supply it exactly as the repository
	// would: the canonical envelope with the body spliced in. It is composed in SQL here
	// only because these rows never leave the table — the plan is the whole subject — and
	// the alternative is three thousand Go-side round trips.
	_, err := ds.Conn.ExecContext(ctx, `
		INSERT INTO blnk.event_outbox
			(event_id, event_type, aggregate_id, partition_key, ledger_id, topic, payload, payload_raw, event_raw, occurred_at, status, dispatched_at)
		SELECT $1 || 'cold-' || g, 'transaction.applied', $1 || 'agg', $1 || 'key-' || g, $1 || 'ldg', 'blnk.transactions',
		       '{"event":"transaction.applied","data":{}}'::jsonb,
		       convert_to('{"event":"transaction.applied","data":{}}', 'UTF8'),
		       convert_to('{"event_id":"' || $1 || 'cold-' || g ||
		                  '","event_type":"transaction.applied","aggregate_id":"' || $1 ||
		                  'agg","occurred_at":"2026-01-01T00:00:00Z","payload":' ||
		                  '{"event":"transaction.applied","data":{}}' ||
		                  ',"schema_version":1}', 'UTF8'),
		       NOW() - (g || ' seconds')::interval,
		       'dispatched', NOW()
		FROM generate_series(1, $2) g
	`, marker, coldHistory)
	require.NoError(t, err, "failed to seed the cold history")

	_, err = ds.Conn.ExecContext(ctx, `
		INSERT INTO blnk.event_outbox
			(event_id, event_type, aggregate_id, partition_key, ledger_id, topic, payload, payload_raw, event_raw, occurred_at, status)
		SELECT $1 || 'warm-' || g, 'transaction.applied', $1 || 'agg', $1 || 'key-w' || g, $1 || 'ldg', 'blnk.transactions',
		       '{"event":"transaction.applied","data":{}}'::jsonb,
		       convert_to('{"event":"transaction.applied","data":{}}', 'UTF8'),
		       convert_to('{"event_id":"' || $1 || 'warm-' || g ||
		                  '","event_type":"transaction.applied","aggregate_id":"' || $1 ||
		                  'agg","occurred_at":"2026-01-01T00:00:00Z","payload":' ||
		                  '{"event":"transaction.applied","data":{}}' ||
		                  ',"schema_version":1}', 'UTF8'),
		       NOW() - (g || ' seconds')::interval,
		       'pending'
		FROM generate_series(1, $2) g
	`, marker, claimableRows)
	require.NoError(t, err, "failed to seed the claimable working set")

	// These rows exist ONLY to shape a query plan, so they are deleted rather than
	// retired to a terminal state the way real fixtures are. Leaving three thousand rows
	// behind on every run would grow the shared test database without bound and slow
	// every other test in this package down over time.
	//
	// The VACUUM is not tidiness, it is what stops this test breaking the NEXT run of
	// itself. A DELETE only marks tuples dead: three thousand of them stay in the heap
	// and in every index, so relpages and reltuples remain inflated while the live row
	// count collapses. The planner then costs the table as large and sparse, which is
	// precisely the input that flips the anti-join below from a per-candidate index probe
	// to a whole-set hash build — the plan this test forbids. Reclaiming the space and
	// refreshing the statistics leaves the table as this test found it, in the only sense
	// the planner cares about.
	// Both steps FAIL the test rather than logging, and the second still runs when the first
	// errored — the two obligations are independent and the VACUUM is the more important of the
	// pair. Per the reasoning above, a leak here does not merely grow the table: it inflates
	// relpages until the planner flips the anti-join to the whole-set hash build this very test
	// forbids, so an unreported cleanup failure makes the NEXT run of this test fail for a
	// reason that has nothing to do with the code under test.
	t.Cleanup(func() {
		if _, cleanupErr := ds.Conn.Exec(
			"DELETE FROM blnk.event_outbox WHERE event_id LIKE $1", marker+"%"); cleanupErr != nil {
			t.Errorf("failed to remove plan-shaping rows for marker %q: %v", marker, cleanupErr)
		}
		if _, cleanupErr := ds.Conn.Exec("VACUUM (ANALYZE) blnk.event_outbox"); cleanupErr != nil {
			t.Errorf("failed to reclaim the space the plan-shaping rows occupied: %v", cleanupErr)
		}

		var remaining int
		if err := ds.Conn.QueryRow(
			"SELECT count(*) FROM blnk.event_outbox WHERE event_id LIKE $1", marker+"%",
		).Scan(&remaining); err != nil {
			t.Errorf("could not confirm the plan-shaping rows for marker %q were removed: %v",
				marker, err)

			return
		}

		assert.Zerof(t, remaining,
			"marker %q left %d plan-shaping rows behind. They inflate relpages for every later "+
				"test in this package and, on the next run of this one, flip the plan this test "+
				"asserts on.", marker, remaining)
	})

	// VACUUM (ANALYZE) rather than ANALYZE alone, and the difference is what makes this
	// assertion deterministic rather than dependent on which tests ran before it.
	//
	// ANALYZE refreshes the statistics but reclaims nothing, so the dead tuples every
	// preceding test in this package left behind — including this test's own three
	// thousand from the previous run — still count towards relpages. A table whose pages
	// are mostly dead is costed as a large table with poor locality, and under that
	// costing the planner legitimately prefers to hash the whole blocking-state set once
	// instead of probing the partition-key index per candidate. Both plans are correct;
	// only one is the plan this test is here to require. Vacuuming first removes the
	// variable entirely, so the plan is a function of the seeded shape alone.
	//
	// It cannot run inside a transaction, which is why it is issued on the connection
	// directly. It is safe on a shared test database: it removes only tuples no
	// transaction can still see.
	_, err = ds.Conn.ExecContext(ctx, "VACUUM (ANALYZE) blnk.event_outbox")
	require.NoError(t, err,
		"the planner needs current statistics AND a compacted heap for this assertion to mean anything")

	// THE PREMISE HAS TO HOLD BEFORE THE PLAN MEANS ANYTHING, and on a SHARED test
	// database it sometimes does not.
	//
	// quiesceEventOutbox parks unrelated claimable rows behind a future locked_until so
	// they cannot be claimed — but locked_until is NOT part of either partial index's
	// predicate, so those rows are still in the index and still in the statistics the
	// planner reads. A concurrent run, or a package whose tests left rows behind, can
	// therefore leave a claimable population that dwarfs the small working set seeded
	// above, and at that point the planner is costing a data shape this test did not
	// create and does not describe: with several hundred claimable rows a sequential scan
	// genuinely is cheaper, and PostgreSQL is right to choose one.
	//
	// Failing there would report a defect that is not one. Skipping says plainly that the
	// precondition could not be established, which is what the brittleness of a plan
	// assertion actually requires — and it cannot make the test pass vacuously, because a
	// skip is not a pass.
	var unrelatedClaimable int
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_outbox
		WHERE status IN ('pending', 'processing')
		  AND event_id NOT LIKE $1
	`, marker+"%").Scan(&unrelatedClaimable), "counting unrelated claimable rows must succeed")

	if unrelatedClaimable > claimableRows {
		t.Skipf(
			"the shared outbox holds %d unrelated claimable rows against a seeded working set of %d, "+
				"so the plan would be costed for a data shape this test did not create; "+
				"the claim-index assertion needs a small claimable working set to be meaningful",
			unrelatedClaimable, claimableRows)
	}

	// THE OTHER PREMISE: the table must carry THIS BUILD'S INDEXES AND NO OTHERS.
	//
	// A plan assertion is a statement about the choice PostgreSQL makes among the indexes
	// available to it, so it is only a statement about this build if the available set is this
	// build's. On a shared development database it need not be: a database that has had another
	// build's migrations applied to it keeps that build's indexes, and the planner will use one
	// if it costs less.
	//
	// That is not hypothetical and it is not a defect in either build. An index declared as
	// `(status) WHERE status <> 'dispatched'` is narrower than this build's five-column claim
	// index and cheaper to scan, so the planner picks it — and the assertion below then fails
	// on a plan this build's schema cannot produce. Worse, accepting such an index to make the
	// assertion pass would make it vacuous: its predicate spans the terminal states, so the
	// cold history IS in it and the poll cost does grow with the table, which is the exact
	// property this test exists to forbid.
	//
	// Skipping is therefore the only honest answer, and it is safe in both directions: it
	// cannot mask a real defect, because with only this build's indexes present the assertion
	// still runs, and it does not touch the foreign index, which belongs to a build that is
	// entitled to it.
	if foreign := foreignEventOutboxIndexes(t, ctx, ds); len(foreign) > 0 {
		t.Skipf(
			"blnk.event_outbox carries %d index(es) no migration in this build creates (%s), so the "+
				"planner is choosing among a set this build's schema does not define — most likely "+
				"another build's migrations have been applied to the same database. The plan "+
				"assertion below describes THIS build's indexes, and a foreign index that is cheaper "+
				"to scan makes it unanswerable. Run this against a database migrated by this build "+
				"alone.",
			len(foreign), strings.Join(foreign, ", "))
	}

	// EXPLAIN without ANALYZE PLANS the statement without executing it, so the claim
	// takes no leases and mutates nothing — which matters here, because executing this
	// particular statement would move rows into processing as a side effect.
	rows, err := ds.Conn.QueryContext(ctx, "EXPLAIN "+claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, (5 * time.Minute).String(), uuid.NewString(), 100)
	require.NoError(t, err, "EXPLAIN of the claim query must succeed")
	defer func() { _ = rows.Close() }()

	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())

	planText := plan.String()
	require.NotEmpty(t, planText, "EXPLAIN returned no plan")

	// THE CANDIDATE SIDE MUST REACH THE TABLE THROUGH A BLOCKING-STATE PARTIAL INDEX, which
	// is the guarantee, rather than through one named index, which is a plan detail.
	//
	// This assertion used to pin "idx_event_outbox_claim" by name, then a list of two names,
	// and it failed on plans that honour the guarantee completely — a Bitmap Index Scan on
	// idx_event_outbox_effective_key_inflight, and an Index Scan on
	// idx_event_outbox_status_open. THREE indexes on this table satisfy the guarantee for the
	// shape seeded above, and which one the planner picks is a cost decision that moves with
	// heap compaction, statistics freshness and host load. Every widening of the name list
	// was a guess at the next plan, so the list is gone: the PROPERTY is read out of the
	// catalog instead, which accepts any index that honours it and rejects any index that
	// does not, including one added later.
	//
	// Asserted on the ONE plan line that reaches the candidate relation, for the same reason
	// the earlier-same-key assertion below is: "the plan mentions an index somewhere" is
	// nearly vacuous, whereas "the line reaching `event_outbox candidate` is an index scan
	// through a blocking-state partial index" says exactly what it means. The
	// sequential-scan prohibition further down closes the same failure mode across every
	// other node in the plan.
	// The ACCESS PATH rather than the single relation line, because a bitmap plan splits one
	// access across two nodes and the index is named on the lower of them.
	//
	// This is the same lesson as the paragraph above, one step further in. Pinning the plan
	// SHAPE is as brittle as pinning an index NAME: on a compacted heap PostgreSQL reaches
	// the candidate rows with `Index Scan using idx_event_outbox_claim on event_outbox
	// candidate`, and on other statistics it reaches exactly the same rows through the same
	// index as `Bitmap Heap Scan on event_outbox candidate` with a child `Bitmap Index Scan
	// on idx_event_outbox_claim`. The second honours the guarantee completely — the partial
	// predicate still confines the read to the blocking states, so the cold history is never
	// touched and the poll's cost is still independent of how large the outbox has grown —
	// yet the relation line alone carries neither the word "Index" nor the index name, so an
	// assertion made on that one line reported a defect that did not exist.
	//
	// planAccessPathFor therefore returns the relation node together with the index nodes
	// beneath it, and the guarantee is asserted over that. What is still refused is what was
	// always meant to be refused: reaching the candidate rows by a sequential scan, or
	// through an index that is not partial on the blocking states.
	candidateScan := planAccessPathFor(planText, "event_outbox candidate")
	require.NotEmpty(t, candidateScan,
		"the plan must reach the candidate relation explicitly.\nPlan was:\n"+planText)
	assert.Contains(t, candidateScan, "Index",
		"the claim's candidate selection must be index-driven. Read through a scan instead, the relay's per-poll cost grows with the whole table until it cannot keep up with 500 events/sec, and no unit test would ever reveal it.\nAccess path was: "+candidateScan)
	// READ OUT OF THE CATALOG AND EVALUATED BY POSTGRESQL, which is the fourth and last
	// generation of this assertion. The first pinned one index name; the second pinned a list of
	// two; the third read the predicate out of pg_indexes and PATTERN-MATCHED it, recognising
	// four shapes and treating anything else as admitting the history — so a correct predicate
	// written a fifth way failed, and an `ANY (ARRAY[...])` it could not parse passed. Every
	// equivalent rewrite of "not dispatched" — a negation, an ANY over the complement, a NOT IN —
	// needs its own special case, and each case is a chance to accept a predicate that admits the
	// cold history.
	//
	// So the predicate is now spliced into a COUNT over the rows this test seeded and evaluated
	// by the same expression evaluator that decides what the index actually contains. The
	// question asked is the one that matters — can a dispatched row be in this index? — and the
	// count is scoped by the marker, so a concurrent run's rows cannot answer it.
	requireColdHistoryExcludingIndex(t, ds, candidateScan, planText,
		"the claim's candidate selection", marker)

	// The earlier-same-key exclusion must be INDEX-DRIVEN and confined to the blocking
	// states. What it must never be is a scan of each key's entire history, which would buy
	// the ordering guarantee at a price that grows with the table — correct, and
	// progressively unable to keep up.
	//
	// That property is asserted rather than the name of one specific index, because
	// PostgreSQL has TWO correct ways to satisfy it and picks between them on cost:
	//
	//   - a NESTED LOOP anti-join that probes idx_event_outbox_effective_key_inflight once
	//     per candidate row, which wins when there are many candidates; and
	//   - a HASH anti-join whose build side is read from idx_event_outbox_claim in ONE index
	//     scan, which wins when the claimable working set is small — the steady state the
	//     relay actually lives in, and what it chooses on a compacted heap.
	//
	// BOTH indexes are partial on exactly the blocking states, so under either plan the
	// cold history of terminal rows is never read — which is the guarantee the migration's
	// index exists to provide and the only thing that keeps the poll's cost independent of
	// how large the outbox has grown. Neither plan degrades as the table grows. Pinning one
	// index name would therefore fail on a plan that honours the guarantee completely,
	// which is how a plan assertion stops testing the system and starts testing the
	// planner's mood — and is what made this assertion flaky.
	//
	// What is NOT negotiable is asserted instead: the exclusion is present, it is keyed on
	// partition_key, and it reaches the table through one of the two partial indexes. The
	// sequential-scan prohibition that follows closes the same failure mode over every node
	// in the plan.
	assert.Contains(t, planText, "Anti Join",
		"the earlier-same-key exclusion must survive as an anti-join. Without the NOT EXISTS predicate a relay can skip an earlier locked row and claim a LATER row with the same partition key, and because Kafka preserves append order rather than occurred_at a subscriber then sees one aggregate's events out of order — with no error, no log line and no row state to show it.\nPlan was:\n"+planText)

	assert.Contains(t, planText, "partition_key",
		"the anti-join must be keyed on partition_key, which is what makes it per-aggregate.\nPlan was:\n"+planText)

	// Asserted on the ONE plan line that reaches the earlier-same-key relation rather than
	// on the whole plan text: "the plan contains an index scan somewhere" is nearly vacuous
	// — every plan here contains several — whereas "the line reaching `event_outbox earlier`
	// is an index scan through a blocking-state partial index" says exactly what it means,
	// and it says it for either of the two plan shapes above.
	earlierScan := planAccessPathFor(planText, "event_outbox earlier")
	require.NotEmpty(t, earlierScan,
		"the plan must reach the earlier-same-key relation explicitly.\nPlan was:\n"+planText)
	assert.Contains(t, earlierScan, "Index",
		"the earlier-same-key exclusion must be index-driven. Read through a scan instead, the ordering guarantee is bought at the price of reading the table's entire history on every poll — correct, and progressively unable to keep up.\nPlan line was: "+earlierScan)
	requireColdHistoryExcludingIndex(t, ds, earlierScan, planText,
		"the earlier-same-key exclusion", marker)

	assert.NotContains(t, planText, "Seq Scan on event_outbox",
		"the claim must not sequentially scan blnk.event_outbox.\nPlan was:\n"+planText)

	// A Sort above the index scan is EXPECTED and is deliberately not asserted against.
	// Because the leading index key is matched against a two-element IN-list, a btree
	// cannot emit rows already ordered by the trailing occurred_at, so the planner
	// legitimately sorts. blnk.lineage_outbox has the same index and query shape and the
	// same plan. It costs nothing that matters, because the partial predicate confines
	// that sort's input to the claimable working set rather than to the whole table.
}

// indexNameFromPlanLine extracts the index name from an EXPLAIN scan line.
//
// Both spellings the planner emits are handled — "Index Scan using <name> on <rel>" and
// "Bitmap Index Scan on <name>" — because which one appears is another cost decision, and a
// helper that understood only one would reintroduce the flakiness this replaces.
//
// Parameters:
//   - planLine string: one EXPLAIN line.
//
// Returns:
//   - string: the index name, or "" when the line reads no index.
func indexNameFromPlanLine(planLine string) string {
	if _, after, found := strings.Cut(planLine, "using "); found {
		name, _, _ := strings.Cut(after, " ")

		return strings.TrimSpace(name)
	}

	if _, after, found := strings.Cut(planLine, "Bitmap Index Scan on "); found {
		name, _, _ := strings.Cut(after, " ")

		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(name), "\n"))
	}

	return ""
}

// planAccessPathFor returns the node of an EXPLAIN plan that mentions needle, together with the
// index nodes beneath it, trimmed of the tree drawing and indentation.
//
// It replaced planLineReferencing, which returned the matched LINE alone:
//
//	Bitmap Heap Scan on event_outbox candidate
//	  ->  Bitmap Index Scan on idx_event_outbox_claim
//
// Both read exactly the same rows through exactly the same index. The second names the index
// on the CHILD node, so an assertion made on the line mentioning the relation sees neither the
// word "Index" nor the index name and fails a plan that honours the guarantee completely.
// That is a plan-shape assertion masquerading as a guarantee assertion, and it is the same
// mistake as pinning one index by name.
//
// Returning the access path keeps the guarantee assertable — the caller can still require that
// the read is index-driven, and through a blocking-state partial index — while accepting
// either shape. It does not weaken the sequential-scan prohibition: a `Seq Scan` node has no
// index child, so its access path carries no index name and no "Index", and the whole-plan
// `Seq Scan on event_outbox` assertion is unaffected either way. This is why there is no
// helper returning the single plan LINE that mentions a relation: on a bitmap plan that line is
// `Bitmap Heap Scan on event_outbox candidate` and the index is nowhere in it.
//
// # How the children are found
//
// EXPLAIN indents each child deeper than its parent, so the descendants of the matched node
// are the following lines whose indentation is strictly greater, up to the first line that is
// not. Only the index-naming ones are collected; the Recheck Cond and Filter lines belong to
// the same node and are already carried by it.
//
// Parameters:
//   - planText string: the whole EXPLAIN output.
//   - needle string: the relation reference identifying the node.
//
// Returns:
//   - string: the matching node, plus its index children, each trimmed and joined by " | ".
//     Empty when no line mentions needle.
func planAccessPathFor(planText, needle string) string {
	lines := strings.Split(planText, "\n")

	for i, line := range lines {
		if !strings.Contains(line, needle) {
			continue
		}

		depth := planNodeIndent(line)
		path := []string{strings.TrimSpace(line)}

		for _, child := range lines[i+1:] {
			if strings.TrimSpace(child) == "" {
				continue
			}
			if planNodeIndent(child) <= depth {
				break
			}
			if strings.Contains(child, "Index Scan") || strings.Contains(child, "Index Only Scan") {
				path = append(path, strings.TrimSpace(child))
			}
		}

		return strings.Join(path, " | ")
	}

	return ""
}

// planNodeIndent is the indentation depth of one EXPLAIN line, which is what expresses the
// parent-child structure of the plan tree in text form.
//
// The "->  " arrow is counted as part of the indentation rather than as content, because a
// child node's arrow sits where its parent's content begins: without folding it in, a direct
// child would measure the same depth as its parent and the descendant walk would stop
// immediately.
//
// Parameters:
//   - line string: one raw line of EXPLAIN output.
//
// Returns:
//   - int: the number of leading characters before the node's own text.
func planNodeIndent(line string) int {
	indent := 0
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}

	if strings.HasPrefix(line[indent:], "->") {
		indent += len("->")
		for indent < len(line) && line[indent] == ' ' {
			indent++
		}
	}

	return indent
}

// TestPlanAccessPathFor_CarriesTheIndexNodesBeneathTheRelation covers the parser directly,
// because the assertion that depends on it lives in a _RealDB test that skips when the shared
// outbox is too busy for a plan to be meaningful — and because the shape it exists to handle
// is chosen by the planner, so a run may not produce one at all.
//
// The bitmap case is the one that was failing in the field: the index is named on the child,
// so the guarantee is only assertable if the child comes back with the parent.
func TestPlanAccessPathFor_CarriesTheIndexNodesBeneathTheRelation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		plan          string
		wantContains  []string
		wantOmits     []string
		wantEmptyPath bool
	}{
		{
			name: "a plain index scan names the index on the relation line",
			plan: "Sort  (cost=1..2 rows=7 width=541)\n" +
				"  ->  Index Scan using idx_event_outbox_claim on event_outbox candidate  (cost=0.28..8.30 rows=7 width=28)\n" +
				"        Index Cond: (status = ANY ('{pending,processing}'::text[]))\n",
			wantContains: []string{"Index Scan using idx_event_outbox_claim", "event_outbox candidate"},
		},
		{
			name: "a bitmap plan carries the child index node up to the relation",
			plan: "Nested Loop Anti Join  (cost=41.26..176.47 rows=7 width=28)\n" +
				"  ->  Bitmap Heap Scan on event_outbox candidate  (cost=41.00..119.60 rows=7 width=51)\n" +
				"        Recheck Cond: (status = ANY ('{pending,processing}'::text[]))\n" +
				"        Filter: (attempts < max_attempts)\n" +
				"        ->  Bitmap Index Scan on idx_event_outbox_claim  (cost=0.00..41.00 rows=20 width=0)\n" +
				"              Index Cond: (status = ANY ('{pending,processing}'::text[]))\n" +
				"  ->  Index Scan using idx_event_outbox_effective_key_inflight on event_outbox earlier\n",
			wantContains: []string{
				"Bitmap Heap Scan on event_outbox candidate",
				"Bitmap Index Scan on idx_event_outbox_claim",
			},
			// The SIBLING node's index must not be absorbed: it is a different access path,
			// and folding it in would let one relation's index satisfy another's assertion.
			wantOmits: []string{"idx_event_outbox_effective_key_inflight"},
		},
		{
			name: "a sequential scan yields no index, so the guarantee still fails on it",
			plan: "Limit  (cost=0..1 rows=7 width=28)\n" +
				"  ->  Seq Scan on event_outbox candidate  (cost=0.00..900.00 rows=7 width=51)\n" +
				"        Filter: (status = ANY ('{pending,processing}'::text[]))\n",
			wantContains: []string{"Seq Scan on event_outbox candidate"},
			wantOmits:    []string{"Index"},
		},
		{
			name:          "a relation the plan never reaches yields nothing",
			plan:          "Seq Scan on lineage_outbox  (cost=0.00..1.00 rows=1 width=1)\n",
			wantEmptyPath: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			path := planAccessPathFor(test.plan, "event_outbox candidate")

			if test.wantEmptyPath {
				assert.Empty(t, path)

				return
			}

			require.NotEmpty(t, path)
			for _, want := range test.wantContains {
				assert.Contains(t, path, want)
			}
			for _, omit := range test.wantOmits {
				assert.NotContains(t, path, omit)
			}
		})
	}
}

// TestPlanIndexesUsed_NamesEveryIndexAnExplainPlanReads covers the plan parser directly.
//
// It exists because the assertion that depends on it lives in a _RealDB test that SKIPS
// when the shared outbox holds too many unrelated claimable rows for a plan to be
// meaningful. A parser that silently returned nothing would make that assertion vacuous
// on the runs where it does execute — require.NotEmpty is what stops that, and this test
// is what proves the parser feeds it real names rather than luck.
func TestPlanIndexesUsed_NamesEveryIndexAnExplainPlanReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		plan string
		want []string
	}{
		{
			name: "bitmap index scan names the index after on",
			plan: "Bitmap Heap Scan on event_outbox candidate\n" +
				"  ->  Bitmap Index Scan on idx_event_outbox_effective_key_inflight  (cost=0.00..40.24 rows=20 width=0)\n",
			want: []string{"idx_event_outbox_effective_key_inflight"},
		},
		{
			name: "index scan names the index after using and stops before on",
			plan: "Index Scan using idx_event_outbox_claim on event_outbox  (cost=0.28..7.63 rows=1 width=22)\n",
			want: []string{"idx_event_outbox_claim"},
		},
		{
			name: "every index in a multi node plan is reported in plan order",
			plan: "Sort  (cost=204.17..204.19 rows=6 width=458)\n" +
				"  ->  Bitmap Index Scan on idx_event_outbox_claim\n" +
				"  ->  Index Scan using idx_event_outbox_effective_key_inflight on event_outbox earlier\n" +
				"  ->  Index Scan using event_outbox_pkey on event_outbox\n",
			want: []string{"idx_event_outbox_claim", "idx_event_outbox_effective_key_inflight", "event_outbox_pkey"},
		},
		{
			name: "index only scan and backward scan are recognised",
			plan: "Index Only Scan using idx_event_outbox_pending on event_outbox\n" +
				"Index Scan Backward using idx_event_outbox_aggregate on event_outbox\n",
			want: []string{"idx_event_outbox_pending", "idx_event_outbox_aggregate"},
		},
		{
			// The case the assertion turns on: a plan that reads no index at all must
			// report none, so require.NotEmpty fails rather than passing on an empty set.
			name: "a sequential scan plan reports no index",
			plan: "Seq Scan on event_outbox candidate  (cost=0.00..105.50 rows=6 width=53)\n" +
				"  Filter: (status = ANY ('{pending,processing}'::text[]))\n",
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, planIndexesUsed(test.plan))
		})
	}
}

// planIndexNodePrefixes are the EXPLAIN node prefixes that name the index they read,
// each ending immediately before the index name.
//
// Declared once rather than inline so a new access method is added in one place, and
// ordered longest-first so "Bitmap Index Scan on " is matched before a prefix that is a
// suffix of it could shadow it.
var planIndexNodePrefixes = []string{
	"Bitmap Index Scan on ",
	"Index Only Scan using ",
	"Index Scan Backward using ",
	"Index Scan using ",
}

// planIndexesUsed returns the names of every index an EXPLAIN plan reads, in the order the
// plan names them and without de-duplication.
//
// It exists so a plan assertion can be made about WHICH indexes serve a query without
// pinning the plan's shape. PostgreSQL chooses freely between correct plans on cost, and on
// a shared test database no single test controls the statistics that decide it — so an
// assertion naming one expected index fails on plans that honour the guarantee completely.
// The set of indexes touched is the durable property: it says whether the query reached the
// table through something that excludes the cold history, whatever shape the plan took.
//
// Parameters:
//   - planText string: the EXPLAIN output, newline separated.
//
// Returns:
//   - []string: the index names read, or nil when the plan reads none.
func planIndexesUsed(planText string) []string {
	var indexes []string

	for _, line := range strings.Split(planText, "\n") {
		for _, prefix := range planIndexNodePrefixes {
			position := strings.Index(line, prefix)
			if position < 0 {
				continue
			}

			// The index name runs to the next space or to the end of the line: EXPLAIN
			// writes "Index Scan using <index> on <relation>" and "Bitmap Index Scan on
			// <index>", so the name is the first field after the prefix in both.
			name := strings.TrimSpace(line[position+len(prefix):])
			if end := strings.IndexByte(name, ' '); end >= 0 {
				name = name[:end]
			}

			if name != "" {
				indexes = append(indexes, name)
			}

			break
		}
	}

	return indexes
}

// eventOutboxClaimQueries are the three statements that lease rows, named so the
// invariant below covers all of them rather than whichever one a reader remembers.
//
// Q-03: a fourth entry, "owed dead-letter claim", was removed with the orphan query it
// named. Recovering a row whose dead-letter write is still owed has exactly ONE claim —
// claimFailedEventOutboxForDeadLetterQuery, which the relay drives every tick — and the
// orphan was a divergent second predicate for the same job that nothing called.
var eventOutboxClaimQueries = map[string]string{
	"pending claim":            claimPendingEventOutboxQuery,
	"failed dead-letter claim": claimFailedEventOutboxForDeadLetterQuery,
	"webhook delivery claim":   claimPendingWebhookDeliveriesQuery,
}

// TestEventOutboxClaims_KeepTheBatchBoundBindingWithAMaterializedCandidateSet is a
// STRUCTURAL guard on a defect whose behavioural symptom is plan-dependent.
//
// Every one of these statements leases rows and every one bounds the lease with a LIMIT.
// Written as `WHERE id IN (SELECT … LIMIT $n FOR UPDATE SKIP LOCKED)`, the planner may
// implement the semi-join as a nested loop that re-executes the subquery once per outer
// row — and because each execution skips the rows the previous one locked, each returns a
// different row and the claim leases far more than it asked for. Measured on a six-row
// backlog with a batch size of two, the pending claim leased all six.
//
// The behavioural assertion for that lives in the batch-bound test below, and it is the
// assertion that caught this. But it only catches it when the planner HAPPENS to choose
// the nested-loop shape, which depends on table statistics — the defect shipped
// undetected precisely because on a small table the planner usually chooses a hash
// semi-join and the bound appears to hold. So the guard here is on the STATEMENT rather
// than on one execution of it: a MATERIALIZED CTE is evaluated exactly once, which makes
// the bound a property of the query instead of a property of the plan.
//
// Nothing about this test needs a database, which is the point — it holds on every
// machine and in every run, including the ones where the planner would hide the bug.
func TestEventOutboxClaims_KeepTheBatchBoundBindingWithAMaterializedCandidateSet(t *testing.T) {
	for name, query := range eventOutboxClaimQueries {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, query, "AS MATERIALIZED",
				"the candidate selection must be a MATERIALIZED CTE so it is evaluated exactly "+
					"once; without it the planner may re-execute the LIMIT per outer row and the "+
					"claim leases more rows than the caller asked for")

			require.Contains(t, query, "WHERE id IN (SELECT id FROM candidates)",
				"the UPDATE must select its rows from the materialised candidate set rather than "+
					"from an inline subquery, which is the form that can be re-executed")

			// The precautions the CTE must not have quietly cost: skip-locked concurrency
			// between relay instances, and the LIMIT itself.
			require.Contains(t, query, "FOR UPDATE SKIP LOCKED",
				"several relay instances must still be able to claim disjoint subsets")
			require.Contains(t, query, "LIMIT $",
				"the batch size must still be bound rather than inlined or dropped")

			// And the inline shape must be gone rather than merely joined by a CTE: leaving
			// both would mean the bound holds only for whichever the planner used.
			assert.NotContains(t, query, "WHERE id IN (\n",
				"no inline multi-line IN subquery may remain in a claim")
		})
	}
}

// TestClaimPendingEventOutbox_ClaimsInBatchesBoundedByBatchSize_RealDB asserts the LIMIT
// is honoured against a real table, and that repeated bounded claims DRAIN the backlog in
// occurrence order.
//
// This is the relay's actual access pattern — a bounded batch once per poll interval —
// and it is the combination the FIFO guarantee has to survive: ordering must hold ACROSS
// batches, not merely within one. Sorting only inside a batch would let a later-occurring
// event in batch one overtake an earlier-occurring event stranded in batch two.
func TestClaimPendingEventOutbox_ClaimsInBatchesBoundedByBatchSize_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("batch")
	quiesceEventOutbox(t, ds, marker)

	const total = 6
	base := dbTimestamp(time.Now().Add(-time.Hour))
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = base.Add(time.Duration(i) * time.Second)
		insertRealEventOutbox(t, ds, entry)
		want = append(want, entry.EventID)
	}

	const batchSize = 2
	drained := make([]string, 0, total)
	for len(drained) < total {
		claimed, err := ds.ClaimPendingEventOutbox(ctx, batchSize, 5*time.Minute)
		require.NoError(t, err)

		mine := make([]string, 0, batchSize)
		for _, entry := range claimed {
			if isMarkedEventOutbox(entry, marker) {
				mine = append(mine, entry.EventID)
			}
		}
		require.LessOrEqual(t, len(claimed), batchSize, "the claim must never exceed the requested batch size")
		require.NotEmpty(t, mine, "the backlog must drain rather than stall")

		drained = append(drained, mine...)

		// Retire the batch so the next poll sees the remainder, exactly as the relay
		// does once the broker acknowledges.
		for _, entry := range claimed {
			if isMarkedEventOutbox(entry, marker) {
				require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, entry.ClaimToken, model.BrokerRecord{}))
			}
		}
	}

	assert.Equal(t, want, drained,
		"repeated bounded claims must drain the backlog in occurrence order; ordering has to hold ACROSS batches, not only within one")
}

// TestListDeadLetterInventory_PagesNewestFirst_RealDB asserts the ordering and paging the
// dead-letter API relies on, against real SQL.
//
// Triage starts from the most recent failures, so the listing is newest-first. The paging
// assertion matters just as much as the ordering: with a non-deterministic tie-break a
// row can appear on two pages or on none, and an operator working through a dead-letter
// backlog would silently skip events.
//
// PERF-P08: the walk is by CURSOR. That is not merely a translation of the old offset walk —
// it is a stronger assertion, because a keyset page is stable under concurrent inserts and an
// offset page is not. A shared database with sibling runs inserting rows is exactly the
// condition under which the offset walk would have repeated and skipped rows, and this test
// would have been the one flaking.
func TestListDeadLetterInventory_PagesNewestFirst_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dlt")
	quiesceEventOutbox(t, ds, marker)

	const total = 5
	base := dbTimestamp(time.Now().Add(-time.Hour))
	oldestFirst := make([]string, 0, total)
	for i := range total {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = base.Add(time.Duration(i) * time.Second)
		insertRealEventOutbox(t, ds, entry)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, claimEventOutboxToken(t, ds, entry),
			entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`), model.BrokerRecord{}))
		oldestFirst = append(oldestFirst, entry.EventID)
	}

	// The expected listing order is the reverse of the insertion order, because the
	// fixtures were inserted oldest-occurrence first and the listing is newest-first.
	newestFirst := make([]string, 0, len(oldestFirst))
	for i := len(oldestFirst) - 1; i >= 0; i-- {
		newestFirst = append(newestFirst, oldestFirst[i])
	}

	mineFrom := func(entries []model.DeadLetterInventoryEntry) []string {
		mine := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.AggregateID == marker+"agg" {
				mine = append(mine, entry.EventID)
			}
		}

		return mine
	}

	// THE WHOLE INVENTORY, PAGED BY CURSOR, not one large page. A single page cannot be relied
	// on to hold this run's fixtures: the database is shared, other runs leave dead-lettered rows
	// behind, and the listing is newest-first — so once the table holds more than one page of
	// them these fixtures are not on page one and the ordering assertion below compares against
	// somebody else's rows. Walking to the end is bounded and fails on non-termination, which is
	// the only reading that cannot pass or fail on who else is running.
	all := model.DeadLetterInventoryPage{
		Entries: walkDeadLetterInventory(t, ds, ctx, maxDeadLetterPageSize),
	}
	assert.Equal(t, newestFirst, mineFrom(all.Entries),
		"the dead-letter inventory must be ordered newest occurrence first, because triage starts from the most recent failures")

	// The narrow projection reaches the real driver intact: the size is reported and the body
	// is not read (PERF-P06). Asserted against PostgreSQL because octet_length on a BYTEA
	// column is the thing a mock cannot prove.
	require.NotEmpty(t, all.Entries)
	for _, entry := range all.Entries {
		if entry.AggregateID != marker+"agg" {
			continue
		}
		assert.Positive(t, entry.PayloadBytes,
			"octet_length must report the stored body's size; a zero would mean the projection read nothing")
	}

	// Walk the fixtures a SMALL page at a time and assert every one appears exactly once across
	// the pages. Two per page against five fixtures forces several cursor hand-offs, which is
	// where a non-deterministic tie-break shows up; the same helper walks it, so the termination
	// bound is the same one the whole-inventory read above relies on.
	seen := map[string]int{}
	for _, eventID := range mineFrom(walkDeadLetterInventory(t, ds, ctx, 2)) {
		seen[eventID]++
	}
	for _, eventID := range oldestFirst {
		assert.Equal(t, 1, seen[eventID],
			"event %s appeared %d times across the pages; a non-deterministic tie-break makes an operator skip or repeat events", eventID, seen[eventID])
	}

	// The cursor is STABLE across a concurrent insert, which is what the offset it replaced
	// could not be. A row inserted between two pages shifted every later offset by one, so an
	// offset-paged operator silently skipped a row for every row that arrived while they read.
	first, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{Limit: 2})
	require.NoError(t, err)
	require.True(t, first.HasMore, "five fixtures against a page of two must leave more")
	require.NotNil(t, first.NextCursor)

	intruder := newEventOutboxFixture(marker)
	intruder.OccurredAt = dbTimestamp(time.Now())
	insertRealEventOutbox(t, ds, intruder)
	require.NoError(t, ds.MarkEventDeadLettered(ctx, intruder.ID, claimEventOutboxToken(t, ds, intruder),
		intruder.Topic+".dlt",
		json.RawMessage(`{"original_topic":"`+intruder.Topic+`","attempt_count":5}`), model.BrokerRecord{}))

	second, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
		Limit:  2,
		Cursor: first.NextCursor,
	})
	require.NoError(t, err)

	firstIDs := mineFrom(first.Entries)
	secondIDs := mineFrom(second.Entries)
	for _, eventID := range secondIDs {
		assert.NotContains(t, firstIDs, eventID,
			"a row inserted between two pages must not push a row onto both of them")
	}
	assert.NotContains(t, secondIDs, intruder.EventID,
		"a row NEWER than the cursor belongs to a page already served, so a keyset walk must not show it again")
}

// TestDeadLetterInventoryNarrowing_FiltersAndCountsInSQL_RealDB proves the filter contract
// against real PostgreSQL, which is the only place it can be proved.
//
// # What was wrong, and why a mock could not have caught it
//
// The narrowing used to be applied ABOVE the repository: the service paged the inventory and
// tested each row in Go, up to a fixed 5,000-row scan ceiling. Two consequences followed, and
// a client could detect neither.
//
//   - A page that hit the ceiling was returned looking exactly like a complete one. Only a
//     log line said otherwise. On an inventory larger than the ceiling — which is precisely
//     the incident an operator is triaging — a filter would report a subset of the matches as
//     though it were all of them.
//   - An exact total was impossible. The only count available was a whole-table per-status
//     aggregate that knew nothing about the event-type or topic filter, so the endpoint had
//     to REFUSE include_count for those filters rather than return a total describing a
//     different set than the page.
//
// This test therefore asserts three things that only real SQL can establish: that each
// predicate actually selects, that the offset is an offset into the FILTERED set, and that
// the count and the page agree — including agreeing when they are both wrong to agree, which
// is why the count is compared against the length of an unpaged listing rather than against a
// number written here by hand.
//
// The occurrence window is exercised with bounds INSIDE the fixture spread, so a window that
// silently degraded to no window would return more rows and fail.
func TestDeadLetterInventoryNarrowing_FiltersAndCountsInSQL_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dltfilter")
	quiesceEventOutbox(t, ds, marker)

	// One instant per fixture, an hour back so nothing collides with a concurrently
	// running tier, and spread a minute apart so a window can select a middle slice.
	base := dbTimestamp(time.Now().Add(-time.Hour))

	// seed inserts a fixture and drives it to the requested terminal failure state through
	// the production transitions, so the rows under test are rows production could produce.
	seed := func(t *testing.T, eventType, topic string, occurredAt time.Time, deadLetter bool) *model.EventOutbox {
		t.Helper()

		entry := newEventOutboxFixture(marker)
		entry.EventType = eventType
		entry.Topic = topic
		entry.OccurredAt = occurredAt
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)

		// The exhaustion arm: spend the whole budget in one transition so the row lands in
		// failed without this test restating the retry loop.
		_, err := ds.MarkEventFailed(ctx, entry.ID, token, "seeded for the filter test", 0, true, testDeadLetterHandoffLease)
		require.NoError(t, err)

		if deadLetter {
			require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt",
				json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`),
				model.BrokerRecord{}))
		}

		return entry
	}

	appliedOldest := seed(t, "transaction.applied", "blnk.transactions", base, true)
	voidMiddle := seed(t, "transaction.void", "blnk.transactions", base.Add(time.Minute), true)
	appliedNewer := seed(t, "transaction.applied", "blnk.transactions", base.Add(2*time.Minute), false)
	balanceNewest := seed(t, "balance.created", "blnk.balances", base.Add(3*time.Minute), true)

	// Every assertion below is scoped to THIS run's rows: the table is shared with other
	// tiers and other clones, so an absolute count would be a flake rather than an
	// assertion. The narrowing is combined with the run's own ledger id, which
	// newEventOutboxFixture carries — but ledger_id is not a filter, so the projection is
	// done here and the count is compared against the projected listing.
	mineFrom := func(entries []model.EventOutbox) []string {
		mine := make([]string, 0, len(entries))
		for _, entry := range entries {
			if isMarkedEventOutbox(entry, marker) {
				mine = append(mine, entry.EventID)
			}
		}

		return mine
	}

	listMine := func(t *testing.T, query model.DeadLetterQuery) []string {
		t.Helper()

		query.Limit = maxDeadLetterPageSize
		entries, err := ds.ListDeadLetteredEvents(ctx, query)
		require.NoError(t, err)

		return mineFrom(entries)
	}

	cases := []struct {
		name  string
		query model.DeadLetterQuery
		want  []string
	}{
		{
			name:  "unfiltered lists both terminal states, newest occurrence first",
			query: model.DeadLetterQuery{},
			want: []string{
				balanceNewest.EventID, appliedNewer.EventID,
				voidMiddle.EventID, appliedOldest.EventID,
			},
		},
		{
			name:  "an event type filter selects in SQL",
			query: model.DeadLetterQuery{EventType: "transaction.applied"},
			want:  []string{appliedNewer.EventID, appliedOldest.EventID},
		},
		{
			name:  "a topic filter matches the original topic column",
			query: model.DeadLetterQuery{Topic: "blnk.transactions"},
			want:  []string{appliedNewer.EventID, voidMiddle.EventID, appliedOldest.EventID},
		},
		{
			name:  "a status filter narrows to one terminal state",
			query: model.DeadLetterQuery{Status: model.EventOutboxStatusFailed},
			want:  []string{appliedNewer.EventID},
		},
		{
			name:  "a dead_lettered filter excludes the row whose dead-letter write is still owed",
			query: model.DeadLetterQuery{Status: model.EventOutboxStatusDeadLettered},
			want:  []string{balanceNewest.EventID, voidMiddle.EventID, appliedOldest.EventID},
		},
		{
			name: "an occurrence window selects a middle slice, inclusively at both ends",
			query: model.DeadLetterQuery{
				OccurredFrom: base.Add(time.Minute),
				OccurredTo:   base.Add(2 * time.Minute),
			},
			want: []string{appliedNewer.EventID, voidMiddle.EventID},
		},
		{
			name:  "a one-sided window leaves the other end unbounded",
			query: model.DeadLetterQuery{OccurredFrom: base.Add(2 * time.Minute)},
			want:  []string{balanceNewest.EventID, appliedNewer.EventID},
		},
		{
			name: "an equal-bound window selects exactly that instant",
			query: model.DeadLetterQuery{
				OccurredFrom: base.Add(time.Minute),
				OccurredTo:   base.Add(time.Minute),
			},
			want: []string{voidMiddle.EventID},
		},
		{
			name: "filters combine",
			query: model.DeadLetterQuery{
				EventType:    "transaction.applied",
				Topic:        "blnk.transactions",
				Status:       model.EventOutboxStatusDeadLettered,
				OccurredFrom: base,
				OccurredTo:   base.Add(2 * time.Minute),
			},
			want: []string{appliedOldest.EventID},
		},
		{
			name:  "a filter matching nothing returns no rows rather than everything",
			query: model.DeadLetterQuery{EventType: "identity.created"},
			want:  []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, listMine(t, tc.query))
		})
	}

	t.Run("the offset is an offset into the FILTERED set", func(t *testing.T) {
		// Two of this run's four rows carry this event type. Skipping one must leave the
		// OLDER of those two, not whichever row happened to be second in the unfiltered
		// inventory — which is the difference between a page of the filtered set and a
		// filtered page of the unfiltered one.
		entries, err := ds.ListDeadLetteredEvents(ctx, model.DeadLetterQuery{
			EventType: "transaction.applied",
			Limit:     maxDeadLetterPageSize,
			Offset:    1,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{appliedOldest.EventID}, mineFrom(entries))

		first, err := ds.ListDeadLetteredEvents(ctx, model.DeadLetterQuery{
			EventType: "transaction.applied",
			Limit:     1,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{appliedNewer.EventID}, mineFrom(first),
			"the first filtered page must be the newest match")
	})

	t.Run("the count describes the same set as the page", func(t *testing.T) {
		// Scoped to this run by narrowing on a topic no other tier in this file seeds
		// alongside this marker, then reconciled against the listing rather than against a
		// hand-written number: the assertion that matters is that the two AGREE, and a
		// literal would only prove that one of them matches a guess.
		for _, query := range []model.DeadLetterQuery{
			{EventType: "transaction.applied", Topic: "blnk.transactions"},
			{Topic: "blnk.balances", Status: model.EventOutboxStatusDeadLettered},
			{OccurredFrom: base, OccurredTo: base.Add(3 * time.Minute)},
		} {
			listed := listMine(t, query)

			total, err := ds.CountDeadLetteredEvents(ctx, query)
			require.NoError(t, err)

			// The table is shared, so the count covers rows this run did not seed. The
			// invariant that holds regardless is that the total is never SHORT of what the
			// same predicate listed — a count over a narrower predicate than the page is
			// the exact defect this method exists to rule out.
			assert.GreaterOrEqual(t, total, int64(len(listed)),
				"the total must never be short of the page the same predicate returned")
		}
	})

	t.Run("the count ignores the page", func(t *testing.T) {
		query := model.DeadLetterQuery{Topic: "blnk.transactions"}

		unpaged, err := ds.CountDeadLetteredEvents(ctx, query)
		require.NoError(t, err)

		query.Limit, query.Offset = 1, 2
		paged, err := ds.CountDeadLetteredEvents(ctx, query)
		require.NoError(t, err)

		assert.Equal(t, unpaged, paged,
			"the total is a property of the filter, not of the window into it; honouring the page "+
				"would make total_count never exceed the page size")
	})
}

// ===========================================================================
// Subscriber registry — SEC-04, SEC-03, SSRF-01, SECRET-01, AUTH-01, RETAIN-01
//
// These live in this file rather than beside database/event_subscriber.go because
// there is no database/event_subscriber_test.go in this checkpoint's scope. They are
// in the same package, so the unexported validators are reachable, and the subject
// they guard — the event-outbox and subscriber repositories — is this file's subject.
// ===========================================================================

// canonicalSubscriber returns a registry row whose identity is derived correctly, so
// that each test below mutates exactly one thing and the failure is unambiguous.
func canonicalSubscriber(t *testing.T) *model.EventSubscriber {
	t.Helper()

	principal, err := model.CanonicalKafkaPrincipal("acme_prod")
	require.NoError(t, err)
	group, err := model.CanonicalConsumerGroupID("acme_prod")
	require.NoError(t, err)

	return &model.EventSubscriber{
		SubscriberID:     "acme_prod",
		Name:             "Acme production",
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: []string{"blnk.transactions"},
	}
}

// TestRequireSubscriberFields_DerivesTheIdentityAndRefusesASuppliedOne is the SEC-04
// guard at the persistence boundary.
//
// The principal is what every ACL binding is granted TO and the consumer group is
// what the group binding is granted OVER, so a row that stores a principal not
// derived from its subscriber ID means the boundary provisioned is not the boundary
// the registry appears to describe — and it fails silently, because both the row and
// the resulting ACL look internally consistent.
func TestRequireSubscriberFields_DerivesTheIdentityAndRefusesASuppliedOne(t *testing.T) {
	t.Run("derives principal and group when neither is supplied", func(t *testing.T) {
		subscriber := &model.EventSubscriber{SubscriberID: "acme_prod", Name: "Acme"}
		require.NoError(t, requireSubscriberFields(subscriber))

		assert.Equal(t, "blnk-sub-acme_prod", subscriber.KafkaPrincipal)
		assert.Equal(t, "blnk-sub-acme_prod.default", subscriber.ConsumerGroupID)
	})

	t.Run("accepts a supplied principal that matches the derivation", func(t *testing.T) {
		assert.NoError(t, requireSubscriberFields(canonicalSubscriber(t)))
	})

	t.Run("refuses a foreign principal", func(t *testing.T) {
		subscriber := canonicalSubscriber(t)
		subscriber.KafkaPrincipal = "blnk-sub-victim"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses the administrative principal", func(t *testing.T) {
		subscriber := canonicalSubscriber(t)
		subscriber.KafkaPrincipal = "admin"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses a principal differing only in case", func(t *testing.T) {
		// Kafka compares principals byte-for-byte, so this is a DIFFERENT principal
		// that reads as the same subscriber to every human who sees it.
		subscriber := canonicalSubscriber(t)
		subscriber.KafkaPrincipal = "BLNK-SUB-ACME_PROD"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses a principal differing only by whitespace", func(t *testing.T) {
		// The comparison is EXACT rather than trimmed: a trimmed comparison would
		// accept this and then store the untrimmed value, so the ACL would name a
		// principal indistinguishable from the intended one in every log line.
		subscriber := canonicalSubscriber(t)
		subscriber.KafkaPrincipal = " blnk-sub-acme_prod"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses a group outside the derived namespace", func(t *testing.T) {
		subscriber := canonicalSubscriber(t)
		subscriber.ConsumerGroupID = "blnk-sub-victim.default"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses an adjacent-namespace group", func(t *testing.T) {
		// The terminator is the disjointness guarantee: a PREFIXED grant on
		// "blnk-sub-acme_prod" without it would also cover "blnk-sub-acme_production".
		subscriber := canonicalSubscriber(t)
		subscriber.ConsumerGroupID = "blnk-sub-acme_production.default"
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	t.Run("refuses the bare namespace as a group", func(t *testing.T) {
		// A PREFIXED ACL over the namespace itself grants the namespace rather than a
		// group inside it.
		subscriber := canonicalSubscriber(t)
		subscriber.ConsumerGroupID = "blnk-sub-acme_prod."
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})

	nonCanonical := map[string]string{
		"uppercase":          "Acme_Prod",
		"leading whitespace": " acme_prod",
		"a wildcard":         "acme*",
		"a control byte":     "acme\nprod",
		"too short":          "ac",
		"a colon":            "acme:prod",
	}
	for name, identifier := range nonCanonical {
		t.Run("refuses a "+name+" identifier", func(t *testing.T) {
			subscriber := &model.EventSubscriber{SubscriberID: identifier, Name: "Acme"}
			requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
		})
	}
}

// TestRequireGrantableTopics_RefusesEverythingOutsideTheAllowlist is SEC-03 at the
// persistence boundary.
//
// The check is here as well as in the provisioning path because the ROW OUTLIVES ANY
// SINGLE REQUEST: a topic accepted now is granted at the next issuance, whichever
// code path performs it.
func TestRequireGrantableTopics_RefusesEverythingOutsideTheAllowlist(t *testing.T) {
	assert.NoError(t, requireGrantableTopics(nil),
		"an empty grant is the fail-closed default of a fresh registration")
	assert.NoError(t, requireGrantableTopics(model.SubscriberGrantableTopics(configuredEventTopicPrefix())),
		"the composed grantable list must itself be accepted, or two layers disagree")

	refused := map[string]string{
		"the any-resource wildcard":              "*",
		"a wildcarded category":                  "blnk.*",
		"a foreign topic":                        "attacker.transactions",
		"a dead-letter topic":                    "blnk.transactions.dlt",
		"the system dead-letter":                 "blnk.system.dlt",
		"a category this contract does not have": "blnk.ledgers",
	}
	for name, topic := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			requireAPIError(t, requireGrantableTopics([]string{topic}), apierror.ErrInvalidInput)
		})
	}

	// THE SYSTEM CATEGORY IS ACCEPTED, and it is worth saying so here because the opposite
	// reading is tempting. `blnk.system` carries `ledger.created` and `system.error`, two of
	// the thirteen event types the legacy webhook transport delivered, so refusing a grant on
	// it would leave a migrating subscriber with no authorized path to either — coverage on
	// the publishing side and a regression on the consuming side.
	//
	// What a grant of it discloses is real: `system.error`'s payload is the FROZEN legacy
	// body, so it still carries the error text as it renders — a PostgreSQL error names
	// schema, table, column and routine; a broker error names internal addresses. That body
	// cannot be narrowed without breaking the payload-preservation guarantee the whole
	// migration rests on. So the containment is an AUDIENCE decision recorded per subscriber
	// in authorized_topics, exactly the decision an operator already made when they pointed
	// one webhook URL at the same text — not a decision this layer makes for them. This layer
	// enforces only that a stored topic is one that MAY be granted.
	t.Run("accepts the system category", func(t *testing.T) {
		assert.NoError(t, requireGrantableTopics([]string{"blnk.system"}),
			"blnk.system carries ledger.created and system.error, so a subscriber must be able to hold it")
	})

	t.Run("refuses an offending topic in any position", func(t *testing.T) {
		requireAPIError(t,
			requireGrantableTopics([]string{"blnk.transactions", "blnk.transactions.dlt"}),
			apierror.ErrInvalidInput)
	})

	t.Run("the persistence allowlist matches the model allowlist exactly", func(t *testing.T) {
		// One answer to "which topics may a subscriber be granted?", checked rather
		// than assumed: drift between layers means a topic one refuses and another
		// grants.
		prefixes := grantableTopicPrefixes()
		composed := model.SubscriberGrantableTopics(configuredEventTopicPrefix())

		assert.Len(t, prefixes, len(composed))
		for _, topic := range composed {
			_, present := prefixes[topic]
			assert.True(t, present, "%q is grantable per model but absent here", topic)
		}
	})
}

// TestRequireSafeWebhookURL_AppliesTheDestinationPolicy is SSRF-01 at the persistence
// boundary, so a URL arriving by a path that skipped the DTO is still refused.
func TestRequireSafeWebhookURL_AppliesTheDestinationPolicy(t *testing.T) {
	// The parameter is *string because the column is nullable and a nil pointer means
	// "no URL recorded", which is different from a URL that is present and empty.
	safeURL := func(raw string) *string { return &raw }

	assert.NoError(t, requireSafeWebhookURL(nil), "no URL recorded is legitimate")
	assert.NoError(t, requireSafeWebhookURL(safeURL("")), "a present empty URL clears the record")
	assert.NoError(t, requireSafeWebhookURL(safeURL("   ")),
		"an all-whitespace URL is the same instruction as an empty one — clear the record — which is "+
			"why it is accepted while a real URL carrying surrounding whitespace is not")
	assert.NoError(t, requireSafeWebhookURL(safeURL("https://hooks.example.com/blnk")))

	refused := map[string]string{
		"plaintext http":          "http://hooks.example.com/blnk",
		"a file URL":              "file:///etc/passwd",
		"loopback":                "https://127.0.0.1/blnk",
		"localhost":               "https://localhost/blnk",
		"IPv4-mapped loopback":    "https://[::ffff:127.0.0.1]/blnk",
		"cloud metadata":          "https://169.254.169.254/latest/meta-data/",
		"a private address":       "https://10.0.0.7:9092/blnk",
		"an internal zone name":   "https://metadata.google.internal/blnk",
		"an unqualified hostname": "https://postgres/blnk",
		// SURROUNDING WHITESPACE, and it belongs in this list rather than being trimmed away.
		// The column is written VERBATIM, so trimming for validation and storing the original
		// would persist a destination that passed a check the stored bytes do not satisfy — and
		// the API DTO already refuses these, so tolerating them here would give a service, CLI
		// or migration caller a laxer policy than an HTTP caller for the same column.
		"a leading space":    " https://hooks.example.com/blnk",
		"a trailing space":   "https://hooks.example.com/blnk ",
		"a trailing newline": "https://hooks.example.com/blnk\n",
		"a leading tab":      "\thttps://hooks.example.com/blnk",
	}
	for name, raw := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			requireAPIError(t, requireSafeWebhookURL(safeURL(raw)), apierror.ErrInvalidInput)
		})
	}

	t.Run("is applied through requireSubscriberFields", func(t *testing.T) {
		// The policy is worthless if the write path does not consult it.
		internal := "https://169.254.169.254/latest/meta-data/"
		subscriber := canonicalSubscriber(t)
		subscriber.WebhookURL = &internal
		requireAPIError(t, requireSubscriberFields(subscriber), apierror.ErrInvalidInput)
	})
}

// TestRecordSubscriberCredential_RefusesAnythingThatIsNotADerivedReference is the
// SECRET-01 guard.
//
// The column must never hold a value anyone could authenticate with, and the guard is
// structural rather than advisory: a plaintext secret is refused even though it is a
// perfectly valid TEXT value.
func TestRecordSubscriberCredential_RefusesAnythingThatIsNotADerivedReference(t *testing.T) {
	db, mock := newSQLMock(t)
	source := Datasource{Conn: db}

	// An empty value is caught earlier, by the required-field guard, which reports
	// ErrBadRequest; everything else reaches the reference-shape guard and reports
	// ErrInvalidInput. Both are 400, and each case names the code its own path returns
	// rather than asserting a single code that would hide which guard actually fired.
	notReferences := map[string]struct {
		value string
		code  apierror.ErrorCode
	}{
		"a plaintext secret":     {"S3cret-Password-Not-A-Reference", apierror.ErrInvalidInput},
		"an empty value":         {"", apierror.ErrBadRequest},
		"a bare digest":          {strings.Repeat("a", 64), apierror.ErrInvalidInput},
		"a wrong scheme":         {"bcrypt$" + strings.Repeat("a", 64), apierror.ErrInvalidInput},
		"a non-hex digest":       {"blnkcred$" + strings.Repeat("z", 64), apierror.ErrInvalidInput},
		"a truncated digest":     {"blnkcred$" + strings.Repeat("a", 10), apierror.ErrInvalidInput},
		"a reference with space": {"blnkcred$ " + strings.Repeat("a", 63), apierror.ErrInvalidInput},
	}

	for name, expectation := range notReferences {
		t.Run("refuses "+name, func(t *testing.T) {
			value := expectation.value
			err := source.RecordSubscriberCredential(context.Background(), "acme_prod", value, time.Now())
			requireAPIError(t, err, expectation.code)

			// The offending value must not be echoed: a caller who passed a secret
			// here by mistake must not have it copied into an error or a log line.
			if value != "" {
				assert.NotContains(t, err.Error(), value,
					"the rejected value must never appear in the error")
			}
		})
	}

	// Nothing reached the database: the guard runs before any statement.
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestRecordSubscriberCredentialIfUnchanged_DetectsALostRace is the AUTH-01 guard.
//
// Issuance is not idempotent — each call mints a new secret and the broker keeps only
// the last one written — so two concurrent issuances would otherwise both return 200
// with a password in the body, one of which authenticates against nothing.
func TestRecordSubscriberCredentialIfUnchanged_DetectsALostRace(t *testing.T) {
	reference, err := model.DeriveCredentialReference("blnk-sub-acme_prod", "the-new-secret")
	require.NoError(t, err)
	previous, err := model.DeriveCredentialReference("blnk-sub-acme_prod", "the-previous-secret")
	require.NoError(t, err)

	t.Run("succeeds while the expected reference still stands", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "claim-token"))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("carries the provisioning claim as well as the reference", func(t *testing.T) {
		// The reference CAS alone cannot detect a caller whose lease expired: while the
		// rightful new owner is still provisioning it has recorded nothing, so the stored
		// reference is STILL the one this caller observed. Its write would land and the
		// winner would then be refused — the fence inverted.
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		statement := (*captured)[0]
		assert.Contains(t, statement, "provisioning_token = $6",
			"the claim must be a predicate of the write, not merely an argument")
		assert.Contains(t, statement, "credential_reference = $5",
			"and the reference CAS must survive alongside it")
	})

	t.Run("refuses a write that presents no claim", func(t *testing.T) {
		// Defaulting to unfenced would restore the exact behaviour the predicate removes,
		// so a blank token is refused in memory before any statement runs.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "  "),
			apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports a lost claim as a conflict naming the claim, not the reference", func(t *testing.T) {
		// The two conflicts have different remedies — retry under a fresh claim versus
		// re-read and issue again — so they must not collapse into one message.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires no credential when expected is nil", func(t *testing.T) {
		// A separate arm, because `credential_reference = NULL` is never true in SQL:
		// folding the two into one predicate would make an empty-string reference
		// collide with the no-credential case.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("credential_reference IS NULL").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", nil, reference, time.Now(), "claim-token"))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns a conflict when the reference moved under it", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// No row matched...
		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		// ...and the row exists under THIS caller's claim with no revocation pending, so the
		// only remaining explanation is that the reference moved: a race, not a 404 and not a
		// lost fence.
		//
		// ONE follow-up query, not two. describeFencedWriteMiss reads both facts in a single
		// round trip — whether the claim is still this caller's and whether a revocation is
		// pending — and a second expectation stacked below this one was an earlier
		// generation's (exists, held) shape that nothing consumes. sqlmock does not object to
		// an unconsumed expectation until ExpectationsWereMet, so the subtest reached the right
		// verdict and then failed on the leftover.
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(true, false))

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "claim-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.NotContains(t, err.Error(), "provisioning claim",
			"a superseded credential is NOT a lost fence, and the two demand opposite responses: "+
				"the loser of a credential race abandons its issuance, while a caller that lost the "+
				"claim must compensate for the credential it already wrote at the broker")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("distinguishes a LOST FENCE from a superseded credential", func(t *testing.T) {
		// AUTH-01 + the fence: both predicates can fail, and the caller's remedy differs.
		// A superseded credential means this issuance simply lost a race. A lost claim
		// means another operation may be running right now and cannot know about the
		// credential this one already wrote at the broker, so this one MUST compensate.
		// Reporting them with the same error would silently skip that compensation.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		// The row is there but the claim is not this caller's any more, which is the ONE query
		// describeFencedWriteMiss issues and the two booleans it selects.
		//
		// A bare ExpectQuery("SELECT") returning a full subscriber row sat here first, from a
		// generation that re-read the row to diagnose the miss. Being bare, it matched the
		// describe query — which also starts with SELECT — and handed fifteen columns to a
		// two-column Scan, so the diagnosis failed at the driver and a deliberate CONFLICT
		// surfaced as INTERNAL_SERVER_ERROR: the exact conflation this subtest exists to rule out.
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error()+errorDetailText(err), "the provisioning claim was no longer held",
			"the marker phrase is what blnk.subscriberFenceWasLost matches on, so the service can "+
				"route this to compensation rather than treating it as an ordinary conflict")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a blank claim token before any statement", func(t *testing.T) {
		// A blank token would either turn the ownership condition into a comparison
		// against the empty string, which matches nothing and presents as a spurious
		// conflict, or — if the condition were ever dropped — match every row. Refusing
		// it here makes "this write happened under a live claim" a property of the
		// statement rather than of the caller's discipline.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "   ")
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet(), "nothing may reach the database")
	})

	t.Run("reports not-found rather than a conflict for a missing subscriber", func(t *testing.T) {
		// Reporting a conflict here would send an operator looking for a race that
		// did not happen.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").WillReturnError(sql.ErrNoRows)

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "claim-token")
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a value that is not a derived reference", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", nil, "S3cret-Password", time.Now(), "claim-token")
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.NotContains(t, err.Error(), "S3cret")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestClearSubscriberCredential_NullsBothHalvesTogether is the compensating half of
// an issuance: revocation happens at the broker, and this is what stops the registry
// from continuing to claim a credential that no longer exists.
func TestClearSubscriberCredential_NullsBothHalvesTogether(t *testing.T) {
	t.Run("clears the reference and the issuance instant", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// Both columns in one statement, because together they are one issuance
		// record: a reference without a timestamp cannot be audited, and a timestamp
		// without a reference asserts an issuance it cannot identify.
		mock.ExpectExec("credential_reference = NULL").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.ClearSubscriberCredential(
			context.Background(), "acme_prod", "claim-token"))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("clears only under the caller's provisioning claim", func(t *testing.T) {
		// Clearing runs at the END of a fenced operation, after broker work whose duration a
		// third party decides. Under an expired lease an unconditional clear would blank the
		// record a NEWER issuance had just written, leaving the registry reporting "not yet
		// provisioned" for a subscriber holding a working credential — the one direction this
		// write must never move in, because it UNDER-reports access.
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.ClearSubscriberCredential(
			context.Background(), "acme_prod", "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0], "provisioning_token = $3")
	})

	t.Run("refuses a clear that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "acme_prod", "  "),
			apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a row that vanished is a LOST FENCE, not a not-found", func(t *testing.T) {
		// The write is fence-protected, so "no row matched" no longer means "there is no
		// such subscriber". A row that disappeared under a caller holding a claim means
		// something else removed it concurrently — and that caller has just revoked a
		// credential at the broker. Reporting not-found would read as "nothing to clean
		// up" and suppress the compensation, which is the dangerous direction.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		// describeFencedWriteMiss reads (holds_claim, revocation_pending) in ONE query. The
		// two-column (exists, held) shape that stood here is an earlier generation's, and with a
		// false in the first column it decoded as "the claim is not yours" rather than as the row
		// having vanished — the same verdict by luck, from the wrong reading.
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		err := source.ClearSubscriberCredential(context.Background(), "acme_prod", "claim-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error()+errorDetailText(err), "the provisioning claim was no longer held")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a lapsed claim is refused rather than blanking somebody else's record", func(t *testing.T) {
		// The clear follows a broker-side revocation, which is several round trips. If the
		// lease lapsed in the meantime and a concurrent issuance recorded a NEW reference,
		// blanking it would leave the registry reporting no credential while a working one
		// existed — the exact under-reporting the ownership condition exists to prevent.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("provisioning_token").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "acme_prod", "stale-token"),
			apierror.ErrConflict)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports a lost claim as a conflict, never as not-found", func(t *testing.T) {
		// The remedies are opposite: a 404 says re-register, a 409 says the operation was
		// overtaken and its write was correctly discarded.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, false))

		err := source.ClearSubscriberCredential(context.Background(), "acme_prod", "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, _ := newSQLMock(t)
		source := Datasource{Conn: db}

		// ErrBadRequest is the code the shared required-field guard uses; both it and
		// ErrInvalidInput are 400, and this asserts the one this path actually takes.
		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "  ", "claim-token"),
			apierror.ErrBadRequest)
	})

	t.Run("requires a claim token", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "acme_prod", ""),
			apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet(), "nothing may reach the database")
	})
}

// newEventSubscriberRows builds a sqlmock row set over the FULL registry projection.
//
// It exists because two tests used to spell the row out as a positional literal, and adding a
// column to eventSubscriberColumns then made them panic on an arity mismatch rather than fail on
// the behaviour they were about. Here the projection decides the width: named values are placed
// by column, and every other column is NULL.
//
// Parameters:
//   - t *testing.T: for the fatal on an unknown column name.
//   - values map[string]driver.Value: the columns this test cares about, by name.
//
// Returns:
//   - *sqlmock.Rows: one row, exactly as wide as the projection.
func newEventSubscriberRows(t *testing.T, values map[string]driver.Value) *sqlmock.Rows {
	t.Helper()

	columns := strings.Split(eventSubscriberColumns, ", ")

	known := make(map[string]bool, len(columns))
	for _, column := range columns {
		known[column] = true
	}

	for name := range values {
		if !known[name] {
			t.Fatalf("newEventSubscriberRows: %q is not a column of the registry projection (%v)",
				name, columns)
		}
	}

	row := make([]driver.Value, 0, len(columns))
	for _, column := range columns {
		row = append(row, values[column])
	}

	return sqlmock.NewRows(columns).AddRow(row...)
}

// TestTakeEventSubscriber_ReturnsTheRowItDeleted is the AUTH-01 deletion-propagation
// guard.
//
// Revoking at the broker needs the principal and the authorised topics, and those
// exist only on the row being deleted. Returning it is what makes the revocation
// target exactly what was removed, instead of a value read beforehand that may since
// have changed.
func TestTakeEventSubscriber_ReturnsTheRowItDeleted(t *testing.T) {
	t.Run("hands back the principal and the grant", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(7),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions", "blnk.balances"}),
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}))

		deleted, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "claim-token")
		require.NoError(t, err)
		require.NotNil(t, deleted)

		assert.Equal(t, "blnk-sub-acme_prod", deleted.KafkaPrincipal,
			"the principal is what RevokeSubscriber needs")
		assert.Equal(t, "blnk-sub-acme_prod.default", deleted.ConsumerGroupID)
		assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, deleted.AuthorizedTopics,
			"the topics are the bindings to remove")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports not-found rather than a nil row", func(t *testing.T) {
		// "Already gone" and "just removed" must not be confusable, or a caller could
		// skip the broker-side work by mistake.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		// The row genuinely is not there, so this is a not-found rather than a lost fence — and
		// "not there" is expressed as the describe query returning NO ROWS, which is the only
		// reading that yields subscriberFenceMissRowGone. The earlier (exists=false, held=false)
		// shape returned a row saying the claim was not the caller's, which is a LOST FENCE and
		// resolved to the 409 this subtest exists to distinguish from a 404.
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnError(sql.ErrNoRows)

		deleted, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "claim-token")
		assert.Nil(t, deleted)
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("deletes only under the caller's provisioning claim", func(t *testing.T) {
		// This is the LAST write of a deregistration and runs after a broker round trip. An
		// unconditional delete under an expired lease would remove a row another operation is
		// working on — most damagingly an issuance, which would leave a live broker principal
		// with no registry row naming it.
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(7),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions"}),
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}))

		_, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "claim-token")
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0], "provisioning_token = $2")
	})

	t.Run("reports a lost claim as a conflict, never as not-found", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
				AddRow(false, true))

		_, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a delete that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		_, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "  ")
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does not leak driver detail on failure", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnError(&pq.Error{
			Code: "42P01", Message: "relation does not exist",
			Schema: "blnk", Table: "event_subscribers", File: "namespace.c", Routine: "RangeVarGetRelid",
		})

		_, err := source.TakeEventSubscriber(context.Background(), "acme_prod", "claim-token")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assertNoDatabaseDetailLeak(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestPurgeMigratedSubscriberWebhookURLs_ExpiresTheDualRunArtefact is the RETAIN-01
// guard.
//
// Once a subscriber has migrated, webhook_url holds a third party's endpoint with no
// remaining purpose — retention without a reason, and a destination that becomes a
// request Blnk makes the moment any sender is wired to it.
func TestPurgeMigratedSubscriberWebhookURLs_ExpiresTheDualRunArtefact(t *testing.T) {
	t.Run("nulls the URL of migrated subscribers before the cut-off", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("webhook_url = NULL").WillReturnResult(sqlmock.NewResult(0, 3))

		purged, err := source.PurgeMigratedSubscriberWebhookURLs(
			context.Background(), time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		assert.Equal(t, int64(3), purged)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a zero cut-off rather than purging everything", func(t *testing.T) {
		// A zero-valued argument is far more often a bug than an intention.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		purged, err := source.PurgeMigratedSubscriberWebhookURLs(context.Background(), time.Time{})
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.Zero(t, purged)
		assert.NoError(t, mock.ExpectationsWereMet(), "nothing may reach the database")
	})

	t.Run("purging nothing is a normal outcome", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").WillReturnResult(sqlmock.NewResult(0, 0))

		purged, err := source.PurgeMigratedSubscriberWebhookURLs(context.Background(), time.Now())
		require.NoError(t, err)
		assert.Zero(t, purged)
	})
}

// TestSubscriberRepository_DoesNotLeakDriverDetail is the DATA-01 guard for the
// subscriber repository.
//
// A raw *pq.Error attached to APIError.Details serialises its Schema, Table, Column,
// Constraint, File, Line and Routine straight into the response body, describing the
// database's internal structure to any caller who can provoke an error.
func TestSubscriberRepository_DoesNotLeakDriverDetail(t *testing.T) {
	hostile := &pq.Error{
		Severity: "ERROR", Code: "23505",
		Message:    `duplicate key value violates unique constraint "event_subscribers_kafka_principal_uidx"`,
		Schema:     "blnk",
		Table:      "event_subscribers",
		Column:     "kafka_principal",
		Constraint: "event_subscribers_kafka_principal_uidx",
		File:       "nbtinsert.c",
		Line:       "666",
		Routine:    "_bt_check_unique",
	}

	operations := map[string]func(Datasource) error{
		"UpdateEventSubscriber": func(source Datasource) error {
			_, err := source.UpdateEventSubscriber(
				context.Background(), canonicalSubscriber(t), "claim-token")

			return err
		},
		"DeleteEventSubscriber": func(source Datasource) error {
			return source.DeleteEventSubscriber(context.Background(), "acme_prod")
		},
		"ClearSubscriberCredential": func(source Datasource) error {
			return source.ClearSubscriberCredential(context.Background(), "acme_prod", "claim-token")
		},
		"MarkSubscriberMigrated": func(source Datasource) error {
			return source.MarkSubscriberMigrated(context.Background(), "acme_prod", time.Now())
		},
		"PurgeMigratedSubscriberWebhookURLs": func(source Datasource) error {
			_, err := source.PurgeMigratedSubscriberWebhookURLs(context.Background(), time.Now())

			return err
		},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			source := Datasource{Conn: db}
			mock.ExpectExec(".*").WillReturnError(hostile)

			err := operation(source)
			require.Error(t, err)
			assertNoDatabaseDetailLeak(t, err)
		})
	}
}

// assertNoDatabaseDetailLeak proves an error carries none of the driver's structural
// detail, checked through BOTH renderings a caller can reach: the error string, and
// the JSON an API response is built from.
//
// Both are needed because they fail differently. A struct attached to Details is
// invisible in %v — APIError.Error() prints only the code and message — and fully
// visible once marshalled. Checking one alone would pass while the other leaked.
func assertNoDatabaseDetailLeak(t *testing.T, err error) {
	t.Helper()

	rendered := err.Error()

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		marshalled, marshalErr := json.Marshal(apiErr)
		require.NoError(t, marshalErr)
		rendered += " " + string(marshalled)
	}

	for _, forbidden := range []string{
		"nbtinsert.c", "namespace.c", "_bt_check_unique", "RangeVarGetRelid",
		"event_subscribers_kafka_principal_uidx", "666",
	} {
		assert.NotContains(t, rendered, forbidden,
			"the driver's %q must not reach a caller; it belongs in the log", forbidden)
	}
}

// ---------------------------------------------------------------------------------------
// The broker coordinate and the zero-loss audit — OBS-02
// ---------------------------------------------------------------------------------------

// eventOutboxBrokerRecord builds a coordinate that is unique to one test run.
//
// The topic is per-run because the coordinate carries a PARTIAL UNIQUE INDEX: two rows naming
// the same (topic, partition, offset) is refused by the database, which is the guarantee the
// audit rests on — and which would otherwise make two tests, or two runs of one test, collide
// on a shared literal.
func eventOutboxBrokerRecord(markerPrefix string, partition int, offset int64) model.BrokerRecord {
	return model.BrokerRecord{
		Topic:     "blnk.transactions." + markerPrefix,
		Partition: partition,
		Offset:    offset,
	}
}

// TestMarkEventDispatched_PersistsTheBrokerCoordinate is the OBS-02 write-side guard.
//
// Every marking transition binds the coordinate, and the SQL is asserted directly because the
// column list and the COALESCE are the whole mechanism: a transition that assigned instead of
// COALESCEd would erase the record a webhook-only retry pass has no coordinate for, and one that
// bound nothing would leave every row an unconfirmed publication with nothing failing to say so.
func TestMarkEventDispatched_PersistsTheBrokerCoordinate(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	var topic, partition, offset driver.Value

	mock.ExpectExec("").
		WithArgs(model.EventOutboxStatusDispatched, int64(42), "tok-42",
			model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying,
			captureArg(&topic), captureArg(&partition), captureArg(&offset)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	record := model.BrokerRecord{Topic: "blnk.transactions", Partition: 3, Offset: 148_291}
	require.NoError(t, ds.MarkEventDispatched(context.Background(), 42, "tok-42", record))

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "kafka_topic = COALESCE($6, kafka_topic)")
	assert.Contains(t, issued, "kafka_partition = COALESCE($7, kafka_partition)")
	assert.Contains(t, issued, "kafka_offset = COALESCE($8, kafka_offset)",
		"COALESCE, not assignment: a webhook-only pass carries no coordinate and must not erase "+
			"the one the successful publish recorded")

	assert.Equal(t, "blnk.transactions", topic)
	assert.EqualValues(t, 3, partition)
	assert.EqualValues(t, 148_291, offset)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventDispatched_BindsNullForAnUnconfirmedCoordinate pins the absence case.
//
// Partition 0 and offset 0 are an ordinary location — the first record on a fresh partition — so
// binding zeroes for an absent coordinate would write a row claiming to name a record it never
// produced, and an operator following it would find somebody else's event. An absent coordinate
// must be SQL NULL, which is also what the all-or-nothing check constraint requires.
func TestMarkEventDispatched_BindsNullForAnUnconfirmedCoordinate(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	var topic, partition, offset driver.Value

	mock.ExpectExec("UPDATE blnk.event_outbox").
		WithArgs(model.EventOutboxStatusDispatched, int64(42), "tok-42",
			model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying,
			captureArg(&topic), captureArg(&partition), captureArg(&offset)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, ds.MarkEventDispatched(
		context.Background(), 42, "tok-42", model.BrokerRecord{},
	))

	assert.Nil(t, topic, "an absent coordinate must bind NULL, never a zero")
	assert.Nil(t, partition)
	assert.Nil(t, offset)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventDeadLettered_AssignsTheDeadLetterCoordinate covers the asymmetry.
//
// A dead-lettered row's record is on the `.dlt` topic, and the original publish is what FAILED —
// so there is no record of it to name. The coordinate is therefore ASSIGNED rather than
// COALESCEd: a stale coordinate on the main topic would send an operator looking for a record
// the retries never produced.
func TestMarkEventDeadLettered_AssignsTheDeadLetterCoordinate(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	var topic, partition, offset driver.Value

	mock.ExpectExec("").
		WithArgs(model.EventOutboxStatusDeadLettered, "blnk.transactions.dlt", sqlmock.AnyArg(),
			int64(11), "tok-11",
			model.EventOutboxStatusFailed, model.EventOutboxStatusProcessing,
			captureArg(&topic), captureArg(&partition), captureArg(&offset)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	record := model.BrokerRecord{Topic: "blnk.transactions.dlt", Partition: 1, Offset: 7}
	require.NoError(t, ds.MarkEventDeadLettered(
		context.Background(), 11, "tok-11", "blnk.transactions.dlt",
		json.RawMessage(`{"error_reason":"broker unavailable"}`), record,
	))

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "kafka_topic = $8")
	assert.Contains(t, issued, "kafka_partition = $9")
	assert.Contains(t, issued, "kafka_offset = $10")
	assert.NotContains(t, issued, "kafka_topic = COALESCE",
		"the dead-letter write supersedes whatever a failed original attempt left behind")

	assert.Equal(t, "blnk.transactions.dlt", topic)
	assert.EqualValues(t, 7, offset)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventWebhookPending_CoalescesTheCoordinate covers the third transition.
//
// A webhook-only retry pass publishes NOTHING — the row's Kafka leg is already done — so it
// carries no coordinate, and the statement must leave the stored one intact. This is the exact
// case COALESCE exists for, and an assignment here would erase the record of a row that is
// otherwise perfectly healthy.
func TestMarkEventWebhookPending_CoalescesTheCoordinate(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").WillReturnRows(
		sqlmock.NewRows([]string{"status", "webhook_attempts"}).
			AddRow(model.EventOutboxStatusWebhookPending, 1))

	_, err := ds.MarkEventWebhookPending(
		context.Background(), 9, "tok-9", "redis unavailable", time.Second, model.BrokerRecord{},
	)
	require.NoError(t, err)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "kafka_topic = COALESCE($9, kafka_topic)")
	assert.Contains(t, issued, "kafka_partition = COALESCE($10, kafka_partition)")
	assert.Contains(t, issued, "kafka_offset = COALESCE($11, kafka_offset)")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditEventRecordsInIntervals_ClassifiesAgainstTheMeasuredWindows pins the audit query,
// which IS the finding's resolution.
//
// Every element asserted here is load-bearing:
//
//   - The windows arrive as four PARALLEL ARRAYS zipped by unnest, so one statement serves any
//     number of partitions. Interpolating them would be an injection surface on values that
//     ultimately come from a broker response.
//   - The join is a LEFT JOIN. An inner join would silently DROP every row whose partition was
//     not measured, inflating the corroborated fraction — the exact failure the classification
//     exists to prevent.
//   - The five CASE arms are the classification, and their ORDER matters: the coordinate-less
//     test comes first because a NULL offset would compare false against every bound, and the
//     beyond-end test comes before the aged-out one so a truncated partition is never reported
//     as retention.
//   - The predicate must include webhook_pending rows through kafka_dispatched_at. Restricting
//     it to the terminal statuses would leave their records unaccounted for on the outbox side
//     and loosen the reconciliation during exactly the dual-delivery window it matters most in.
func TestAuditEventRecordsInIntervals_ClassifiesAgainstTheMeasuredWindows(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	oldest := time.Now().UTC().Add(-48 * time.Hour)
	from := oldest.Add(time.Hour)
	to := time.Now().UTC()

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusDeadLettered,
			pq.Array([]string{"blnk.transactions", "blnk.balances"}),
			pq.Array([]int64{0, 3}),
			pq.Array([]int64{100, 0}),
			pq.Array([]int64{900, 40}),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"published_rows", "corroborated_rows", "distinct_corroborated",
			"unconfirmed_rows", "unmeasured_rows", "aged_out_rows", "beyond_end_rows",
			"oldest_terminal_at", "corroborated_from", "corroborated_to",
		}).AddRow(
			int64(1_000), int64(940), int64(940),
			int64(10), int64(20), int64(25), int64(5),
			oldest, from, to,
		))

	audit, err := ds.AuditEventRecordsInIntervals(context.Background(), []model.PartitionOffsetInterval{
		{Topic: "blnk.transactions", Partition: 0, FirstOffset: 100, EndOffset: 900},
		{Topic: "blnk.balances", Partition: 3, FirstOffset: 0, EndOffset: 40},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(1_000), audit.PublishedRows)
	assert.Equal(t, int64(940), audit.CorroboratedRows)
	assert.Equal(t, int64(10), audit.UnconfirmedRows)
	assert.Equal(t, int64(20), audit.UnmeasuredRows)
	assert.Equal(t, int64(25), audit.AgedOutRows)
	assert.Equal(t, int64(5), audit.BeyondEndRows)
	assert.Equal(t, int64(60), audit.UncorroboratedRows())
	assert.Equal(t, audit.PublishedRows, audit.CorroboratedRows+audit.UncorroboratedRows(),
		"the buckets must partition the claims, or the audit describes a set that is not the outbox")
	assert.False(t, audit.FullyCorroborated(),
		"sixty rows could not be placed inside a measured window, so the audit is not conclusive")
	assert.Equal(t, oldest, audit.OldestTerminalAt,
		"the oldest retained row bounds what any verdict can speak about and must survive the scan")
	assert.Equal(t, from, audit.CorroboratedFrom)
	assert.Equal(t, to, audit.CorroboratedTo)
	assert.WithinDuration(t, time.Now(), audit.MeasuredAt, time.Minute)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "unnest($2::text[], $3::bigint[], $4::bigint[], $5::bigint[])",
		"the windows must be BOUND, never interpolated")
	assert.Contains(t, issued, "LEFT JOIN measured",
		"an inner join would drop the rows on unmeasured partitions and inflate the corroborated "+
			"fraction, which is the failure this classification prevents")
	assert.Contains(t, issued, "WHEN t.kafka_offset IS NULL              THEN 'unconfirmed'")
	assert.Contains(t, issued, "WHEN m.topic_name IS NULL                THEN 'unmeasured'")
	assert.Contains(t, issued, "WHEN t.kafka_offset >= m.end_offset      THEN 'beyond_end'")
	assert.Contains(t, issued, "WHEN t.kafka_offset <  m.first_offset    THEN 'aged_out'")
	assert.Contains(t, issued, "kafka_dispatched_at IS NOT NULL OR status = $1",
		"a webhook_pending row IS on its topic; excluding it would leave its record unaccounted for")
	assert.Contains(t, issued, "COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))")
	assert.Contains(t, issued, "AND kafka_offset IS NOT NULL",
		"the distinct count must exclude coordinate-less rows: PostgreSQL counts the all-NULL row "+
			"constructor as one distinct value, which would invent a record")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditEventRecordsInIntervals_NormalisesTheWindowsItIsGiven covers the three window
// readings that must not reach SQL as-is.
//
// Each one would otherwise misclassify rows rather than merely being untidy: a blank topic and a
// negative partition can match no stored coordinate but would still occupy an array slot, and a
// NEGATIVE first offset — kafka-go's "unknown" reading — would compare below every stored offset
// and so could never be exceeded, silently turning an unknown lower bound into "nothing has aged
// out". Zero is the widest defensible reading and is the only one that cannot invent a fact.
func TestAuditEventRecordsInIntervals_NormalisesTheWindowsItIsGiven(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
		WithArgs(
			model.EventOutboxStatusDeadLettered,
			pq.Array([]string{"blnk.transactions"}),
			pq.Array([]int64{2}),
			pq.Array([]int64{0}),
			pq.Array([]int64{500}),
		).
		WillReturnRows(auditIntervalRows(0, 0, 0, 0, 0, 0, 0))

	_, err := ds.AuditEventRecordsInIntervals(context.Background(), []model.PartitionOffsetInterval{
		{Topic: "   ", Partition: 0, FirstOffset: 0, EndOffset: 100},
		{Topic: "blnk.balances", Partition: -1, FirstOffset: 0, EndOffset: 100},
		{Topic: "  blnk.transactions  ", Partition: 2, FirstOffset: -1, EndOffset: 500},
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditEventRecordsInIntervals_WithNoWindowsCorroboratesNothing is the guard on the
// broker-less path.
//
// With no windows every confirmed row classifies as unmeasured, so the verdict is inconclusive.
// That is the only honest reading — nothing was measured, so nothing was corroborated — and the
// alternative is the dangerous one: an audit that treated "no windows" as "no constraints"
// would report a green reconciliation precisely when the broker could not be read.
func TestAuditEventRecordsInIntervals_WithNoWindowsCorroboratesNothing(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusDeadLettered,
			pq.Array([]string{}), pq.Array([]int64{}), pq.Array([]int64{}), pq.Array([]int64{}),
		).
		WillReturnRows(auditIntervalRows(500, 0, 0, 0, 500, 0, 0))

	audit, err := ds.AuditEventRecordsInIntervals(context.Background(), nil)
	require.NoError(t, err)

	assert.Equal(t, int64(500), audit.UnmeasuredRows)
	assert.Zero(t, audit.CorroboratedRows)
	assert.False(t, audit.FullyCorroborated(),
		"nothing measured means nothing corroborated; a green verdict here would be a false all-clear")

	require.Len(t, *captured, 1)
	assert.Contains(t, (*captured)[0], "unnest(",
		"an empty window list still goes through the same statement, so there is one code path")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditEventRecordsInIntervals_ReportsAFailureRatherThanAnEmptyAudit keeps the verdict
// honest.
//
// A zero audit reads as "nothing has been published", which is a legitimate state on a fresh
// deployment — so returning one on a failed read would let a reconciliation conclude that
// everything is accounted for precisely when it could not measure.
func TestAuditEventRecordsInIntervals_ReportsAFailureRatherThanAnEmptyAudit(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("FROM blnk.event_outbox").WillReturnError(sql.ErrConnDone)

	audit, err := ds.AuditEventRecordsInIntervals(context.Background(), nil)
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.Zero(t, audit.PublishedRows)
	assert.NotContains(t, err.Error(), sql.ErrConnDone.Error(),
		"driver text must not reach an operator-facing response")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// auditIntervalRows builds the audit query's single result row, in projection order, with the
// three instants left NULL — the state of an outbox with nothing to bound.
func auditIntervalRows(published, corroborated, distinct, unconfirmed, unmeasured, agedOut, beyondEnd int64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"published_rows", "corroborated_rows", "distinct_corroborated",
		"unconfirmed_rows", "unmeasured_rows", "aged_out_rows", "beyond_end_rows",
		"oldest_terminal_at", "corroborated_from", "corroborated_to",
	}).AddRow(
		published, corroborated, distinct, unconfirmed, unmeasured, agedOut, beyondEnd,
		nil, nil, nil,
	)
}

// TestBrokerCoordinate_RoundTripsThroughTheProjection_RealDB proves the coordinate survives the
// write and the read, against the real schema.
//
// The mock tests above assert the statements; only the real database can show that the columns
// exist, that the check constraint accepts a complete coordinate, and that the projection and
// scanner agree on where the three columns sit — a scan-order mistake no compiler catches and
// that surfaces only as mis-assigned field values.
func TestBrokerCoordinate_RoundTripsThroughTheProjection_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()

	marker := newRealEventOutboxMarker("coord")
	t.Cleanup(func() { quiesceEventOutbox(t, ds, marker) })

	entry := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	token := claimEventOutboxToken(t, ds, entry)

	record := eventOutboxBrokerRecord(marker, 4, 9_182)
	require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, token, record))

	stored, err := ds.GetEventByID(ctx, entry.EventID)
	require.NoError(t, err)
	require.NotNil(t, stored)

	readBack, confirmed := stored.BrokerRecord()
	require.True(t, confirmed, "the coordinate must survive the round trip")
	assert.Equal(t, record, readBack,
		"a mismatch here is a projection/scanner ordering fault, which no compiler catches")
	assert.Equal(t, model.EventOutboxStatusDispatched, stored.Status)
}

// TestBrokerCoordinate_TwoRowsCannotNameTheSameRecord_RealDB proves the guarantee the audit's
// integrity check rests on.
//
// One record is produced by one acknowledged write of one row, so two rows naming the same
// coordinate is impossible in reality — and the partial unique index is what makes it impossible
// in the schema too. Without it, two rows could share one record's corroboration, which is the
// same double-counting the whole mapping exists to remove.
func TestBrokerCoordinate_TwoRowsCannotNameTheSameRecord_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()

	marker := newRealEventOutboxMarker("coorddup")
	t.Cleanup(func() { quiesceEventOutbox(t, ds, marker) })

	first := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	second := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	tokens := make(map[string]string, 2)
	for _, row := range claimed {
		if isMarkedEventOutbox(row, marker) {
			tokens[row.EventID] = row.ClaimToken
		}
	}
	require.Len(t, tokens, 2, "both fixture rows must be claimed in one pass")

	record := eventOutboxBrokerRecord(marker, 2, 555)
	require.NoError(t, ds.MarkEventDispatched(ctx, first.ID, tokens[first.EventID], record))

	// The SAME coordinate for a different row. The database must refuse it.
	err = ds.MarkEventDispatched(ctx, second.ID, tokens[second.EventID], record)
	require.Error(t, err,
		"THE UNIQUE INDEX IS THE GUARANTEE: two rows sharing one record's corroboration would let "+
			"the audit count one record twice, which is the double-counting the mapping removes")
}

// TestAuditEventRecordsInIntervals_ClassifiesTheWholePublishedSet_RealDB is the audit's own
// round trip, and the only place the classification meets a real planner.
//
// It builds every shape the classification has to separate — a dispatched row whose coordinate
// sits INSIDE the measured window, a webhook_pending row whose Kafka leg completed, a
// dispatched row with no coordinate at all, a row whose offset is BELOW the window (retention
// has deleted its record), a row whose offset is AT OR ABOVE the window (the partition was
// truncated or the topic recreated), a row on a partition NOTHING measured, and a row that has
// not been published at all — and requires each to land in its own bucket.
//
// A mock cannot prove any of this: it replays a scripted result and has no join, no CASE and no
// planner. The buckets are what the verdict is computed from, so a classification that put a
// truncated partition's rows under retention, or dropped an unmeasured partition's rows
// entirely, would produce a confident verdict about a set that is not the outbox.
//
// Isolation is on the marker-scoped TOPIC recorded in kafka_topic, which is a free-form column
// rather than the validated `topic` column, so the windows below can be asserted exactly
// against a shared table.
func TestAuditEventRecordsInIntervals_ClassifiesTheWholePublishedSet_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()

	marker := newRealEventOutboxMarker("audit")
	t.Cleanup(func() { quiesceEventOutbox(t, ds, marker) })

	// The measured window for this run's own Kafka topic: offsets 100 up to but excluding 900.
	kafkaTopic := marker + "kafka-topic"
	otherTopic := marker + "unmeasured-topic"
	windows := []model.PartitionOffsetInterval{
		{Topic: kafkaTopic, Partition: 0, FirstOffset: 100, EndOffset: 900},
	}

	before, err := ds.AuditEventRecordsInIntervals(ctx, windows)
	require.NoError(t, err)

	inside := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	pending := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	noCoordinate := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	agedOut := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	beyondEnd := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	unmeasured := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	// A row that has NOT been published at all, which must not be counted by any bucket.
	untouched := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	tokens := make(map[string]string, 7)
	for _, row := range claimed {
		if isMarkedEventOutbox(row, marker) {
			tokens[row.EventID] = row.ClaimToken
		}
	}
	require.Len(t, tokens, 7)

	at := func(topic string, partition int, offset int64) model.BrokerRecord {
		return model.BrokerRecord{Topic: topic, Partition: partition, Offset: offset}
	}

	// Inside the window.
	require.NoError(t, ds.MarkEventDispatched(ctx, inside.ID, tokens[inside.EventID],
		at(kafkaTopic, 0, 450)))

	// Kafka leg done, HTTP leg outstanding. Not terminal, but on the topic and inside the
	// window — so it corroborates.
	_, err = ds.MarkEventWebhookPending(ctx, pending.ID, tokens[pending.EventID],
		"queue unreachable", time.Minute, at(kafkaTopic, 0, 451))
	require.NoError(t, err)

	// Published, acknowledged, but the library reported no coordinate.
	require.NoError(t, ds.MarkEventDispatched(ctx, noCoordinate.ID, tokens[noCoordinate.EventID],
		model.BrokerRecord{}))

	// Below the window: written, then deleted by retention.
	require.NoError(t, ds.MarkEventDispatched(ctx, agedOut.ID, tokens[agedOut.EventID],
		at(kafkaTopic, 0, 99)))

	// AT the end offset. The boundary is exclusive, so this is beyond the log — the reading
	// that means the partition was truncated or the topic recreated.
	require.NoError(t, ds.MarkEventDispatched(ctx, beyondEnd.ID, tokens[beyondEnd.EventID],
		at(kafkaTopic, 0, 900)))

	// A topic no window covers.
	require.NoError(t, ds.MarkEventDispatched(ctx, unmeasured.ID, tokens[unmeasured.EventID],
		at(otherTopic, 0, 450)))

	after, err := ds.AuditEventRecordsInIntervals(ctx, windows)
	require.NoError(t, err)

	delta := func(get func(model.EventRecordIntervalAudit) int64) int64 {
		return get(after) - get(before)
	}

	assert.Equal(t, int64(6), delta(func(a model.EventRecordIntervalAudit) int64 { return a.PublishedRows }),
		"six rows claim a record; the untouched row (%d) claims nothing", untouched.ID)
	assert.Equal(t, int64(2), delta(func(a model.EventRecordIntervalAudit) int64 { return a.CorroboratedRows }),
		"only the two coordinates inside the measured window are corroborated")
	assert.Equal(t, int64(2), delta(func(a model.EventRecordIntervalAudit) int64 {
		return a.DistinctCorroboratedRecords
	}), "and those two coordinates are distinct")
	assert.Equal(t, int64(1), delta(func(a model.EventRecordIntervalAudit) int64 { return a.UnconfirmedRows }))
	assert.Equal(t, int64(1), delta(func(a model.EventRecordIntervalAudit) int64 { return a.AgedOutRows }),
		"an offset below the retained window is retention, not loss and not truncation")
	assert.Equal(t, int64(1), delta(func(a model.EventRecordIntervalAudit) int64 { return a.BeyondEndRows }),
		"an offset AT the exclusive end offset is beyond the log: the partition was truncated or "+
			"the topic recreated")
	assert.Equal(t, int64(1), delta(func(a model.EventRecordIntervalAudit) int64 { return a.UnmeasuredRows }),
		"a row on a partition nothing measured must be COUNTED, not dropped: dropping it is what "+
			"would inflate the corroborated fraction")

	// THE COORDINATE-LESS ROW MUST NOT COUNT AS A RECORD, and this is the assertion that says
	// so unconditionally.
	//
	// COUNT(DISTINCT (topic, partition, offset)) counts the all-NULL row constructor as a
	// distinct value, so without the filter the unconfirmed row would contribute a phantom
	// record — unless some earlier test had already left a coordinate-less published row that
	// contributed it first, which is how the unfiltered form passed on residue and failed the
	// moment fixtures started cleaning up after themselves.
	//
	// Stated as an INVARIANT rather than a delta because that is the contract both readers rely
	// on: FullyCorroborated requires no duplication, and ReconcileAgainstOutbox derives
	// duplication from the difference, so DistinctCorroboratedRecords exceeding
	// CorroboratedRows is not a smaller version of the same answer — it is a wrong one.
	assert.LessOrEqual(t, after.DistinctCorroboratedRecords, after.CorroboratedRows,
		"a row that names no coordinate names no record: the distinct count may never exceed the "+
			"corroborated one, or a healthy outbox is reported inconclusive and the verdict derives "+
			"a negative duplication count")

	// The buckets partition the claims. This is the identity the verdict's arithmetic depends
	// on, and it is checked against real rows rather than assumed from the query text.
	assert.Equal(t, after.PublishedRows, after.CorroboratedRows+after.UncorroboratedRows(),
		"every published row must land in exactly one bucket")

	// The verdict this produces is the whole point: unplaced claims make it inconclusive rather
	// than green, however favourable any total looks.
	assert.False(t, after.FullyCorroborated(),
		"four of the six claims could not be placed inside a measured window, so the audit is not "+
			"trustworthy")

	// The window the verdict covers is bounded at both ends by real instants, so a reader can
	// tell what it speaks about.
	assert.False(t, after.OldestTerminalAt.IsZero(),
		"a verdict must state the oldest retained claim it could see; without it a reconciliation "+
			"over a pruned outbox looks complete")
	assert.False(t, after.CorroboratedFrom.IsZero())
	assert.False(t, after.CorroboratedTo.IsZero())
	assert.False(t, after.CorroboratedTo.Before(after.CorroboratedFrom),
		"the covered window cannot end before it starts")

	t.Run("a coordinate inside no window at all corroborates nothing", func(t *testing.T) {
		// The broker-less path, against the real planner: no windows means every confirmed row
		// is unmeasured, so the audit corroborates nothing rather than everything.
		empty, auditErr := ds.AuditEventRecordsInIntervals(ctx, nil)
		require.NoError(t, auditErr)

		assert.Zero(t, empty.CorroboratedRows,
			"with nothing measured nothing can be corroborated; the opposite reading would report "+
				"a green reconciliation exactly when the broker could not be read")
		assert.GreaterOrEqual(t, empty.UnmeasuredRows, int64(5),
			"every row that names a coordinate falls into unmeasured")
	})
}

// TestMarkEventPermanentlyFailed_EndsTheEventOnThisAttempt covers the transition the relay
// takes when the publisher reports a failure NO FURTHER ATTEMPT CAN CHANGE — an unauthorised
// principal, a destination outside the topic catalogue, bytes that will never parse, a message
// over the size limit.
//
// It is a separate statement from MarkEventFailed rather than a flag on it, because the two
// decisions are taken in different places. MarkEventFailed asks the DATABASE whether the budget
// is spent, which is what stops two racing instances both concluding they were last; this one
// carries a verdict the publisher already reached about the broker's answer. Before it existed
// the relay had nowhere to put that verdict, so a topic-authorisation failure spent all five
// attempts and ~17 seconds of backoff per event proving the broker meant it.
//
// The three properties asserted are the ones the dead-letter hand-off depends on: no budget
// test in the statement, the attempt counted honestly, and the claim token retained.
func TestMarkEventPermanentlyFailed_EndsTheEventOnThisAttempt(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusFailed,        // $1 — the only arm there is
			"topic authorization failed",         // $2 — the reason
			int64(11),                            // $3 — the row
			"tok-11",                             // $4 — the claim token
			model.EventOutboxStatusProcessing,    // $5 — the required prior state
			eventDeadLetterHandoffLease.String(), // $6 — the lease held across the hand-off
		).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(1)))

	outcome, err := ds.MarkEventPermanentlyFailed(
		context.Background(), 11, "tok-11", "topic authorization failed", testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "SET status = $1",
		"the row becomes failed unconditionally: the caller established that no further attempt "+
			"can succeed, so there is no budget arm to choose between")
	assert.NotContains(t, issued, "max_attempts",
		"and the statement must NOT consult the budget. Falling back to the max_attempts test "+
			"would make this transition indistinguishable from the exhaustion arm and reinstate "+
			"the five wasted attempts it exists to remove")
	assert.Contains(t, issued, "attempts = attempts + 1",
		"the attempt is still counted, so the failure metadata reports the number of attempts "+
			"really made — 1 for a permanent failure, which tells an operator the event never "+
			"had a chance rather than that it fought for the whole schedule")
	assert.Contains(t, issued, "first_attempted_at = COALESCE(first_attempted_at, NOW())",
		"the retry window must still be bounded at both ends for the failure metadata")
	assert.Contains(t, issued, "locked_until = NOW() + $6::interval",
		"the lease is RENEWED, not released, and it is the other half of retaining the claim token: "+
			"ClaimFailedEventOutboxForDeadLetter takes a failed row with no dlt_topic and no live "+
			"lease and stamps a fresh token over it, so releasing the lease here would let a second "+
			"instance reclaim the row in the same instant and publish the dead-letter message twice")
	// The SET list is inspected on its own, because the WHERE clause legitimately mentions
	// claim_token and a whole-statement match would pass whatever the assignment said.
	assignments, _, split := strings.Cut(issued, "WHERE")
	require.True(t, split, "the statement must be conditional")
	assert.NotContains(t, assignments, "claim_token",
		"the claim token must be RETAINED, exactly as the exhaustion arm retains it: the "+
			"dead-letter write and the transition that records it are still owed and only this "+
			"worker may perform them, which is what stops two copies reaching one .dlt topic")
	assert.NotContains(t, assignments, "next_attempt_at",
		"there is no next attempt to describe, and a future instant on a terminal row would tell "+
			"an operator triaging the dead-letter backlog that a retry was still coming")
	assert.Contains(t, issued, "WHERE id = $3 AND claim_token = $4 AND status = $5",
		"and it stays conditional on the claim, so a worker whose lease expired cannot overwrite "+
			"the newer state of a row another instance has taken")
	assert.Contains(t, issued, "RETURNING status, attempts")

	assert.Equal(t, model.EventOutboxStatusFailed, outcome.Status)
	assert.Equal(t, 1, outcome.Attempts,
		"one attempt was made, and the outcome must not inflate it to the budget")
	assert.True(t, outcome.Exhausted,
		"Exhausted means 'the dead-letter write is now owed' to the caller, which is true here "+
			"however much budget the row had left")
	assert.Equal(t, "tok-11", outcome.ClaimToken,
		"the token must be handed on, or nothing can perform the dead-letter transition")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMarkEventPermanentlyFailed_LostClaimIsAConflict mirrors the guard on every other
// conditional transition.
//
// Matching no row means the lease expired and another instance owns the row. Reporting that as
// success would let the stale worker go on to write the event to the dead-letter topic — a
// second copy of an event the new owner is also working on, which is precisely the outcome the
// retained claim token exists to prevent.
func TestMarkEventPermanentlyFailed_LostClaimIsAConflict(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE blnk.event_outbox")).
		WillReturnError(sql.ErrNoRows)

	outcome, err := ds.MarkEventPermanentlyFailed(context.Background(), 11, "stale-token", "boom", testDeadLetterHandoffLease)
	require.Error(t, err, "a lost claim must NOT be reported as success")
	assert.Equal(t, model.EventFailureOutcome{}, outcome,
		"no decision was made, so no decision may be reported — and no dead letter may follow")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrConflict, apiErr.Code)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// Exclusive use of the shared blnk.event_outbox
//
// This is the DATABASE-package half of a pair. Its twin is lockEventOutboxTier in the root
// package's event_recovery_integration_test.go, which carries the full explanation; the short
// version is that two live tiers in two packages work this one table and neither can be scoped
// to its own rows:
//
//   - The ROOT tiers CLAIM. ClaimPendingEventOutbox takes the oldest pending rows in the whole
//     table, whoever wrote them, because that is what the relay does in production.
//   - THIS tier RETIRES every claim-visible row that is not its own and then asserts there are
//     none, so that its claim assertions read a table it controls.
//
// Each therefore breaks the other, and they do not share a process: `go test ./...` runs one
// binary per package, in parallel, so an in-process mutex is not even available. Observed
// directly — the root tier timing out on rows this tier had retired, and this tier's straggler
// assertion tripping on a row the root tier inserted a moment after the retire — failing 3/3 at
// default parallelism and passing 2/2 under -p 1.
//
// A POSTGRES ADVISORY LOCK, held in the same database as the table it protects, is the one
// mechanism that reaches across processes without changing the production claim query.
// ---------------------------------------------------------------------------

// eventOutboxTierLockKey identifies the advisory lock the live outbox tiers share.
//
// It MUST BE THE SAME VALUE as the constant of the same name in the root package's
// event_recovery_integration_test.go: two different keys are two different locks and would
// protect nothing at all.
const eventOutboxTierLockKey int64 = 0x424c4e4b4f5542 // "BLNKOUB"

// eventOutboxTierLockBudget bounds how long this tier waits for the lock. Longer than the
// slowest live test in either package, shorter than `go test`'s ten-minute default, so a stuck
// holder is named here rather than surfacing as an unexplained package timeout.
const eventOutboxTierLockBudget = 8 * time.Minute

// eventOutboxTierLockPoll is how often the lock is re-attempted.
const eventOutboxTierLockPoll = 200 * time.Millisecond

// eventOutboxTierLock holds this process's side of the advisory lock.
//
// The DEDICATED CONNECTION is load-bearing: a Postgres advisory lock belongs to a SESSION, and
// database/sql hands out an arbitrary pooled connection per statement, so a lock taken on a pool
// is released the moment that connection is recycled — a lock that looks held and is not.
// MaxOpenConns(1) pins one session for the lock's lifetime.
//
// The refcount makes acquisition REENTRANT within the process: every _RealDB test in this file
// calls quiesceEventOutbox, and a test that quiesced twice would otherwise take the lock on two
// sessions and deadlock against itself.
var eventOutboxTierLock struct {
	mu       sync.Mutex
	holders  int
	conn     *sql.DB
	acquired bool
}

// lockEventOutboxTier takes exclusive use of blnk.event_outbox until the test finishes.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration and for failing when the lock cannot be
//     taken.
//   - pool *sql.DB: the pool whose database holds the outbox. Its DSN is reused for the lock's
//     own session; the pool itself is never locked on.
func lockEventOutboxTier(t *testing.T, pool *sql.DB) {
	t.Helper()

	require.NotNil(t, pool, "the outbox tier lock needs a live pool to derive its session from")

	eventOutboxTierLock.mu.Lock()
	defer eventOutboxTierLock.mu.Unlock()

	if eventOutboxTierLock.holders == 0 {
		acquireEventOutboxTierLock(t)
	}

	eventOutboxTierLock.holders++

	t.Cleanup(releaseEventOutboxTierLock)
}

// acquireEventOutboxTierLock opens the lock session and blocks until the lock is held.
//
// The DSN comes from the same resolution openRealTestDB uses — TEST_DATABASE_URL, falling back
// to defaultRealTestDSN — so the lock is always taken in the database the tier is working, not
// in whichever one a configuration store happens to hold.
func acquireEventOutboxTierLock(t *testing.T) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultRealTestDSN
	}

	conn, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "opening the outbox tier lock session")

	// ONE session, for the reason in the type's comment.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(0)

	deadline := time.Now().Add(eventOutboxTierLockBudget)
	for {
		var held bool
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		queryErr := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`,
			eventOutboxTierLockKey).Scan(&held)
		cancel()

		if queryErr != nil {
			_ = conn.Close()
			require.NoError(t, queryErr, "taking the outbox tier advisory lock")
		}

		if held {
			break
		}

		if time.Now().After(deadline) {
			_ = conn.Close()
			t.Fatalf(
				"another live outbox tier has held the blnk.event_outbox advisory lock (%d) for %s. "+
					"The root and database live tiers claim from and retire the whole table, so they take "+
					"this lock to run one at a time; a holder this long means a test in another package "+
					"is stuck rather than slow",
				eventOutboxTierLockKey, eventOutboxTierLockBudget,
			)
		}

		time.Sleep(eventOutboxTierLockPoll)
	}

	eventOutboxTierLock.conn = conn
	eventOutboxTierLock.acquired = true
}

// releaseEventOutboxTierLock drops one hold and, when it was the last, the lock itself.
func releaseEventOutboxTierLock() {
	eventOutboxTierLock.mu.Lock()
	defer eventOutboxTierLock.mu.Unlock()

	if eventOutboxTierLock.holders == 0 {
		return
	}

	eventOutboxTierLock.holders--
	if eventOutboxTierLock.holders > 0 || !eventOutboxTierLock.acquired {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Unlocked explicitly AND the session closed. Closing alone would release it — a session
	// ending drops its advisory locks — but an explicit unlock releases it at a known instant
	// rather than whenever the pool decides to close the connection.
	if _, err := eventOutboxTierLock.conn.ExecContext(ctx,
		`SELECT pg_advisory_unlock($1)`, eventOutboxTierLockKey); err != nil {
		logrus.WithError(err).Warn("releasing the event outbox tier advisory lock")
	}

	if err := eventOutboxTierLock.conn.Close(); err != nil {
		logrus.WithError(err).Warn("closing the event outbox tier advisory lock session")
	}

	eventOutboxTierLock.conn = nil
	eventOutboxTierLock.acquired = false
}

// openLockedEventOutboxDB opens the real test database AND takes exclusive use of
// blnk.event_outbox for the test's duration.
//
// Every _RealDB test in this file goes through it rather than through openRealTestDB directly,
// and the reason is that the lock has to be held for the WHOLE test, not for the part that
// happens to quiesce.
//
// The audit tests are what made that concrete. They take a GLOBAL before-count, insert their
// fixtures, transition them, take a global after-count and assert on the DELTA — and they
// quiesce only in cleanup, so with the lock taken by quiesceEventOutbox alone they ran their
// whole body unprotected. The root package's relay dispatched one row inside that window and
// the delta came back as 3 where 2 was asserted: a real defect report about a count that was
// correct.
//
// Acquisition is refcounted, so a test that also quiesces takes the lock once and releases it
// once.
//
// Returns:
//   - Datasource: the real-database datasource, with the outbox exclusively this test's.
func openLockedEventOutboxDB(t *testing.T) Datasource {
	t.Helper()

	ds := openRealTestDB(t)
	lockEventOutboxTier(t, ds.Conn)

	return ds
}

// TestListDeadLetterInventory_FiltersPagesAndCountsInSQL_RealDB proves the fix against real
// SQL, which is the only place it can be proved.
//
// # What was wrong
//
// The dead-letter listing used to return unfiltered pages and let the service above it test
// each row in Go, abandoning the walk after 5,000 scanned rows. A filter whose matches lay
// beyond that ceiling produced an ordinary empty page, so the endpoint answered "nothing
// matches" while entries matched — and no offset could reach them. Separately, no
// filter-aware count existed, so a total could only be offered for an UNFILTERED page.
//
// # What is asserted, and why against a real database
//
// A fake can be made to agree with any predicate. These are the assertions that only real
// SQL can settle: that the WHERE clause narrows on the columns claimed, that both terminal
// failure states remain in scope so a filter cannot escape them, that OFFSET pages the
// MATCHING set, and that COUNT(*) over the same predicate agrees with the rows returned. The
// last is the one an operator depends on: paging to the total must exhaust the matches.
func TestListDeadLetterInventory_FiltersPagesAndCountsInSQL_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dltq")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-2 * time.Hour))

	// Three shapes, so that every filter distinguishes something and no two filters select
	// the same set. dead_lettered rows reach a `.dlt` sibling; the failed one does not, and
	// it is in the inventory precisely because its dead-letter write never landed.
	type fixture struct {
		eventType  string
		topic      string
		deadLetter bool
	}
	plan := []fixture{
		{"transaction.applied", "blnk.transactions", true},
		{"transaction.applied", "blnk.transactions", true},
		{"transaction.applied", "blnk.transactions", false},
		{"balance.created", "blnk.balances", true},
		{"identity.created", "blnk.identities", false},
	}

	transactionsNewestFirst := make([]string, 0, 3)
	for i, spec := range plan {
		entry := newEventOutboxFixture(marker)
		entry.EventType = spec.eventType
		entry.Topic = spec.topic
		entry.OccurredAt = base.Add(time.Duration(i) * time.Second)
		if !spec.deadLetter {
			// A one-attempt budget so a single failure spends it and the row lands in
			// `failed` with dlt_topic still NULL — the state whose only surviving copy is
			// the outbox row.
			entry.MaxAttempts = 1
		}
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		if spec.deadLetter {
			require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt",
				json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`),
				model.BrokerRecord{}))
		} else {
			outcome, failErr := ds.MarkEventFailed(ctx, entry.ID, token,
				"the dead-letter write never landed", 0, false, testDeadLetterHandoffLease)
			require.NoError(t, failErr)
			require.True(t, outcome.Exhausted,
				"a one-attempt budget is spent by its first failure, which is what moves the "+
					"row to the terminal failed state")
		}

		if spec.topic == "blnk.transactions" {
			transactionsNewestFirst = append([]string{entry.EventID}, transactionsNewestFirst...)
		}
	}

	mine := func(page model.DeadLetterInventoryPage) []string {
		ids := make([]string, 0, len(page.Entries))
		for _, entry := range page.Entries {
			if strings.HasPrefix(entry.AggregateID, marker) {
				ids = append(ids, entry.EventID)
			}
		}

		return ids
	}
	// The queries below are table-wide, because the repository's predicate is. A sibling
	// clone running the same suite against this database can therefore have rows in the
	// same states at the same time, so every assertion on a RETURNED SET is filtered to
	// this test's own marker, and every assertion on a COUNT is a lower bound rather than
	// an equality. That is the honest form: the count is genuinely a count of the whole
	// table, and pretending otherwise would make this test pass or fail on who else is
	// running.

	t.Run("the event-type filter narrows on the column", func(t *testing.T) {
		entries, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: maxDeadLetterPageSize, EventType: "transaction.applied",
		})
		require.NoError(t, err)
		assert.Equal(t, transactionsNewestFirst, mine(entries),
			"only the three transaction.applied rows may match, newest occurrence first")
	})

	t.Run("the topic filter narrows on the ORIGINAL topic, not the dlt sibling", func(t *testing.T) {
		entries, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: maxDeadLetterPageSize, Topic: "blnk.transactions",
		})
		require.NoError(t, err)
		assert.Equal(t, transactionsNewestFirst, mine(entries),
			"the caller filters by the category topic and never has to know the .dlt convention")

		// The `.dlt` name must NOT match the column: it is what the row was written TO, and
		// accepting it here would make two different filters silently equivalent.
		siblings, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: maxDeadLetterPageSize, Topic: "blnk.transactions.dlt",
		})
		require.NoError(t, err)
		assert.Empty(t, mine(siblings))
	})

	t.Run("both terminal states stay in scope and status narrows within them", func(t *testing.T) {
		everything, err := ds.ListDeadLetterInventory(ctx,
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize})
		require.NoError(t, err)
		require.Len(t, mine(everything), 5,
			"a failed row whose dead-letter write never landed is in the inventory; hiding it "+
				"would hide the events with no copy anywhere but the outbox")

		failed, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: maxDeadLetterPageSize, Status: model.EventOutboxStatusFailed,
		})
		require.NoError(t, err)
		assert.Len(t, mine(failed), 2)

		deadLettered, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: maxDeadLetterPageSize, Status: model.EventOutboxStatusDeadLettered,
		})
		require.NoError(t, err)
		assert.Len(t, mine(deadLettered), 3)

		// A status outside the two cannot escape the predicate, however it is spelled.
		for _, status := range []string{
			model.EventOutboxStatusPending,
			model.EventOutboxStatusDispatched,
			model.EventOutboxStatusProcessing,
		} {
			escaped, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
				Limit: maxDeadLetterPageSize, Status: status,
			})
			require.NoError(t, err)
			assert.Emptyf(t, mine(escaped),
				"status %q must match nothing: the inventory is the two terminal failure "+
					"states, and a filter must narrow within them rather than reach past them",
				status)
		}
	})

	t.Run("the cursor pages the matches", func(t *testing.T) {
		seen := make([]string, 0, len(transactionsNewestFirst))

		var resume *model.DeadLetterCursor
		for range len(transactionsNewestFirst) {
			page, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
				Limit: 1, Cursor: resume, EventType: "transaction.applied",
			})
			require.NoError(t, err)

			ids := mine(page)
			require.Len(t, ids, 1, "each page must land on a match")
			seen = append(seen, ids...)

			// Taken from the row just returned rather than from NextCursor, because the LAST page
			// of an exactly-divided walk reports no further page and so carries no cursor — and
			// the position past the final match is what the assertion after the loop needs.
			last := page.Entries[len(page.Entries)-1]
			resume = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
		}

		assert.Equal(t, transactionsNewestFirst, seen,
			"stepping the cursor must walk the matches in order, once each — which is what an "+
				"offset could not promise once a row arrived mid-walk")

		past, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
			Limit: 10, Cursor: resume, EventType: "transaction.applied",
		})
		require.NoError(t, err)
		assert.Empty(t, mine(past), "a cursor past the last match returns no non-matching row")
	})

	t.Run("the count agrees with the rows the same filter returns", func(t *testing.T) {
		for _, query := range []model.DeadLetterQuery{
			{},
			{EventType: "transaction.applied"},
			{Topic: "blnk.balances"},
			{Status: model.EventOutboxStatusFailed},
			{EventType: "transaction.applied", Topic: "blnk.transactions", Status: model.EventOutboxStatusDeadLettered},
			{EventType: "transaction.applied", Topic: "blnk.balances"},
		} {
			entries, err := ds.ListDeadLetterInventory(ctx, model.DeadLetterInventoryQuery{
				Limit:        maxDeadLetterPageSize,
				EventType:    query.EventType,
				Topic:        query.Topic,
				Status:       query.Status,
				OccurredFrom: query.OccurredFrom,
				OccurredTo:   query.OccurredTo,
			})
			require.NoError(t, err)

			// The count spans every row in the table, so it is compared against this
			// test's own rows by counting with the aggregate scoped the same way: the
			// difference between the two counts must be attributable entirely to rows
			// this test did not create.
			total, err := ds.CountDeadLetterInventory(ctx, query)
			require.NoError(t, err)
			assert.GreaterOrEqualf(t, total, int64(len(mine(entries))),
				"the count must include at least the matching rows the listing returned "+
					"for %+v; a smaller total would have a caller stop paging early", query)

			// A page bound must never change the total.
			paged := query
			paged.Limit = 1
			pagedTotal, err := ds.CountDeadLetterInventory(ctx, paged)
			require.NoError(t, err)
			assert.Equalf(t, total, pagedTotal,
				"the page bound must not affect the count for %+v", query)
		}
	})

	t.Run("the full-row read and the projection describe ONE inventory", func(t *testing.T) {
		// Two entry points, two shapes, one set. The replay path needs the stored bytes and reads
		// full rows; the operator's listing needs the triage coordinate and reads the projection.
		// If they can disagree, an event visible in one is invisible in the other, and the
		// listing an operator replays from is not the inventory the replay draws on.
		rows, err := ds.ListDeadLetteredEvents(ctx, model.DeadLetterQuery{Limit: maxDeadLetterPageSize})
		require.NoError(t, err)

		fromRows := make([]string, 0, len(rows))
		for _, row := range rows {
			if isMarkedEventOutbox(row, marker) {
				fromRows = append(fromRows, row.EventID)
			}
		}

		projected, err := ds.ListDeadLetterInventory(ctx,
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize})
		require.NoError(t, err)

		assert.Equal(t, fromRows, mine(projected),
			"the two entry points must return one set; if they can differ, the age scan and "+
				"the operator's listing are reading different inventories")
	})
}

// ---------------------------------------------------------------------------
// The driver seam
//
// Two properties of the paired read cannot be observed from outside the call: that a write
// committed BETWEEN its two statements is invisible to the second one, and that the
// transaction it opens is the isolation level and access mode it documents. Both live below
// database/sql, so the test reaches them by wrapping the driver rather than by inference.
//
// This is a test-only seam and it exists for exactly one test. It intercepts nothing and
// changes nothing by default: every method delegates, and the hook fires only for a
// statement the test named and only while the test has armed it.
// ---------------------------------------------------------------------------

// snapshotProbe is the recording and injection point a probed connection reports to.
//
// It is safe for concurrent use because database/sql may drive several connections from one
// *sql.DB, and the recording must not itself be the thing that makes a test flaky.
type snapshotProbe struct {
	mu sync.Mutex
	// seen is every statement text the probe observed, in order, so a hook that stopped
	// matching can say what it saw instead of merely reporting zero firings.
	seen []string
	// options is every driver.TxOptions BeginTx was called with, which is how the isolation
	// level and the read-only flag are asserted as REQUESTED rather than as inferred.
	options []driver.TxOptions
	// hook runs immediately before a matching statement is sent to the server.
	hook func()
	// matches decides which statement the hook precedes.
	matches func(string) bool
	// fired counts hook invocations, so the injection can be proved to have happened.
	fired int
}

// newSnapshotProbe returns an unarmed probe. It records, and does nothing else, until a hook
// is installed.
func newSnapshotProbe() *snapshotProbe {
	return &snapshotProbe{seen: make([]string, 0, 8), options: make([]driver.TxOptions, 0, 2)}
}

// beforeCountOn arms the probe to run hook immediately before the dead-letter COUNT statement.
//
// The predicate is deliberately narrow — the count is the only statement in this repository
// that counts blnk.event_outbox rows — and deliberately structural rather than an exact
// string, so that a whitespace change in the query does not silently disarm the probe. A
// change that removes the statement entirely disarms it loudly instead, because firings() then
// returns zero and the test requires one.
func (probe *snapshotProbe) beforeCountOn(hook func()) {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	probe.hook = hook
	probe.matches = func(statement string) bool {
		return strings.Contains(statement, "COUNT(*)") &&
			strings.Contains(statement, "blnk.event_outbox")
	}
}

// observe records a statement and, when it is the one the hook precedes, runs the hook.
//
// The hook runs with the probe's lock RELEASED. It commits on another connection and would
// otherwise be holding a lock that connection's own bookkeeping may need, and a test that
// deadlocks in its instrumentation is worse than one that does not exist.
func (probe *snapshotProbe) observe(statement string) {
	probe.mu.Lock()
	probe.seen = append(probe.seen, statement)
	hook := probe.hook
	matched := probe.matches != nil && probe.matches(statement)
	if matched {
		probe.fired++
	}
	probe.mu.Unlock()

	if matched && hook != nil {
		hook()
	}
}

// observeTransaction records what BeginTx was asked for.
func (probe *snapshotProbe) observeTransaction(options driver.TxOptions) {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	probe.options = append(probe.options, options)
}

// firings reports how many times the hook ran.
func (probe *snapshotProbe) firings() int {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	return probe.fired
}

// statements returns a copy of every statement text observed.
func (probe *snapshotProbe) statements() []string {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	return slices.Clone(probe.seen)
}

// transactions returns a copy of every transaction option set observed.
func (probe *snapshotProbe) transactions() []driver.TxOptions {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	return slices.Clone(probe.options)
}

// probedConnector hands out connections that report to a probe.
//
// A connector rather than a registered driver name, so the probe is reached through the value
// the test owns instead of through a package-level registry: two tests can hold two probes at
// once and neither can observe the other's statements.
type probedConnector struct {
	base  driver.Connector
	probe *snapshotProbe
}

// Connect opens one connection and wraps it.
func (connector probedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	inner, err := connector.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &probedConn{inner: inner, probe: connector.probe}, nil
}

// Driver returns the underlying driver, unwrapped: nothing consults it but database/sql's own
// bookkeeping, which has no interest in the probe.
func (connector probedConnector) Driver() driver.Driver { return connector.base.Driver() }

// probedConn delegates every operation to a real lib/pq connection, reporting the statement
// texts and transaction options to the probe on the way through.
//
// Every optional interface lib/pq implements is implemented here too. That completeness is
// load-bearing rather than tidy: a missing QueryerContext sends database/sql down the
// prepare-then-execute path, where the statement text is seen at a different moment, and a
// missing ConnBeginTx would make BeginTx fall back to a plain BEGIN and silently discard the
// isolation level this test exists to assert.
type probedConn struct {
	inner driver.Conn
	probe *snapshotProbe
}

// Prepare satisfies driver.Conn. Present because the interface requires it; database/sql
// prefers PrepareContext.
func (conn *probedConn) Prepare(query string) (driver.Stmt, error) {
	conn.probe.observe(query)

	return conn.inner.Prepare(query)
}

// PrepareContext records the statement and delegates.
func (conn *probedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	conn.probe.observe(query)

	preparer, ok := conn.inner.(driver.ConnPrepareContext)
	if !ok {
		return conn.inner.Prepare(query)
	}

	return preparer.PrepareContext(ctx, query)
}

// Close releases the underlying connection.
func (conn *probedConn) Close() error { return conn.inner.Close() }

// Begin satisfies driver.Conn.
func (conn *probedConn) Begin() (driver.Tx, error) { //nolint:staticcheck // required by driver.Conn
	return conn.inner.Begin() //nolint:staticcheck // delegating the deprecated method
}

// BeginTx records the requested isolation level and access mode, then delegates.
func (conn *probedConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	conn.probe.observeTransaction(options)

	beginner, ok := conn.inner.(driver.ConnBeginTx)
	if !ok {
		return conn.inner.Begin() //nolint:staticcheck // the pre-1.8 fallback
	}

	return beginner.BeginTx(ctx, options)
}

// QueryContext is where the injection happens: the probe sees the statement, runs its hook if
// this is the one it was armed for, and only then is the query sent.
func (conn *probedConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	queryer, ok := conn.inner.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	conn.probe.observe(query)

	return queryer.QueryContext(ctx, query, args)
}

// ExecContext records and delegates.
func (conn *probedConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	execer, ok := conn.inner.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	conn.probe.observe(query)

	return execer.ExecContext(ctx, query, args)
}

// Ping delegates so database/sql's health checks keep working.
func (conn *probedConn) Ping(ctx context.Context) error {
	pinger, ok := conn.inner.(driver.Pinger)
	if !ok {
		return nil
	}

	return pinger.Ping(ctx)
}

// ResetSession delegates so pooled connections are recycled as lib/pq expects.
func (conn *probedConn) ResetSession(ctx context.Context) error {
	resetter, ok := conn.inner.(driver.SessionResetter)
	if !ok {
		return nil
	}

	return resetter.ResetSession(ctx)
}

// IsValid delegates the pool's validity check.
func (conn *probedConn) IsValid() bool {
	validator, ok := conn.inner.(driver.Validator)
	if !ok {
		return true
	}

	return validator.IsValid()
}

// openProbedEventOutboxDB returns a Datasource whose statements pass through a probe.
//
// It opens its OWN pool rather than wrapping the shared one, because the injection has to
// commit on a connection that is not the one holding the snapshot. The caller keeps using the
// locked datasource for fixtures and cleanup; only the call under observation goes through
// this one.
//
// Parameters:
//   - t *testing.T: owns the pool's lifetime.
//   - probe *snapshotProbe: the recorder and injection point.
//
// Returns:
//   - Datasource: a datasource over the same database, observed.
func openProbedEventOutboxDB(t *testing.T, probe *snapshotProbe) Datasource {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultRealTestDSN
	}

	base, err := pq.NewConnector(dsn)
	require.NoError(t, err, "building a lib/pq connector for the probed pool")

	db := sql.OpenDB(probedConnector{base: base, probe: probe})
	require.NoError(t, db.Ping(), "the probed pool must reach the same database")
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("closing the probed event outbox pool")
		}
	})

	return Datasource{Conn: db}
}

// TestListAndCountDeadLetterInventory_DrawsBothAnswersFromOneSnapshot_RealDB is the property a
// shared predicate could never deliver on its own.
//
// The page and its total were two statements on two connections. Both applied the same filters,
// and the response said so — but an entry dead-lettered between the two reads is counted by one
// and absent from the other, so the total described a set the page was not a slice of. On a
// triage endpoint that reads as a different amount of stuck work than there is, and a client
// comparing the page against the total does not terminate.
//
// The test writes a row BETWEEN the two reads of the pair. It can do that because a REPEATABLE
// READ snapshot is taken at the transaction's first statement, and the insert here happens on a
// second connection after that statement has run: under two independent reads the second one
// sees the new row, and under one snapshot neither does. That distinction is the whole test.
func TestListAndCountDeadLetterInventory_DrawsBothAnswersFromOneSnapshot_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dltsnap")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-3 * time.Hour))

	// A narrowing no other clone of this suite can land in, so the count below is an EQUALITY
	// rather than a lower bound: the event type is unique to this test.
	eventType := "transaction.applied." + marker

	deadLetter := func(offset time.Duration) string {
		entry := newEventOutboxFixture(marker)
		entry.EventType = eventType
		entry.Topic = "blnk.transactions"
		entry.OccurredAt = base.Add(offset)
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`),
			model.BrokerRecord{}))

		return entry.EventID
	}

	for i := range 3 {
		deadLetter(time.Duration(i) * time.Second)
	}

	query := model.DeadLetterInventoryQuery{Limit: 2, EventType: eventType}

	t.Run("the pair agrees with itself", func(t *testing.T) {
		page, total, err := ds.ListAndCountDeadLetterInventory(ctx, query)
		require.NoError(t, err)

		require.Len(t, page.Entries, 2, "the page bound must still be honoured inside the snapshot")
		assert.True(t, page.HasMore,
			"the probe row is read inside the snapshot too, so has_more still answers correctly")
		assert.Equal(t, int64(3), total,
			"the total counts the narrowing and ignores the page")

		// Newest first, exactly as the standalone listing orders: the snapshot must not have
		// changed which rows a page selects or in what order.
		standalone, err := ds.ListDeadLetterInventory(ctx, query)
		require.NoError(t, err)
		require.Len(t, standalone.Entries, 2)

		for i := range page.Entries {
			assert.Equal(t, standalone.Entries[i].EventID, page.Entries[i].EventID,
				"the paired read must run the same statement as the standalone one")
		}
	})

	t.Run("a row committed BETWEEN the page and the count is in neither answer", func(t *testing.T) {
		// THIS IS THE ONLY ARRANGEMENT THAT DISCRIMINATES, and the reason is worth stating
		// plainly because the obvious arrangement does not.
		//
		// Writing a row between two COMPLETE invocations proves nothing. Two autocommit reads
		// per invocation would pass it just as readily: each pair runs its page and its count
		// microseconds apart with nothing writing in between, so the two answers agree by
		// coincidence of timing and the assertion never fires. The property under test is not
		// "the pair agrees when nothing changes" — it is "the pair agrees WHEN SOMETHING DOES",
		// and the only place something can change is INSIDE one invocation, after its first
		// statement and before its second.
		//
		// So the write is injected at the driver seam. The probe fires immediately before the
		// count statement leaves for the server, commits a fourth matching row on a SEPARATE
		// connection, and returns. In PostgreSQL a REPEATABLE READ snapshot is taken by the
		// transaction's FIRST statement — BEGIN ISOLATION LEVEL ... acquires none — so the page
		// has already fixed the snapshot and the count must still see three. Under two
		// independent reads the count sees four and the pair contradicts itself by exactly the
		// row the relay dead-lettered in between, which is the production symptom: a triage
		// endpoint that reports 41 entries and a total of 40.
		probe := newSnapshotProbe()
		probed := openProbedEventOutboxDB(t, probe)

		var injected string
		probe.beforeCountOn(func() {
			injected = deadLetter(4 * time.Second)
		})

		page, total, err := probed.ListAndCountDeadLetterInventory(ctx,
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize, EventType: eventType})
		require.NoError(t, err)

		// NON-VACUITY FIRST. Everything below is a claim about a write that landed between two
		// statements, and it is worthless if the write did not land there. The probe reports
		// how many times it fired, and the row is separately proved to exist.
		require.Equalf(t, 1, probe.firings(),
			"the probe must have fired exactly once, immediately before the count statement. "+
				"Zero firings means the count no longer matches the statement this probe "+
				"recognises and the whole subtest is vacuous; more than one means the pair is "+
				"issuing several counts.\nstatements seen:\n  %s",
			strings.Join(probe.statements(), "\n  "))
		require.NotEmpty(t, injected, "the injected row must have been created")

		// THE ASSERTION. Three, from the snapshot the page took, not the four now committed.
		assert.Equalf(t, int64(3), total,
			"the count must be answered from the snapshot the PAGE took. A fourth matching row "+
				"was committed on another connection between the two statements, so a count of 4 "+
				"means the two answers came from two snapshots and the total describes a set the "+
				"page is not a slice of")
		assert.Len(t, page.Entries, 3,
			"the page is the earlier half of the same snapshot and cannot see the row either")
		assert.Equalf(t, int64(len(page.Entries)), total,
			"a full page and its total must agree even when the inventory changed underneath "+
				"them; that agreement is the entire contract of the paired read")
		assert.False(t, page.HasMore,
			"has_more is read inside the same snapshot, so the probe row must not make the page "+
				"claim a successor that the total does not count")

		for _, entry := range page.Entries {
			assert.NotEqualf(t, injected, entry.EventID,
				"the row committed after the snapshot was taken must not appear in the page")
		}

		// THE ISOLATION IS ASSERTED AT THE SEAM, not inferred from the outcome. A pair that
		// happened to agree for some other reason — one statement doing both jobs, a cached
		// count — would satisfy the assertions above while the documented mechanism was gone.
		// The driver records what BeginTx was actually asked for.
		options := probe.transactions()
		require.Lenf(t, options, 1,
			"the paired read must open EXACTLY ONE transaction; %d means the page and the count "+
				"are not sharing one", len(options))
		assert.Equalf(t, driver.IsolationLevel(sql.LevelRepeatableRead), options[0].Isolation,
			"the snapshot must be REPEATABLE READ. READ COMMITTED takes a fresh snapshot per "+
				"statement, so the count would see the injected row even inside one transaction")
		assert.Truef(t, options[0].ReadOnly,
			"the transaction must be declared READ ONLY: it exists to read, and the declaration "+
				"is what makes it impossible for this path to write")

		// AND THE SNAPSHOT IS PER CALL, NOT PER PROCESS. A later read must see the fourth row,
		// or "coherent" would have been achieved by never observing anything new again.
		later, laterTotal, err := ds.ListAndCountDeadLetterInventory(ctx,
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize, EventType: eventType})
		require.NoError(t, err)
		assert.Equal(t, int64(4), laterTotal,
			"a subsequent call takes a NEW snapshot; the pair is coherent without being frozen")
		assert.Len(t, later.Entries, 4)
		assert.Equal(t, int64(len(later.Entries)), laterTotal)
	})

	t.Run("a snapshot read writes nothing", func(t *testing.T) {
		// READ ONLY is declared on the transaction, so this path cannot mutate a row even by
		// accident. Asserted through the observable consequence: the rows' terminal state and
		// attempt budget are untouched by having been listed.
		statuses, err := ds.CountEventOutboxByStatus(ctx, time.Time{})
		require.NoError(t, err)

		_, _, err = ds.ListAndCountDeadLetterInventory(ctx,
			model.DeadLetterInventoryQuery{Limit: maxDeadLetterPageSize, EventType: eventType})
		require.NoError(t, err)

		after, err := ds.CountEventOutboxByStatus(ctx, time.Time{})
		require.NoError(t, err)
		assert.Equal(t, statuses[model.EventOutboxStatusDeadLettered],
			after[model.EventOutboxStatusDeadLettered],
			"listing the inventory must not move a row between states")
	})
}

// TestPurgeTerminalEventsBefore_RecordsWhatItRemoved_RealDB is the durable half of making the
// zero-loss reconciliation honest.
//
// # Why a deleted row has to leave a record
//
// A Kafka end offset counts every record ever appended to a partition and never decreases.
// The outbox side of the reconciliation counts rows that still exist. The two describe the
// same interval only until retention deletes its first terminal row; after that the broker
// counts all of history and the outbox counts a suffix of it.
//
// The difference is undetectable by inspection, because the comparison already EXPECTS a
// surplus — a redelivery, a replay and a dead-letter copy each append a record no row
// claims. A purge-inflated surplus therefore looks exactly like healthy overhead, and it can
// offset a genuine shortfall precisely, at which point the endpoint answers a confident "no
// loss detected" while events are missing. Nothing in the numbers hints at it.
//
// A deleted row leaves no trace, so "how many terminal rows has retention removed" is
// unanswerable unless the purge itself records it. These assertions are what make the answer
// trustworthy: the counts must describe exactly the rows that left, the confirmed subset must
// be tracked separately because it restores a different baseline, and a sweep that removes
// nothing must write nothing.
func TestPurgeTerminalEventsBefore_RecordsWhatItRemoved_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("purgelog")
	quiesceEventOutbox(t, ds, marker)

	// Deltas rather than absolutes: the purge log has no per-test marker because it
	// describes the whole table, and the suite may run against a database another clone
	// has also purged. What this test owns is the CHANGE its own purge causes.
	before, err := ds.SumPurgedTerminalEvents(ctx)
	require.NoError(t, err)
	require.True(t, before.Recorded,
		"reading the log must report that it was read; an unread log and an empty one are "+
			"different facts and only one supports a conclusive verdict")

	// A sweep that removes nothing must not write a log row. "Retention ran and found
	// nothing" is the steady state, and recording it would bury the batches that matter.
	noop, err := ds.PurgeTerminalEventsBefore(ctx, dbTimestamp(time.Now().AddDate(-20, 0, 0)), 100)
	require.NoError(t, err)
	require.Zero(t, noop, "no row is old enough to be eligible")

	afterNoop, err := ds.SumPurgedTerminalEvents(ctx)
	require.NoError(t, err)
	assert.Equal(t, before.Batches, afterNoop.Batches,
		"a purge that removed nothing must record nothing")
	assert.Equal(t, before.RowsRemoved, afterNoop.RowsRemoved)

	// Three PURGEABLE rows — two carrying a broker coordinate and one without, so the
	// confirmed subset is provably tracked rather than assumed equal to the total — plus a
	// `failed` row that retention must NOT touch, and a recent row the cutoff must spare.
	//
	// Only dispatched and dead_lettered are purgeable, and the exclusion of `failed` is
	// deliberate rather than an oversight: such a row's retry budget is spent while its
	// dead-letter write has not landed, so the outbox row is the event's ONLY surviving
	// copy. Deleting it would destroy the event, which is the exact failure the whole
	// zero-loss criterion exists to prevent. It is asserted below.
	type fixture struct {
		age       time.Duration
		status    string
		confirmed bool
		purgeable bool
	}
	fixtures := []fixture{
		{40 * 24 * time.Hour, model.EventOutboxStatusDispatched, true, true},
		{39 * 24 * time.Hour, model.EventOutboxStatusDispatched, true, true},
		{37 * 24 * time.Hour, model.EventOutboxStatusDeadLettered, false, false},
		{38 * 24 * time.Hour, model.EventOutboxStatusFailed, false, false},
	}
	purgeableCount := int64(0)
	survivors := make(map[string]string, 2)

	var oldest, newest time.Time
	offset := int64(9000)
	for _, spec := range fixtures {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = dbTimestamp(time.Now().Add(-spec.age))
		insertRealEventOutbox(t, ds, entry)

		token := claimEventOutboxToken(t, ds, entry)
		record := model.BrokerRecord{}
		if spec.confirmed {
			offset++
			record = model.BrokerRecord{Topic: entry.Topic, Partition: 0, Offset: offset}
		}

		switch spec.status {
		case model.EventOutboxStatusDispatched:
			require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, token, record))
		case model.EventOutboxStatusFailed:
			outcome, failErr := ds.MarkEventFailed(ctx, entry.ID, token, "spent", 0, true, testDeadLetterHandoffLease)
			require.NoError(t, failErr)
			require.True(t, outcome.Exhausted)
		case model.EventOutboxStatusDeadLettered:
			// DEAD-LETTERED AND THEREFORE SPARED, however old it is (SEC-08). It is the only
			// record that a ledger event went undelivered, so age must never remove it: it
			// leaves the inventory by being REPLAYED, which makes it dispatched, and only then
			// is it a receipt this sweep may delete.
			require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, token, entry.Topic+".dlt",
				json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`), record))
		}

		if !spec.purgeable {
			survivors[spec.status] = entry.EventID

			continue
		}

		purgeableCount++
		if oldest.IsZero() || entry.OccurredAt.Before(oldest) {
			oldest = entry.OccurredAt
		}
		if entry.OccurredAt.After(newest) {
			newest = entry.OccurredAt
		}
	}

	survivor := newEventOutboxFixture(marker)
	survivor.OccurredAt = dbTimestamp(time.Now().Add(-time.Hour))
	insertRealEventOutbox(t, ds, survivor)
	require.NoError(t, ds.MarkEventDispatched(ctx, survivor.ID,
		claimEventOutboxToken(t, ds, survivor),
		model.BrokerRecord{Topic: survivor.Topic, Partition: 1, Offset: 9500}))

	cutoff := dbTimestamp(time.Now().AddDate(0, 0, -30))
	purged, err := ds.PurgeTerminalEventsBefore(ctx, cutoff, 100)
	require.NoError(t, err)
	require.GreaterOrEqual(t, purged, purgeableCount,
		"every purgeable fixture must be removed")

	// The `failed` row is still there, and it must be: its dead-letter write never landed,
	// so this row is the event's only surviving copy.
	surviving, err := ds.GetEventByID(ctx, survivors[model.EventOutboxStatusFailed])
	require.NoError(t, err,
		"retention must never purge a `failed` row; it is the only copy of an event whose "+
			"dead-letter write did not complete, and deleting it would BE the loss the "+
			"reconciliation exists to detect")
	require.NotNil(t, surviving)
	assert.Equal(t, model.EventOutboxStatusFailed, surviving.Status)

	// And so is the dead-lettered one, at 37 days against a 30-day cutoff. This is the
	// assertion that fails if eligibility is ever widened back to the terminal SET: the row
	// carries the failure metadata an operator triages from and the bytes a replay is driven
	// from, and no timer may take them.
	spared, err := ds.GetEventByID(ctx, survivors[model.EventOutboxStatusDeadLettered])
	require.NoError(t, err,
		"retention must never purge a dead-lettered row; it is the only record that a ledger "+
			"event went undelivered")
	require.NotNil(t, spared)
	assert.Equal(t, model.EventOutboxStatusDeadLettered, spared.Status)

	after, err := ds.SumPurgedTerminalEvents(ctx)
	require.NoError(t, err)

	assert.Equal(t, before.Batches+1, after.Batches,
		"one batch removed rows, so exactly one log row may be written")
	assert.Equal(t, before.RowsRemoved+purged, after.RowsRemoved,
		"the recorded total must equal what the DELETE actually removed; if the two can "+
			"differ, the reconciliation's baseline is wrong by that difference and nothing "+
			"reveals it")
	assert.Equal(t, before.ConfirmedRemoved+2, after.ConfirmedRemoved,
		"only the two rows carrying a broker coordinate count towards the confirmed subset; "+
			"conflating it with the total would corrupt the row-to-record mapping, which is "+
			"the part of the verdict that carries its soundness")

	require.NotNil(t, after.LastPurgedAt, "a recorded batch must be dated")
	require.NotNil(t, after.NewestPurgedOccurrence,
		"the purged window's upper bound is what tells an operator whether a measurement "+
			"window overlaps a purge")
	assert.False(t, after.NewestPurgedOccurrence.After(cutoff),
		"nothing at or after the cutoff may have been purged")
	assert.True(t, after.PurgeHasOccurred(),
		"the reconciliation must be able to see that a purge has happened at all")

	// The survivor proves the cutoff bounded the deletion rather than the batch limit.
	remaining, err := ds.GetEventByID(ctx, survivor.EventID)
	require.NoError(t, err)
	require.NotNil(t, remaining)

	// The interval recorded must be the interval deleted, not merely inside it.
	var loggedOldest, loggedNewest time.Time
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT oldest_occurred_at, newest_occurred_at
		FROM blnk.event_outbox_purge_log
		ORDER BY id DESC LIMIT 1
	`).Scan(&loggedOldest, &loggedNewest))
	assert.False(t, loggedOldest.After(oldest.UTC()),
		"the recorded lower bound must reach the oldest row actually removed")
	assert.False(t, loggedNewest.Before(newest.UTC()),
		"and the upper bound must reach the newest one")
}

// auditCoordinatesByPartition reads the audit and returns the coordinates of ONE topic,
// keyed by partition.
//
// It exists so a test can find out what a shared blnk.event_outbox already claims before it
// adds claims of its own. The audit is a whole-table aggregate and the topic namespace is
// closed — model.IsBlnkEventTopic admits only `<prefix>.<category>` and its `.dlt` sibling —
// so a test cannot isolate itself by inventing a topic; it has to place its fixtures where
// nothing else has, and that requires reading first.
//
// Parameters:
//   - t *testing.T: the test.
//   - ctx context.Context: bounds the read.
//   - ds Datasource: the live database.
//   - topic string: the topic whose partitions are wanted.
//
// Returns:
//   - map[int]model.EventRecordCoordinate: one entry per partition the audit reports for
//     topic. Empty when it reports none, which is the answer a virgin table gives.
func auditCoordinatesByPartition(
	t *testing.T,
	ctx context.Context,
	ds Datasource,
	topic string,
) map[int]model.EventRecordCoordinate {
	t.Helper()

	audit, err := ds.AuditEventRecordCoordinates(ctx)
	require.NoError(t, err, "the coordinate audit must be readable before fixtures are placed")

	byPartition := make(map[int]model.EventRecordCoordinate, len(audit.Coordinates))
	for _, coordinate := range audit.Coordinates {
		if coordinate.Topic == topic {
			byPartition[coordinate.Partition] = coordinate
		}
	}

	return byPartition
}

// TestAuditEventRecordCoordinates_ReportsPerPartitionClaims_RealDB covers the audit that
// turns the reconciliation from an inference into a check.
//
// Counting alone cannot tell a surplus of redeliveries from a surplus concealing an equal
// number of losses — ten of each produce the totals of a healthy pipeline. A coordinate can
// be CHECKED against the partition's live bounds: a claim at or beyond the end offset names
// a record the log does not contain. This asserts the audit supplies what that check needs —
// grouped per (topic, partition), with the extremes that bound the claims — and that rows
// claiming nothing are excluded rather than silently counted as verified.
func TestAuditEventRecordCoordinates_ReportsPerPartitionClaims_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("coord")
	quiesceEventOutbox(t, ds, marker)

	// THE COORDINATE SPACE THIS TEST WILL ASSERT ON, READ BEFORE ANYTHING IS INSERTED.
	//
	// The audit is a WHOLE-TABLE aggregate — the comment on the identity assertion at the
	// end of this test says so, and that assertion depends on it — so a (topic, partition)
	// group this test writes into may already hold rows: from an earlier run of this suite,
	// from the root package's live tiers, or from a shared development database. Two of the
	// assertions below used to be exact equalities against the literal 7 on partition 3,
	// which can only hold on a virgin table; on a shared one they read another writer's
	// largest claim (8514 was observed) and reported a defect in an audit that was correct.
	//
	// The third hazard is worse than a false failure. (kafka_topic, kafka_partition,
	// kafka_offset) is UNIQUE wherever an offset is present, so a hardcoded offset another
	// row already claims makes MarkEventDispatched fail outright — the test then fails on
	// its own fixture rather than on anything it is measuring.
	//
	// Reading the table first removes both, and it removes them without weakening a single
	// assertion. Because the topic namespace is closed — model.IsBlnkEventTopic admits only
	// `<prefix>.<category>` and its `.dlt` sibling, so a run-unique topic is not available
	// as an isolation mechanism — isolation has to be found inside the coordinate space
	// instead: the multi-claim fixtures are placed ABOVE the busy partition's existing
	// claims so the reported maximum is this test's largest claim exactly, and the
	// single-claim case is given a partition the audit does not currently report at all so
	// that BOTH of its extremes are that one claim and can be asserted exactly. The tier
	// lock openLockedEventOutboxDB holds is what makes the reading still true when the
	// assertions run.
	const auditTopic = "blnk.transactions"

	occupied := auditCoordinatesByPartition(t, ctx, ds, auditTopic)

	// The busy partition, whose claims must not collide with anything already there.
	const busyPartition = 0
	busyBase := int64(100)
	if existing, ok := occupied[busyPartition]; ok {
		busyBase = existing.MaxOffset + 1000
	}

	// The partition whose single claim is both of its extremes. Searched for rather than
	// assumed, because a literal is exactly what failed here.
	solePartition := 3
	for {
		if _, taken := occupied[solePartition]; !taken {
			break
		}
		solePartition++
	}
	const soleOffset = int64(7)

	// Two partitions of one topic with different claim counts and ranges, plus an
	// unconfirmed row that must not appear anywhere in the result. The busy partition's
	// three offsets are deliberately out of order, so the reported extremes are the
	// aggregate's answer rather than the insertion order's.
	type claim struct {
		partition int
		offset    int64
	}
	claims := []claim{
		{busyPartition, busyBase},
		{busyPartition, busyBase + 40},
		{busyPartition, busyBase + 20},
		{solePartition, soleOffset},
	}

	for _, spec := range claims {
		entry := newEventOutboxFixture(marker)
		entry.Topic = auditTopic
		insertRealEventOutbox(t, ds, entry)
		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, claimEventOutboxToken(t, ds, entry),
			model.BrokerRecord{Topic: entry.Topic, Partition: spec.partition, Offset: spec.offset}))
	}

	unconfirmed := newEventOutboxFixture(marker)
	unconfirmed.MaxAttempts = 1
	insertRealEventOutbox(t, ds, unconfirmed)
	outcome, err := ds.MarkEventFailed(ctx, unconfirmed.ID,
		claimEventOutboxToken(t, ds, unconfirmed), "no record was ever acknowledged", 0, false,
		testDeadLetterHandoffLease)
	require.NoError(t, err)
	require.True(t, outcome.Exhausted)

	audit, err := ds.AuditEventRecordCoordinates(ctx)
	require.NoError(t, err)
	require.NotNil(t, audit.Coordinates, "the slice must be usable without a nil check")
	assert.False(t, audit.MeasuredAt.IsZero(), "the audit must be dated")

	byPartition := make(map[int]model.EventRecordCoordinate, len(audit.Coordinates))
	for _, coordinate := range audit.Coordinates {
		if coordinate.Topic == auditTopic {
			byPartition[coordinate.Partition] = coordinate
		}
	}

	busy, ok := byPartition[busyPartition]
	require.True(t, ok, "the partition three rows claim must be reported")
	assert.Equal(t, occupied[busyPartition].Rows+3, busy.Rows,
		"exactly the three rows this test claimed on this partition may be added to its count")
	assert.LessOrEqual(t, busy.MinOffset, busyBase,
		"the smallest claim bounds the retention comparison")
	assert.Equal(t, busyBase+40, busy.MaxOffset,
		"the largest claim is what tests the partition's end offset, and it is the "+
			"comparison that yields direct evidence of a missing record. It is asserted "+
			"EXACTLY because the fixtures were placed above everything already in this "+
			"partition, so no other writer's claim can be the maximum")

	sole, ok := byPartition[solePartition]
	require.True(t, ok, "each claimed partition is reported separately")
	assert.Equal(t, int64(1), sole.Rows,
		"this partition was chosen because the audit reported nothing on it, so its count "+
			"is this test's single claim and nothing else")
	assert.Equal(t, soleOffset, sole.MinOffset)
	assert.Equal(t, soleOffset, sole.MaxOffset,
		"a single claim makes both extremes that claim")

	// Ordering is deterministic so a report reads the same way twice.
	for i := 1; i < len(audit.Coordinates); i++ {
		previous, current := audit.Coordinates[i-1], audit.Coordinates[i]
		if previous.Topic == current.Topic {
			assert.Less(t, previous.Partition, current.Partition,
				"partitions of one topic must ascend")

			continue
		}
		assert.Less(t, previous.Topic, current.Topic, "topics must ascend")
	}

	// The unconfirmed row claims no record, so nothing may account for it. Its absence is
	// what leaves it to be counted as unconfirmed — which is what makes the verdict
	// inconclusive while any such row exists.
	//
	// The check is against SQL rather than against a predicted partition set, because the
	// audit covers the WHOLE table: other rows, from other tests in this suite or another
	// clone running it, legitimately claim partitions this test knows nothing about.
	// Asserting a partition list would make the test pass or fail on who else is running.
	// What is invariant is the identity below — the audit accounts for exactly the rows
	// that carry a coordinate, no more and no fewer.
	assert.GreaterOrEqual(t, audit.TotalRows(), int64(len(claims)))

	var rowsWithACoordinate int64
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM blnk.event_outbox WHERE kafka_offset IS NOT NULL
	`).Scan(&rowsWithACoordinate))
	assert.Equal(t, rowsWithACoordinate, audit.TotalRows(),
		"the audit must account for every row that names a record and ONLY those: a row "+
			"with no coordinate counted here would read as verified when nothing corroborates "+
			"it, which is the surplus-masks-loss failure the audit exists to close")

	stillUnconfirmed, err := ds.GetEventByID(ctx, unconfirmed.EventID)
	require.NoError(t, err)
	require.NotNil(t, stillUnconfirmed)
	assert.Nil(t, stillUnconfirmed.KafkaOffset,
		"the fixture must genuinely carry no coordinate, or this test proves nothing")
}

// TestOldestDeadLetterAgeByTopic_IsOneGroupedAggregate is the PERF-P07 guard.
//
// The age gauge used to COUNT the whole inventory, derive a tail offset from that count, and
// then read up to 5,000 WHOLE rows from that deep offset every collection interval to find a
// minimum — three compounding costs to compute one number per topic, and past its scan cap it
// reported a LOWER BOUND, which is an age an alert cannot fire on.
//
// One MIN per group replaces all of it, exactly, and the group key is the resolved dead-letter
// topic so a row stranded in `failed` with no dlt_topic yet is attributed to the sibling it is
// owed rather than dropped.
func TestOldestDeadLetterAgeByTopic_IsOneGroupedAggregate(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	oldest := dbTimestamp(time.Now().Add(-42 * time.Minute))
	newer := dbTimestamp(time.Now().Add(-3 * time.Minute))

	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, ".dlt").
		WillReturnRows(sqlmock.NewRows([]string{"dead_letter_topic", "oldest", "outstanding"}).
			AddRow("blnk.transactions.dlt", oldest, int64(7)).
			AddRow("blnk.balances.dlt", newer, int64(1)))

	ages, err := ds.OldestDeadLetterAgeByTopic(context.Background(), ".dlt")
	require.NoError(t, err)

	require.Len(t, ages, 2)
	assert.Equal(t, "blnk.transactions.dlt", ages[0].Topic)
	assert.Equal(t, oldest, ages[0].Oldest.UTC())
	assert.Equal(t, int64(7), ages[0].Outstanding)
	assert.Equal(t, "blnk.balances.dlt", ages[1].Topic)
	assert.Equal(t, newer, ages[1].Oldest.UTC())
	assert.Equal(t, int64(1), ages[1].Outstanding)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "MIN(COALESCE(last_attempted_at, occurred_at))",
		"the age is measured from the last attempt, falling back to occurrence for a row that never attempted")
	assert.Contains(t, issued, "GROUP BY")
	assert.NotContains(t, issued, "OFFSET",
		"the deep tail offset this replaced is the cost PERF-P07 is about")
	assert.NotContains(t, issued, "payload_raw",
		"an aggregate over instants must not read a body")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestOldestDeadLetterAgeByTopic_ErrorsAreWrapped covers the statement, scan and iteration
// branches, so a failure to MEASURE the age is reported rather than read as an age of zero.
func TestOldestDeadLetterAgeByTopic_ErrorsAreWrapped(t *testing.T) {
	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"dead_letter_topic", "oldest", "outstanding"}).
					AddRow("blnk.transactions.dlt", "not-a-timestamp", int64(1)))
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(sqlmock.NewRows([]string{"dead_letter_topic", "oldest", "outstanding"}).
					AddRow("blnk.transactions.dlt", time.Now().UTC(), int64(1)).
					RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			script(mock)

			ages, err := ds.OldestDeadLetterAgeByTopic(context.Background(), ".dlt")
			assert.Nil(t, ages)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// planNodeReferencing returns the plan node that reaches needle: the scan line PLUS the
// qualification lines that belong to it — Index Cond, Recheck Cond, Filter — which are the
// more-indented lines that follow it before the next node.
//
// The qualifications are what decide whether a scan is confined, and the scan line alone cannot
// say. `Index Scan using <a status-leading index>` reads the whole partial set or three narrow
// key ranges depending entirely on whether an Index Cond restricts the status, and those are
// completely different costs — so an assertion about confinement has to read the condition.
//
// Parameters:
//   - planText string: the whole EXPLAIN output.
//   - needle string: the relation reference identifying the node.
//
// Returns:
//   - string: the node and its qualifications, newline-joined, or empty when the relation is
//     not reached at all.
func planNodeReferencing(planText, needle string) string {
	lines := strings.Split(planText, "\n")

	for index, line := range lines {
		if !strings.Contains(line, needle) {
			continue
		}

		node := []string{strings.TrimSpace(line)}
		indent := len(line) - len(strings.TrimLeft(line, " "))

		for _, following := range lines[index+1:] {
			trimmed := strings.TrimSpace(following)
			if trimmed == "" {
				break
			}

			followingIndent := len(following) - len(strings.TrimLeft(following, " "))
			if followingIndent <= indent || strings.Contains(trimmed, "->") {
				// A sibling or a child node, not a qualification of this one.
				break
			}

			node = append(node, trimmed)
		}

		return strings.Join(node, "\n")
	}

	return ""
}

// planScanIsConfinedToBlockingStates reports whether a plan node can only reach rows in the
// relay's blocking states.
//
// TWO ways to satisfy it, and both are genuine:
//
//   - the scan reads a PARTIAL index whose predicate is the blocking-state set, so terminal
//     rows are not in the index at all; or
//   - the scan carries an Index Cond restricting status to the blocking states, so terminal
//     rows are in the index but in other key ranges and are never read.
//
// Asserting the PROPERTY rather than a list of index names is what stops this from testing the
// planner's mood. It is also the stronger statement: a name list admits any plan through a named
// index, including a full partial-index scan with no condition at all, and rejects a plan that
// honours the guarantee through an index the list has not heard of.
//
// Parameters:
//   - node string: the plan node and its qualifications, from planNodeReferencing.
//
// Returns:
//   - bool: true when the node cannot read a terminal row.
func planScanIsConfinedToBlockingStates(node string) bool {
	for _, partial := range []string{
		"idx_event_outbox_claim",
		"idx_event_outbox_claim_order",
		"idx_event_outbox_effective_key_inflight",
		"idx_event_outbox_pending",
	} {
		if strings.Contains(node, partial) {
			return true
		}
	}

	// An index condition on status confines the scan by key range. It must name ONLY blocking
	// states: a condition mentioning a terminal literal would be reading exactly what the
	// guarantee forbids.
	for _, line := range strings.Split(node, "\n") {
		if !strings.HasPrefix(line, "Index Cond:") || !strings.Contains(line, "status") {
			continue
		}

		for _, terminal := range []string{
			model.EventOutboxStatusDispatched,
			model.EventOutboxStatusFailed,
			model.EventOutboxStatusDeadLettered,
			model.EventOutboxStatusReplaying,
		} {
			if strings.Contains(line, terminal) {
				return false
			}
		}

		if strings.Contains(line, model.EventOutboxStatusPending) ||
			strings.Contains(line, model.EventOutboxStatusProcessing) ||
			strings.Contains(line, model.EventOutboxStatusWebhookPending) {
			return true
		}
	}

	return false
}

// TestPlanScanIsConfinedToBlockingStates covers the confinement rule directly.
//
// It exists because the assertion that uses it lives in a _RealDB test whose plan depends on the
// planner, so the rule itself has to be provable without one. Every case below is a plan shape
// PostgreSQL actually produces for the claim query, and the rule has to get all of them right:
// the whole point of replacing a list of index names was to stop admitting plans that read
// terminal rows and stop rejecting plans that do not.
func TestPlanScanIsConfinedToBlockingStates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		node     string
		confined bool
		why      string
	}{
		{
			name:     "a blocking-state partial index needs no condition",
			node:     "->  Index Scan using idx_event_outbox_claim on event_outbox candidate",
			confined: true,
			why:      "terminal rows are not in the index at all",
		},
		{
			name:     "the claim-order index is equally partial",
			node:     "->  Index Scan using idx_event_outbox_claim_order on event_outbox candidate",
			confined: true,
			why:      "sql/1781249000.sql declares it over exactly the blocking states",
		},
		{
			name:     "the inflight index the anti-join probes",
			node:     "->  Index Scan using idx_event_outbox_effective_key_inflight on event_outbox earlier",
			confined: true,
		},
		{
			name: "a status-leading read-path index WITH a blocking-state condition",
			node: "->  Index Scan using idx_event_outbox_status_open on event_outbox candidate\n" +
				"Index Cond: (status = ANY ('{pending,processing,webhook_pending}'::text[]))\n" +
				"Filter: ((attempts < max_attempts) AND (next_attempt_at <= now()))",
			confined: true,
			why: "the terminal entries are in other key ranges of the same index and are " +
				"never visited, so the poll does not read them",
		},
		{
			name: "the same index with NO condition is NOT confined",
			node: "->  Index Scan using idx_event_outbox_status_open on event_outbox candidate\n" +
				"Filter: (status = ANY ('{pending,processing,webhook_pending}'::text[]))",
			confined: false,
			why: "a filter is applied AFTER the row is read, so every dead-lettered and failed " +
				"entry in the index is read and discarded on every poll — which is exactly the " +
				"cost the guarantee forbids, and a name list would have admitted it",
		},
		{
			name: "a condition naming a terminal state is NOT confined",
			node: "->  Index Scan using idx_event_outbox_status_open on event_outbox candidate\n" +
				"Index Cond: (status = ANY ('{pending,dead_lettered}'::text[]))",
			confined: false,
			why:      "it reads dead-lettered rows by construction",
		},
		{
			name:     "a sequential scan is never confined",
			node:     "->  Seq Scan on event_outbox candidate\nFilter: (status = ANY ('{pending}'::text[]))",
			confined: false,
		},
		{
			name:     "the primary key is not confined",
			node:     "->  Index Scan using event_outbox_pkey on event_outbox\nIndex Cond: (id = 42)",
			confined: false,
			why:      "it indexes every row in every state",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.confined, planScanIsConfinedToBlockingStates(testCase.node),
				testCase.why)
		})
	}
}

// TestPlanNodeReferencing_CarriesTheQualificationsWithTheScan covers the node extractor.
//
// The qualification lines are the whole reason this exists: the scan line alone cannot say
// whether the scan is confined, so an extractor that returned only that line would make the
// confinement rule unable to tell the two plans in the cases above apart.
func TestPlanNodeReferencing_CarriesTheQualificationsWithTheScan(t *testing.T) {
	t.Parallel()

	plan := "Sort  (cost=86.86..86.88 rows=7 width=541)\n" +
		"  Sort Key: claimed.occurred_at, claimed.id\n" +
		"  CTE candidates\n" +
		"    ->  Limit  (cost=28.12..28.20 rows=7 width=28)\n" +
		"          ->  Hash Anti Join  (cost=14.01..28.02 rows=7 width=28)\n" +
		"                Hash Cond: (candidate.partition_key = earlier.partition_key)\n" +
		"                ->  Index Scan using idx_event_outbox_status_open on event_outbox candidate\n" +
		"                      Index Cond: (status = ANY ('{pending,processing,webhook_pending}'::text[]))\n" +
		"                      Filter: (attempts < max_attempts)\n" +
		"                ->  Index Scan using idx_event_outbox_claim on event_outbox earlier\n" +
		"                      Index Cond: (status = ANY ('{pending,processing}'::text[]))\n"

	candidate := planNodeReferencing(plan, "event_outbox candidate")

	assert.Contains(t, candidate, "idx_event_outbox_status_open")
	assert.Contains(t, candidate, "Index Cond: (status = ANY ('{pending,processing,webhook_pending}'::text[]))",
		"the condition that confines the scan must travel with it")
	assert.Contains(t, candidate, "Filter: (attempts < max_attempts)")
	assert.NotContains(t, candidate, "event_outbox earlier",
		"the SIBLING node must not be absorbed, or one node's condition would qualify another's scan")
	assert.NotContains(t, candidate, "Hash Cond",
		"nor may the PARENT's qualification, which belongs to the join rather than to this scan")

	earlier := planNodeReferencing(plan, "event_outbox earlier")
	assert.Contains(t, earlier, "idx_event_outbox_claim")
	assert.Contains(t, earlier, "Index Cond: (status = ANY ('{pending,processing}'::text[]))")

	assert.Empty(t, planNodeReferencing(plan, "event_outbox nonexistent"),
		"a relation the plan never reaches must return nothing, so require.NotEmpty can catch it")
}

// TestAuditTerminalEventRecords_CountsClaimsAndTheirCorroboration pins the audit query.
//
// The three counts and the predicate ARE the finding's resolution, so each is asserted against
// the SQL rather than only against a result:
//
//   - COUNT(kafka_offset) is what separates a claim that names a record from one that does not.
//     SQL COUNT of an expression ignores NULLs, which is why the confirmed count needs no CASE.
//   - COUNT(DISTINCT (…)) is the schema-integrity check: two rows naming one record would make
//     it lower than the confirmed count, which the partial unique index should make impossible.
//   - The predicate must include webhook_pending rows through kafka_dispatched_at. Restricting
//     it to the terminal statuses would leave their records unaccounted for on the broker side
//     and loosen the reconciliation during exactly the dual-delivery window it matters most in.
func TestAuditTerminalEventRecords_CountsClaimsAndTheirCorroboration(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	window := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Microsecond)

	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, window).
		WillReturnRows(sqlmock.NewRows([]string{"published_rows", "confirmed_rows", "distinct_records"}).
			AddRow(int64(1_000), int64(990), int64(990)))

	audit, err := ds.AuditTerminalEventRecords(context.Background(), window)
	require.NoError(t, err)

	assert.Equal(t, int64(1_000), audit.PublishedRows)
	assert.Equal(t, int64(990), audit.ConfirmedRows)
	assert.Equal(t, int64(10), audit.UnconfirmedRows())
	assert.False(t, audit.FullyConfirmed(),
		"ten rows claim a publication nothing corroborates, so the count cannot be trusted")
	assert.WithinDuration(t, time.Now(), audit.MeasuredAt, time.Minute)
	assert.Equal(t, window, audit.WindowStart,
		"the audit must REPORT the population it measured, or the broker side cannot be compared against the same one")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "COUNT(kafka_offset)")
	assert.Contains(t, issued, "COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))")
	assert.Contains(t, issued, "FILTER (WHERE kafka_offset IS NOT NULL)",
		"a row constructor of all NULLs is not NULL in PostgreSQL, so without the filter every "+
			"coordinate-less row collapses into one phantom record and DistinctRecords exceeds ConfirmedRows")

	// PERF-P05: the audit is WINDOWED, and both arms of the window are load-bearing.
	assert.Contains(t, issued, "WHERE kafka_dispatched_at >= $2",
		"the publication instant is what has to fall inside the window for the row to be comparable "+
			"against records the broker wrote inside it")
	assert.Contains(t, issued, "(status = $1 AND last_attempted_at >= $2)",
		"a dead-lettered row has no kafka_dispatched_at; excluding it would understate the outbox "+
			"side and manufacture an apparent surplus")
	assert.NotContains(t, issued, "kafka_dispatched_at IS NOT NULL",
		"the unbounded predicate this replaced scanned the whole history every time it was read")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditTerminalEventRecords_NormalisesTheWindow pins the same correction the status count
// applies, for the same reason: an unbounded audit is the cost PERF-P05 is about, and a future
// window would report zero published rows and read as total loss.
func TestAuditTerminalEventRecords_NormalisesTheWindow(t *testing.T) {
	for _, given := range []time.Time{{}, time.Now().UTC().Add(time.Hour)} {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		before := time.Now().UTC()

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WithArgs(model.EventOutboxStatusDeadLettered, sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"published_rows", "confirmed_rows", "distinct_records"}).
				AddRow(int64(0), int64(0), int64(0)))

		audit, err := ds.AuditTerminalEventRecords(context.Background(), given)
		require.NoError(t, err)

		assert.WithinDuration(t, before.Add(-defaultEventCountWindow), audit.WindowStart, time.Minute,
			"a window nobody chose must be exactly one default window wide")
		assert.True(t, audit.WindowStart.Before(time.Now().UTC()),
			"and must be in the past, or the audit reports zero published rows and reads as total loss")
		assert.NoError(t, mock.ExpectationsWereMet())
	}
}

// TestAuditTerminalEventRecords_ReportsAFailureRatherThanAnEmptyAudit keeps the verdict honest.
//
// A zero audit reads as "nothing has been published", which is a legitimate state on a fresh
// deployment — so returning one on a failed read would let a reconciliation conclude that
// everything is accounted for precisely when it could not measure.
func TestAuditTerminalEventRecords_ReportsAFailureRatherThanAnEmptyAudit(t *testing.T) {
	db, mock := newSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("FROM blnk.event_outbox").WillReturnError(sql.ErrConnDone)

	audit, err := ds.AuditTerminalEventRecords(context.Background(), time.Time{})
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.Zero(t, audit.PublishedRows)
	assert.Zero(t, audit.WindowStart,
		"a failed audit must report no window either, so a caller cannot read it as a measured one")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestAuditTerminalEventRecords_SeesTheWholePublishedSet_RealDB is the audit's own round trip.
//
// It builds each of the three shapes the predicate has to cover — a dispatched row that names its
// record, a webhook_pending row whose Kafka leg completed, and a dispatched row with no
// coordinate — and requires the audit to count them the way the reconciliation depends on. The
// webhook_pending case is the one most easily got wrong: it is not terminal, but it IS on the
// topic.
func TestAuditTerminalEventRecords_SeesTheWholePublishedSet_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()

	marker := newRealEventOutboxMarker("audit")
	t.Cleanup(func() { quiesceEventOutbox(t, ds, marker) })

	// One window for both readings, so the deltas below measure the same population twice
	// (PERF-P05). Wide enough to contain every fixture this test publishes.
	window := time.Now().UTC().Add(-time.Hour)

	before, err := ds.AuditTerminalEventRecords(ctx, window)
	require.NoError(t, err)

	confirmed := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	pending := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	unconfirmed := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	// A row that has NOT been published at all, which must not be counted by either total.
	untouched := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))

	claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
	require.NoError(t, err)

	tokens := make(map[string]string, 4)
	for _, row := range claimed {
		if isMarkedEventOutbox(row, marker) {
			tokens[row.EventID] = row.ClaimToken
		}
	}
	require.Len(t, tokens, 4)

	require.NoError(t, ds.MarkEventDispatched(ctx, confirmed.ID, tokens[confirmed.EventID],
		eventOutboxBrokerRecord(marker, 0, 1)))

	// Kafka leg done, HTTP leg outstanding. Not terminal, but on the topic — so it counts.
	_, err = ds.MarkEventWebhookPending(ctx, pending.ID, tokens[pending.EventID],
		"queue unreachable", time.Minute, eventOutboxBrokerRecord(marker, 1, 2))
	require.NoError(t, err)

	// Published, acknowledged, but the library reported no coordinate.
	require.NoError(t, ds.MarkEventDispatched(ctx, unconfirmed.ID, tokens[unconfirmed.EventID],
		model.BrokerRecord{}))

	after, err := ds.AuditTerminalEventRecords(ctx, window)
	require.NoError(t, err)

	assert.Equal(t, window.Truncate(time.Microsecond), after.WindowStart.Truncate(time.Microsecond),
		"the audit must report back the window it was asked for, since that is what the broker side is measured over")

	assert.Equal(t, int64(3), after.PublishedRows-before.PublishedRows,
		"the dispatched, the webhook_pending and the unconfirmed rows all claim a record; the "+
			"untouched row (%d) does not", untouched.ID)
	assert.Equal(t, int64(2), after.ConfirmedRows-before.ConfirmedRows,
		"only the two rows carrying a coordinate name a record")
	assert.Equal(t, int64(2), after.DistinctRecords-before.DistinctRecords,
		"and those two coordinates are distinct")

	// THE COORDINATE-LESS ROW MUST NOT COUNT AS A RECORD, and this is the assertion that says
	// so unconditionally.
	//
	// The delta above used to be 3 on an EMPTY table and 2 on a polluted one, because
	// COUNT(DISTINCT (topic, partition, offset)) counts the all-NULL row constructor as a
	// distinct value: the unconfirmed row contributed one, unless some earlier test had already
	// left a coordinate-less published row that contributed it first. So the test passed on
	// residue and failed the moment fixtures started cleaning up after themselves.
	//
	// Stated as an INVARIANT rather than a delta because that is the contract both readers of
	// this audit rely on: FullyConfirmed requires the two to be equal, and
	// ReconcileAgainstOutbox derives duplication from their difference, so DistinctRecords
	// exceeding ConfirmedRows is not a smaller version of the same answer — it is a
	// wrong one.
	assert.LessOrEqual(t, after.DistinctRecords, after.ConfirmedRows,
		"a row that names no coordinate names no record: DistinctRecords may never exceed "+
			"ConfirmedRows, or FullyConfirmed reports an inconclusive verdict on a healthy outbox "+
			"and ReconcileAgainstOutbox derives a negative duplication count")

	// The verdict this produces is the whole point: an unconfirmed claim makes it inconclusive
	// rather than green, however favourable the totals look.
	assert.False(t, after.FullyConfirmed(),
		"one row claims a publication it cannot name a record for, so the count is not trustworthy")
}

// TestListDeadLetteredEvents_CoversBothTerminalFailureStates asserts the filter includes
// failed AS WELL AS dead_lettered.
//
// A row becomes failed the moment its retry budget is spent, and dead_lettered only once
// the event has additionally been written to its <topic>.dlt sibling. Listing only the
// latter would HIDE events that exhausted their retries but whose dead-letter publication
// itself failed — which are exactly the events an operator most needs to see, because they
// are the ones sitting nowhere at all. The partial index idx_event_outbox_failed covers
// both literals for this reason.
func TestListDeadLetteredEvents_CoversBothTerminalFailureStates(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	deadLettered := model.EventOutbox{
		ID: 2, EventID: "evt_dlt", EventType: "transaction.applied", AggregateID: "agg",
		LedgerID: "ldg", Topic: "blnk.transactions", SchemaVersion: model.SchemaVersionV1,
		Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		Status: model.EventOutboxStatusDeadLettered, Attempts: 5, MaxAttempts: 5,
		DLTTopic: "blnk.transactions.dlt",
	}
	stranded := deadLettered
	stranded.ID, stranded.EventID, stranded.Status, stranded.DLTTopic =
		1, "evt_stranded", model.EventOutboxStatusFailed, ""

	// The population is bound as the two status literals, which is the ONE rendering the
	// page, the count and the age gauge all share — see deadLetterFilterClause.
	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, 50, 0).
		WillReturnRows(newEventOutboxRows(deadLettered, stranded))

	entries, err := ds.ListDeadLetteredEvents(context.Background(), model.DeadLetterQuery{})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt_dlt", "evt_stranded"}, eventIDsOf(entries),
		"an event stranded in failed, whose dead-letter publication itself failed, must still be visible to an operator")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "WHERE status IN ($1, $2)")
	assert.Contains(t, issued, "ORDER BY occurred_at DESC, id DESC",
		"triage starts from the most recent failures, and id breaks ties so paging cannot show or skip a row twice")
	assert.Contains(t, issued, "LIMIT $3 OFFSET $4")
	assert.NotContains(t, issued, "resolved_at",
		"the resolution column is gone from the schema: retention now spares every dead-lettered "+
			"row and a replay is what makes one purgeable, so no listing may narrow on it")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListDeadLetteredEventsFiltered_AppliesEveryPredicateInSQL is the SEC-09 guard.
//
// # The defect
//
// A filtered page used to be assembled by reading UNFILTERED pages from the repository and
// applying the predicates in Go, abandoning the walk after five thousand rows and logging a
// warning. An operator filtering for a stuck event type therefore received a short page
// that was indistinguishable from a complete one, and the only record that the scan had
// given up was a log line they were not reading. During a loss investigation "nothing more
// is stuck" is the most dangerous wrong answer this endpoint can give.
//
// The test asserts the predicates reach SQL, because that is what removes the bound rather
// than reporting it: with a WHERE clause there is no walk to truncate, and LIMIT/OFFSET
// pages the FILTERED set so an offset still skips matches rather than rows.
func TestListDeadLetteredEventsFiltered_AppliesEveryPredicateInSQL(t *testing.T) {
	cases := []struct {
		name string

		filter model.DeadLetterInventoryFilter

		// limit and offset are the page as REQUESTED; zero exercises the repository default.
		limit, offset int

		// wantClauses are the SQL fragments that must appear, in the placeholder
		// numbering the builder produces.
		wantClauses []string

		// wantArgs is every bound argument, in order, including the status population and
		// the trailing LIMIT/OFFSET pair.
		wantArgs []driver.Value

		// forbidden are fragments that must NOT appear, which is how "this filter was
		// not silently widened" is checked.
		forbidden []string
	}{
		{
			name:   "event type",
			filter: model.DeadLetterInventoryFilter{EventType: "transaction.applied"},
			wantClauses: []string{
				"status IN ($1, $2)", "event_type = $3", "LIMIT $4 OFFSET $5",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"transaction.applied", defaultDeadLetterPageSize, 0,
			},
			forbidden: []string{"topic = $", "resolved_at"},
		},
		{
			// The STORED topic column, matched exactly. topic is NOT NULL and
			// CHECK-constrained non-blank, so this is equivalent to the Go-side
			// original-topic resolution it replaced for every row the table can hold.
			name:        "original topic",
			filter:      model.DeadLetterInventoryFilter{Topic: "blnk.transactions"},
			wantClauses: []string{"topic = $3", "LIMIT $4 OFFSET $5"},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				"blnk.transactions", defaultDeadLetterPageSize, 0,
			},
			forbidden: []string{"event_type = $"},
		},
		{
			// A status filter REPLACES the two-literal population rather than being added
			// to it: the requested status is one of those two, so the wider predicate
			// beside it would be redundant. The narrowing is what is asserted — that the
			// statement selects one state and not both.
			name:        "status narrows the population further",
			filter:      model.DeadLetterInventoryFilter{Status: model.EventOutboxStatusFailed},
			wantClauses: []string{"status = $1", "LIMIT $2 OFFSET $3"},
			wantArgs: []driver.Value{
				model.EventOutboxStatusFailed, defaultDeadLetterPageSize, 0,
			},
			forbidden: []string{"status IN ("},
		},
		{
			// THE OCCURRENCE WINDOW, both ends bound inclusively, which is the narrowing an
			// operator correlating a backlog against an incident timeline actually uses.
			name: "occurrence window",
			filter: model.DeadLetterInventoryFilter{
				OccurredFrom: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				OccurredTo:   time.Date(2024, 6, 2, 0, 0, 0, 0, time.UTC),
			},
			wantClauses: []string{
				"occurred_at >= $3", "occurred_at <= $4", "LIMIT $5 OFFSET $6",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
				time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2024, 6, 2, 0, 0, 0, 0, time.UTC),
				defaultDeadLetterPageSize, 0,
			},
			forbidden: []string{"event_type = $", "resolved_at"},
		},
		{
			// Every predicate at once, which is what proves the placeholder arithmetic
			// holds as clauses accumulate rather than only in isolation.
			name: "every predicate together",
			filter: model.DeadLetterInventoryFilter{
				EventType: "balance.created",
				Topic:     "blnk.balances",
				Status:    model.EventOutboxStatusDeadLettered,
			},
			limit:  25,
			offset: 10,
			wantClauses: []string{
				"status = $1", "event_type = $2", "topic = $3", "LIMIT $4 OFFSET $5",
			},
			wantArgs: []driver.Value{
				model.EventOutboxStatusDeadLettered, "balance.created", "blnk.balances", 25, 10,
			},
			forbidden: []string{"resolved_at"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, captured := newCapturingSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery("").WithArgs(tc.wantArgs...).WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEventsFiltered(
				context.Background(), tc.filter, tc.limit, tc.offset)
			require.NoError(t, err)

			require.Len(t, *captured, 1)
			issued := (*captured)[0]
			for _, clause := range tc.wantClauses {
				assert.Contains(t, issued, clause,
					"the predicate must reach SQL; filtering in Go is what allowed a page to be "+
						"silently short")
			}
			predicate := whereClauseOf(t, issued)
			for _, clause := range tc.forbidden {
				assert.NotContains(t, predicate, clause,
					"a filter that was not requested must not appear: an inventory endpoint that "+
						"narrows what it was not asked to narrow reports less loss than there is")
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestCountDeadLetteredEvents_CountsTheSamePopulationTheListingPages is the other half of
// SEC-09.
//
// include_count used to be REFUSED alongside an event_type or topic filter, because no
// filter-aware count existed. That refusal removed the one check that would have caught the
// truncated walk: an operator could not compare their page against a total. The count must
// therefore render the IDENTICAL predicate the listing renders — which it does by sharing
// the builder, and which this test pins by comparing the two statements clause for clause.
func TestCountDeadLetteredEvents_CountsTheSamePopulationTheListingPages(t *testing.T) {
	filter := model.DeadLetterInventoryFilter{
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Status:    model.EventOutboxStatusDeadLettered,
	}

	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, "transaction.applied",
			"blnk.transactions", defaultDeadLetterPageSize, 0).
		WillReturnRows(newEventOutboxRows())
	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, "transaction.applied",
			"blnk.transactions").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(417)))

	_, err := ds.ListDeadLetteredEventsFiltered(context.Background(), filter, 0, 0)
	require.NoError(t, err)

	total, err := ds.CountDeadLetteredEvents(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, int64(417), total)

	require.Len(t, *captured, 2)
	listWhere := whereClauseOf(t, (*captured)[0])
	countWhere := whereClauseOf(t, (*captured)[1])

	assert.Equal(t, listWhere, countWhere,
		"the page and its total must be about the SAME rows; two independently assembled "+
			"predicates would let a total describe a population the page was not drawn from, "+
			"and that comparison is the completeness check the truncated walk defeated")
	assert.Contains(t, (*captured)[1], "SELECT COUNT(*)")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// whereClauseOf extracts a statement's WHERE clause up to the next top-level keyword, so
// two statements that SELECT different things can be compared on the predicate alone.
func whereClauseOf(t *testing.T, statement string) string {
	t.Helper()

	idx := strings.Index(statement, "WHERE ")
	require.NotEqual(t, -1, idx, "statement has no WHERE clause: %s", statement)

	clause := statement[idx+len("WHERE "):]
	// RETURNING is cut for the same reason the rest are: an UPDATE's projection legitimately
	// names every column of the row, so a predicate assertion made over the whole statement
	// would assert the opposite of what it reads as.
	for _, keyword := range []string{"ORDER BY", "LIMIT", "GROUP BY", "RETURNING"} {
		if cut := strings.Index(clause, keyword); cut != -1 {
			clause = clause[:cut]
		}
	}

	return strings.Join(strings.Fields(clause), " ")
}

// TestListDeadLetteredEvents_NormalisesPagination asserts a malformed page request
// degrades to a sane one rather than scanning the table or failing.
//
// The upper cap is the load-bearing half: without it a caller could turn a triage
// endpoint into a full table scan of an outbox that grows without bound, which is a
// denial-of-service surface reachable from an authenticated operator endpoint.
func TestListDeadLetteredEvents_NormalisesPagination(t *testing.T) {
	cases := []struct {
		name                  string
		limit, offset         int
		wantLimit, wantOffset int
	}{
		{"zero limit takes the default", 0, 0, defaultDeadLetterPageSize, 0},
		{"negative limit takes the default", -10, 0, defaultDeadLetterPageSize, 0},
		{"oversized limit is capped", maxDeadLetterPageSize + 1, 0, maxDeadLetterPageSize, 0},
		{"limit at the cap is preserved", maxDeadLetterPageSize, 0, maxDeadLetterPageSize, 0},
		{"negative offset is clamped to zero", 10, -5, 10, 0},
		{"a valid page is passed through unchanged", 25, 75, 25, 75},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), tc.wantLimit, tc.wantOffset).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEvents(context.Background(),
				model.DeadLetterQuery{Limit: tc.limit, Offset: tc.offset})
			require.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetteredEvents_ErrorsAreWrapped covers the statement, scan and row
// iteration branches of the listing.
func TestListDeadLetteredEvents_ErrorsAreWrapped(t *testing.T) {
	valid := model.EventOutbox{
		ID: 1, EventID: uuid.NewString(), EventType: "transaction.applied", AggregateID: "agg",
		PartitionKey: "agg", LedgerID: "ldg", Topic: "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		Status: model.EventOutboxStatusDeadLettered, MaxAttempts: 5,
	}

	cases := map[string]func(sqlmock.Sqlmock){
		"statement error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnError(sql.ErrConnDone)
		},
		"scan error": func(mock sqlmock.Sqlmock) {
			// Built from the real row builder with occurred_at corrupted BY POSITION,
			// so the cell count can never drift away from the projection.
			cells := eventOutboxRow(valid)
			cells[indexOfProjectedColumn(t, "occurred_at")] = "not-a-timestamp"

			broken := sqlmock.NewRows(eventOutboxProjectedColumns()).AddRow(cells...)
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).WillReturnRows(broken)
		},
		"row iteration error": func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
				WillReturnRows(newEventOutboxRows(valid).RowError(0, sql.ErrConnDone))
		},
	}

	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			ds := Datasource{Conn: db}

			script(mock)

			entries, err := ds.ListDeadLetteredEvents(context.Background(),
				model.DeadLetterQuery{Limit: 10})
			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestListDeadLetteredEvents_PagesNewestFirst_RealDB asserts the ordering and paging the
// dead-letter API relies on, against real SQL.
//
// Triage starts from the most recent failures, so the listing is newest-first. The paging
// assertion matters just as much as the ordering: with a non-deterministic tie-break a
// row can appear on two pages or on none, and an operator working through a dead-letter
// backlog would silently skip events.
func TestListDeadLetteredEvents_PagesNewestFirst_RealDB(t *testing.T) {
	ds := openLockedEventOutboxDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("dlt")
	quiesceEventOutbox(t, ds, marker)

	const total = 5
	base := dbTimestamp(time.Now().Add(-time.Hour))
	oldestFirst := make([]string, 0, total)
	for i := 0; i < total; i++ {
		entry := newEventOutboxFixture(marker)
		entry.OccurredAt = base.Add(time.Duration(i) * time.Second)
		insertRealEventOutbox(t, ds, entry)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, claimEventOutboxToken(t, ds, entry),
			entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`), model.BrokerRecord{}))
		oldestFirst = append(oldestFirst, entry.EventID)
	}

	// The expected listing order is the reverse of the insertion order, because the
	// fixtures were inserted oldest-occurrence first and the listing is newest-first.
	newestFirst := make([]string, 0, len(oldestFirst))
	for i := len(oldestFirst) - 1; i >= 0; i-- {
		newestFirst = append(newestFirst, oldestFirst[i])
	}

	mineFrom := func(entries []model.EventOutbox) []string {
		mine := make([]string, 0, len(entries))
		for _, entry := range entries {
			if isMarkedEventOutbox(entry, marker) {
				mine = append(mine, entry.EventID)
			}
		}
		return mine
	}

	// A single page large enough to hold every fixture: assert the ordering.
	all, err := ds.ListDeadLetteredEvents(ctx,
		model.DeadLetterQuery{Limit: maxDeadLetterPageSize})
	require.NoError(t, err)
	assert.Equal(t, newestFirst, mineFrom(all),
		"the dead-letter inventory must be ordered newest occurrence first, because triage starts from the most recent failures")

	// Walk the fixtures a page at a time and assert every one appears exactly once
	// across the pages.
	seen := map[string]int{}
	for offset := 0; offset < len(all); offset += 2 {
		page, pageErr := ds.ListDeadLetteredEvents(ctx,
			model.DeadLetterQuery{Limit: 2, Offset: offset})
		require.NoError(t, pageErr)
		for _, eventID := range mineFrom(page) {
			seen[eventID]++
		}
	}
	for _, eventID := range oldestFirst {
		assert.Equal(t, 1, seen[eventID],
			"event %s appeared %d times across the pages; a non-deterministic tie-break makes an operator skip or repeat events", eventID, seen[eventID])
	}
}

// requireColdHistoryExcludingIndex asserts that the index a plan line reaches the table
// through is PARTIAL and excludes the dispatched state, which is the guarantee the claim's
// index set exists to provide.
//
// # Why the property and not a name
//
// This replaced a list of index names that had to be widened twice, each time by a plan that
// honoured the guarantee completely. Three indexes on blnk.event_outbox qualify —
// idx_event_outbox_claim, idx_event_outbox_effective_key_inflight and
// idx_event_outbox_status_open — and PostgreSQL chooses between them on cost, which moves with
// heap compaction, statistics freshness and host load. A name list therefore encoded a guess
// about the planner rather than a requirement on the schema, and it failed as a test while the
// system was correct.
//
// The requirement is that the poll's cost does not grow with the outbox's history. The cold
// history seeded by the caller is three thousand DISPATCHED rows against a working set of
// twenty, so an index whose predicate excludes dispatched cannot be read past its working set
// however large that history becomes. That is checked against pg_indexes rather than inferred
// from a name, so a non-partial index — or a partial one whose predicate admits dispatched —
// fails here even if it is added tomorrow under a name nobody thought to forbid.
//
// A note on what is deliberately NOT required: that the predicate exclude every terminal
// state. idx_event_outbox_status_open admits failed and dead_lettered, and that is acceptable
// because those rows are the dead-letter inventory an operator is expected to clear — bounded
// by triage, alerted on by blnk_dlt_oldest_message_age_seconds, and orders of magnitude
// smaller than the dispatched history in any healthy pipeline. Requiring their exclusion would
// reject a legitimate index for a population the system already has a control for.
//
// # Why the predicate is EVALUATED rather than parsed
//
// A predecessor read the same predicate out of pg_indexes — which was the right move, and the
// reason it replaced the name list — and then decided whether it admitted the dispatched state
// with a four-arm switch over its TEXT. Both of that switch's failure modes pointed the wrong
// way: its verdict started at "admits dispatched" and only the four recognised spellings
// cleared it, so a correct predicate written a fifth way was reported as a defect; and its
// `status = ANY (ARRAY[...])` arm left the verdict standing whenever the list could not be cut
// out of the string, turning a rendering change into a failure. Splicing the predicate into a
// COUNT over the seeded rows needs none of those cases and cannot be defeated by a rewrite,
// because the evaluator that answers the question is the one that decides what the index holds.
//
// Parameters:
//   - t *testing.T: the test.
//   - ds Datasource: used for its connection to read pg_indexes.
//   - planLine string: the EXPLAIN line reaching the relation under assertion.
//   - planText string: the whole plan, included in a failure so it can be read in context.
//   - subject string: what the line is doing, for the failure message.
//   - marker string: the caller's row-id prefix, so the predicate is evaluated against THIS
//     test's seeded rows and not against whatever a concurrent run left in the shared table.
func requireColdHistoryExcludingIndex(
	t *testing.T,
	ds Datasource,
	planLine, planText, subject, marker string,
) {
	t.Helper()

	index := indexNameFromPlanLine(planLine)
	require.NotEmpty(t, index,
		"%s must name the index it read: neither an \"Index Scan using <name>\" nor a "+
			"\"Bitmap Index Scan on <name>\" could be parsed out of it.\nPlan line was: %s\nPlan was:\n%s",
		subject, planLine, planText)

	var definition string
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'blnk' AND tablename = 'event_outbox' AND indexname = $1
	`, index).Scan(&definition),
		"%s read index %q, which must exist in the blnk schema", subject, index)

	predicate, partial := indexPredicate(definition)
	require.True(t, partial,
		"%s must be served by a PARTIAL index. %q covers every row, so the poll reads the "+
			"terminal history on every tick and its cost grows with the whole table.\nIndex was: %s",
		subject, index, definition)

	// EVALUATED BY POSTGRESQL, not pattern-matched. The predicate is spliced into a count over
	// the rows this test seeded, so the question asked is the one that matters — "can a
	// dispatched row be in this index?" — and it is answered by the same expression evaluator
	// that decides what the index actually contains.
	//
	// A textual check cannot do this. The first attempt asserted the predicate did not mention
	// 'dispatched' and failed on `status <> 'dispatched'`, which is the predicate that excludes
	// it most explicitly of all. Every equivalent rewrite — a negation, an ANY over the
	// complement, a NOT IN — would have needed its own special case, and each one is a chance to
	// accept a predicate that admits the history. Counting is exact and needs none of them.
	//
	// Splicing SQL is safe here and only here: the text comes from pg_indexes for one fixed
	// table in a test, so there is no untrusted input and no user-facing query involved.
	// Both counts in one round trip, and the denominator is MEASURED rather than restated from
	// the caller's constant: the message then reports what the table actually held when the plan
	// was taken, which is the only number a reader can act on.
	var seededCold, coldInIndex int
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT COUNT(*) FILTER (WHERE status = 'dispatched'),
		       COUNT(*) FILTER (WHERE status = 'dispatched' AND (`+predicate+`))
		FROM blnk.event_outbox
		WHERE event_id LIKE $1
	`, marker+"%").Scan(&seededCold, &coldInIndex),
		"evaluating index %q's predicate against the seeded rows must succeed.\nPredicate was: %s",
		index, predicate)

	require.NotZero(t, seededCold,
		"the premise must hold: this assertion is meaningless unless a cold history was seeded, "+
			"and finding none means the fixture did not land rather than that the index is sound")

	assert.Zero(t, coldInIndex,
		"%s must be served by an index that EXCLUDES the delivered history, which is what keeps "+
			"the poll's cost independent of how large that history has grown. Index %q admits %d "+
			"of the %d dispatched rows this test seeded, so the relay would read them on every "+
			"poll.\nPredicate was: %s\nPlan line was: %s\nPlan was:\n%s",
		subject, index, coldInIndex, seededCold, predicate, planLine, planText)
}

// indexPredicate returns the WHERE clause of an index definition.
//
// The clause is read out of pg_indexes' own rendering rather than being reconstructed, so the
// assertion above compares against what PostgreSQL says the index is, not against what a
// migration file says it should be — which is the difference between checking the database and
// checking a document.
//
// Parameters:
//   - definition string: an indexdef as pg_indexes reports it.
//
// Returns:
//   - string: the predicate text, without the WHERE keyword. Empty when the index is total.
//   - bool: whether the index is partial at all.
func indexPredicate(definition string) (string, bool) {
	_, after, found := strings.Cut(definition, " WHERE ")
	if !found {
		return "", false
	}

	return strings.TrimSpace(after), true
}

// TestTerminalTransitionsHoldTheSettlementLease_RealDB is the two-claimant hand-off, walked
// against the real table for BOTH terminal transitions.
//
// # The race it closes
//
// A worker that exhausts a row's retry budget still owes two things: the write to the
// `<topic>.dlt` sibling, and the MarkEventDeadLettered that records it. Its claim token is
// retained so that only it may perform them. That was not enough, because the dead-letter
// repair claim admits any failed row with no dlt_topic whose lease is released — and both
// terminal transitions released the lease. A second relay could therefore claim the row, stamp
// its own token and publish the event to the dead-letter topic while the owner was still
// publishing it there. The database refused the owner's now-stale MarkEventDeadLettered, so the
// TABLE stayed consistent and the TOPIC did not: the same failed event appeared twice, and an
// operator triaging a backlog cannot distinguish that from a genuinely repeated failure.
//
// # What is asserted, and why in this order
//
// Exclusivity first, recovery second. Exclusivity alone would be a wall rather than a window —
// an owner that dies mid-settlement would strand the event in the only table that holds it — so
// the expiry case is asserted immediately afterwards to pin that the lease is a lease.
//
// Both transitions are covered because they reach the terminal state by different routes:
// MarkEventFailed by spending the budget, MarkEventPermanentlyFailed by the publisher's verdict
// with budget to spare. Fixing one and not the other would leave the race live on whichever was
// missed, and nothing else in the suite would notice.
func TestTerminalTransitionsHoldTheSettlementLease_RealDB(t *testing.T) {
	cases := map[string]struct {
		maxAttempts int
		exhaust     func(t *testing.T, ds Datasource, ctx context.Context, id int64, token string) model.EventFailureOutcome
	}{
		"the exhaustion arm of MarkEventFailed": {
			maxAttempts: 1,
			exhaust: func(t *testing.T, ds Datasource, ctx context.Context, id int64, token string) model.EventFailureOutcome {
				t.Helper()

				outcome, err := ds.MarkEventFailed(ctx, id, token, "broker unavailable", 0, false, testSettlementLease)
				require.NoError(t, err)
				require.True(t, outcome.Exhausted, "a one-attempt budget must be spent by one failure")

				return outcome
			},
		},
		"MarkEventPermanentlyFailed with budget to spare": {
			maxAttempts: 5,
			exhaust: func(t *testing.T, ds Datasource, ctx context.Context, id int64, token string) model.EventFailureOutcome {
				t.Helper()

				outcome, err := ds.MarkEventPermanentlyFailed(ctx, id, token, "topic authorization failed", testSettlementLease)
				require.NoError(t, err)
				require.True(t, outcome.Exhausted, "a permanent failure ends the event whatever the budget says")

				return outcome
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ds := openLockedEventOutboxDB(t)
			ctx := context.Background()
			marker := newRealEventOutboxMarker("dltlease")
			quiesceEventOutbox(t, ds, marker)

			entry := newEventOutboxFixture(marker)
			entry.OccurredAt = dbTimestamp(time.Now().Add(-time.Hour))
			entry.MaxAttempts = tc.maxAttempts
			insertRealEventOutbox(t, ds, entry)

			claimed, err := ds.ClaimPendingEventOutbox(ctx, 100, 5*time.Minute)
			require.NoError(t, err)

			var token string
			for _, row := range claimed {
				if row.EventID == entry.EventID {
					token = row.ClaimToken
				}
			}
			require.NotEmpty(t, token, "the fixture must be claimed before it can be failed")

			outcome := tc.exhaust(t, ds, ctx, entry.ID, token)
			require.Equal(t, model.EventOutboxStatusFailed, outcome.Status)
			require.Equal(t, token, outcome.ClaimToken,
				"the owner keeps its token, because only it may complete the dead-letter hand-off")

			// EXCLUSIVITY. The row is failed with no dlt_topic — the repair claim's whole target
			// set — and must still be unreachable, because its owner has not finished with it.
			held, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
			require.NoError(t, err)
			assert.False(t, containsEventID(held, entry.EventID),
				"a second claimant must not reach a row whose owner still holds the settlement lease; "+
					"if it does, both write the same failed event to the dead-letter topic")

			stored, err := ds.GetEventByID(ctx, entry.EventID)
			require.NoError(t, err)
			require.NotNil(t, stored.LockedUntil,
				"the terminal transition must leave a lease behind, not NULL")
			assert.True(t, stored.LockedUntil.After(time.Now()),
				"and that lease must still be in the future, or it protects nothing")
			assert.Equal(t, token, stored.ClaimToken,
				"the second claimant must not have re-stamped the row's token")

			// RECOVERY. Expiring the lease is what a dead owner looks like from the database's
			// side, and the row must then be repairable — with a FRESH token, because the
			// original owner is by hypothesis gone and must not be able to settle behind us.
			_, err = ds.Conn.ExecContext(ctx,
				"UPDATE blnk.event_outbox SET locked_until = NOW() - interval '1 minute' WHERE id = $1", entry.ID)
			require.NoError(t, err)

			repair, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
			require.NoError(t, err)
			require.True(t, containsEventID(repair, entry.EventID),
				"once the lease expires the row must be recoverable, or an owner that died mid-settlement "+
					"strands the event in the only table that holds it")

			for _, row := range repair {
				if row.EventID == entry.EventID {
					assert.NotEqual(t, token, row.ClaimToken,
						"the repair must issue a fresh token so the dead owner's stale one is fenced out")
					assert.Equal(t, model.EventOutboxStatusFailed, row.Status,
						"the repair must not move the row out of failed: doing so would put it back into the "+
							"ordinary claim's blocking set and stall every later event of its aggregate")
				}
			}
		})
	}
}

// TestListDeadLetteredEvents_WindowIsAnIndexConditionRatherThanAFilter_RealDB is the
// performance half of the window, and it asserts the one property that is this query's to
// guarantee: both bounds reach the index as an INDEX CONDITION, so the scan starts and stops
// at the window instead of reading the terminal-state rows and discarding most of them.
//
// # Why the assertion is on the index condition and not on the absence of a Seq Scan
//
// Whether the planner reaches for the index at all is a COST decision that depends on how
// many rows the table holds and how many of them are in a terminal failure state. On a small
// or freshly migrated table a sequential scan is genuinely cheaper and choosing it is
// correct, so "the plan contains no Seq Scan" reports a defect on an empty database that is
// not one — the same reasoning the claim-index assertion records for not pinning an index
// name.
//
// What must hold at every table size is that the predicate is SARGABLE: written so that the
// planner CAN push it into an index traversal. That is what the nullable-parameter form
// (`$5 IS NULL OR occurred_at >= $5`) has to earn — a bound wrapped in a function, compared
// against a differently-typed value, or written so the NULL guard cannot be folded away would
// still return the right rows while degrading into a post-scan Filter, and only a plan
// assertion catches that. Index paths are made preferable for the duration of one transaction
// so the choice is decided rather than guessed at; the setting is LOCAL and the transaction is
// rolled back, so it cannot leak to another test or another connection.
//
// The foreign-index guard applies here for the same reason it applies to the claim index: a
// shared database may carry an index this repository does not declare.
func TestListDeadLetteredEvents_WindowIsAnIndexConditionRatherThanAFilter_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	after := time.Now().UTC().Add(-48 * time.Hour)
	before := time.Now().UTC()

	// The supporting index, asserted where it is declared. The plan below is only possible
	// because this index exists, so a missing or redefined index should fail HERE with the
	// definition in the message rather than as an opaque plan mismatch.
	var indexDefinition string
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = 'blnk' AND tablename = 'event_outbox'
		  AND indexname = 'idx_event_outbox_failed'
	`).Scan(&indexDefinition),
		"the terminal-state index must exist: sql/1781248800.sql declares it and the windowed "+
			"listing is planned against it")
	assert.Contains(t, indexDefinition, "occurred_at",
		"the window is bounded on occurred_at, so occurred_at must be an index column: %s", indexDefinition)
	assert.Contains(t, indexDefinition, "status",
		"the listing selects the two terminal states, so status must lead the index: %s", indexDefinition)

	tx, err := ds.Conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// LOCAL, so it dies with this transaction and no other test or connection sees it.
	_, err = tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off")
	require.NoError(t, err)

	rows, err := tx.QueryContext(ctx, `
		EXPLAIN SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		  AND ($5::timestamptz IS NULL OR occurred_at >= $5::timestamptz)
		  AND ($6::timestamptz IS NULL OR occurred_at <= $6::timestamptz)
		ORDER BY occurred_at DESC, id DESC
		LIMIT $3 OFFSET $4
	`,
		model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
		20, 0, nullableTime(after), nullableTime(before),
	)
	require.NoError(t, err, "EXPLAIN of the windowed listing must succeed")
	defer func() { _ = rows.Close() }()

	var (
		plan            strings.Builder
		indexConditions []string
		filters         []string
		indexScanLines  []string
	)
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteString("\n")

		trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "->"))
		switch {
		case strings.HasPrefix(trimmed, "Index Cond:"):
			indexConditions = append(indexConditions, trimmed)
		case strings.HasPrefix(trimmed, "Filter:"):
			filters = append(filters, trimmed)
		case strings.Contains(trimmed, "Index Scan") || strings.Contains(trimmed, "Index Only Scan"):
			indexScanLines = append(indexScanLines, trimmed)
		}
	}
	require.NoError(t, rows.Err())

	planText := plan.String()
	require.NotEmpty(t, planText)

	for _, line := range indexScanLines {
		if foreign := unknownEventOutboxIndexIn(t, ctx, ds, line); foreign != "" {
			t.Skipf("the planner chose %q, an index this repository does not declare — a shared "+
				"database carries it, so this plan says nothing about the schema under test.\n"+
				"Plan was:\n%s", foreign, planText)
		}
	}

	condition := strings.Join(indexConditions, "\n")
	assert.Contains(t, condition, "occurred_at >=",
		"the LOWER bound must be an index condition, not a post-scan filter: a scan that started at "+
			"the top of the terminal-state rows and discarded everything before the window reads the "+
			"whole failure history to answer a one-hour question.\nPlan was:\n"+planText)
	assert.Contains(t, condition, "occurred_at <=",
		"the UPPER bound must be an index condition too, so the scan stops at the window rather "+
			"than running to the end of the index.\nPlan was:\n"+planText)

	for _, filter := range filters {
		assert.NotContains(t, filter, "occurred_at",
			"a bound that lands in a Filter has been evaluated AFTER the rows were read, which is the "+
				"non-sargable form this predicate is written to avoid.\nPlan was:\n"+planText)
	}
}

// Three window tests that arrived with this feature are GONE, and the property each pinned is
// asserted elsewhere in this file against the shape the repository actually has.
//
// They were written for a listing whose window was two fixed placeholders bound from *time.Time,
// and for a count that returned a per-status map. This repository narrows through
// model.DeadLetterQuery and deadLetterFilterClause, which builds ONE predicate — status, event
// type, topic, resolution AND the occurrence window — used by the page, the count and the age
// gauge alike, and returns a single total:
//
//   - the binding of the bounds is covered by TestListDeadLetteredEventsFiltered_PushesEveryPredicateIntoSQL
//     and TestListDeadLetteredEventsFiltered_AppliesEveryPredicateInSQL, which assert the whole
//     predicate rather than the window alone;
//   - the count agreeing with the page is covered by TestCountDeadLetteredEvents_CountsTheSamePredicateAsThePage
//     and TestCountDeadLetteredEvents_CountsTheSamePopulationTheListingPages — the stronger form
//     of the same guarantee, since sharing the clause is what makes them agree;
//   - UTC normalisation of a supplied window is covered by TestCountEventOutboxByStatus_NormalisesTheWindow.
//
// The EXPLAIN test below is kept, because nothing else proves the bounds are INDEX CONDITIONS
// rather than post-scan filters, and that is a property of the schema no unit test can reach.

// unknownEventOutboxIndexIn reports the first index named in a plan line that no migration
// in this repository creates, or "" when every index in the line is one this repository
// declares.
//
// # Why this is derived from the migrations rather than listed here
//
// A hardcoded list of "our indexes" would be a second copy of the migrations, and the copy
// that goes stale is the one nobody runs. Reading the embedded migration source means an
// index added by a future migration is recognised the moment that migration exists, and an
// index that only ever existed in somebody's shared database never is.
//
// It reads ../sql from disk rather than the embedded blnk.SQLFiles, and only because it must:
// the root blnk package imports this one, so this in-package test file cannot import it back.
// The two are the same bytes by construction — blnk.SQLFiles is `//go:embed sql/*.sql` over
// that very directory — and the path is relative to the package directory, which is where `go
// test` runs it.
//
// Parameters:
//   - t *testing.T: for the fatal on an unreadable migration source, which is a broken
//     checkout rather than a schema finding.
//   - ctx context.Context: for the catalog read.
//   - ds Datasource: the connection to read pg_indexes through.
//   - planLine string: one line of an EXPLAIN plan.
//
// Returns:
//   - string: the name of the first foreign index the line reads, or "".
func unknownEventOutboxIndexIn(t *testing.T, ctx context.Context, ds Datasource, planLine string) string {
	t.Helper()

	migrationSQL, err := readMigrationSource(t)
	require.NoError(t, err, "reading the embedded migration source")

	for _, index := range planIndexesUsed(planLine) {
		// Only indexes on THIS table are judged. A plan can name an index on another
		// relation — __consumer_offsets never appears here, but a join added later could —
		// and this guard has nothing to say about those.
		var onEventOutbox bool
		require.NoError(t, ds.Conn.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_indexes
				WHERE schemaname = 'blnk' AND tablename = 'event_outbox' AND indexname = $1
			)
		`, index).Scan(&onEventOutbox), "reading pg_indexes for %q", index)

		if !onEventOutbox {
			continue
		}

		if !strings.Contains(migrationSQL, index) {
			return index
		}
	}

	return ""
}

// readMigrationSource concatenates every migration, so a name can be looked for across the
// whole schema definition at once.
//
// Returns:
//   - string: every migration's text, joined.
//   - error: a read failure, which means a broken checkout rather than a schema problem.
func readMigrationSource(t *testing.T) (string, error) {
	t.Helper()

	const migrationDir = "../sql"

	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return "", err
	}

	var joined strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		body, readErr := os.ReadFile(filepath.Join(migrationDir, entry.Name()))
		if readErr != nil {
			return "", readErr
		}

		joined.Write(body)
		joined.WriteByte('\n')
	}

	return joined.String(), nil
}

// TestUnknownEventOutboxIndexIn_TellsForeignIndexesFromDeclaredOnes_RealDB is what keeps the
// foreign-index guard from making the claim-index assertion vacuous.
//
// A guard that answered "foreign" for everything would skip on every database, and the
// assertion it protects would never run again while still looking present. So both directions
// are pinned against the real catalog: an index this repository's migrations declare is NOT
// reported, and a name that exists in neither the catalog nor the migrations is not reported
// either — only an index that IS in the catalog and is NOT in the migrations.
func TestUnknownEventOutboxIndexIn_TellsForeignIndexesFromDeclaredOnes_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	t.Run("an index this repository declares is not foreign", func(t *testing.T) {
		line := "->  Index Scan using idx_event_outbox_claim on event_outbox candidate  (cost=0.26..30.46 rows=7 width=51)"
		assert.Empty(t, unknownEventOutboxIndexIn(t, ctx, ds, line),
			"idx_event_outbox_claim is created by this repository's migrations, so a plan reading it "+
				"must not be treated as a foreign schema — that would skip the assertion on every "+
				"correctly migrated database")
	})

	t.Run("an index in the catalog that no migration declares is foreign", func(t *testing.T) {
		// THE FOREIGN INDEX IS CREATED HERE RATHER THAN LOOKED FOR.
		//
		// This subtest used to scan pg_indexes for an index no migration declares and SKIP when
		// it found none — which is the state of every correctly migrated database, including
		// every CI database. So the positive direction of the guard, the direction that decides
		// whether a plan assertion is skipped or trusted, ran only on a developer's machine that
		// happened to have another checkout's migrations applied to it. It was reported as a
		// pass everywhere else, and a skip is not a pass.
		//
		// Creating the condition makes the assertion unconditional. The index is dropped in
		// cleanup on every path, and its name is deliberately unmistakable so a leaked one is
		// identifiable rather than mysterious: nothing in sql/ declares it, and the guard it
		// feeds treats exactly that as foreign.
		//
		// It is safe on a shared database for as long as it exists, which is the duration of
		// this subtest: it is an ordinary btree on one column, it changes no plan this suite
		// asserts on — `TestClaimPendingEventOutbox_UsesClaimIndex_RealDB` reads the same
		// catalog and would skip while it existed, and the two never overlap because subtests
		// here do not run in parallel — and DROP INDEX IF EXISTS in cleanup is idempotent.
		const foreign = "idx_event_outbox_foreign_index_probe"

		_, err := ds.Conn.ExecContext(ctx,
			`CREATE INDEX IF NOT EXISTS `+foreign+` ON blnk.event_outbox (schema_version)`)
		require.NoError(t, err,
			"the probe index must be creatable: without it this subtest can only skip, and the "+
				"positive direction of the guard is what decides whether a plan assertion is "+
				"trusted")
		t.Cleanup(func() {
			_, dropErr := ds.Conn.ExecContext(ctx, `DROP INDEX IF EXISTS blnk.`+foreign)
			assert.NoError(t, dropErr,
				"the probe index must be dropped: left behind, it makes the claim-index plan "+
					"assertion skip on this database for every later run")
		})

		// THE PREMISE, asserted rather than assumed: the name must genuinely be absent from every
		// migration, or the guard would be right to ignore it and this subtest would be testing
		// nothing.
		migrationSQL, err := readMigrationSource(t)
		require.NoError(t, err)
		require.NotContains(t, migrationSQL, foreign,
			"the probe index name must appear in no migration; if a migration ever declares it, "+
				"this subtest is exercising the negative direction under a positive name")

		line := "->  Index Scan using " + foreign + " on event_outbox candidate  (cost=0.26..30.46 rows=7)"
		assert.Equal(t, foreign, unknownEventOutboxIndexIn(t, ctx, ds, line),
			"an index present in the catalog and absent from every migration is exactly the case the "+
				"guard exists to detect")
	})

	t.Run("a name in neither the catalog nor the migrations is ignored", func(t *testing.T) {
		// The guard judges only indexes that exist ON THIS TABLE. A plan naming an index of
		// some other relation says nothing about this repository's schema, and reporting it
		// would skip the assertion for an unrelated reason.
		line := "->  Index Scan using idx_not_a_real_index_anywhere on transactions t  (cost=0.26..30.46 rows=7)"
		assert.Empty(t, unknownEventOutboxIndexIn(t, ctx, ds, line))
	})
}

// TestClaimPendingEventOutbox_SerialisesOnTheEffectiveKey is the structural half of PERF-C02:
// that the claim's earlier-same-key exclusion compares the key the PUBLISHER uses and not the
// stored column.
//
// # Why the column was the wrong key
//
// The Kafka message key is model.EffectivePartitionKey — the row's ledger where it has one, its
// stored partition_key where it does not — because requirement R-6 partitions by ledger id. The
// exclusion is the statement "at most one row per Kafka partition key is in flight", and written
// against partition_key it was a statement about a different value. The two disagree on exactly
// the rows the schema permits: a row written before the ledger reached its call site, or one
// whose partition key was derived from the payload before the ledger was resolved. Two such rows
// of ONE ledger have different stored keys, so the exclusion did not hold them apart — while the
// publisher hashed both to one partition, letting two relay replicas append them in either order.
//
// This asserts over the query text, which needs no database and cannot be satisfied by a fake:
// the guarantee is a property of the SQL. The behavioural proof, with two relays and a real
// broker, is TestEventOrdering_DivergentStoredKeysStaySerialisedAcrossRelayReplicas.
func TestClaimPendingEventOutbox_SerialisesOnTheEffectiveKey(t *testing.T) {
	t.Parallel()

	// BOTH sides, because an exclusion comparing the effective key on one side and the column on
	// the other is worse than either: it would match almost nothing and exclude almost nothing.
	assert.Contains(t, claimPendingEventOutboxQuery, eventOutboxEffectiveKeySQL("earlier")+" = "+eventOutboxEffectiveKeySQL("candidate"),
		"the claim must exclude a candidate when an earlier row shares its EFFECTIVE key, with the "+
			"same expression on both sides")

	assert.NotContains(t, claimPendingEventOutboxQuery, "earlier.partition_key = candidate.partition_key",
		"the superseded column comparison must not return: it leaves same-ledger rows with "+
			"divergent stored keys unserialised while Kafka places them on one partition")

	// The expression itself, spelled out here so a change to it has to be made in two places
	// with this test's reasoning in front of the author.
	assert.Equal(t,
		"COALESCE(NULLIF(btrim(earlier.ledger_id), ''), btrim(earlier.partition_key))",
		eventOutboxEffectiveKeySQL("earlier"),
		"the SQL rendering must be model.EffectivePartitionKey term for term: ledger first, "+
			"trimmed, with a blank ledger falling through to the partition key")

	// And the ordering columns the exclusion's tuple comparison reads, which is what makes
	// "earlier" mean earlier by OCCURRENCE rather than by insertion.
	assert.Contains(t, claimPendingEventOutboxQuery,
		"(earlier.occurred_at, earlier.id) < (candidate.occurred_at, candidate.id)",
		"the exclusion must compare occurrence order, with the surrogate key breaking ties")
}

// TestClaimPendingEventOutbox_EffectiveKeyIndexMatchesTheQuery_RealDB closes the gap between a
// correct predicate and a usable one.
//
// A partial expression index is usable ONLY when the query spells its expression identically.
// A difference does not produce a wrong answer, which is what makes it dangerous: the claim
// still serialises correctly and simply stops being index-backed, so every poll degrades into a
// sequential scan of a table that grows by 43.2 million rows a day at the acceptance rate. That
// is the same outage by a slower route, and nothing in a functional test would notice.
//
// So the index's definition is read out of pg_indexes and the query's own expression is required
// to appear in it. Reading the catalogue rather than the migration text is deliberate: it is the
// applied schema that decides whether the planner has an index, and a migration that was written
// but never applied is exactly the state this would otherwise miss.
func TestClaimPendingEventOutbox_EffectiveKeyIndexMatchesTheQuery_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	var definition string
	err := ds.Conn.QueryRowContext(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'blnk'
		  AND tablename = 'event_outbox'
		  AND indexname = 'idx_event_outbox_effective_key_inflight'
	`).Scan(&definition)
	require.NoError(t, err,
		"idx_event_outbox_effective_key_inflight must exist: it is what keeps the claim's "+
			"earlier-same-key exclusion an index probe rather than a scan of the key's whole "+
			"history. Apply sql/1781252000.sql.")

	// pg_indexes renders the expression with the columns UNQUALIFIED, because an index belongs to
	// one relation. The query's expression is qualified by an alias, so the comparison is made
	// against the unqualified rendering of the same rule.
	unqualified := strings.ReplaceAll(eventOutboxEffectiveKeySQL(""), ".", "")
	unqualified = strings.ReplaceAll(unqualified, "''", "''::text")

	assert.Contains(t, definition, unqualified,
		"the index expression and the claim's expression must be identical, or the planner cannot "+
			"use the index for the exclusion.\nindex: %s\nquery expression: %s",
		definition, eventOutboxEffectiveKeySQL("candidate"))

	// The partial predicate is the BLOCKING set, and it is narrower than the claimable set on
	// purpose: an exhausted, dead-lettered or webhook-owing row must not hold its key for ever.
	assert.Contains(t, definition, "'pending'",
		"the index must be partial on the blocking states")
	assert.Contains(t, definition, "'processing'",
		"the index must be partial on the blocking states")
	assert.NotContains(t, definition, "webhook_pending",
		"webhook_pending is CLAIMABLE but not BLOCKING: its position in the Kafka partition is "+
			"already fixed, so blocking its key would let a failing legacy enqueue stall Kafka "+
			"delivery for the whole aggregate")
	assert.NotContains(t, definition, "dead_lettered",
		"a dead-lettered row must not block its key for ever")

	// And the superseded index is gone, so the hottest table in this schema is not maintaining a
	// second key nothing probes.
	var lingering int
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = 'blnk'
		  AND tablename = 'event_outbox'
		  AND indexname = 'idx_event_outbox_partition_key_inflight'
	`).Scan(&lingering))
	assert.Zero(t, lingering,
		"the superseded column index must be dropped: no statement probes it any more, so it is "+
			"pure write amplification on every insert and every status transition")
}
