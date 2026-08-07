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
	"reflect"
	"regexp"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Shared scaffolding
//
// Every helper below is named so it cannot collide with the identifiers this
// package's other test files already declare — openRealTestDB, eqFilter,
// mockCache, newMockCache, quiesceOutbox and newOutboxEntry are all defined
// elsewhere in package database and are reused, never redeclared.
// ---------------------------------------------------------------------------

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
		// UNIQUE PER FIXTURE, deliberately. At most one row per partition key is
		// claimable at any instant — that is what preserves per-aggregate ordering —
		// so fixtures sharing a key would drain one at a time and every test that
		// claims a batch would see a single row. Tests that are ABOUT same-key
		// behaviour set this explicitly; every other test wants independent rows.
		PartitionKey:  markerPrefix + "key-" + uuid.NewString(),
		LedgerID:      markerPrefix + "ldg",
		Topic:         "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload: json.RawMessage(
			`{"event":"transaction.applied","data":{"transaction_id":"` + markerPrefix + `txn","status":"APPLIED"}}`),
		OccurredAt:  dbTimestamp(time.Now()),
		MaxAttempts: defaultEventMaxAttempts,
	}
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
	// LATER row of the same partition key. Kafka preserves append order rather than
	// occurred_at, so that reordering reaches subscribers with nothing anywhere to show
	// it. The NOT EXISTS clause is what makes at most one row per key claimable, and it
	// is asserted here because it is invisible in behaviour until two relays run.
	assert.Contains(t, issued, "NOT EXISTS",
		"the claim must exclude a row whose partition key already has an earlier row in flight; SKIP LOCKED alone permits same-key reordering")
	assert.Contains(t, issued, "earlier.partition_key = candidate.partition_key",
		"the exclusion must be scoped BY PARTITION KEY; scoping it wider would serialise unrelated aggregates and destroy throughput")
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
// may declare it. ListDeadLetteredEvents covers both literals precisely so that an
// event stranded between them is still visible to an operator.
func TestMarkEventFailed_ChoosesRetryOrExhaustionInSQL(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	mock.ExpectQuery("").
		WithArgs(
			model.EventOutboxStatusFailed,     // $1 — the exhaustion arm
			model.EventOutboxStatusPending,    // $2 — the retry arm
			"broker unavailable",              // $3 — the reason
			"2s",                              // $4 — the backoff the caller computed
			int64(7),                          // $5 — the row
			"tok-7",                           // $6 — the claim token
			model.EventOutboxStatusProcessing, // $7 — the required prior state
			false,                             // $8 — the caller's permanent-failure verdict
		).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7", "broker unavailable", 2*time.Second, false)
	require.NoError(t, err)
	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN $1 ELSE $2 END",
		"the retry-versus-exhaustion decision must be made in SQL, atomically with the attempt increment, "+
			"and it must read BOTH inputs: the caller's permanent-failure verdict and the attempt budget")
	assert.Contains(t, issued, "attempts = attempts + 1",
		"the attempt counter must be incremented by the same statement that reads it")
	assert.Contains(t, issued, "locked_until = NULL",
		"the lease must be released on BOTH arms, otherwise the retry is never picked up")
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

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7", "final attempt failed", 0, false)
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
			true, // $8 — the caller's PERMANENT verdict
		).
		// The database resolves the CASE, so the returned row is what a real PostgreSQL would
		// have produced for a terminal verdict on attempt one: failed, with the count at 1.
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(1)))

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "tok-7",
		"the serialised event is over the size ceiling", 0, true)
	require.NoError(t, err)
	require.Len(t, *captured, 1)

	assert.Contains(t, (*captured)[0], "$8::boolean OR attempts + 1 >= max_attempts",
		"the verdict must be BOUND and read by the CASE, never applied by a Go-side branch: deciding "+
			"outside the statement is what lets two instances racing on one row both conclude they were last")
	assert.NotContains(t, (*captured)[0], "max_attempts = ",
		"the budget itself must not be rewritten to force exhaustion; that would corrupt the row's own history")

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

	outcome, err := ds.MarkEventFailed(context.Background(), 7, "stale-token", "boom", 0, false)
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
			_, err := ds.MarkEventFailed(ctx, 1, "", "boom", 0, false)
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
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "transient", 0, false)
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
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusFailed, int64(5)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "final attempt failed", 0, false)
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
	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", "boom", 0, false)
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
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "attempts"}).
			AddRow(model.EventOutboxStatusPending, int64(1)))

	_, err := ds.MarkEventFailed(context.Background(), 7, "tok", hostile, 0, false)
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
// The safety property is that ONLY terminal rows are eligible. A pending, processing,
// replaying or failed row is still owed a delivery attempt. failed is the subtle one
// and is asserted explicitly: its retry budget is spent but its dead-letter write is
// not done, so this table is the ONLY copy of that event in existence and deleting it
// would destroy it.
func TestPurgeTerminalEventsBefore_DeletesOnlyTerminalRowsInABoundedSlice(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	ds := Datasource{Conn: db}

	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM blnk.event_outbox")).
		WithArgs(pq.Array(model.TerminalEventOutboxStatuses()), cutoff, 250).
		WillReturnResult(sqlmock.NewResult(0, 3))

	purged, err := ds.PurgeTerminalEventsBefore(context.Background(), cutoff, 250)
	require.NoError(t, err)
	assert.Equal(t, int64(3), purged, "the caller sweeps in a loop and needs the true count to know when to stop")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "status = ANY($1)", "the eligible states must be bound, not interpolated")
	assert.Contains(t, issued, "occurred_at < $2")
	assert.Contains(t, issued, "LIMIT $3",
		"the delete must be bounded, or one sweep blocks the relay and bloats the WAL in a single transaction")

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
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM blnk.event_outbox")).
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), captureArg(&boundLimit)).
					WillReturnResult(sqlmock.NewResult(0, 0))

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

	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, 50, 0).
		WillReturnRows(newEventOutboxRows(deadLettered, stranded))

	entries, err := ds.ListDeadLetteredEvents(context.Background(), 0, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"evt_dlt", "evt_stranded"}, eventIDsOf(entries),
		"an event stranded in failed, whose dead-letter publication itself failed, must still be visible to an operator")

	require.Len(t, *captured, 1)
	issued := (*captured)[0]
	assert.Contains(t, issued, "WHERE status IN ($1, $2)")
	assert.Contains(t, issued, "ORDER BY occurred_at DESC, id DESC",
		"triage starts from the most recent failures, and id breaks ties so paging cannot show or skip a row twice")
	assert.Contains(t, issued, "LIMIT $3 OFFSET $4")

	assert.NoError(t, mock.ExpectationsWereMet())
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
				WithArgs(
					model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
					tc.wantLimit, tc.wantOffset,
				).
				WillReturnRows(newEventOutboxRows())

			_, err := ds.ListDeadLetteredEvents(context.Background(), tc.limit, tc.offset)
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

			entries, err := ds.ListDeadLetteredEvents(context.Background(), 10, 0)
			assert.Nil(t, entries)
			requireAPIError(t, err, apierror.ErrInternalServer)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
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

	counts, err := ds.CountEventOutboxByStatus(context.Background())
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{
		model.EventOutboxStatusPending:      3,
		model.EventOutboxStatusProcessing:   1,
		model.EventOutboxStatusDispatched:   1204,
		model.EventOutboxStatusFailed:       2,
		model.EventOutboxStatusDeadLettered: 7,
	}, counts)

	require.Len(t, *captured, 1)
	assert.Contains(t, (*captured)[0], "GROUP BY status",
		"the reconciliation needs one row per status, aggregated in the database rather than by counting rows in Go")

	// The reconciliation arithmetic the runbook actually performs, asserted here so the
	// two terminal states cannot drift out of the sum.
	assert.Equal(t, int64(1211),
		counts[model.EventOutboxStatusDispatched]+counts[model.EventOutboxStatusDeadLettered],
		"dispatched + dead_lettered is what the daily reconciliation compares against the broker's end offsets")

	assert.NoError(t, mock.ExpectationsWereMet())
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

		counts, err := ds.CountEventOutboxByStatus(context.Background())
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

		counts, err := ds.CountEventOutboxByStatus(context.Background())
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

		counts, err := ds.CountEventOutboxByStatus(context.Background())
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

			counts, err := ds.CountEventOutboxByStatus(context.Background())
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

	t.Run("ListDeadLetteredEvents", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("FROM blnk.event_outbox")).
			WillReturnRows(newEventOutboxRows(entry).CloseError(closeErr))

		entries, err := ds.ListDeadLetteredEvents(context.Background(), 10, 0)
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

		counts, err := ds.CountEventOutboxByStatus(context.Background())
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
//     partition key and is still pending or processing. A foreign row that survives parking
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
func quiesceEventOutbox(t *testing.T, ds Datasource, markerPrefix string) {
	t.Helper()

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

	t.Cleanup(func() {
		_, cleanupErr := ds.Conn.Exec(`
			UPDATE blnk.event_outbox
			SET status = 'dispatched', dispatched_at = NOW(), locked_until = NULL, claim_token = NULL
			WHERE aggregate_id LIKE $1
			  AND status IN ('pending', 'processing', 'replaying')
		`, markerPrefix+"%")
		if cleanupErr != nil {
			t.Logf("failed to retire event outbox fixtures: %v", cleanupErr)
		}
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
		entry.PartitionKey = marker + sharedKey
		entry.LedgerID = marker + "shared-ledger"
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
	ds := openRealTestDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("onekey")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "one-key"

	earlier := newEventOutboxFixture(marker)
	earlier.PartitionKey = sharedKey
	earlier.OccurredAt = base
	insertRealEventOutbox(t, ds, earlier)

	later := newEventOutboxFixture(marker)
	later.PartitionKey = sharedKey
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
	outcome, err := ds.MarkEventFailed(ctx, earlier.ID, earlierToken, "transient", 0, false)
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
	ds := openRealTestDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("nostall")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "stall-key"

	doomed := newEventOutboxFixture(marker)
	doomed.PartitionKey = sharedKey
	doomed.OccurredAt = base
	doomed.MaxAttempts = 1
	insertRealEventOutbox(t, ds, doomed)

	successor := newEventOutboxFixture(marker)
	successor.PartitionKey = sharedKey
	successor.OccurredAt = base.Add(time.Minute)
	insertRealEventOutbox(t, ds, successor)

	token := claimEventOutboxToken(t, ds, doomed)
	outcome, err := ds.MarkEventFailed(ctx, doomed.ID, token, "permanently undeliverable", 0, false)
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
	ds := openRealTestDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("whpending")
	quiesceEventOutbox(t, ds, marker)

	base := dbTimestamp(time.Now().Add(-time.Hour))
	sharedKey := marker + "webhook-pending-key"

	published := newEventOutboxFixture(marker)
	published.PartitionKey = sharedKey
	published.OccurredAt = base
	insertRealEventOutbox(t, ds, published)

	successor := newEventOutboxFixture(marker)
	successor.PartitionKey = sharedKey
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
	ds := openRealTestDB(t)
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
//   - IS reachable by the repair claim, which leaves the status at 'failed' and stamps a fresh
//     token;
//   - is protected by its new lease from being repaired twice at once;
//   - can then complete through the ORDINARY MarkEventDeadLettered transition, which accepts
//     'failed' as a prior state precisely so no second code path is needed; and
//   - fences the token the failed worker was holding, so a straggler cannot preserve a row the
//     repair has taken over.
//
// A row that WAS preserved is seeded alongside it and must never be claimed, because a repair
// pass that re-claimed dead-lettered rows would rewrite their dead-letter records for ever.
func TestClaimFailedEventOutboxForDeadLetter_RecoversAnUnpreservedRowNothingElseCanReach_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
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

	strandedOutcome, err := ds.MarkEventFailed(ctx, stranded.ID, strandedToken, "broker refused the publish", 0, false)
	require.NoError(t, err)
	require.True(t, strandedOutcome.Exhausted, "a one-attempt budget must be spent by one failure")
	require.Equal(t, model.EventOutboxStatusFailed, strandedOutcome.Status)

	preservedToken := tokens[preserved.EventID]
	require.NotEmpty(t, preservedToken)

	preservedOutcome, err := ds.MarkEventFailed(ctx, preserved.ID, preservedToken, "broker refused the publish", 0, false)
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

	// THE WAY OUT.
	repair, err := ds.ClaimFailedEventOutboxForDeadLetter(ctx, 20, time.Minute)
	require.NoError(t, err)

	require.True(t, containsEventID(repair, stranded.EventID),
		"the repair claim must reach a failed row with no dead-letter record; without it the event is "+
			"the only copy of itself and nothing will ever act on it again")
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

// TestClaimPendingEventOutbox_ConcurrentClaimsAreDisjoint_RealDB is the FOR UPDATE SKIP
// LOCKED proof, and sqlmock cannot give it: a mock has no locking semantics at all.
//
// Two properties are asserted, and both are needed:
//
//   - DISJOINTNESS. No row may appear in two workers' batches. Without SKIP LOCKED — or
//     with the row lock dropped entirely — two relay instances can claim the same row and
//     publish the same event twice.
//   - NO STARVATION. Every fixture must be claimed exactly once across the workers, so
//     the whole backlog is drained. A claim that avoided duplicates by making workers
//     block on each other would satisfy disjointness while destroying the throughput the
//     500-events-per-second target depends on, and asserting the full set was claimed is
//     what rules that out.
func TestClaimPendingEventOutbox_ConcurrentClaimsAreDisjoint_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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
		_, failErr := ds.MarkEventFailed(ctx, entry.ID, staleToken, "stale attempt", 0, false)
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
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "broker unavailable", 0, false)
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

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "broker unavailable", time.Hour, false)
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

		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "gave up", time.Hour, false)
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
		_, err := ds.MarkEventFailed(ctx, entry.ID, firstToken, "attempt one", 0, false)
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

		_, err = ds.MarkEventFailed(ctx, entry.ID, secondToken, "attempt two", 0, false)
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
		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "final failure", 0, false)
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
		listed, err := ds.ListDeadLetteredEvents(ctx, maxDeadLetterPageSize, 0)
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
		outcome, err := ds.MarkEventFailed(ctx, entry.ID, token, "gave up", 0, false)
		require.NoError(t, err)
		require.True(t, outcome.Exhausted)
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, outcome.ClaimToken,
			entry.Topic+".dlt", json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":1}`), model.BrokerRecord{}))

		claimed, err := ds.ClaimEventForReplay(ctx, entry.EventID, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, model.EventOutboxStatusReplaying, claimed.Status)
		require.NotEmpty(t, claimed.ClaimToken, "the replay claim must issue a token")

		// THE POINT OF THE CLAIM: a second concurrent replay request finds nothing to
		// claim, so it cannot publish a duplicate.
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
	ds := openRealTestDB(t)
	ctx := context.Background()
	marker := newRealEventOutboxMarker("count")
	quiesceEventOutbox(t, ds, marker)

	baseline, err := ds.CountEventOutboxByStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, baseline)

	const (
		wantPending      = 3
		wantDispatched   = 2
		wantDeadLettered = 1
	)

	// ORDER MATTERS HERE, and getting it wrong is subtle. Driving a row to a terminal
	// state requires CLAIMING it first, and a claim takes a whole batch — so any row
	// left pending when the dispatched and dead-lettered fixtures are claimed would be
	// swept into processing along with them and would then be counted in the wrong
	// bucket. The terminal fixtures are therefore created first, and the pending ones
	// last, so nothing claims them.
	for i := 0; i < wantDispatched; i++ {
		entry := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
		require.NoError(t, ds.MarkEventDispatched(ctx, entry.ID, claimEventOutboxToken(t, ds, entry), model.BrokerRecord{}))
	}
	for i := 0; i < wantDeadLettered; i++ {
		entry := insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
		require.NoError(t, ds.MarkEventDeadLettered(ctx, entry.ID, claimEventOutboxToken(t, ds, entry),
			entry.Topic+".dlt",
			json.RawMessage(`{"original_topic":"`+entry.Topic+`","attempt_count":5}`), model.BrokerRecord{}))
	}
	for i := 0; i < wantPending; i++ {
		insertRealEventOutbox(t, ds, newEventOutboxFixture(marker))
	}

	after, err := ds.CountEventOutboxByStatus(ctx)
	require.NoError(t, err)

	delta := func(status string) int64 { return after[status] - baseline[status] }

	assert.Equal(t, int64(wantPending), delta(model.EventOutboxStatusPending))
	assert.Equal(t, int64(wantDispatched), delta(model.EventOutboxStatusDispatched))
	assert.Equal(t, int64(wantDeadLettered), delta(model.EventOutboxStatusDeadLettered))

	assert.Equal(t, int64(wantDispatched+wantDeadLettered),
		delta(model.EventOutboxStatusDispatched)+delta(model.EventOutboxStatusDeadLettered),
		"dispatched + dead_lettered is the quantity the daily zero-loss reconciliation compares against the broker's main-topic and dead-letter-topic end offsets")

	// Every row inserted here is accounted for in exactly one status, which is the
	// zero-loss property itself: no event may be missing from the totals, and none may
	// be double-counted.
	var totalDelta int64
	for status := range after {
		totalDelta += delta(status)
	}
	assert.Equal(t, int64(wantPending+wantDispatched+wantDeadLettered), totalDelta,
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
	ds := openRealTestDB(t)
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
	t.Cleanup(func() {
		if _, cleanupErr := ds.Conn.Exec(
			"DELETE FROM blnk.event_outbox WHERE event_id LIKE $1", marker+"%"); cleanupErr != nil {
			t.Logf("failed to remove plan-shaping rows: %v", cleanupErr)
		}
		if _, cleanupErr := ds.Conn.Exec("VACUUM (ANALYZE) blnk.event_outbox"); cleanupErr != nil {
			t.Logf("failed to reclaim the space the plan-shaping rows occupied: %v", cleanupErr)
		}
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
	// This assertion used to pin "idx_event_outbox_claim" by name and it failed roughly one
	// package run in six, always on a plan that honours the guarantee completely: PostgreSQL
	// reached the candidate rows through a Bitmap Index Scan on
	// idx_event_outbox_partition_key_inflight instead. Both indexes are PARTIAL on exactly
	// the blocking states, so under either plan the cold history of terminal rows is never
	// read and the poll's cost stays independent of how large the outbox has grown — which
	// is the entire reason the migration's index exists. Which of the two the planner picks
	// is a cost decision that moves with heap compaction, statistics freshness and host
	// load, and pinning it made this assertion report a defect that was not one. The
	// anti-join half of this test already learned that lesson; this half had not.
	//
	// Asserted on the ONE plan line that reaches the candidate relation, for the same reason
	// the earlier-same-key assertion below is: "the plan mentions an index somewhere" is
	// nearly vacuous, whereas "the line reaching `event_outbox candidate` is an index scan
	// through a blocking-state partial index" says exactly what it means. The
	// sequential-scan prohibition further down closes the same failure mode across every
	// other node in the plan.
	candidateScan := planLineReferencing(planText, "event_outbox candidate")
	require.NotEmpty(t, candidateScan,
		"the plan must reach the candidate relation explicitly.\nPlan was:\n"+planText)
	assert.Contains(t, candidateScan, "Index",
		"the claim's candidate selection must be index-driven. Read through a scan instead, the relay's per-poll cost grows with the whole table until it cannot keep up with 500 events/sec, and no unit test would ever reveal it.\nPlan line was: "+candidateScan)
	assert.True(t,
		strings.Contains(candidateScan, "idx_event_outbox_claim") ||
			strings.Contains(candidateScan, "idx_event_outbox_partition_key_inflight"),
		"the claim's candidate selection must be served by a partial index whose predicate is the blocking-state set, so the cold history is excluded from the index entirely. Anything else reads terminal rows on every poll.\nPlan line was: "+candidateScan+"\nPlan was:\n"+planText)

	// The earlier-same-key exclusion must be INDEX-DRIVEN and confined to the blocking
	// states. What it must never be is a scan of each key's entire history, which would buy
	// the ordering guarantee at a price that grows with the table — correct, and
	// progressively unable to keep up.
	//
	// That property is asserted rather than the name of one specific index, because
	// PostgreSQL has TWO correct ways to satisfy it and picks between them on cost:
	//
	//   - a NESTED LOOP anti-join that probes idx_event_outbox_partition_key_inflight once
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
	earlierScan := planLineReferencing(planText, "event_outbox earlier")
	require.NotEmpty(t, earlierScan,
		"the plan must reach the earlier-same-key relation explicitly.\nPlan was:\n"+planText)
	assert.Contains(t, earlierScan, "Index",
		"the earlier-same-key exclusion must be index-driven. Read through a scan instead, the ordering guarantee is bought at the price of reading the table's entire history on every poll — correct, and progressively unable to keep up.\nPlan line was: "+earlierScan)
	assert.True(t,
		strings.Contains(earlierScan, "idx_event_outbox_partition_key_inflight") ||
			strings.Contains(earlierScan, "idx_event_outbox_claim"),
		"the earlier-same-key exclusion must be served by a partial index whose predicate is the blocking-state set — either idx_event_outbox_partition_key_inflight probed per candidate, or idx_event_outbox_claim scanned once to build an anti join — so the cold history is excluded entirely. Anything else scans each key's history on every poll.\nPlan line was: "+earlierScan)

	assert.NotContains(t, planText, "Seq Scan on event_outbox",
		"the claim must not sequentially scan blnk.event_outbox.\nPlan was:\n"+planText)

	// A Sort above the index scan is EXPECTED and is deliberately not asserted against.
	// Because the leading index key is matched against a two-element IN-list, a btree
	// cannot emit rows already ordered by the trailing occurred_at, so the planner
	// legitimately sorts. blnk.lineage_outbox has the same index and query shape and the
	// same plan. It costs nothing that matters, because the partial predicate confines
	// that sort's input to the claimable working set rather than to the whole table.
}

// planLineReferencing returns the first line of an EXPLAIN plan that mentions needle,
// trimmed of the tree drawing and indentation.
//
// It exists so a plan assertion can be made about ONE relation rather than about the whole
// plan text. Asserting "the plan contains an index scan somewhere" is nearly vacuous — every
// plan here contains several — whereas asserting that the line reaching
// `event_outbox earlier` is an index scan says exactly what it means.
//
// Parameters:
//   - planText string: the EXPLAIN output, newline separated.
//   - needle string: the relation alias to find.
//
// Returns:
//   - string: the matching line with surrounding whitespace removed, or "" when none
//     matches.
func planLineReferencing(planText, needle string) string {
	for _, line := range strings.Split(planText, "\n") {
		if strings.Contains(line, needle) {
			return strings.TrimSpace(line)
		}
	}

	return ""
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
				"  ->  Bitmap Index Scan on idx_event_outbox_partition_key_inflight  (cost=0.00..40.24 rows=20 width=0)\n",
			want: []string{"idx_event_outbox_partition_key_inflight"},
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
				"  ->  Index Scan using idx_event_outbox_partition_key_inflight on event_outbox earlier\n" +
				"  ->  Index Scan using event_outbox_pkey on event_outbox\n",
			want: []string{"idx_event_outbox_claim", "idx_event_outbox_partition_key_inflight", "event_outbox_pkey"},
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

// TestClaimPendingEventOutbox_ClaimsInBatchesBoundedByBatchSize_RealDB asserts the LIMIT
// is honoured against a real table, and that repeated bounded claims DRAIN the backlog in
// occurrence order.
//
// This is the relay's actual access pattern — a bounded batch once per poll interval —
// and it is the combination the FIFO guarantee has to survive: ordering must hold ACROSS
// batches, not merely within one. Sorting only inside a batch would let a later-occurring
// event in batch one overtake an earlier-occurring event stranded in batch two.
func TestClaimPendingEventOutbox_ClaimsInBatchesBoundedByBatchSize_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
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

// TestListDeadLetteredEvents_PagesNewestFirst_RealDB asserts the ordering and paging the
// dead-letter API relies on, against real SQL.
//
// Triage starts from the most recent failures, so the listing is newest-first. The paging
// assertion matters just as much as the ordering: with a non-deterministic tie-break a
// row can appear on two pages or on none, and an operator working through a dead-letter
// backlog would silently skip events.
func TestListDeadLetteredEvents_PagesNewestFirst_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
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
	all, err := ds.ListDeadLetteredEvents(ctx, maxDeadLetterPageSize, 0)
	require.NoError(t, err)
	assert.Equal(t, newestFirst, mineFrom(all),
		"the dead-letter inventory must be ordered newest occurrence first, because triage starts from the most recent failures")

	// Walk the fixtures a page at a time and assert every one appears exactly once
	// across the pages.
	seen := map[string]int{}
	for offset := 0; offset < len(all); offset += 2 {
		page, pageErr := ds.ListDeadLetteredEvents(ctx, 2, offset)
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
	assert.NoError(t, requireGrantableTopics(model.SubscriberGrantableTopics(expectedEventTopicPrefix())),
		"the composed grantable list must itself be accepted, or two layers disagree")

	refused := map[string]string{
		"the any-resource wildcard": "*",
		"a wildcarded category":     "blnk.*",
		"a foreign topic":           "attacker.transactions",
		"a dead-letter topic":       "blnk.transactions.dlt",
		"the system category":       "blnk.system",
		"the system dead-letter":    "blnk.system.dlt",
	}
	for name, topic := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			requireAPIError(t, requireGrantableTopics([]string{topic}), apierror.ErrInvalidInput)
		})
	}

	// THE SYSTEM CATEGORY IS REFUSED, and it is in the table above rather than here for a
	// reason worth stating, because the opposite reading is tempting: `blnk.system` carries
	// `ledger.created` and `system.error`, two of the thirteen event types the legacy webhook
	// transport delivered, so refusing a grant on it does leave a migrating subscriber with no
	// authorized path to either.
	//
	// That cost is accepted, because the migration changes the AUDIENCE and not only the
	// transport. The webhook transport had exactly one recipient — the single URL in
	// notification.webhook.url, an endpoint the operator owns — while Kafka has one principal
	// per subscriber. `system.error`'s payload is the FROZEN legacy body, so it still carries
	// the error text as it renders: a PostgreSQL error names schema, table, column and
	// routine, a broker error names internal addresses. Granting that to third-party
	// principals to preserve "everything reaches everyone" would WIDEN the disclosure the
	// webhook made, not preserve it — and the body cannot be narrowed without breaking the
	// payload-preservation guarantee the whole migration rests on.
	//
	// Coverage is unaffected: both event types are captured in the outbox, published to the
	// topic, counted by the metrics and replayable from the dead-letter inventory. What an
	// internal category withholds is an ACL, not the event — and a broker administrator can
	// still grant one out of band, which Blnk's own provisioning deliberately will not do.
	// See model.EventCategorySystem, which is the single source of this decision.

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
		composed := model.SubscriberGrantableTopics(expectedEventTopicPrefix())

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
			context.Background(), "acme_prod", &previous, reference, time.Now()))
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
			context.Background(), "acme_prod", nil, reference, time.Now()))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns a conflict when the reference moved under it", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// No row matched...
		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		// ...and the subscriber does exist, so this is a race and not a 404.
		mock.ExpectQuery("SELECT").WillReturnRows(newEventSubscriberRows(t, map[string]driver.Value{
			"id":                   int64(1),
			"subscriber_id":        "acme_prod",
			"name":                 "Acme",
			"kafka_principal":      "blnk-sub-acme_prod",
			"consumer_group_id":    "blnk-sub-acme_prod.default",
			"authorized_topics":    pq.Array([]string{"blnk.transactions"}),
			"credential_reference": previous,
			"credential_issued_at": time.Now(),
			"created_at":           time.Now(),
			"updated_at":           time.Now(),
		}))

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now())
		requireAPIError(t, err, apierror.ErrConflict)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports not-found rather than a conflict for a missing subscriber", func(t *testing.T) {
		// Reporting a conflict here would send an operator looking for a race that
		// did not happen.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now())
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a value that is not a derived reference", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		err := source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", nil, "S3cret-Password", time.Now())
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

		require.NoError(t, source.ClearSubscriberCredential(context.Background(), "acme_prod"))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports not-found for a missing subscriber", func(t *testing.T) {
		// Succeeding silently would let a caller believe it cleaned up a row that
		// does not exist.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))

		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "acme_prod"),
			apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, _ := newSQLMock(t)
		source := Datasource{Conn: db}

		// ErrBadRequest is the code the shared required-field guard uses; both it and
		// ErrInvalidInput are 400, and this asserts the one this path actually takes.
		requireAPIError(t,
			source.ClearSubscriberCredential(context.Background(), "  "),
			apierror.ErrBadRequest)
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

		deleted, err := source.TakeEventSubscriber(context.Background(), "acme_prod")
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

		deleted, err := source.TakeEventSubscriber(context.Background(), "acme_prod")
		assert.Nil(t, deleted)
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does not leak driver detail on failure", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("DELETE FROM blnk.event_subscribers").WillReturnError(&pq.Error{
			Code: "42P01", Message: "relation does not exist",
			Schema: "blnk", Table: "event_subscribers", File: "namespace.c", Routine: "RangeVarGetRelid",
		})

		_, err := source.TakeEventSubscriber(context.Background(), "acme_prod")
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
			return source.UpdateEventSubscriber(context.Background(), canonicalSubscriber(t))
		},
		"DeleteEventSubscriber": func(source Datasource) error {
			return source.DeleteEventSubscriber(context.Background(), "acme_prod")
		},
		"ClearSubscriberCredential": func(source Datasource) error {
			return source.ClearSubscriberCredential(context.Background(), "acme_prod")
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

	mock.ExpectQuery("").
		WithArgs(model.EventOutboxStatusDeadLettered).
		WillReturnRows(sqlmock.NewRows([]string{"published_rows", "confirmed_rows", "distinct_records"}).
			AddRow(int64(1_000), int64(990), int64(990)))

	audit, err := ds.AuditTerminalEventRecords(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(1_000), audit.PublishedRows)
	assert.Equal(t, int64(990), audit.ConfirmedRows)
	assert.Equal(t, int64(10), audit.UnconfirmedRows())
	assert.False(t, audit.FullyConfirmed(),
		"ten rows claim a publication nothing corroborates, so the count cannot be trusted")
	assert.WithinDuration(t, time.Now(), audit.MeasuredAt, time.Minute)

	require.Len(t, *captured, 1)
	issued := (*captured)[0]

	assert.Contains(t, issued, "COUNT(kafka_offset)")
	assert.Contains(t, issued, "COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))")
	assert.Contains(t, issued, "kafka_dispatched_at IS NOT NULL OR status = $1",
		"a webhook_pending row IS on its topic; excluding it would inflate the apparent surplus")
	assert.NoError(t, mock.ExpectationsWereMet())
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

	audit, err := ds.AuditTerminalEventRecords(context.Background())
	requireAPIError(t, err, apierror.ErrInternalServer)
	assert.Zero(t, audit.PublishedRows)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBrokerCoordinate_RoundTripsThroughTheProjection_RealDB proves the coordinate survives the
// write and the read, against the real schema.
//
// The mock tests above assert the statements; only the real database can show that the columns
// exist, that the check constraint accepts a complete coordinate, and that the projection and
// scanner agree on where the three columns sit — a scan-order mistake no compiler catches and
// that surfaces only as mis-assigned field values.
func TestBrokerCoordinate_RoundTripsThroughTheProjection_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
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
	ds := openRealTestDB(t)
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

// TestAuditTerminalEventRecords_SeesTheWholePublishedSet_RealDB is the audit's own round trip.
//
// It builds each of the three shapes the predicate has to cover — a dispatched row that names its
// record, a webhook_pending row whose Kafka leg completed, and a dispatched row with no
// coordinate — and requires the audit to count them the way the reconciliation depends on. The
// webhook_pending case is the one most easily got wrong: it is not terminal, but it IS on the
// topic.
func TestAuditTerminalEventRecords_SeesTheWholePublishedSet_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := newRealEventOutboxMarker("audit")
	t.Cleanup(func() { quiesceEventOutbox(t, ds, marker) })

	before, err := ds.AuditTerminalEventRecords(ctx)
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

	after, err := ds.AuditTerminalEventRecords(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(3), after.PublishedRows-before.PublishedRows,
		"the dispatched, the webhook_pending and the unconfirmed rows all claim a record; the "+
			"untouched row (%d) does not", untouched.ID)
	assert.Equal(t, int64(2), after.ConfirmedRows-before.ConfirmedRows,
		"only the two rows carrying a coordinate name a record")
	assert.Equal(t, int64(2), after.DistinctRecords-before.DistinctRecords,
		"and those two coordinates are distinct")

	// The verdict this produces is the whole point: an unconfirmed claim makes it inconclusive
	// rather than green, however favourable the totals look.
	assert.False(t, after.FullyConfirmed(),
		"one row claims a publication it cannot name a record for, so the count is not trustworthy")
}
