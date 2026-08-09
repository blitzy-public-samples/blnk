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
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// event_subscriber_test.go covers the SCHEMA half of one property of the subscriber
// registry: a subscriber's Kafka identity is DERIVED from its business identifier and may
// not be supplied independently of it.
//
// Why that matters beyond tidiness. blnk.event_subscribers.kafka_principal is the SASL
// principal every ACL binding is attached to, and consumer_group_id is exported as a METRIC
// LABEL VALUE on blnk.kafka.consumer_lag and interpolated into that alert's annotations. So
// the two columns are simultaneously an authorization boundary and a value that leaves the
// process — scraped into a monitoring system, held for the retention period, rendered on
// dashboards and pasted into incident tickets. A caller able to supply a principal of its
// own choosing would be choosing which broker identity Blnk binds ACLs to: a request for a
// NAME would silently be a request for a BOUNDARY.
//
// The contract is enforced at three layers:
//
//  1. THE REPOSITORY derives the principal and the group and refuses a supplied value that
//     differs. Covered by TestRequireSubscriberFields_DerivesTheIdentityAndRefusesASuppliedOne
//     in event_outbox_test.go, beside the rest of the repository's validation.
//  2. THE SCHEMA restates the derivation as CHECK constraints, so a writer that never comes
//     through the repository is refused too. That is what this file covers.
//  3. THE MEASUREMENT collapses a non-conforming value instead of exporting it, covered in
//     the root package where the label resolvers live.
//
// Layer 2 is not redundant with layer 1, and it is the layer most easily assumed rather than
// proven. A migration, a manual INSERT during an incident, or a future service that skips
// the repository is precisely the case an application-layer check cannot cover — the same
// reasoning sql/1781248900.sql gives for enforcing its no-secret-column rule in the schema
// rather than in code.

// TestSubscriberIdentityGenerators_ProduceCanonicalValues pins the property every other
// assertion in this file and in the root package's fixtures depends on.
//
// The generators exist so that a subscriber identifier is OPAQUE: a human-readable one ends
// up naming the customer, and the identifier is copied verbatim into a Kafka principal, a
// consumer group id and a metric label. That is only useful if what they emit is also
// storable — an opaque identifier the schema rejects would simply push every caller back to
// inventing a literal, which is the failure the generators exist to prevent.
//
// It is repeated rather than sampled once because the identifier carries a UUID, and a rule
// that happened to reject a value with, say, a leading digit would pass a single-sample test
// most of the time.
func TestSubscriberIdentityGenerators_ProduceCanonicalValues(t *testing.T) {
	for i := 0; i < 64; i++ {
		subscriberID := model.GenerateSubscriberID()

		canonical, err := model.CanonicalizeSubscriberIdentifier(subscriberID)
		require.NoError(t, err, "a generated subscriber id must canonicalize")
		assert.Equal(t, subscriberID, canonical,
			"canonicalization must be the identity on a generated value, or the stored row and "+
				"the derived principal would disagree")

		assert.True(t, strings.HasPrefix(subscriberID, model.SubscriberIDPrefix+"_"),
			"the module prefix is what makes a subscriber id recognisable in a broker ACL listing")

		group := model.GenerateConsumerGroupID()
		assert.True(t, strings.HasPrefix(group, model.ConsumerGroupIDPrefix),
			"a generated group must lie in the namespace Blnk reserves, since that namespace is "+
				"what the group ACL binding covers")
	}
}

// TestEventSubscriberSchema_RefusesAnUnderivedIdentity is layer 2.
//
// It runs against a real database because a CHECK constraint cannot be proven any other way:
// sqlmock would assert what the test told it to assert. The insert deliberately BYPASSES
// CreateEventSubscriber and issues raw SQL, because going through the repository would be
// stopped by layer 1 and would prove nothing about the schema.
//
// It skips rather than fails when no database is reachable, matching openRealTestDB's
// existing contract in this package.
func TestEventSubscriberSchema_RefusesAnUnderivedIdentity(t *testing.T) {
	datasource := openRealTestDB(t)
	ctx := context.Background()

	if _, err := datasource.Conn.ExecContext(ctx, `SELECT 1 FROM blnk.event_subscribers LIMIT 1`); err != nil {
		t.Skipf("blnk.event_subscribers is not present in the test database: %v", err)
	}

	// THE PRECONDITION, ASSERTED BEFORE THE BEHAVIOUR. Naming the absent object and the command
	// that restores it is the difference between one line and ten minutes; see the helper.
	for _, constraint := range []string{
		"event_subscribers_principal_derived_chk",
		"event_subscribers_group_derived_chk",
	} {
		requireSubscriberConstraint(ctx, t, datasource, constraint)
	}

	insert := func(subscriberID, principal, groupID string) error {
		_, err := datasource.Conn.ExecContext(ctx, `
			INSERT INTO blnk.event_subscribers
				(subscriber_id, name, kafka_principal, consumer_group_id)
			VALUES ($1, $2, $3, $4)
		`, subscriberID, "schema constraint probe", principal, groupID)

		return err
	}

	derived := func(t *testing.T, subscriberID string) (string, string) {
		t.Helper()

		principal, err := model.CanonicalKafkaPrincipal(subscriberID)
		require.NoError(t, err)
		group, err := model.CanonicalConsumerGroupID(subscriberID)
		require.NoError(t, err)

		return principal, group
	}

	t.Run("a derived triple is accepted", func(t *testing.T) {
		subscriberID := model.GenerateSubscriberID()
		principal, group := derived(t, subscriberID)

		require.NoError(t, insert(subscriberID, principal, group),
			"the constraints must accept exactly what the derivation produces, or the repository "+
				"and the schema disagree and no row can be written at all")

		t.Cleanup(func() {
			_, err := datasource.Conn.ExecContext(ctx,
				`DELETE FROM blnk.event_subscribers WHERE subscriber_id = $1`, subscriberID)
			require.NoError(t, err)
		})
	})

	t.Run("a name-derived principal is refused by the database", func(t *testing.T) {
		// A plausible, well-intentioned, human-readable principal. Every character in it
		// would satisfy a "letters, digits, dots, hyphens and underscores" class rule, and it
		// names a customer — which is why the constraint compares against the derivation
		// instead of against a character class.
		subscriberID := model.GenerateSubscriberID()
		_, group := derived(t, subscriberID)

		err := insert(subscriberID, "acme-recon", group)
		require.Error(t, err, "the schema must refuse a principal it did not derive")
		assert.Contains(t, strings.ToLower(err.Error()), "principal_derived",
			"the violated constraint must be named, so the failure is diagnosable")
	})

	t.Run("a name-derived consumer group is refused by the database", func(t *testing.T) {
		subscriberID := model.GenerateSubscriberID()
		principal, _ := derived(t, subscriberID)

		err := insert(subscriberID, principal, "acme-recon-group")
		require.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "group_derived")
	})

	t.Run("an upper-case identifier is refused, matching the canonical lower-case form", func(t *testing.T) {
		// Two identifiers differing only in case read as the same subscriber to every human
		// who sees them, and Kafka compares principals byte-for-byte — so accepting both
		// would provision two boundaries under one apparent identity.
		subscriberID := strings.ToUpper(model.GenerateSubscriberID())
		principal := model.SubscriberPrincipalNamespace + subscriberID
		group := principal + model.SubscriberGroupTerminator + model.SubscriberDefaultGroupLeaf

		err := insert(subscriberID, principal, group)
		require.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "subscriber_id")
	})

	t.Run("another subscriber's group is refused at rest", func(t *testing.T) {
		// The hazard this closes is not a malformed value but a well-formed one belonging to
		// SOMEONE ELSE: two subscribers in one consumer group SPLIT the stream rather than
		// each receiving it, so each silently sees a subset of its own events — an event loss
		// that looks like a publisher fault.
		subscriberID := model.GenerateSubscriberID()
		principal, _ := derived(t, subscriberID)
		_, foreignGroup := derived(t, model.GenerateSubscriberID())

		err := insert(subscriberID, principal, foreignGroup)
		require.Error(t, err)
		assert.Contains(t, strings.ToLower(err.Error()), "group_derived")
	})

	t.Run("a key scope alongside a credential is accepted by the database", func(t *testing.T) {
		// THERE IS NO PRECONDITION TO ASSERT HERE, and there used to be one that asserted the
		// opposite of what this subtest now proves: it required
		// event_subscribers_key_scope_chk to be PRESENT before exercising the refusal it
		// enforced. The refusal is withdrawn — sql/1781248930.sql drops the constraint, this
		// subtest's body asserts the combination is writable in both orders, and its closing
		// assertion requires the constraint to be absent BY NAME. A precondition demanding the
		// presence of the very object the closing assertion forbids can only ever fail, and it
		// failed with the words "PRECONDITION MISSING ... the schema has drifted", which sent a
		// reader to re-run migrations that had in fact applied correctly.
		//
		// The absence is checked at the END rather than as a precondition, and deliberately: this
		// is a test about what the database ACCEPTS, so the writes are the evidence and the
		// name-level check is the guard against a future migration reintroducing it.

		// SEC-05's THIRD barrier, and the only one that also covers a psql session, a data
		// migration and a restored backup.
		//
		// event_subscribers_key_scope_chk once forbade this combination outright, on the
		// reasoning that a prefix records a boundary Kafka cannot enforce and so a row holding
		// both states a tenancy boundary that does not exist. What that actually did was make a
		// REQUIRED capability unreachable: a subscriber registered with a prefix could never be
		// issued credentials, from either direction, permanently.
		//
		// The combination is legitimate, because a prefix is a routing hint rather than an access
		// boundary — Kafka has no message-key ACL resource, so there is no narrower credential
		// being withheld — and the API states the boundary instead of prohibiting the state.
		// sql/1781248930.sql drops the constraint, including on databases that already created
		// it, and this asserts that the drop reached THIS database rather than only the file.
		//
		// It matters that the assertion lives here. The service layer can be tested against a
		// double, but a lingering CHECK would surface only as a raw write failure at runtime —
		// no code maps it to a typed error any more — and only a real database can answer
		// whether it is gone.
		subscriberID := model.GenerateSubscriberID()
		principal, group := derived(t, subscriberID)

		require.NoError(t, insert(subscriberID, principal, group))
		t.Cleanup(func() {
			_, err := datasource.Conn.ExecContext(ctx,
				`DELETE FROM blnk.event_subscribers WHERE subscriber_id = $1`, subscriberID)
			require.NoError(t, err)
		})

		reference, err := model.DeriveCredentialReference(principal, "a-secret-that-was-returned-once")
		require.NoError(t, err)

		keyScope := "ldg_" + subscriberID

		_, err = datasource.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET partition_key_prefix = $1
			WHERE subscriber_id = $2
		`, keyScope, subscriberID)
		require.NoError(t, err, "a key scope on a row with no credential must remain writable")

		// THE COMBINATION, recorded in the order "prefix first, then credential" — which is
		// registering with a prefix and then asking for credentials, the exact sequence the
		// constraint made impossible.
		_, err = datasource.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET credential_reference = $1, credential_issued_at = NOW()
			WHERE subscriber_id = $2
		`, reference, subscriberID)
		require.NoError(t, err,
			"THE DATABASE MUST ACCEPT A CREDENTIAL RECORD ON A KEY-SCOPED ROW. Refusing it is the "+
				"withdrawn capability surviving in the schema: issuance would fail on a check "+
				"violation that nothing maps to a typed error, so the mandatory credential endpoint "+
				"would answer 500 for a state this table is designed to hold")

		// And the reverse order too — recording a prefix on a row that already holds a
		// credential — because the constraint blocked both and a caller can arrive either way.
		_, err = datasource.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET partition_key_prefix = $1
			WHERE subscriber_id = $2
		`, "ldg_late_"+subscriberID, subscriberID)
		require.NoError(t, err,
			"and the reverse order must be writable too: an operator that issued first and "+
				"recorded a prefix afterwards is making an ordinary registry edit, not widening "+
				"any grant")

		// The row really holds both afterwards. A write that succeeded while silently dropping
		// one of the two would satisfy the assertions above and lose the operator's intent.
		var storedPrefix, storedReference *string
		require.NoError(t, datasource.Conn.QueryRowContext(ctx, `
			SELECT partition_key_prefix, credential_reference
			FROM blnk.event_subscribers
			WHERE subscriber_id = $1
		`, subscriberID).Scan(&storedPrefix, &storedReference))

		require.NotNil(t, storedPrefix, "the prefix must be persisted, not silently dropped")
		assert.Equal(t, "ldg_late_"+subscriberID, *storedPrefix)
		require.NotNil(t, storedReference, "and the credential reference must survive beside it")
		assert.Equal(t, reference, *storedReference)

		// The constraint is gone by NAME, not merely inoperative for these values. This is what
		// fails loudly if a future migration reintroduces it.
		var constraintCount int
		require.NoError(t, datasource.Conn.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM pg_constraint
			WHERE conname = 'event_subscribers_key_scope_chk'
			  AND conrelid = 'blnk.event_subscribers'::regclass
		`).Scan(&constraintCount))
		assert.Zero(t, constraintCount,
			"event_subscribers_key_scope_chk must not exist: reintroducing it restores the "+
				"permanent 409 on the mandatory credential endpoint")
	})

	t.Run("a group leaf inside the subscriber's own namespace is accepted at rest", func(t *testing.T) {
		// A subscriber running several consumer instances legitimately commits under its own
		// leaves, and the provisioned ACL grants the namespace with a PREFIXED pattern
		// precisely so it may. A constraint that pinned the default leaf would make the
		// grant unusable.
		subscriberID := model.GenerateSubscriberID()
		principal, _ := derived(t, subscriberID)

		namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID)
		require.NoError(t, err)

		require.NoError(t, insert(subscriberID, principal, namespace+"worker-1"))

		t.Cleanup(func() {
			_, err := datasource.Conn.ExecContext(ctx,
				`DELETE FROM blnk.event_subscribers WHERE subscriber_id = $1`, subscriberID)
			require.NoError(t, err)
		})
	})
}

// TestCountSubscriberRevocationsPending_ReadsTheBacklogAsAnAggregate covers the read that gave
// two declared-but-never-recorded gauges a value, and with them an alert rule that could not
// fire.
//
// A registry row carrying revocation_pending_at is a principal that may still authenticate at
// the broker while nothing in Blnk records an issuance for it. Deregistration revokes first and
// deletes only once the revocation is confirmed, so the marker is a durable to-do item — and
// until this read existed, nothing observed it: blnk_subscribers_revocation_pending and
// blnk_subscribers_oldest_revocation_age_seconds were exported by nothing and
// SubscriberRevocationOutstanding sat permanently inactive with a healthy-looking rule.
//
// The three assertions are the three properties the gauges depend on: it is an AGGREGATE (two
// scalars, no row scan), it filters to unsettled markers only, and a NULL minimum — the healthy
// case — comes back as the zero instant rather than a scan error.
func TestCountSubscriberRevocationsPending_ReadsTheBacklogAsAnAggregate(t *testing.T) {
	t.Run("an outstanding backlog reports its count and its oldest instant", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		ds := Datasource{Conn: db}

		oldest := time.Date(2026, 8, 7, 9, 30, 0, 0, time.UTC)
		mock.ExpectQuery("").WillReturnRows(
			sqlmock.NewRows([]string{"count", "min"}).AddRow(int64(3), oldest))

		backlog, err := ds.CountSubscriberRevocationsPending(context.Background())
		require.NoError(t, err)
		require.Len(t, *captured, 1)
		issued := (*captured)[0]

		assert.Contains(t, issued, "SELECT COUNT(*), MIN(revocation_pending_at)",
			"both figures must come from ONE aggregate: the cost of observing a backlog must not "+
				"grow with the backlog, which is the same reasoning the outbox backlog gauge uses")
		assert.Contains(t, issued, "WHERE revocation_pending_at IS NOT NULL",
			"a settled subscriber is not an outstanding obligation, and counting one would make the "+
				"gauge report exposure that does not exist")
		assert.NotContains(t, issued, "LIMIT",
			"there is no paging here: the aggregate returns two scalars whatever the registry size, "+
				"so it needs neither a page nor the lag sweep's cardinality budget")
		assert.NotContains(t, issued, "subscriber_id",
			"and no per-subscriber breakdown: subscriber identifiers are unbounded in cardinality "+
				"and would export a tenant identifier into every series and every notification")

		assert.Equal(t, int64(3), backlog.Pending)
		assert.Equal(t, oldest, backlog.OldestPendingAt)
		assert.Equal(t, 30*time.Minute,
			backlog.OldestAge(oldest.Add(30*time.Minute)),
			"and the age the alert reads is measured from that instant")

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nothing outstanding is a reading and not an absence", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		// COUNT returns 0 and MIN returns NULL. The NULL must be scanned through a nullable
		// time, or the healthy steady state — by far the commonest one — fails the read and the
		// caller publishes nothing for a system with nothing wrong.
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*), MIN(revocation_pending_at)")).
			WillReturnRows(sqlmock.NewRows([]string{"count", "min"}).AddRow(int64(0), nil))

		backlog, err := ds.CountSubscriberRevocationsPending(context.Background())
		require.NoError(t, err, "a NULL minimum is the healthy case, not a failure")

		assert.Zero(t, backlog.Pending)
		assert.True(t, backlog.OldestPendingAt.IsZero(),
			"the zero instant is what tells the caller to publish a zero age rather than the age "+
				"of the epoch, which would exceed every threshold for ever")
		assert.Zero(t, backlog.OldestAge(time.Now()))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a failed read reports rather than returning a fabricated zero", func(t *testing.T) {
		db, mock := newSQLMock(t)
		ds := Datasource{Conn: db}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*), MIN(revocation_pending_at)")).
			WillReturnError(errors.New("dial tcp: connection refused"))

		backlog, err := ds.CountSubscriberRevocationsPending(context.Background())
		require.Error(t, err)
		assert.Equal(t, model.SubscriberRevocationBacklog{}, backlog)

		var apiErr apierror.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// fenceMissRows builds the two-column answer describeFencedWriteMiss reads.
//
// The two facts are read in ONE statement rather than through GetEventSubscriberByID because
// provisioning_token is deliberately not a field of the read model — exposing a
// concurrency-control secret on every API response is the disclosure that omission prevents —
// so it cannot be inspected from a row struct.
//
// Parameters:
//   - holdsClaim bool: whether the stored claim is the one the caller presented.
//   - revocationPending bool: whether the row carries the revocation tombstone.
//
// Returns:
//   - *sqlmock.Rows: the answer row.
func fenceMissRows(holdsClaim, revocationPending bool) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"holds_claim", "revocation_pending"}).
		AddRow(holdsClaim, revocationPending)
}

// TestUpdateEventSubscriber_IsFencedAndRefusesATombstonedRow covers both predicates the
// authorization write carries, and the two fail-OPEN paths their absence left behind.
//
// The statement used to match on subscriber_id alone:
//
//   - WITHOUT THE CLAIM, an operation whose lease had expired still landed its write, on top of
//     the authorization the new owner had just reconciled with the broker. The registry then
//     described one boundary while Kafka enforced another, and the call reported success.
//   - WITHOUT THE TOMBSTONE CHECK, an update was accepted on a row being deregistered. A
//     non-nil AuthorizedTopics REPLACES the whole set, so it could WIDEN the authorization —
//     and the service's grant step then created bindings for a principal whose revocation was
//     already in flight. Access regained while being removed, recorded as an ordinary edit.
func TestUpdateEventSubscriber_IsFencedAndRefusesATombstonedRow(t *testing.T) {
	t.Run("the claim and the tombstone are both predicates of the write", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		stored := time.Now().UTC()
		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(4),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions"}),
				"created_at":        stored.Add(-time.Hour),
				"updated_at":        stored,
			}))

		updated, err := source.UpdateEventSubscriber(
			context.Background(), canonicalSubscriber(t), "claim-token")
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		statement := (*captured)[0]
		assert.Contains(t, statement, "provisioning_token = $9",
			"a caller whose lease expired must not be able to write")
		assert.Contains(t, statement, "revocation_pending_at IS NULL",
			"an authorization change must not be accepted on a row being deregistered")

		// RETURNING, so the caller answers with the row as STORED. An ExecContext left the
		// service returning the row it had assembled, carrying the updated_at it read BEFORE
		// the write — an instant strictly older than the stored one, which a client using it
		// to detect concurrent modification compares against its own write and reads as
		// unchanged.
		assert.Contains(t, statement, "RETURNING",
			"the write must return the row it wrote, or the response describes a pre-write state")
		require.NotNil(t, updated)
		assert.WithinDuration(t, stored, updated.UpdatedAt, 0,
			"the returned updated_at must be the stored one")
	})

	t.Run("refuses a write that presents no claim", func(t *testing.T) {
		// Defaulting a missing token to "unfenced" would restore the exact behaviour the
		// predicate removes, so the guard runs in memory before any statement.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		_, err := source.UpdateEventSubscriber(
			context.Background(), canonicalSubscriber(t), "   ")
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a lost claim is a conflict naming the claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// NO ROWS is what a failed predicate looks like now that the statement RETURNS the row:
		// the same condition the affected-row count used to report, reached through the scan.
		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(fenceMissRows(false, false))

		_, err := source.UpdateEventSubscriber(
			context.Background(), canonicalSubscriber(t), "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a tombstoned row is a conflict naming the deregistration", func(t *testing.T) {
		// Distinct from the lost-claim message because the remedies differ: one says retry
		// under a fresh claim, the other says finish or reverse the deregistration.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(fenceMissRows(true, true))

		_, err := source.UpdateEventSubscriber(
			context.Background(), canonicalSubscriber(t), "claim-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "being deregistered")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a vanished row is a not-found, never a conflict", func(t *testing.T) {
		// "Your claim expired" and "this subscriber does not exist" call for opposite
		// responses, and collapsing them sends an operator to the wrong one.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnError(sql.ErrNoRows)

		_, err := source.UpdateEventSubscriber(
			context.Background(), canonicalSubscriber(t), "claim-token")
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestMarkSubscriberRevocationPending_IsFencedButAcceptsAnExistingTombstone pins the ONE
// deliberate asymmetry in the fenced-write family.
//
// Every other mutation refuses a tombstoned row. This one must not: an already-tombstoned row
// IS a deregistration that failed part way through, and the correct response to retrying it is
// to FINISH it. That is also why the stamp keeps the first instant — the value an operator needs
// is how long the revocation has been outstanding, not the age of the last attempt.
func TestMarkSubscriberRevocationPending_IsFencedButAcceptsAnExistingTombstone(t *testing.T) {
	t.Run("carries the claim but not the tombstone predicate", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                    int64(3),
				"subscriber_id":         "acme_prod",
				"name":                  "Acme",
				"kafka_principal":       "blnk-sub-acme_prod",
				"consumer_group_id":     "blnk-sub-acme_prod.default",
				"authorized_topics":     pq.Array([]string{"blnk.transactions"}),
				"revocation_pending_at": time.Now(),
				"created_at":            time.Now(),
				"updated_at":            time.Now(),
			}))

		marked, err := source.MarkSubscriberRevocationPending(
			context.Background(), "acme_prod", time.Now(), "claim-token")
		require.NoError(t, err)
		require.NotNil(t, marked)
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		statement := (*captured)[0]
		assert.Contains(t, statement, "provisioning_token = $4")
		assert.NotContains(t, statement, "revocation_pending_at IS NULL",
			"a deregistration retry must be able to re-mark a row that is already tombstoned")
		assert.Contains(t, statement, "COALESCE(revocation_pending_at",
			"and re-marking must keep the FIRST instant, or the backlog age always reads small")
	})

	t.Run("refuses a mark that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		_, err := source.MarkSubscriberRevocationPending(
			context.Background(), "acme_prod", time.Now(), "")
		requireAPIError(t, err, apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a lost claim is a conflict", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(fenceMissRows(false, true))

		_, err := source.MarkSubscriberRevocationPending(
			context.Background(), "acme_prod", time.Now(), "stale-token")
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestRenewSubscriberProvisioningFence_ExtendsOnlyTheCallersOwnClaim covers the write that
// makes a short lease compatible with a long operation.
//
// The lease has to be SHORT, because it is also the recovery time after a crash. But the work
// under it is a variable-length sequence of Kafka administrative round trips, so no single
// fixed lease is both short enough to recover quickly and long enough for the worst case. Before
// renewal existed the operation simply continued past its expired claim.
func TestRenewSubscriberProvisioningFence_ExtendsOnlyTheCallersOwnClaim(t *testing.T) {
	t.Run("extends from NOW under the caller's token", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RenewSubscriberProvisioningFence(
			context.Background(), "acme_prod", "claim-token", 25*time.Second))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		statement := (*captured)[0]
		assert.Contains(t, statement, "provisioning_token = $4",
			"renewal must be conditional, or it would revive a claim already taken over")
		assert.Contains(t, statement, "provisioning_until = NOW() +",
			"the deadline is recomputed from now, so a renewal grants exactly one lease of headroom")
		assert.NotContains(t, statement, "provisioning_token = $1",
			"renewal must NOT re-claim: the new owner is mid-flight against the broker")
	})

	t.Run("refuses a renewal that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.RenewSubscriberProvisioningFence(
			context.Background(), "acme_prod", " ", time.Second),
			apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a taken-over claim is a conflict, not a silent success", func(t *testing.T) {
		// This is the outcome the caller must act on: it has learned it does not own the
		// subscriber, so the broker phase that was about to run must be abandoned.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(fenceMissRows(false, false))

		err := source.RenewSubscriberProvisioningFence(
			context.Background(), "acme_prod", "stale-token", time.Second)
		requireAPIError(t, err, apierror.ErrConflict)
		assert.Contains(t, err.Error(), "no longer held by this caller")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("normalises a non-positive lease rather than failing", func(t *testing.T) {
		// A claim that expires on arrival fences nothing, but refusing the call outright would
		// stop provisioning altogether — so the fallback matches the claim path's.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RenewSubscriberProvisioningFence(
			context.Background(), "acme_prod", "claim-token", 0))
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// requireSubscriberConstraint fails the test unless a named constraint is present.
//
// IT IS THE PRECONDITION ASSERTION for the schema-enforced refusals below, and it exists because
// a constraint-backed test has two distinct ways to fail that produce the same words: the
// constraint is missing from a drifted schema, or it is present and has stopped refusing. Both
// report "an error was expected but got nil", and they send a reader to opposite places.
//
// It asserts that a named constraint is present on blnk.event_subscribers, and says what to do
// about it when it is not. Its earlier caller went away because the constraint THAT one named was
// dropped by design, not because the pattern stopped being right.
//
// It exists to separate a MISSING PRECONDITION from a BROKEN BEHAVIOUR. A constraint-backed test
// whose constraint has gone reports "an error was expected but got nil" — the same words the
// genuine regression produces — so the two become indistinguishable at exactly the moment the
// distinction matters. Naming the missing object and the command that restores it turns ten
// minutes of confusion into one line.
//
// Parameters:
//   - ctx context.Context: cancels the lookup.
//   - t *testing.T: the test.
//   - datasource Datasource: the connection.
//   - constraint string: the constraint name, as it appears in pg_constraint.
func requireSubscriberConstraint(
	ctx context.Context,
	t *testing.T,
	datasource Datasource,
	constraint string,
) {
	t.Helper()

	var present bool
	err := datasource.Conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = $1
			  AND conrelid = 'blnk.event_subscribers'::regclass
		)
	`, constraint).Scan(&present)
	require.NoError(t, err, "could not read pg_constraint to check the test's precondition")

	require.True(t, present,
		"PRECONDITION MISSING, not a behavioural regression: blnk.event_subscribers carries no "+
			"constraint %q, so there is nothing here for this test to exercise. The schema has "+
			"drifted from the migrations — re-run `blnk migrate up`, and if the migration ledger "+
			"already records it as applied (which happens when the database is shared with "+
			"something that rolled it back), re-apply that migration's own ADD CONSTRAINT body by "+
			"hand. Restore the constraint and run this test again before reading anything else "+
			"into it.", constraint)
}

// TestCompleteSubscriberWebhookMigration_MovesBothColumnsInOneStatement is the repository half of
// the cutover atomicity guard.
//
// The endpoint used to issue two statements with no transaction spanning them, and a failure
// between them left the row absent from BOTH halves of the migration report: webhook_url gone, so
// nothing still to migrate from; migrated_at NULL, so not counted as migrated. The property that
// removes the window is not "the write succeeds" but that there is only ONE statement, so this
// test reads the SQL the database would really have received rather than trusting the call.
func TestCompleteSubscriberWebhookMigration_MovesBothColumnsInOneStatement(t *testing.T) {
	t.Run("one statement sets both columns and returns the row", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		migratedAt := time.Now().UTC()

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(11),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions"}),
				"webhook_url":       nil,
				"migrated_at":       migratedAt,
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}))

		migrated, err := source.CompleteSubscriberWebhookMigration(
			context.Background(), "acme_prod", migratedAt)
		require.NoError(t, err)
		require.NotNil(t, migrated)

		assert.Nil(t, migrated.WebhookURL)
		require.NotNil(t, migrated.MigratedAt)
		assert.WithinDuration(t, migratedAt, migrated.MigratedAt.UTC(), time.Second)

		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1,
			"the cutover must be ONE statement; a second is the window that stranded the row")

		statement := (*captured)[0]
		assert.Contains(t, statement, "webhook_url = NULL")
		assert.Contains(t, statement, "RETURNING",
			"the row is returned so a caller can confirm both facts without re-reading")

		// COALESCE, AND NOT A BARE ASSIGNMENT. This statement is idempotent by design — the
		// endpoint can legitimately be called again on a row already migrated, and webhook_url is
		// already NULL by then so nothing else distinguishes the repeat — but an unconditional
		// `migrated_at = $1` rewrote the instant on every call. The column records WHEN a
		// subscriber left HTTP delivery, which is what migration-progress reporting reads and what
		// an operator uses to judge whether the sunset window has been served, so a repeat made a
		// long-migrated subscriber look like it moved today.
		assert.Contains(t, statement, "migrated_at = COALESCE(migrated_at, $1)",
			"the FIRST transition must be preserved: a repeated call must not move the migration "+
				"instant forward")
		assert.NotContains(t, statement, "migrated_at = $1,",
			"a bare assignment is the defect: it overwrites the first transition on every repeat")
	})

	t.Run("carries no fence and no tombstone predicate, deliberately", func(t *testing.T) {
		// The provisioning claim guards the writes whose state the BROKER also holds — the access
		// model and the credential record. Neither column here has a broker counterpart, so there
		// is no second system for a stale caller to make disagree.
		//
		// And the tombstone predicate is absent because this write ERASES third-party data Blnk
		// has a retention obligation for. Blocking that because the subscriber happens to be
		// mid-deregistration would put the obligation behind somebody else's stuck operation, and
		// PurgeMigratedSubscriberWebhookURLs — the bulk form of the same erasure — carries no such
		// predicate either. The two forms must not disagree about when the data may go.
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnRows(
			newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(11),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions"}),
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}))

		_, err := source.CompleteSubscriberWebhookMigration(
			context.Background(), "acme_prod", time.Now())
		require.NoError(t, err)
		require.Len(t, *captured, 1)

		// The PREDICATE only. Both column names also appear in the RETURNING projection, which
		// every reader of the row legitimately needs, so asserting on the whole statement would
		// assert the opposite of what it looks like.
		predicate := whereClauseOf(t, (*captured)[0])

		assert.NotContains(t, predicate, "provisioning_token",
			"a bookkeeping write with no broker counterpart must not require a provisioning claim")
		assert.NotContains(t, predicate, "revocation_pending_at",
			"erasing third-party data must not wait on another operation's state")
		assert.Contains(t, predicate, "subscriber_id",
			"and the row must still be located by its business key")
	})

	t.Run("substitutes the current time for a zero instant", func(t *testing.T) {
		// A zero timestamp is still NOT NULL, so it would read back as "migrated in year 1" and
		// quietly corrupt progress reporting rather than leaving the row counted as unmigrated.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		var bound driver.Value
		mock.ExpectQuery("UPDATE blnk.event_subscribers").
			WithArgs(captureArg(&bound), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(newEventSubscriberRows(t, map[string]driver.Value{
				"id":                int64(11),
				"subscriber_id":     "acme_prod",
				"name":              "Acme",
				"kafka_principal":   "blnk-sub-acme_prod",
				"consumer_group_id": "blnk-sub-acme_prod.default",
				"authorized_topics": pq.Array([]string{"blnk.transactions"}),
				"created_at":        time.Now(),
				"updated_at":        time.Now(),
			}))

		_, err := source.CompleteSubscriberWebhookMigration(
			context.Background(), "acme_prod", time.Time{})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())

		stamped, isTime := bound.(time.Time)
		require.True(t, isTime, "migrated_at must be bound as a time, not interpolated")
		assert.False(t, stamped.IsZero(), "a zero instant must never be stored")
		assert.WithinDuration(t, time.Now(), stamped, time.Minute)
	})

	t.Run("reports not-found for a missing subscriber", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(sql.ErrNoRows)

		_, err := source.CompleteSubscriberWebhookMigration(
			context.Background(), "acme_prod", time.Now())
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		_, err := source.CompleteSubscriberWebhookMigration(context.Background(), "  ", time.Now())
		requireAPIError(t, err, apierror.ErrBadRequest)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does not leak driver detail on failure", func(t *testing.T) {
		// DATA-01: a raw *pq.Error attached to APIError.Details serialises the database's schema,
		// table, column and constraint names straight into the response body.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("UPDATE blnk.event_subscribers").WillReturnError(&pq.Error{
			Code: "42P01", Message: "relation does not exist",
			Schema: "blnk", Table: "event_subscribers", File: "namespace.c", Routine: "RangeVarGetRelid",
		})

		_, err := source.CompleteSubscriberWebhookMigration(
			context.Background(), "acme_prod", time.Now())
		requireAPIError(t, err, apierror.ErrInternalServer)
		assertNoDatabaseDetailLeak(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// ---------------------------------------------------------------------------------------
// SETTLEMENT OBLIGATIONS — the durable record of broker-side work a request could not finish
//
// A subscriber's state lives in two systems that cannot be written atomically. Every
// intermediate state of an authorization change or a credential issuance is reachable, and each
// used to be recorded as nothing more than a log line — some ending in "revoke it by hand
// immediately". The five columns these statements maintain are what replace that.
//
// The properties pinned below are the ones the mechanism's correctness rests on: the marker
// keeps its FIRST instant so its age is the age of the divergence, every write is conditional on
// the caller's claim, the scan's predicate matches the partial index EXACTLY, and a successful
// issuance or clear DISCHARGES a pending cleanup in the same statement — which is what stops
// settlement destroying a credential that has just been issued.
// ---------------------------------------------------------------------------------------

// TestRecordSubscriberGrantReconcilePending_KeepsTheFirstInstantUnderTheCallersClaim covers the
// marker an authorization change writes before it touches the broker.
func TestRecordSubscriberGrantReconcilePending_KeepsTheFirstInstantUnderTheCallersClaim(t *testing.T) {
	t.Run("coalesces so the age is the age of the divergence", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("grant_reconcile_pending_at").WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", time.Now(), "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0],
			"grant_reconcile_pending_at = COALESCE(grant_reconcile_pending_at, $1)",
			"overwriting the instant on every retry would make an obligation outstanding for a day "+
				"look freshly raised — which is exactly the signal an age-based alert reads")
		assert.Contains(t, (*captured)[0], "provisioning_token = $4",
			"an operation whose lease expired must not raise an obligation about work a newer "+
				"owner is now doing")
	})

	t.Run("substitutes now for a zero instant", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// A zero instant would render as the epoch and make the obligation's age exceed every
		// threshold for ever, so it is replaced rather than stored.
		mock.ExpectExec("grant_reconcile_pending_at").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "acme_prod", "claim-token").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", time.Time{}, "claim-token"))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("refuses a write that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.RecordSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", time.Now(), "  "), apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, _ := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.RecordSubscriberGrantReconcilePending(
			context.Background(), "  ", time.Now(), "claim-token"), apierror.ErrBadRequest)
	})

	t.Run("reports a lost claim as a conflict, never as not-found", func(t *testing.T) {
		// The remedies are opposite: a 404 says re-register, a 409 says the operation was
		// overtaken and its write was correctly discarded.
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("provisioning_token IS NOT DISTINCT FROM").
			WillReturnRows(fenceMissRows(false, false))

		requireAPIError(t, source.RecordSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", time.Now(), "stale-token"), apierror.ErrConflict)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a driver failure leaks no detail", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("UPDATE blnk.event_subscribers").
			WillReturnError(errors.New("dial tcp 10.1.2.3:5432: connection refused"))

		err := source.RecordSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", time.Now(), "claim-token")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assertNoDatabaseDetailLeak(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestClearSubscriberGrantReconcilePending_ResetsTheCountersOnlyWhenNothingRemains covers the
// discharge.
//
// The conditional reset is the subtle half: the settlement counters belong to whatever is STILL
// outstanding, so a row that also owes a credential cleanup keeps its history. Resetting
// unconditionally would restart the pacing of an obligation that is still failing.
func TestClearSubscriberGrantReconcilePending_ResetsTheCountersOnlyWhenNothingRemains(t *testing.T) {
	t.Run("nulls the marker and conditionally resets the counters", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("grant_reconcile_pending_at = NULL").WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.ClearSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		issued := (*captured)[0]
		assert.Contains(t, issued, "grant_reconcile_pending_at = NULL")
		assert.Contains(t, issued, "WHEN credential_cleanup_pending_at IS NULL THEN 0",
			"the attempt history belongs to whatever is still owed; resetting it while a "+
				"credential cleanup remains outstanding would restart the pacing of a failing retry")
		assert.Contains(t, issued, "provisioning_token = $3")
	})

	t.Run("refuses a write that presents no claim", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.ClearSubscriberGrantReconcilePending(
			context.Background(), "acme_prod", ""), apierror.ErrInvalidInput)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestRecordSubscriberCredentialCleanupPending_KeepsTheFirstInstantUnderTheCallersClaim covers
// the marker raised by the two credential failure paths.
func TestRecordSubscriberCredentialCleanupPending_KeepsTheFirstInstantUnderTheCallersClaim(t *testing.T) {
	t.Run("coalesces and is fenced", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("credential_cleanup_pending_at").WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialCleanupPending(
			context.Background(), "acme_prod", time.Now(), "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0],
			"credential_cleanup_pending_at = COALESCE(credential_cleanup_pending_at, $1)")
		assert.Contains(t, (*captured)[0], "provisioning_token = $4")
	})

	t.Run("requires a claim and a subscriber", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.RecordSubscriberCredentialCleanupPending(
			context.Background(), "acme_prod", time.Now(), ""), apierror.ErrInvalidInput)
		requireAPIError(t, source.RecordSubscriberCredentialCleanupPending(
			context.Background(), "", time.Now(), "claim-token"), apierror.ErrBadRequest)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestRecordSubscriberCredentialIfUnchanged_DischargesAPendingCleanup is the SELF-SATISFYING
// case, and the one that would be dangerous to get wrong.
//
// Provisioning UPSERTS the principal's SCRAM credential, so an issuance that reaches this write
// has replaced whatever orphan a pending cleanup was about. Left outstanding, the next settlement
// pass would revoke the credential just issued and handed to a subscriber. Two statements would
// leave a window in which exactly that could happen, so it is cleared in the SAME statement.
func TestRecordSubscriberCredentialIfUnchanged_DischargesAPendingCleanup(t *testing.T) {
	reference, err := model.DeriveCredentialReference("User:blnk-sub-acme_prod", "s3cret-value")
	require.NoError(t, err)

	t.Run("first issuance", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("credential_reference IS NULL").WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", nil, reference, time.Now(), "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0], "credential_cleanup_pending_at = NULL",
			"the same statement that records the issuance must discharge the cleanup, or "+
				"settlement destroys the credential this write just recorded")
	})

	t.Run("re-issuance", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		previous := "blnk-cred-ref-" + strings.Repeat("0", 64)
		mock.ExpectExec("credential_reference = ").WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.RecordSubscriberCredentialIfUnchanged(
			context.Background(), "acme_prod", &previous, reference, time.Now(), "claim-token"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		assert.Contains(t, (*captured)[0], "credential_cleanup_pending_at = NULL",
			"BOTH arms must discharge it; the re-issuance arm is the one the dangerous case "+
				"actually takes")
		assert.Contains(t, (*captured)[0], "WHEN grant_reconcile_pending_at IS NULL THEN 0")
	})
}

// TestClearSubscriberCredential_DischargesAPendingCleanup is the other statement that satisfies
// the obligation, and the one settlement itself calls.
//
// The row's credential columns and the cleanup marker describe ONE fact — whether Blnk records a
// credential it has not settled — so they must move together. Clearing them separately would let
// the registry report "no credential" while still owing a cleanup, or the reverse.
func TestClearSubscriberCredential_DischargesAPendingCleanup(t *testing.T) {
	db, mock, captured := newCapturingSQLMock(t)
	source := Datasource{Conn: db}

	mock.ExpectExec("credential_reference = NULL").WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, source.ClearSubscriberCredential(
		context.Background(), "acme_prod", "claim-token"))
	require.NoError(t, mock.ExpectationsWereMet())
	require.Len(t, *captured, 1)

	issued := (*captured)[0]
	assert.Contains(t, issued, "credential_reference = NULL")
	assert.Contains(t, issued, "credential_cleanup_pending_at = NULL",
		"one statement, one fact")
	assert.Contains(t, issued, "WHEN grant_reconcile_pending_at IS NULL THEN 0")
}

// TestListSubscriberSettlementObligations_MatchesThePartialIndexAndIsUnfenced covers the scan.
//
// Two properties, both load-bearing. The predicate must repeat the partial index's EXACTLY, or
// the planner cannot prove the query's condition implies the index's and every poll becomes a
// sequential scan of the registry. And the NULL arms must be spelled out, because
// `last_attempt < $1` is NULL rather than true for a row that has never been attempted — which
// would make the never-tried obligations, the newest and most urgent, permanently invisible.
func TestListSubscriberSettlementObligations_MatchesThePartialIndexAndIsUnfenced(t *testing.T) {
	t.Run("returns the outstanding obligations oldest attempt first", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("").WillReturnRows(
			sqlmock.NewRows([]string{"subscriber_id", "grant", "credential", "attempts", "error"}).
				AddRow("acme_prod", true, false, 2, "broker unreachable").
				AddRow("globex_prod", false, true, 0, ""))

		obligations, err := source.ListSubscriberSettlementObligations(
			context.Background(), 5, time.Now().Add(-5*time.Minute))
		require.NoError(t, err)
		require.Len(t, obligations, 2)

		assert.Equal(t, "acme_prod", obligations[0].SubscriberID)
		assert.True(t, obligations[0].GrantReconcilePending)
		assert.False(t, obligations[0].CredentialCleanupPending)
		assert.Equal(t, 2, obligations[0].Attempts)
		assert.Equal(t, "broker unreachable", obligations[0].LastError)
		assert.True(t, obligations[0].Outstanding())
		assert.True(t, obligations[1].CredentialCleanupPending)

		issued := (*captured)[0]
		assert.Contains(t, issued,
			"WHERE (grant_reconcile_pending_at IS NOT NULL\n\t\t\t   OR credential_cleanup_pending_at IS NOT NULL)",
			"the predicate must repeat the partial index's exactly, or the planner falls back to a "+
				"sequential scan of the whole registry on every poll")
		assert.Contains(t, issued, "settlement_last_attempt_at IS NULL",
			"a row never attempted is ALWAYS eligible, and it has to be admitted explicitly "+
				"because a NULL comparison is NULL rather than true")
		assert.Contains(t, issued, "ORDER BY settlement_last_attempt_at ASC NULLS FIRST, id ASC")
		assert.NotContains(t, issued, "provisioning_token",
			"the scan is UNFENCED deliberately: the worker owns no subscriber, and a scan needing "+
				"a claim could never find the obligations left by an owner that died holding one")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a zero bound means no pacing at all", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		// NULL rather than Go's zero time, which would compare against year 1 and silently
		// bound nothing while looking like it did.
		mock.ExpectQuery("settlement_last_attempt_at").
			WithArgs(nil, 20).
			WillReturnRows(sqlmock.NewRows(
				[]string{"subscriber_id", "grant", "credential", "attempts", "error"}))

		obligations, err := source.ListSubscriberSettlementObligations(
			context.Background(), 20, time.Time{})
		require.NoError(t, err)
		assert.Empty(t, obligations)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("bounds the page", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("LIMIT").
			WithArgs(nil, maxSubscriberPageSize).
			WillReturnRows(sqlmock.NewRows(
				[]string{"subscriber_id", "grant", "credential", "attempts", "error"}))

		_, err := source.ListSubscriberSettlementObligations(
			context.Background(), maxSubscriberPageSize*10, time.Time{})
		require.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a failed read leaks no detail", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("grant_reconcile_pending_at").
			WillReturnError(errors.New("dial tcp 10.1.2.3:5432: connection refused"))

		obligations, err := source.ListSubscriberSettlementObligations(
			context.Background(), 10, time.Time{})
		require.Error(t, err)
		assert.Nil(t, obligations, "an unreadable backlog is not an empty one")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assertNoDatabaseDetailLeak(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestGetSubscriberSettlementObligation_ReReadsTheFlagsForOneSubscriber covers the read that
// makes settlement safe.
//
// A pass finds an obligation, then takes the claim, and time passes in between. A successful
// re-issuance in that window discharges the cleanup obligation — so acting on the scanned flag
// would revoke a credential that had just been issued. This is the read taken once the claim is
// held, which is the first moment the flags cannot change underneath the remedy.
func TestGetSubscriberSettlementObligation_ReReadsTheFlagsForOneSubscriber(t *testing.T) {
	t.Run("projects the five columns narrowly", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("").WillReturnRows(
			sqlmock.NewRows([]string{"subscriber_id", "grant", "credential", "attempts", "error"}).
				AddRow("acme_prod", false, true, 1, "revocation refused"))

		obligation, err := source.GetSubscriberSettlementObligation(context.Background(), "acme_prod")
		require.NoError(t, err)

		assert.Equal(t, "acme_prod", obligation.SubscriberID)
		assert.False(t, obligation.GrantReconcilePending)
		assert.True(t, obligation.CredentialCleanupPending)
		assert.True(t, obligation.Outstanding())

		assert.NotContains(t, (*captured)[0], "kafka_principal",
			"a NARROW projection: putting these columns on the read model would place them in "+
				"every registry response and every wire-contract assertion for one worker's benefit")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nothing owed is a reading and not an absence", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("grant_reconcile_pending_at").WillReturnRows(
			sqlmock.NewRows([]string{"subscriber_id", "grant", "credential", "attempts", "error"}).
				AddRow("acme_prod", false, false, 0, ""))

		obligation, err := source.GetSubscriberSettlementObligation(context.Background(), "acme_prod")
		require.NoError(t, err)
		assert.False(t, obligation.Outstanding(),
			"and Outstanding() is what stops every caller re-spelling the OR of two flags")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports not-found for a subscriber that is gone", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("grant_reconcile_pending_at").WillReturnError(sql.ErrNoRows)

		_, err := source.GetSubscriberSettlementObligation(context.Background(), "acme_prod")
		requireAPIError(t, err, apierror.ErrSubscriberNotFound)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, _ := newSQLMock(t)
		source := Datasource{Conn: db}

		_, err := source.GetSubscriberSettlementObligation(context.Background(), "  ")
		requireAPIError(t, err, apierror.ErrBadRequest)
	})
}

// TestMarkSubscriberSettlementAttempt_PacesWithoutDischarging covers the pacing write.
//
// Two properties. It does NOT discharge either obligation, so a pass that logged its attempt and
// then crashed cannot be mistaken for one that succeeded. And a row that no longer exists is not
// an error, because a subscriber deregistered between the scan and the attempt owes nothing.
func TestMarkSubscriberSettlementAttempt_PacesWithoutDischarging(t *testing.T) {
	t.Run("increments the attempt and stores the bounded failure", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("settlement_attempts = settlement_attempts + 1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, source.MarkSubscriberSettlementAttempt(
			context.Background(), "acme_prod", time.Now(), "broker unreachable"))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, *captured, 1)

		issued := (*captured)[0]
		assert.Contains(t, issued, "settlement_attempts = settlement_attempts + 1")
		assert.Contains(t, issued, "settlement_last_error = NULLIF($2, '')",
			"a successful pass stores NULL rather than an empty string, so the column reads as "+
				"'no failure' rather than 'a failure with no message'")
		assert.NotContains(t, issued, "grant_reconcile_pending_at",
			"pacing must not discharge; a pass that recorded its attempt and then crashed would "+
				"otherwise be indistinguishable from one that succeeded")
		assert.NotContains(t, issued, "credential_cleanup_pending_at")
		assert.NotContains(t, issued, "provisioning_token",
			"UNFENCED, because an attempt that could not be recorded would never pace the next "+
				"one — turning a broker outage into a busy loop")
	})

	t.Run("a deregistered subscriber is not an error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectExec("settlement_attempts").WillReturnResult(sqlmock.NewResult(0, 0))

		require.NoError(t, source.MarkSubscriberSettlementAttempt(
			context.Background(), "acme_prod", time.Now(), ""),
			"a subscriber that vanished between the scan and the attempt owes nothing, so there "+
				"is nothing to report")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("requires a subscriber ID", func(t *testing.T) {
		db, _ := newSQLMock(t)
		source := Datasource{Conn: db}

		requireAPIError(t, source.MarkSubscriberSettlementAttempt(
			context.Background(), " ", time.Now(), ""), apierror.ErrBadRequest)
	})
}

// TestCountSubscriberSettlementObligations_ReadsTheBacklogAsAnAggregate covers the read the four
// settlement gauges depend on.
//
// Three properties: it is an AGGREGATE, so observing the backlog costs the same whatever its
// size; Outstanding is counted with an OR rather than summed, because one subscriber can owe
// both; and a NULL minimum — the healthy case — comes back as the zero instant rather than a
// scan error.
func TestCountSubscriberSettlementObligations_ReadsTheBacklogAsAnAggregate(t *testing.T) {
	t.Run("reports the split and the oldest instant", func(t *testing.T) {
		db, mock, captured := newCapturingSQLMock(t)
		source := Datasource{Conn: db}

		oldest := time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)
		mock.ExpectQuery("").WillReturnRows(
			sqlmock.NewRows([]string{"outstanding", "grant", "credential", "oldest"}).
				AddRow(int64(3), int64(2), int64(2), oldest))

		backlog, err := source.CountSubscriberSettlementObligations(context.Background())
		require.NoError(t, err)

		assert.Equal(t, int64(3), backlog.Outstanding,
			"three subscribers owe something, and the two component counts sum to four because "+
				"one of them owes BOTH — which is why the total is an OR and not a sum")
		assert.Equal(t, int64(2), backlog.GrantReconcilePending)
		assert.Equal(t, int64(2), backlog.CredentialCleanupPending)
		assert.Equal(t, oldest, backlog.OldestPendingAt)
		assert.Equal(t, time.Hour, backlog.OldestAge(oldest.Add(time.Hour)))

		issued := (*captured)[0]
		assert.Contains(t, issued, "COUNT(*) FILTER (")
		assert.Contains(t, issued,
			"LEAST(MIN(grant_reconcile_pending_at), MIN(credential_cleanup_pending_at))",
			"LEAST ignores NULL arguments, so a deployment owing only one kind still reports that "+
				"kind's oldest instant instead of NULL")
		assert.NotContains(t, issued, "LIMIT",
			"no paging: four scalars whatever the registry size")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nothing outstanding is a reading and not an absence", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("COUNT").WillReturnRows(
			sqlmock.NewRows([]string{"outstanding", "grant", "credential", "oldest"}).
				AddRow(int64(0), int64(0), int64(0), nil))

		backlog, err := source.CountSubscriberSettlementObligations(context.Background())
		require.NoError(t, err, "a NULL minimum is the healthy case, not a failure")
		assert.Zero(t, backlog.Outstanding)
		assert.True(t, backlog.OldestPendingAt.IsZero())
		assert.Zero(t, backlog.OldestAge(time.Now()),
			"the zero instant is what tells the caller to publish a zero age rather than the age "+
				"of the epoch, which would exceed every threshold for ever")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a failed read reports rather than returning a fabricated zero", func(t *testing.T) {
		db, mock := newSQLMock(t)
		source := Datasource{Conn: db}

		mock.ExpectQuery("COUNT").WillReturnError(errors.New("dial tcp: connection refused"))

		backlog, err := source.CountSubscriberSettlementObligations(context.Background())
		require.Error(t, err)
		assert.Equal(t, model.SubscriberSettlementBacklog{}, backlog,
			"a zero published from a failed read would assert that everything is settled")
		requireAPIError(t, err, apierror.ErrInternalServer)
		assertNoDatabaseDetailLeak(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestNullableTime_TurnsTheZeroInstantIntoSQLNull pins the helper the scan's pacing bound relies
// on.
//
// Passing Go's zero time through as a literal would compare against year 1 rather than meaning
// "no bound" — a predicate that looks like it is pacing and is not.
func TestNullableTime_TurnsTheZeroInstantIntoSQLNull(t *testing.T) {
	assert.Nil(t, nullableTime(time.Time{}))

	instant := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	assert.Equal(t, instant, nullableTime(instant))
}
