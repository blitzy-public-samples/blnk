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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
