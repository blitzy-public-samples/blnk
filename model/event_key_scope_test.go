// Copyright 2024 Blnk Finance Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Subscriber key-scope predicates.
//
// These six functions decide whether a direct broker credential may be issued at
// all, and what boundary the credential contract claims. They were exercised
// only from the root package, so the mutation gate — which scores a package by
// running that package's own tests — reported them NOT COVERED and never scored
// them. For a fail-closed authorization decision that is the least acceptable
// place to have no mutation score, hence this file.
//
// Each assertion pins something a mutant would break: a connective, a
// comparison, a normalisation, or the nil-receiver answer. The receiver cases
// are not defensive padding — the repository represents "subscriber not found"
// as a nil pointer, so every one of these predicates is genuinely called on nil
// and must refuse rather than panic.
// -----------------------------------------------------------------------------

// stringPtr addresses the nullable partition_key_prefix column.
func stringPtr(v string) *string { return &v }

func TestEventSubscriber_RequestedKeyScopeFlattensTheNullableColumn(t *testing.T) {
	// A NIL RECEIVER answers the empty string. The repository hands back nil for a
	// subscriber that does not exist, and every predicate below reaches this one,
	// so a panic here would turn a 404 into a crashed ledger process. It also
	// kills the mutant that turns the || into an &&, which would dereference nil.
	var absent *EventSubscriber
	assert.Equal(t, "", absent.RequestedKeyScope(),
		"a subscriber that does not exist requested no scope")

	// A nil column answers the empty string too: no scope requested and "every
	// key on the authorised topics" are the same reading, and collapsing them
	// here is what spares every caller a third state.
	assert.Equal(t, "", (&EventSubscriber{}).RequestedKeyScope(),
		"an unset prefix column means no scope was requested")

	// A recorded scope is returned VERBATIM. The value is a message-key prefix and
	// keys are opaque identifiers, so any normalisation applied here would filter
	// a different set of records than the registry recorded.
	assert.Equal(t, "ldg_9f2c",
		(&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).RequestedKeyScope(),
		"a recorded scope is returned exactly as recorded")
}

func TestEventSubscriber_RequiresKeyScopeEnforcementIsTrueOnlyForANarrowerBoundary(t *testing.T) {
	// No scope: a whole-topic entitlement, which a Kafka ACL expresses exactly, so
	// nothing has to be enforced above the broker.
	assert.False(t, (&EventSubscriber{}).RequiresKeyScopeEnforcement(),
		"a subscriber entitled to whole topics needs no enforcement Kafka cannot provide")

	// A scope: narrower than any ACL can express, so this must be true and
	// issuance must fail closed on it. Killing the mutant that flips the
	// comparison matters more here than anywhere else in the file — inverted, it
	// would refuse every whole-topic subscriber and issue to every key-scoped one,
	// which is the exact opposite of the intended boundary.
	assert.True(t,
		(&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).RequiresKeyScopeEnforcement(),
		"a recorded key prefix is narrower than a topic ACL and must be reported as needing "+
			"enforcement the broker cannot give it")

	// Nil receiver: no request, so no enforcement. Answerable rather than fatal,
	// via RequestedKeyScope's own nil guard.
	var absent *EventSubscriber
	assert.False(t, absent.RequiresKeyScopeEnforcement(),
		"a subscriber that does not exist asked for no boundary")
}

func TestEventSubscriber_HasKeyAccessIsAByteExactPrefixTest(t *testing.T) {
	// With NO scope recorded every key is in bounds, including the empty key. This
	// is the branch that makes an unprovisioned prefix and an explicit whole-topic
	// entitlement read identically.
	open := &EventSubscriber{}
	assert.True(t, open.HasKeyAccess("ldg_9f2c"), "with no scope recorded every key is in bounds")
	assert.True(t, open.HasKeyAccess(""), "including the empty key")

	scoped := &EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}

	// The key IS the scope: a prefix test includes the exact value, and excluding
	// it would deny a subscriber the one key it was registered for.
	assert.True(t, scoped.HasKeyAccess("ldg_9f2c"), "the scope itself is in bounds")
	assert.True(t, scoped.HasKeyAccess("ldg_9f2c_balances"), "and so is anything extending it")

	// A different ledger is out of bounds — the whole purpose of the rule.
	assert.False(t, scoped.HasKeyAccess("ldg_0000"), "another ledger's keys are out of bounds")

	// A key SHORTER than the scope is out of bounds. A mutant that reversed the
	// prefix test — asking whether the scope starts with the key — would admit
	// this one, and with it every ancestor of the boundary.
	assert.False(t, scoped.HasKeyAccess("ldg_"),
		"a key shorter than the scope is not inside it; reversing the prefix test would admit it")

	// NO trimming and NO case folding. Keys are opaque identifiers, so a key that
	// differs by whitespace or case is a different key, and admitting it would
	// hand over records the registry never authorised.
	assert.False(t, scoped.HasKeyAccess(" ldg_9f2c"),
		"a leading space makes it a different key — no trimming is applied")
	assert.False(t, scoped.HasKeyAccess("LDG_9F2C"),
		"case is significant — no folding is applied")

	// The empty key against a recorded scope is out of bounds, which is the
	// unkeyed-record case: a record with no key cannot satisfy a key boundary.
	assert.False(t, scoped.HasKeyAccess(""),
		"an unkeyed record cannot satisfy a key-scoped entitlement")

	// Nil receiver: no scope, so everything is nominally in bounds. Recorded
	// because it follows from the nil guard rather than from a decision about
	// missing subscribers — authorization for a subscriber that does not exist is
	// refused by the caller that failed to find it, not here.
	var absent *EventSubscriber
	assert.True(t, absent.HasKeyAccess("ldg_9f2c"),
		"a nil subscriber records no scope, so this predicate has nothing to refuse")
}

func TestEventSubscriber_EffectiveKeyScopeReportsWhatACredentialWouldActuallyCarry(t *testing.T) {
	// No scope requested: the honest description of what a topic ACL grants, and
	// it IS enforced, because the broker itself is the thing enforcing it.
	scope, enforced := (&EventSubscriber{}).EffectiveKeyScope()
	assert.Equal(t, SubscriberKeyScopeAllKeys, scope,
		"a whole-topic credential carries every key, and says so in words")
	assert.Equal(t, "all-keys", scope,
		"the constant is a descriptive word, not Kafka's match-anything \"*\" — a client "+
			"comparing against it must not be comparing against an ACL pattern")
	assert.True(t, enforced, "and that boundary is the one the broker really keeps")

	// A scope requested: reported back as itself and as UNENFORCED. This pair is
	// the defect the method closes — echoing a requested prefix beside a
	// credential that can read the whole topic would state a boundary that does
	// not exist. Both halves are asserted, because a mutant that returned true
	// here would make the contract claim enforcement Kafka cannot perform.
	scope, enforced = (&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).EffectiveKeyScope()
	assert.Equal(t, "ldg_9f2c", scope, "a requested scope is reported as the boundary asked for")
	assert.False(t, enforced,
		"and reported as UNENFORCED, which is precisely why issuance refuses it rather than "+
			"handing this pair to a subscriber")

	// Nil receiver: the all-keys pair, reached through RequestedKeyScope's guard.
	var absent *EventSubscriber
	scope, enforced = absent.EffectiveKeyScope()
	assert.Equal(t, SubscriberKeyScopeAllKeys, scope)
	assert.True(t, enforced)
}

func TestEventSubscriber_IsRevocationPendingTestsOnlyThePresenceOfTheTimestamp(t *testing.T) {
	revokedAt := time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)

	assert.True(t,
		(&EventSubscriber{RevocationPendingAt: &revokedAt}).IsRevocationPending(),
		"a row with an outstanding revocation is not an active subscriber, and issuing for it "+
			"would re-arm a principal being taken out of service")

	assert.False(t, (&EventSubscriber{}).IsRevocationPending(),
		"no revocation timestamp means no revocation in flight")

	// The ZERO time still counts as present. The predicate is about the pointer,
	// not about what it addresses: a row whose timestamp somehow serialised as the
	// zero instant is still a row with a revocation in flight, and treating it as
	// active would re-arm exactly the principal this guards.
	zero := time.Time{}
	assert.True(t, (&EventSubscriber{RevocationPendingAt: &zero}).IsRevocationPending(),
		"a set-but-zero timestamp is still a revocation in flight")

	// Nil receiver answers false rather than panicking: this is read as a GUARD on
	// a row that may not have been found, and a guard that panics where it should
	// refuse is worse than no guard.
	var absent *EventSubscriber
	assert.False(t, absent.IsRevocationPending(),
		"a subscriber that does not exist has no revocation in flight")
}

func TestEventSubscriber_KeyScopeUnenforceableTreatsABlankPrefixAsAbsent(t *testing.T) {
	// A real prefix: unenforceable, and issuance fails closed on exactly this.
	assert.True(t,
		(&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).KeyScopeUnenforceable(),
		"a recorded prefix is narrower than any credential Blnk can mint")

	// No column at all: nothing unenforceable.
	assert.False(t, (&EventSubscriber{}).KeyScopeUnenforceable(),
		"an unset prefix records no constraint")

	// A BLANK prefix is absent, not a constraint on the empty string. Without the
	// TrimSpace this returns true and refuses a subscriber that asked for nothing.
	//
	// Note that RequestedKeyScope returns such a value verbatim, so this predicate
	// and RequiresKeyScopeEnforcement would disagree about a whitespace-only
	// prefix. That row cannot be persisted: normalizeSubscriberKeyScope collapses
	// a blank prefix to nil and REFUSES any value with surrounding whitespace, so
	// the disagreement is unreachable defence-in-depth rather than a contract. It
	// is recorded here so a reader does not mistake the asymmetry for intent.
	for name, blank := range map[string]string{"empty": "", "space": " ", "tab": "\t", "newline": "\n"} {
		t.Run(name, func(t *testing.T) {
			assert.False(t,
				(&EventSubscriber{PartitionKeyPrefix: stringPtr(blank)}).KeyScopeUnenforceable(),
				"a %s prefix is an absent constraint, not a constraint on nothing", name)
		})
	}

	// Nil receiver answers false, for the same reason IsRevocationPending does.
	var absent *EventSubscriber
	assert.False(t, absent.KeyScopeUnenforceable(),
		"a subscriber that does not exist records no unenforceable constraint")
}
