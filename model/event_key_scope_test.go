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
// These six functions decide what boundary the credential contract CLAIMS, and —
// for IsRevocationPending — whether a credential may be issued at all. They were
// exercised only from the root package, so the mutation gate — which scores a
// package by running that package's own tests — reported them NOT COVERED and
// never scored them. A predicate whose wrong answer makes a credential response
// overstate its own boundary is the least acceptable place to have no mutation
// score, hence this file.
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
	// credential that can read the whole topic, without saying which of the two is
	// real, would state a boundary that does not exist. Both halves are asserted,
	// because a mutant that returned true here would make the contract claim
	// enforcement Kafka cannot perform.
	scope, enforced = (&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).EffectiveKeyScope()
	assert.Equal(t, "ldg_9f2c", scope, "a requested scope is reported as the boundary asked for")
	assert.False(t, enforced,
		"and reported as UNENFORCED, which is precisely why the credential response states the "+
			"prefix as the subscriber's own filtering obligation rather than as a boundary it "+
			"may rely on")

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

// TestEventSubscriber_KeyScopeEnforcementNamesWhereTheScopeIsKept pins the accessor that
// replaced a boolean predicate named for unenforceability, now deleted.
//
// The old predicate answered this same boolean under a name asserting a policy, and three
// barriers read it as licence to deny the subscriber a credential outright. Reporting the
// enforcement POINT is what let the credential be issued while every surface showing the prefix
// shows the component that keeps it beside the value.
//
// Its VALUE changed with the isolation correction, and the change is the point of this test: it
// used to answer "consumer_side", meaning the platform granted whole-topic Read and asked the
// subscriber to discard what it was not entitled to. It answers "broker_gateway" now — the same
// word a deployment declares in KAFKA_KEY_SCOPE_ENFORCEMENT — because a key-scoped subscriber is
// granted no topic Read at all and its records are filtered by the component the operator declared
// in front of the brokers. A regression to the old value would be a regression to the old exposure.
//
// This method reads the ROW and nothing else, so it reports where a recorded prefix WOULD be kept.
// Whether a credential may be issued at all is config.KafkaConfig.KeyScopeGateway's question,
// asked once at issuance — which is why a registry read of a row on a deployment that declared
// nothing still describes the row truthfully instead of reporting "none" and hiding the intent.
func TestEventSubscriber_KeyScopeEnforcementNamesWhereTheScopeIsKept(t *testing.T) {
	// A real prefix: recorded, and kept by the DECLARED KEY-AUTHORISING COMPONENT, because Kafka
	// has no message-key dimension to enforce it with and the subscriber therefore holds no topic
	// Read.
	scoped := &EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}
	assert.True(t, scoped.DeclaresKeyScope(), "a recorded prefix is a recorded scope")
	assert.Equal(t, KeyScopeEnforcementGateway, scoped.KeyScopeEnforcement(),
		"the broker cannot evaluate a message key, so this boundary is kept outside it rather than "+
			"granting the whole topic and asking the consumer to filter")
	assert.Equal(t, KeyScopeEnforcementStatus("broker_gateway"), scoped.KeyScopeEnforcement(),
		"AND THE WIRE VALUE IS THE CONFIGURATION'S OWN WORD. It is serialised into the credential "+
			"response, so a value naming a component the deployment does not configure would leave a "+
			"client unable to connect the two")
	assert.False(t, scoped.GrantsBrokerRecordAccess(),
		"which is only true because record access is withheld: a key-scoped subscriber that also "+
			"held topic Read would have the declared component as one path among two")

	// No column at all: the topic and group ACLs are the entire boundary.
	assert.False(t, (&EventSubscriber{}).DeclaresKeyScope(), "an unset prefix records no scope")
	assert.Equal(t, KeyScopeEnforcementNone, (&EventSubscriber{}).KeyScopeEnforcement(),
		"with no scope recorded the topic grant is the whole boundary and the broker keeps it")
	assert.True(t, (&EventSubscriber{}).GrantsBrokerRecordAccess(),
		"so such a subscriber consumes directly, exactly as the access model describes")

	// A BLANK prefix is absent, not a constraint on the empty string. Without the
	// TrimSpace this reports gateway enforcement of nothing — which would withhold record access
	// from a subscriber that asked for no narrowing, breaking its direct consumption over
	// whitespace.
	//
	// Note that RequestedKeyScope returns such a value verbatim, so this accessor and a
	// predicate reading the scope untrimmed would disagree about a whitespace-only prefix. That row
	// cannot be persisted: normalizeSubscriberKeyScope collapses a blank prefix to nil and
	// REFUSES any value with surrounding whitespace, so the disagreement is unreachable
	// defence-in-depth rather than a contract. It is recorded here so a reader does not
	// mistake the asymmetry for intent.
	for name, blank := range map[string]string{"empty": "", "space": " ", "tab": "\t", "newline": "\n"} {
		t.Run(name, func(t *testing.T) {
			blankScoped := &EventSubscriber{PartitionKeyPrefix: stringPtr(blank)}
			assert.False(t, blankScoped.DeclaresKeyScope(),
				"a %s prefix is an absent scope, not a scope on nothing", name)
			assert.Equal(t, KeyScopeEnforcementNone, blankScoped.KeyScopeEnforcement())
		})
	}

	// Nil receiver answers the absent case, for the same reason IsRevocationPending does.
	var absent *EventSubscriber
	assert.False(t, absent.RequiresGatewayDelivery(),
		"a subscriber that does not exist records no key scope, so nothing routes it to the gateway")
	assert.True(t, absent.GrantsBrokerRecordAccess(),
		"and the complement answers too rather than panicking; nothing is granted on the strength "+
			"of it, because provisioning derives bindings from a row it has loaded")
}

// TestEventSubscriber_DeclaresKeyScopeIsTrueOnlyForANarrowerBoundary is the surviving guard on
// the registry's own presence question.
//
// A near-identical test over RequiresKeyScopeEnforcement stood beside it. That predicate was a
// FOURTH spelling of this one question, and its name claimed something remained to be ENFORCED
// after the topic and group ACLs had been checked. Nothing enforces it — Kafka has no message-key
// resource type, so the narrowing is the consumer's own filter — so the name has been deleted
// along with the unenforceability spelling, and its coverage collapses into this test rather than
// being duplicated under a second name.
func TestEventSubscriber_DeclaresKeyScopeIsTrueOnlyForANarrowerBoundary(t *testing.T) {
	// No scope: a whole-topic entitlement, which a Kafka ACL expresses exactly, so nothing
	// is left for the consumer to apply.
	assert.False(t, (&EventSubscriber{}).DeclaresKeyScope(),
		"a subscriber entitled to whole topics declares no key boundary")

	// A scope: narrower than any ACL can express, so this must be true and the credential
	// contract must deliver the prefix to the consumer. Killing the mutant that flips the
	// comparison matters more here than anywhere else in the file — inverted, it would tell
	// every whole-topic subscriber to filter and tell every key-scoped one not to, which is
	// the exact opposite of the intended boundary and silences a real consumer.
	assert.True(t,
		(&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).DeclaresKeyScope(),
		"a recorded key prefix is narrower than a topic ACL and must be reported as a declared "+
			"scope, because the consumer is what applies it")

	// Nil receiver: no declaration. Answerable rather than fatal, via RequestedKeyScope's
	// own nil guard.
	var absent *EventSubscriber
	assert.False(t, absent.DeclaresKeyScope(),
		"a subscriber that does not exist declares no boundary")
}

// TestEventSubscriber_EffectiveKeyScopeNeverDeliversABlankScope is a narrow guard on one
// asymmetry, and it earns its place because the consequence is total and silent.
//
// RequestedKeyScope returns the column verbatim, which is right: message keys are opaque, so
// trimming one would select a different set of records than the registry recorded.
// DeclaresKeyScope trims, which is also right: whitespace is not a recorded intent. The two
// therefore differ on a whitespace-only prefix, and EffectiveKeyScope is where that difference
// would escape — it is the pair the credential endpoint delivers to a consumer.
//
// A consumer told its key scope is "\t" applies it and discards EVERY record, because no ledger
// id starts with a tab. Nothing errors, nothing is logged, and the subscriber simply receives
// nothing — the worst shape of failure for this feature. So the presence test must be shared,
// and this is the assertion that keeps it shared.
func TestEventSubscriber_EffectiveKeyScopeNeverDeliversABlankScope(t *testing.T) {
	for name, blank := range map[string]string{
		"empty":   "",
		"space":   " ",
		"tab":     "\t",
		"newline": "\n",
		"mixed":   " \t\n ",
	} {
		t.Run(name, func(t *testing.T) {
			subscriber := &EventSubscriber{PartitionKeyPrefix: stringPtr(blank)}

			scope, enforced := subscriber.EffectiveKeyScope()
			assert.Equal(t, SubscriberKeyScopeAllKeys, scope,
				"a %s prefix must be delivered as every-key. Delivering it verbatim would tell a "+
					"consumer to discard its entire stream, silently", name)
			assert.True(t, enforced,
				"and with no real scope to apply, the topic grant is the whole boundary")

			assert.True(t, subscriber.HasKeyAccess("ldg_9f2c"),
				"the rule itself must agree: a blank scope excludes nothing")
		})
	}
}

func TestEventSubscriber_DeclaresKeyScopeTreatsABlankPrefixAsAbsent(t *testing.T) {
	// A real prefix: a declared scope, which the credential contract delivers.
	assert.True(t,
		(&EventSubscriber{PartitionKeyPrefix: stringPtr("ldg_9f2c")}).DeclaresKeyScope(),
		"a recorded prefix is a boundary the consumer must apply")

	// No column at all: nothing declared.
	assert.False(t, (&EventSubscriber{}).DeclaresKeyScope(),
		"an unset prefix records no scope")

	// A BLANK prefix is absent, not a constraint on the empty string. Without the TrimSpace
	// this returns true and hands a consumer a boundary nobody asked for — which, applied,
	// discards nothing but reports a scope in every response.
	//
	// RequestedKeyScope returns such a value VERBATIM — deliberately, because message keys are
	// opaque and normalising one would select a different set of records — so this predicate and
	// that accessor genuinely differ on a whitespace-only prefix. EffectiveKeyScope resolves it
	// by branching on THIS predicate rather than on the raw value, which is what stops a tab in
	// the column being delivered to a consumer as its key scope: applied, it would discard the
	// subscriber's entire stream. normalizeSubscriberKeyScope refusing such a value at
	// persistence is the outer layer of the same defence.
	for name, blank := range map[string]string{"empty": "", "space": " ", "tab": "\t", "newline": "\n"} {
		t.Run(name, func(t *testing.T) {
			assert.False(t,
				(&EventSubscriber{PartitionKeyPrefix: stringPtr(blank)}).DeclaresKeyScope(),
				"a %s prefix is an absent scope, not a scope on nothing", name)
		})
	}

	// Nil receiver answers false, for the same reason IsRevocationPending does.
	var absent *EventSubscriber
	assert.False(t, absent.DeclaresKeyScope(),
		"a subscriber that does not exist declares no scope")
}
