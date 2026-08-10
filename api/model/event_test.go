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
package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers the executable logic on the event-streaming DTOs: the request
// validators, the two response projections, and the failure-reason classifier.
//
// The DTOs were once pure structs, which is why the original plan listed no test
// beside them. They are not pure structs any more — they now decide which topics may
// be granted, which URLs may be stored, and which parts of a stored row reach a
// caller — and each of those decisions is the entire fix for a finding. A decision
// with no test is a decision that can be reverted by an innocent-looking edit, so the
// tests live here, beside the code that makes them.

// testTopicPrefix is the default namespace, used explicitly rather than passing ""
// so that each case states which namespace it is reasoning about.
const testTopicPrefix = "blnk"

// derivedReference produces a genuinely derived credential reference, so the
// fingerprint assertions run against a real value rather than a hand-written string
// that only looks like one.
func derivedReference(t *testing.T) string {
	t.Helper()
	reference, err := model.DeriveCredentialReference("blnk-sub-acme", "a-secret-nobody-should-see")
	require.NoError(t, err)

	return reference
}

// ---------------------------------------------------------------------------
// SEC-03 — authorized-topic allowlist
// ---------------------------------------------------------------------------

// TestValidateGrantableTopics_AcceptsOnlySubscriberFacingCategoryTopics is the
// SEC-03 guard at the request boundary.
//
// The list becomes the ACL bindings, so anything accepted here is something the
// issued credential can read. Each rejected case below is a distinct route to an
// unintended grant, and the wildcard case is the one that matters most: Kafka reads
// the resource name "*" as matching every resource, so a single such entry converts
// a per-topic grant into a cluster-wide one.
//
// All FOUR category topics are accepted, blnk.system included, for the reason set out
// at the system subtest below. What is refused is every DEAD-LETTER name — those carry
// failure metadata and other subscribers' failed events, and are read under the master
// key instead — along with foreign names, wildcards, and names whose category does not
// exist in the closed four-category catalogue.
func TestValidateGrantableTopics_AcceptsOnlySubscriberFacingCategoryTopics(t *testing.T) {
	grantable := model.SubscriberGrantableTopics(testTopicPrefix)
	require.NotEmpty(t, grantable, "there must be at least one grantable topic to test against")

	t.Run("every grantable topic is accepted", func(t *testing.T) {
		assert.NoError(t, validateGrantableTopics(grantable, testTopicPrefix),
			"the composed grantable list must itself validate, or the two disagree")
	})

	t.Run("an empty list is accepted as the fail-closed default", func(t *testing.T) {
		assert.NoError(t, validateGrantableTopics(nil, testTopicPrefix))
		assert.NoError(t, validateGrantableTopics([]string{}, testTopicPrefix))
	})

	refused := map[string]string{
		"the any-resource wildcard":     "*",
		"a wildcarded category":         "blnk.*",
		"a prefixed wildcard":           "blnk.transactions*",
		"a foreign topic of same shape": "attacker.transactions",
		"a dead-letter topic":           "blnk.transactions.dlt",
		"the system dead-letter topic":  "blnk.system.dlt",
		"a topic under another prefix":  "acme.transactions",
		"an untrimmed grantable name":   " blnk.transactions ",
		"an uppercased category":        "blnk.TRANSACTIONS",
		"a retired category name":       "blnk.ledgers",
	}
	for name, topic := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			err := validateGrantableTopics([]string{topic}, testTopicPrefix)
			require.Error(t, err, "topic %q must not be grantable", topic)
			assert.Contains(t, err.Error(), "not grantable",
				"the error must say why, so an operator is not left guessing")
		})
	}

	// THE SYSTEM CATEGORY IS GRANTABLE, and it is the one member of the set that has to
	// argue for itself, because blnk.system carries system.error, whose payload is the
	// frozen legacy body and therefore carries raw error text verbatim.
	//
	// It is grantable because that is EXACT PARITY with the transport being replaced.
	// internal/notification.NotifyError already delivers system.error, with that same
	// verbatim error text, to the single configured webhook URL — so an event carrying
	// deployment detail to a subscriber-supplied endpoint is what Blnk does today, not
	// something this change introduces. Refusing the category would have made the Kafka
	// pipeline deliver strictly LESS than the webhook it replaces, which R-1 forbids: the
	// requirement is 100% of the event types SendWebhook routes, and an event published to
	// a topic no credential may name is not delivered to anybody.
	//
	// THE COST IS REAL AND IS RECORDED RATHER THAN GLOSSED. Because ledger.created shares
	// blnk.system under the frozen four-category catalogue, a subscriber that wants ledger
	// events is granted a topic that also carries system.error. That is a decision to take per
	// subscriber, which is why the grant lives in authorized_topics and this allowlist only
	// says what MAY be named. A fifth grantable `ledgers` category was implemented to separate
	// them and removed: the catalogue is enumerated
	// identically by the provisioning script, both Compose files, the Kubernetes manifests and
	// every subscriber's topic list, so it is four everywhere or it is a deliberate contract
	// change made in all of them at once. docs/event-streaming.md states the consequence for
	// subscribers plainly.
	t.Run("accepts the system category", func(t *testing.T) {
		assert.NoError(t, validateGrantableTopics([]string{"blnk.system"}, testTopicPrefix),
			"blnk.system must be grantable, or ledger.created — which routes to it under the "+
				"four-category catalogue — is published to a topic no credential may name, and "+
				"the Kafka pipeline delivers strictly less than the webhook it replaces")
	})

	t.Run("the grantable set is exactly the four category topics", func(t *testing.T) {
		// Stated as an EXACT set rather than as a series of accept/refuse cases, because
		// every over-grant finding in this area reduces to the same question — which topics
		// may a credential ever name — and a boundary is only checkable if it is enumerated
		// in one place. A FIFTH category appearing here would fail, which is the point, and so
		// would a category quietly dropped from the set.
		assert.ElementsMatch(t,
			[]string{"blnk.transactions", "blnk.balances", "blnk.identities", "blnk.system"},
			model.SubscriberGrantableTopics(testTopicPrefix),
			"all four category topics are grantable and no dead-letter topic is; which of them a "+
				"PARTICULAR subscriber holds is decided per subscriber by authorized_topics, not "+
				"by this allowlist")
	})

	t.Run("refuses every dead-letter sibling, grantable category or not", func(t *testing.T) {
		// A `<topic>.dlt` holds Blnk's failure metadata alongside the full payload of every
		// event that failed, for every subscriber, so it is operator-facing whatever the
		// category it belongs to. Asserting it for a TENANT category as well as the internal
		// one is what stops "the category is grantable" being read as "so is its sibling".
		for _, topic := range []string{
			"blnk.transactions.dlt",
			"blnk.balances.dlt",
			"blnk.identities.dlt",
			"blnk.system.dlt",
		} {
			assert.Errorf(t, validateGrantableTopics([]string{topic}, testTopicPrefix),
				"%q is a dead-letter topic and must never be grantable", topic)
		}
	})

	t.Run("refuses a category this contract does not have", func(t *testing.T) {
		// The plausible mistake: ledger events are real and are published, but they are
		// published to blnk.system, so blnk.ledgers is a name nothing creates and nobody may
		// be granted.
		err := validateGrantableTopics([]string{"blnk.ledgers"}, testTopicPrefix)
		require.Error(t, err,
			"ledger events live in the system category, so a ledgers topic is not grantable")
		assert.Contains(t, err.Error(), "not grantable")
	})

	t.Run("refuses an offending topic anywhere in the list", func(t *testing.T) {
		// A loop that returned on the first valid entry, or that only checked
		// index zero, would pass every case above and still admit this one.
		withTrailingWildcard := append(append([]string{}, grantable...), "*")
		assert.Error(t, validateGrantableTopics(withTrailingWildcard, testTopicPrefix))
	})

	t.Run("refuses an empty entry rather than skipping it", func(t *testing.T) {
		// Skipping would let a caller believe it had requested a grant it did not
		// receive, which is a silent divergence between request and stored state.
		err := validateGrantableTopics([]string{"", "blnk.transactions"}, testTopicPrefix)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty topic name")

		assert.Error(t, validateGrantableTopics([]string{"   "}, testTopicPrefix))
	})

	t.Run("a configured prefix moves the whole allowlist", func(t *testing.T) {
		assert.NoError(t, validateGrantableTopics([]string{"acme.transactions"}, "acme"))
		assert.Error(t, validateGrantableTopics([]string{"blnk.transactions"}, "acme"),
			"under the acme prefix, blnk.transactions is a foreign topic")
	})

	t.Run("a blank prefix falls back to the strictest namespace", func(t *testing.T) {
		// Never to a permissive one: an unconfigured caller must not widen what it
		// grants over.
		assert.NoError(t, validateGrantableTopics([]string{"blnk.transactions"}, ""))
		assert.Error(t, validateGrantableTopics([]string{"anything.transactions"}, ""))
	})
}

// ---------------------------------------------------------------------------
// SEC-01 / SEC-03 — the recorded key-scoped authorization
// ---------------------------------------------------------------------------

// TestSubscriberRequests_AdjudicateTheKeyScopeShape pins where the key-scope SHAPE rules live.
//
// This test is the inverse of the one it replaces, and the reversal is the whole point. The
// earlier version asserted that the DTO must PASS EVERY VALUE THROUGH — newlines, surrounding
// whitespace, half a kilobyte of it — because the service was going to refuse any non-blank
// prefix outright with a typed 409, and answering the same state with two different codes is
// worse for a client than answering it with one.
//
// The service no longer refuses it. A partition-key prefix is a CONSUMER-SIDE FILTERING
// CONTRACT, disclosed with the credential rather than standing in the way of it, and
// SUBSCRIBER_ISOLATION_UNENFORCEABLE is gone rather than retained unused. That removes the
// argument for passing the value through and replaces it with the opposite one: the prefix is
// persisted on the registry row, echoed in the credential response, and written into the log
// fields of every issuance, revocation and provisioning failure. This validator is now the only
// thing between a caller and a control character or an unbounded value in all three.
//
// So the shape rules are back, and they are asserted on BOTH paths. What must still hold is the
// part that never depended on the refusal: a BLANK prefix is always fine, on create and as the
// documented way to clear a legacy row on update.
func TestSubscriberRequests_AdjudicateTheKeyScopeShape(t *testing.T) {
	t.Run("a well-formed prefix is accepted on both paths", func(t *testing.T) {
		create := validCreateSubscriber()
		create.PartitionKeyPrefix = "ldg_9f1c8a72"
		assert.NoError(t, create.Validate(testTopicPrefix))

		supplied := "ldg_9f1c8a72"
		assert.NoError(t, UpdateSubscriber{PartitionKeyPrefix: &supplied}.Validate(testTopicPrefix))
	})

	for name, prefix := range map[string]string{
		"a prefix with a newline":  "ldg_9f1c\n8a72",
		"a prefix with whitespace": " ldg_9f1c ",
		"an over-long prefix":      strings.Repeat("k", 500),
	} {
		t.Run(name, func(t *testing.T) {
			create := validCreateSubscriber()
			create.PartitionKeyPrefix = prefix
			assert.Error(t, create.Validate(testTopicPrefix),
				"the value is persisted, echoed in the credential response and logged, and "+
					"nothing downstream refuses it any more")

			supplied := prefix
			assert.Error(t, UpdateSubscriber{PartitionKeyPrefix: &supplied}.Validate(testTopicPrefix),
				"and the same on update: a rule applied only on creation has an edit-shaped hole")
		})
	}

	t.Run("an omitted prefix is still fine", func(t *testing.T) {
		create := validCreateSubscriber()
		create.PartitionKeyPrefix = ""
		assert.NoError(t, create.Validate(testTopicPrefix))

		cleared := ""
		assert.NoError(t, UpdateSubscriber{PartitionKeyPrefix: &cleared}.Validate(testTopicPrefix),
			"clearing remains the documented remedy for a legacy row that carries one")
	})
}

// TestValidateSubscriberName_RefusesUnstorableLabels covers the human label. CWE-20.
//
// The label was previously required and trimmed and nothing more, which bounds nothing: the
// column is TEXT, which has no length limit in PostgreSQL, and NOT NULL does not stop a
// megabyte of text or a value carrying newlines and terminal escapes. It is not inert either —
// it is echoed in the subscriber list and the credential response and written into
// operator-facing log lines — so these rules are about the string being storable and
// displayable.
func TestValidateSubscriberName_RefusesUnstorableLabels(t *testing.T) {
	assert.NoError(t, validateSubscriberName("ledger-ops", true),
		"an ordinary label must be accepted")
	assert.NoError(t, validateSubscriberName("  ledger-ops  ", true),
		"surrounding whitespace is trimmed rather than refused: unlike a key prefix, a label is "+
			"not compared against anything, so the difference is cosmetic")

	assert.Error(t, validateSubscriberName("", true), "a blank label defeats the reason the field exists")
	assert.Error(t, validateSubscriberName("   ", true), "a label of spaces is a blank label")

	assert.Error(t, validateSubscriberName("ledger\nops", true), "a newline forges a second log entry")
	assert.Error(t, validateSubscriberName("ledger\rops", true), "a carriage return overwrites a line")
	assert.Error(t, validateSubscriberName("ledger\x00ops", true), "a NUL truncates C-side consumers")
	assert.Error(t, validateSubscriberName("ledger\x1b[31mops", true), "an escape sequence rewrites a terminal")
	assert.Error(t, validateSubscriberName("ledger\x7fops", true), "DEL is a control character too")

	assert.Error(t, validateSubscriberName(strings.Repeat("n", maxSubscriberNameLen+1), true),
		"an unbounded label must be refused")
	assert.NoError(t, validateSubscriberName(strings.Repeat("n", maxSubscriberNameLen), true),
		"the bound itself must be accepted, or the limit is off by one")

	// THE BOUND IS IN RUNES, NOT BYTES. Counting bytes would give a label written in a
	// non-Latin script roughly a third of the allowance the same label gets in English, which
	// is a bound on the alphabet rather than on the value.
	assert.NoError(t, validateSubscriberName(strings.Repeat("台", maxSubscriberNameLen), true),
		"a label at the bound in runes must be accepted however many bytes it occupies")
	assert.Error(t, validateSubscriberName(strings.Repeat("台", maxSubscriberNameLen+1), true))

	assert.NoError(t, validateSubscriberName("réconciliation d'équipe 🇫🇷", true),
		"the alphabet is deliberately unrestricted: a name is a human label")
}

// ---------------------------------------------------------------------------
// SSRF-01 — the legacy webhook destination policy
// ---------------------------------------------------------------------------

// TestValidateLegacyWebhookURL_EnforcesTheDestinationPolicy is the SSRF-01 guard.
//
// Nothing sends to this URL today, which is exactly why it is constrained now: a
// stored URL is a future sink, and the moment any code sends to it, whatever is in
// the column becomes a request Blnk makes from inside its own network.
func TestValidateLegacyWebhookURL_EnforcesTheDestinationPolicy(t *testing.T) {
	t.Run("accepts an external https endpoint", func(t *testing.T) {
		assert.NoError(t, validateLegacyWebhookURL("https://hooks.example.com/blnk"))
		assert.NoError(t, validateLegacyWebhookURL("https://hooks.example.com:8443/blnk?v=1"))
	})

	t.Run("accepts an empty value", func(t *testing.T) {
		// Empty means "none recorded" on create and "clear it" on update; both are
		// legitimate and neither is a destination.
		assert.NoError(t, validateLegacyWebhookURL(""))
	})

	schemes := map[string]string{
		"http":   "http://hooks.example.com/blnk",
		"file":   "file:///etc/passwd",
		"gopher": "gopher://hooks.example.com/",
		"ftp":    "ftp://hooks.example.com/",
	}
	for scheme, raw := range schemes {
		t.Run("refuses the "+scheme+" scheme", func(t *testing.T) {
			err := validateLegacyWebhookURL(raw)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "https",
				"the error must name the requirement, not just refuse")
		})
	}

	internal := map[string]string{
		"loopback literal":          "https://127.0.0.1/blnk",
		"loopback by name":          "https://localhost/blnk",
		"loopback subdomain":        "https://api.localhost/blnk",
		"IPv6 loopback":             "https://[::1]/blnk",
		"IPv4-mapped IPv6 loopback": "https://[::ffff:127.0.0.1]/blnk",
		"cloud metadata endpoint":   "https://169.254.169.254/latest/meta-data/",
		"private 10/8":              "https://10.0.0.7:9092/blnk",
		"private 172.16/12":         "https://172.16.4.9/blnk",
		"private 192.168/16":        "https://192.168.1.1/blnk",
		"unspecified address":       "https://0.0.0.0/blnk",
		"multicast":                 "https://239.1.2.3/blnk",
		"mDNS hostname":             "https://broker.local/blnk",
		"internal zone hostname":    "https://metadata.google.internal/blnk",
		"unqualified hostname":      "https://postgres/blnk",
	}
	for name, raw := range internal {
		t.Run("refuses "+name, func(t *testing.T) {
			err := validateLegacyWebhookURL(raw)
			require.Error(t, err, "%s must be refused", raw)

			// The wording is the SHARED policy's, because the policy is now defined once in
			// model.ValidateWebhookURL rather than copied here and in the repository. Asserting on
			// the shared phrase is what makes the two doors — an HTTP caller and a service, CLI or
			// migration caller reaching the repository directly — provably answer the same way for
			// the same host.
			assert.Contains(t, err.Error(), "internal destination")
			assert.Contains(t, err.Error(), "webhook_url",
				"a DTO validation error must name the body key the caller has to correct")
		})
	}

	t.Run("refuses a URL with no host", func(t *testing.T) {
		assert.Error(t, validateLegacyWebhookURL("https:///blnk"))
	})

	t.Run("refuses surrounding whitespace", func(t *testing.T) {
		assert.Error(t, validateLegacyWebhookURL(" https://hooks.example.com/blnk "))
	})

	t.Run("does not echo the input when parsing fails", func(t *testing.T) {
		// The input is a third party's endpoint, and url.Parse's own error quotes it.
		hostile := "https://hooks.example.com/\x7f\x00secret-path"
		err := validateLegacyWebhookURL(hostile)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "secret-path",
			"a parse failure must not republish the URL it failed on")
	})
}

// TestInternalWebhookDestinationReason_ExplainsRatherThanRefuses checks that the
// policy says WHICH rule was hit.
//
// "not allowed" sends an operator looking for a policy document; naming the metadata
// endpoint tells them what they just pointed Blnk at.
func TestInternalWebhookDestinationReason_ExplainsRatherThanRefuses(t *testing.T) {
	assert.Contains(t, model.InternalDestinationReason("169.254.169.254"), "metadata")
	assert.Contains(t, model.InternalDestinationReason("127.0.0.1"), "loopback")
	assert.Contains(t, model.InternalDestinationReason("10.1.2.3"), "private")
	assert.Contains(t, model.InternalDestinationReason("postgres"), "unqualified")

	assert.Empty(t, model.InternalDestinationReason("hooks.example.com"),
		"an external name must produce no reason at all")
	assert.Empty(t, model.InternalDestinationReason("93.184.216.34"))
}

// ---------------------------------------------------------------------------
// SEC-03 — the principal and group are not fields
// ---------------------------------------------------------------------------

// TestCreateSubscriber_HasNoPrincipalOrGroupField is a STRUCTURAL guard, and it is
// deliberately about the type rather than about behaviour.
//
// The fields were once accepted as "omit to have the service derive it". A caller
// able to choose the principal chooses which identity receives a grant; a caller
// able to choose the consumer group chooses a PREFIXED ACL pattern that can span
// other subscribers' group namespaces. Validating such a field is weaker than not
// having it: a field validated on every path today can be read by a path added
// tomorrow. This test fails the moment either is reintroduced, which is the only way
// to keep a removal removed.
func TestCreateSubscriber_HasNoPrincipalOrGroupField(t *testing.T) {
	forbidden := []string{"KafkaPrincipal", "ConsumerGroupID"}

	for _, name := range forbidden {
		_, present := reflect.TypeOf(CreateSubscriber{}).FieldByName(name)
		assert.False(t, present,
			"CreateSubscriber must not accept %s: it is an authorization boundary derived "+
				"from the subscriber ID, not a name a caller may choose", name)
	}

	for _, name := range forbidden {
		_, present := reflect.TypeOf(UpdateSubscriber{}).FieldByName(name)
		assert.False(t, present,
			"UpdateSubscriber must not accept %s: editing it re-points or widens a live grant", name)
	}
}

// TestCreateSubscriber_IgnoresACallerSuppliedPrincipal proves the removal holds
// through JSON, which is the only surface a caller actually reaches.
func TestCreateSubscriber_IgnoresACallerSuppliedPrincipal(t *testing.T) {
	body := []byte(`{
		"subscriber_id": "acme_prod",
		"name": "Acme production",
		"kafka_principal": "admin",
		"consumer_group_id": "blnk-sub-victim.default",
		"authorized_topics": ["blnk.transactions"]
	}`)

	var request CreateSubscriber
	require.NoError(t, json.Unmarshal(body, &request))

	principal, group, err := request.Derived()
	require.NoError(t, err)

	assert.Equal(t, "blnk-sub-acme_prod", principal,
		"the principal must come from the subscriber ID, never from the body")
	assert.Equal(t, "blnk-sub-acme_prod.default", group,
		"the consumer group must come from the subscriber ID, never from the body")
	assert.NotContains(t, group, "victim",
		"a supplied group must have no influence whatsoever")
}

// validCreateSubscriber returns a registration request that passes validation, so a test can
// change exactly the one field it is about.
func validCreateSubscriber() CreateSubscriber {
	return CreateSubscriber{
		SubscriberID:     "acme_prod",
		Name:             "Acme production",
		AuthorizedTopics: []string{"blnk.transactions"},
	}
}

// TestCreateSubscriber_Validate covers what remains caller-supplied.
func TestCreateSubscriber_Validate(t *testing.T) {
	valid := validCreateSubscriber()
	assert.NoError(t, valid.Validate(testTopicPrefix))

	t.Run("an omitted subscriber ID is accepted for generation", func(t *testing.T) {
		// Generation produces a canonical value by construction, so there is nothing
		// to check; requiring one here would make the ID mandatory by accident.
		omitted := valid
		omitted.SubscriberID = ""
		assert.NoError(t, omitted.Validate(testTopicPrefix))
	})

	nonCanonical := map[string]string{
		"uppercase":            "Acme_Prod",
		"surrounding space":    " acme_prod",
		"leading underscore":   "_acme",
		"too short":            "ac",
		"a dot":                "acme.prod",
		"a control character":  "acme\nprod",
		"a wildcard":           "acme*",
		"a colon":              "acme:prod",
		"a forward slash":      "acme/prod",
		"non-ASCII homoglyphs": "acmе_prod",
	}
	for name, identifier := range nonCanonical {
		t.Run("refuses a "+name+" identifier", func(t *testing.T) {
			request := valid
			request.SubscriberID = identifier
			assert.Error(t, request.Validate(testTopicPrefix),
				"identifier %q must be refused: the principal and group are derived from it, "+
					"so two identifiers that differ only cosmetically would collapse onto one "+
					"principal sharing one credential", identifier)
		})
	}

	t.Run("refuses a non-grantable topic", func(t *testing.T) {
		request := valid
		request.AuthorizedTopics = []string{"blnk.transactions.dlt"}
		assert.Error(t, request.Validate(testTopicPrefix))
	})

	t.Run("refuses an unstorable key scope", func(t *testing.T) {
		request := valid
		request.PartitionKeyPrefix = "ldg\n_9f1c"
		assert.Error(t, request.Validate(testTopicPrefix))
	})

	t.Run("refuses a missing name", func(t *testing.T) {
		// Enforced by Validate rather than by a binding:"required" tag, so an omitted
		// name and a whitespace-only one answer the same code — GEN_VALIDATION_ERROR —
		// instead of the binder's GEN_MALFORMED_REQUEST for one and this for the other.
		for name, value := range map[string]string{"omitted": "", "whitespace": "  \t "} {
			t.Run(name, func(t *testing.T) {
				request := valid
				request.Name = value
				assert.Error(t, request.Validate(testTopicPrefix),
					"name is the one field no service can invent, and it is NOT NULL in the table")
			})
		}
	})

	t.Run("carries no webhook_url field at all", func(t *testing.T) {
		// C-01. The four deprecated webhook-subscription routes are fronted by the
		// sunset guard and answer 410 Gone after the retirement instant; THIS route is
		// not deprecated and not guarded. A webhook_url accepted here would therefore
		// let a caller keep writing legacy webhook state after the surface that owns it
		// had been retired, which is the same as not retiring it.
		//
		// Asserted against the JSON shape rather than the Go struct, because the shape
		// is what a client sees: an unknown key is simply ignored by encoding/json, so
		// a caller sending one is not refused — it has no effect, which is the point.
		body, err := json.Marshal(valid)
		require.NoError(t, err)
		assert.NotContains(t, string(body), "webhook_url",
			"the general registration route must neither accept nor echo legacy webhook state; "+
				"POST /subscribers/:id/webhook-subscription is the guarded write path")
	})

	t.Run("Derived refuses a non-canonical identifier", func(t *testing.T) {
		request := valid
		request.SubscriberID = "Acme"
		_, _, err := request.Derived()
		assert.ErrorIs(t, err, model.ErrInvalidSubscriberIdentifier)
	})
}

// TestUpdateSubscriber_Validate checks the present/absent distinction, which is the
// whole reason the fields are pointers.
func TestUpdateSubscriber_Validate(t *testing.T) {
	empty := UpdateSubscriber{}
	assert.NoError(t, empty.Validate(testTopicPrefix),
		"an entirely absent update changes nothing and must validate")

	t.Run("a present topic list is checked", func(t *testing.T) {
		update := UpdateSubscriber{AuthorizedTopics: []string{"*"}}
		assert.Error(t, update.Validate(testTopicPrefix))
	})

	t.Run("a present empty topic list is accepted as a revocation", func(t *testing.T) {
		update := UpdateSubscriber{AuthorizedTopics: []string{}}
		assert.NoError(t, update.Validate(testTopicPrefix))
	})

	t.Run("a present prefix is checked and a present empty one clears", func(t *testing.T) {
		hostile := "ldg\r"
		assert.Error(t, UpdateSubscriber{PartitionKeyPrefix: &hostile}.Validate(testTopicPrefix))

		cleared := ""
		assert.NoError(t, UpdateSubscriber{PartitionKeyPrefix: &cleared}.Validate(testTopicPrefix))
	})

	// A rule applied only on creation is a rule with an edit-shaped hole, which is the same
	// reason the URL is re-checked below. A rename is the ordinary way a hostile label would
	// arrive: creation is often scripted from a template, editing is done by hand.
	t.Run("a present name is checked at the same standard as create", func(t *testing.T) {
		hostile := "ledger\nops"
		assert.Error(t, UpdateSubscriber{Name: &hostile}.Validate(testTopicPrefix))

		oversized := strings.Repeat("n", maxSubscriberNameLen+1)
		assert.Error(t, UpdateSubscriber{Name: &oversized}.Validate(testTopicPrefix))

		blank := "   "
		assert.Error(t, UpdateSubscriber{Name: &blank}.Validate(testTopicPrefix),
			"a present blank name is an attempt to erase the label, not an omission — the pointer "+
				"is what distinguishes the two, and omitting the field is how you leave it alone")

		valid := "ledger-ops"
		assert.NoError(t, UpdateSubscriber{Name: &valid}.Validate(testTopicPrefix))
	})

	// The legacy webhook URL is NOT a field on this request any more: the subscriber
	// registry no longer carries a per-subscriber destination, so there is nothing here for
	// an edit to reach. The "a policy applied only on creation is a policy with an
	// edit-shaped hole" assertion it used to make is made instead against the deprecated
	// webhook-subscription requests, which are the only shapes that still take a URL — see
	// TestWebhookSubscriptionRequests_ValidateTheirURL below.
}

// TestWebhookSubscriptionRequests_ValidateTheirURL covers the one route whose entire
// purpose is to accept a URL, and therefore the most likely way an internal address
// reaches the column.
func TestWebhookSubscriptionRequests_ValidateTheirURL(t *testing.T) {
	assert.NoError(t, CreateWebhookSubscription{WebhookURL: "https://hooks.example.com/b"}.Validate())
	assert.Error(t, CreateWebhookSubscription{WebhookURL: "http://hooks.example.com/b"}.Validate())
	assert.Error(t, CreateWebhookSubscription{WebhookURL: "https://127.0.0.1/b"}.Validate())

	assert.NoError(t, UpdateWebhookSubscription{WebhookURL: "https://hooks.example.com/b"}.Validate())
	assert.Error(t, UpdateWebhookSubscription{WebhookURL: "https://metadata.google.internal/b"}.Validate())
}

// ---------------------------------------------------------------------------
// SECRET-01 — the subscriber response carries a fingerprint, not a reference
// ---------------------------------------------------------------------------

// TestNewSubscriberResponse_ReportsAFingerprintNeverTheReference is the SECRET-01
// guard.
//
// The reference is not a secret — nobody can authenticate with it — but returning it
// makes internal correlation state part of the API contract, and "it is
// non-reversible" is a property of the code that DERIVES it rather than of the column
// that stores it.
func TestNewSubscriberResponse_ReportsAFingerprintNeverTheReference(t *testing.T) {
	reference := derivedReference(t)
	issued := time.Now().UTC()

	response := NewSubscriberResponse(model.EventSubscriber{
		SubscriberID:        "acme_prod",
		Name:                "Acme production",
		KafkaPrincipal:      "blnk-sub-acme_prod",
		ConsumerGroupID:     "blnk-sub-acme_prod.default",
		AuthorizedTopics:    []string{"blnk.transactions"},
		CredentialReference: &reference,
		CredentialIssuedAt:  &issued,
	})

	assert.Equal(t, model.CredentialFingerprint(reference), response.CredentialFingerprint)
	assert.NotEmpty(t, response.CredentialFingerprint)
	assert.Len(t, response.CredentialFingerprint, model.CredentialFingerprintLen)

	body, err := json.Marshal(response)
	require.NoError(t, err)
	assert.NotContains(t, string(body), reference,
		"the full reference must not appear in the response body")
	assert.NotContains(t, string(body), "credential_reference",
		"the response must not carry a credential_reference key at all")

	_, present := reflect.TypeOf(SubscriberResponse{}).FieldByName("CredentialReference")
	assert.False(t, present, "SubscriberResponse must have no CredentialReference field")
}

// TestNewSubscriberResponse_DropsAMisStoredReference proves the projection fails
// closed rather than echoing whatever the column held.
func TestNewSubscriberResponse_DropsAMisStoredReference(t *testing.T) {
	// The value a future path might mistakenly store: a raw secret in the reference
	// column. It must produce NO output rather than a partial rendering of itself.
	misStored := "S3cret-Password-Not-A-Reference"

	response := NewSubscriberResponse(model.EventSubscriber{
		SubscriberID:        "acme_prod",
		CredentialReference: &misStored,
	})

	assert.Empty(t, response.CredentialFingerprint,
		"a value that is not a derived reference must yield no fingerprint")

	body, err := json.Marshal(response)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "S3cret",
		"not even a fragment of a mis-stored secret may reach the response")
}

// TestNewSubscriberResponse_ProjectsTheNullableColumns covers the flattening, and in
// particular that an empty grant is REPORTED rather than dropped.
func TestNewSubscriberResponse_ProjectsTheNullableColumns(t *testing.T) {
	prefix := "ldg_9f1c"
	webhook := "https://hooks.example.com/blnk"
	migrated := time.Now().UTC()

	full := NewSubscriberResponse(model.EventSubscriber{
		SubscriberID:       "acme_prod",
		AuthorizedTopics:   []string{"blnk.transactions"},
		PartitionKeyPrefix: &prefix,
		WebhookURL:         &webhook,
		MigratedAt:         &migrated,
	})
	assert.Equal(t, prefix, full.PartitionKeyPrefix)
	require.NotNil(t, full.MigratedAt)

	// C-01: the recorded legacy URL is NOT projected, even though the row carries one.
	// It is readable through GET /subscribers/:id/webhook-subscription alone, which the
	// sunset guard fronts — so it stops being disclosed at the retirement instant.
	// Echoing it here as well would keep it readable through an unguarded route after
	// that, and the two reads would then disagree about whether the legacy surface still
	// exists.
	//
	// Asserted on the marshalled body rather than a struct field, because a field that
	// no longer exists cannot be asserted absent in Go, and the body is what a client
	// reads.
	fullBody, err := json.Marshal(full)
	require.NoError(t, err)
	assert.NotContains(t, string(fullBody), "webhook_url",
		"the general subscriber projection must not disclose the legacy endpoint")
	assert.NotContains(t, string(fullBody), webhook)

	// migrated_at IS projected, and deliberately. It is migration progress about this
	// deployment rather than legacy webhook state — no endpoint, no third-party data —
	// and a progress report needs it before and after the sunset alike.
	assert.Contains(t, string(fullBody), "migrated_at")

	bare := NewSubscriberResponse(model.EventSubscriber{SubscriberID: "acme_prod"})
	assert.Empty(t, bare.PartitionKeyPrefix)
	assert.Nil(t, bare.MigratedAt)
	assert.Nil(t, bare.CredentialIssuedAt, "nil is the reliable no-credential test")

	assert.NotNil(t, bare.AuthorizedTopics,
		"an empty grant is a fail-closed state an operator must see, so it marshals as [] not null")
	body, err := json.Marshal(bare)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"authorized_topics":[]`)
}

// ---------------------------------------------------------------------------
// DATA-01 — the dead-letter projection is an inventory, not a dump
// ---------------------------------------------------------------------------

// deadLetteredEntry is the NARROW inventory entry the repository returns for the stored row
// below, projected exactly as the listing statement projects it (PERF-P06): every column the
// response needs, the body's SIZE, and not the body.
//
// The two fixtures are kept separate deliberately. deadLetteredRow describes what is STORED,
// including the payload whose content DATA-01 is about; this one describes what the repository
// is willing to READ. Deriving one from the other is what makes the removal assertions
// meaningful — a fixture that simply never mentioned the payload would assert absence rather
// than removal, and would keep passing if the projection widened again.
func deadLetteredEntry(t *testing.T) model.DeadLetterInventoryEntry {
	t.Helper()

	row := deadLetteredRow(t)

	return model.DeadLetterInventoryEntry{
		ID:               row.ID,
		EventID:          row.EventID,
		EventType:        row.EventType,
		AggregateID:      row.AggregateID,
		PartitionKey:     row.PartitionKey,
		LedgerID:         row.LedgerID,
		Topic:            row.Topic,
		SchemaVersion:    row.SchemaVersion,
		OccurredAt:       row.OccurredAt,
		Status:           row.Status,
		Attempts:         row.Attempts,
		LastError:        row.LastError,
		FirstAttemptedAt: row.FirstAttemptedAt,
		LastAttemptedAt:  row.LastAttemptedAt,
		DLTTopic:         row.DLTTopic,
		FailureMetadata:  row.FailureMetadata,
		PayloadBytes:     len(row.Payload),
	}
}

// deadLetteredRow is a stored row whose every sensitive field is populated, so the
// projection assertions are testing removal rather than absence.
func deadLetteredRow(t *testing.T) model.EventOutbox {
	t.Helper()

	first := time.Now().UTC().Add(-10 * time.Minute)
	last := time.Now().UTC().Add(-1 * time.Minute)

	metadata, err := json.Marshal(model.FailureMetadata{
		OriginalTopic:    "blnk.identities",
		ErrorReason:      "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe",
		AttemptCount:     5,
		FirstAttemptedAt: first,
		LastAttemptedAt:  last,
	})
	require.NoError(t, err)

	return model.EventOutbox{
		EventID:     "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c",
		EventType:   "identity.created",
		AggregateID: "idt_9f1c8a72",
		LedgerID:    "ldg_4b1e7c30",
		// DELIBERATELY DIFFERENT from both AggregateID and LedgerID, and not the value
		// today's keying would produce for an identity event. It is what a row committed
		// before a keying change looks like, and it is the only fixture shape that can
		// tell "the projection read the stored key" apart from "the projection read the
		// aggregate id, or recomputed one, and happened to agree".
		PartitionKey:  "bln_legacy_key_51d0",
		Topic:         "blnk.identities",
		DLTTopic:      "blnk.identities.dlt",
		SchemaVersion: 1,
		Status:        "dead_lettered",
		Attempts:      5,
		Payload: json.RawMessage(`{"event":"identity.created","data":{` +
			`"first_name":"Ada","last_name":"Lovelace","email_address":"ada@example.com",` +
			`"phone_number":"+15550100","street":"12 Analytical Way","dob":"1815-12-10"}}`),
		OccurredAt:       first,
		LastError:        "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe",
		FirstAttemptedAt: &first,
		LastAttemptedAt:  &last,
		FailureMetadata:  metadata,
	}
}

// TestNewDeadLetterEvent_CarriesNoPayloadAndNoRawFailureText is the DATA-01 guard.
//
// The payload is the marshaled ledger event: an identity event carries a name,
// email, phone, address and date of birth. Listing a page of dead-lettered events
// would have returned all of it, for a triage task that needs none of it — replay
// re-publishes the stored bytes server-side, so the operator never supplies them.
func TestNewDeadLetterEvent_CarriesNoPayloadAndNoRawFailureText(t *testing.T) {
	row := deadLetteredEntry(t)
	item := NewDeadLetterEvent(row)

	body, err := json.Marshal(item)
	require.NoError(t, err)
	rendered := string(body)

	for _, sensitive := range []string{
		"Ada", "Lovelace", "ada@example.com", "+15550100", "Analytical Way", "1815-12-10",
	} {
		assert.NotContains(t, rendered, sensitive,
			"the projection must not carry payload content (%q leaked)", sensitive)
	}

	// The BROKER ADDRESS AS AN ADDRESS, not the bare port digits. "9092" on its own is four
	// digits, and the rendered body carries RFC3339 timestamps with nanosecond precision — so
	// a substring test for it fails whenever a nanosecond fraction happens to contain that
	// sequence, which is roughly one run in a few hundred and has nothing to do with the
	// property under test. Asserting the host:port form keeps the guard exact: a leaked broker
	// endpoint always appears with its host, because that is how the failure text names it.
	for _, internal := range []string{
		"10.0.0.4", "10.0.0.7", ":9092", "broken pipe", "write tcp",
	} {
		assert.NotContains(t, rendered, internal,
			"the projection must not describe the deployment (%q leaked)", internal)
	}

	assert.NotContains(t, rendered, "last_error")
	assert.NotContains(t, rendered, "failure_metadata")
	assert.NotContains(t, rendered, `"payload"`)

	// The partition key IS rendered, and belongs in this test as well as in the
	// keeps-what-triage-needs one: it is an identifier of the same class as aggregate_id
	// and ledger_id, which are already carried, and asserting it here records that its
	// inclusion was weighed against DATA-01 rather than overlooked.
	//
	// The value is the EFFECTIVE key — the ledger, on this fixture — because that is the key the
	// publish path resolved and therefore the partition the message is in. The stored column is
	// not rendered anywhere, which is deliberate: it is the publisher's second choice and reporting
	// it would answer an ordering question with the wrong key.
	assert.Contains(t, rendered, `"partition_key":"`+row.EffectiveKey()+`"`,
		"the effective partition key must reach the response: it is an identifier of the same class "+
			"as aggregate_id, and without it no ordering question can be answered from this API")
	assert.NotContains(t, rendered, "bln_legacy_key_51d0",
		"the superseded stored key must not be rendered: it names a partition this event is not in")

	for _, field := range []string{"Payload", "LastError", "FailureMetadata"} {
		_, present := reflect.TypeOf(DeadLetterEvent{}).FieldByName(field)
		assert.False(t, present, "DeadLetterEvent must have no %s field", field)
	}

	// The guarantee moved one layer DOWN as well (PERF-P06). The projection this response is
	// built from carries the body's size and not the body, so the bytes are never read out of
	// the database at all — the response could not carry them even if it wanted to. Asserted
	// structurally, because a widened projection is how a payload would find its way back
	// into a listing, and it would do so without any change to DeadLetterEvent.
	entryType := reflect.TypeOf(model.DeadLetterInventoryEntry{})
	for _, field := range []string{"Payload", "PayloadRaw", "EventRaw"} {
		_, present := entryType.FieldByName(field)
		assert.False(t, present,
			"model.DeadLetterInventoryEntry must have no %s field: the listing statement must not "+
				"read a body it has no use for", field)
	}

	sizeField, hasSize := entryType.FieldByName("PayloadBytes")
	require.True(t, hasSize, "the projection must still report the body's SIZE")
	assert.Equal(t, reflect.Int, sizeField.Type.Kind(),
		"the size is a count, which is what octet_length returns")
}

// TestNewDeadLetterEvent_KeepsWhatTriageActuallyNeeds is the other half: minimizing
// must not leave the endpoint useless.
func TestNewDeadLetterEvent_KeepsWhatTriageActuallyNeeds(t *testing.T) {
	row := deadLetteredEntry(t)
	item := NewDeadLetterEvent(row)

	assert.Equal(t, row.EventID, item.EventID, "the replay handle must survive")
	assert.Equal(t, row.EventType, item.EventType)
	assert.Equal(t, row.AggregateID, item.AggregateID)
	assert.Equal(t, row.LedgerID, item.LedgerID)
	// THE EFFECTIVE KEY, which on this fixture is NOT the stored column.
	//
	// The fixture is deliberately the divergent case: it carries a ledger AND a partition key
	// left over from an earlier keying rule, and the publish path prefers the ledger because
	// requirement R-6 partitions by ledger ID. So the message went to the ledger's partition, and
	// a response reporting `bln_legacy_key_51d0` would name the key the event was NOT routed by —
	// on exactly the row an operator is investigating, and with nothing to indicate the answer
	// might be wrong. It is the only field here an ordering question can be answered from, so
	// naming the wrong key is worse than naming none.
	//
	// This assertion also still covers the field being unassigned, which is how it started: the
	// tag is omitempty, so leaving it unset rendered no field at all and the answer read as "this
	// event had no key" rather than as a gap.
	require.NotEqual(t, row.LedgerID, row.PartitionKey,
		"the fixture must keep the two values different, or this assertion proves nothing")
	assert.Equal(t, row.EffectiveKey(), item.PartitionKey,
		"the projection must report the key the publish path resolved, not the stored column")
	assert.Equal(t, row.LedgerID, item.PartitionKey,
		"and on a row carrying a ledger that key IS the ledger")
	assert.NotEqual(t, row.AggregateID, item.PartitionKey,
		"never the aggregate id: that is the last rung of the chain, reached only when a row has "+
			"neither a ledger nor a stored key")
	assert.Equal(t, row.Topic, item.Topic, "replay targets this topic")
	assert.Equal(t, row.DLTTopic, item.DLTTopic)
	assert.Equal(t, row.Status, item.Status)
	assert.Equal(t, row.Attempts, item.Attempts)
	assert.Equal(t, row.SchemaVersion, item.SchemaVersion)
	assert.Equal(t, row.OccurredAt, item.OccurredAt)

	assert.Equal(t, row.PayloadBytes, item.PayloadBytes,
		"the size is what confirms or refutes a message_too_large classification")
	assert.Positive(t, item.PayloadBytes)

	require.NotNil(t, item.FirstAttemptedAt)
	require.NotNil(t, item.LastAttemptedAt)
	assert.Equal(t, FailureReasonBrokerUnavailable, item.FailureReason,
		"a broken pipe is a broker problem, and saying so is the point of classifying")
}

// TestNewDeadLetterEvent_ReportsTheKeyThePublisherActuallyUsed is the whole of MD-8 in one
// place: the reported key and the routed key must be one value resolved by one rule.
//
// The response used to read the stored partition_key column. The publish path prefers the
// LEDGER — requirement R-6 partitions by ledger ID — so on any row where the two disagree the
// API named a partition the event was not in. Nothing in the response said which value it was,
// so an operator investigating "why are these two events out of order" got a confident wrong
// answer from the only field that can answer the question.
//
// The three cases below are the fallback chain's three rungs, asserted through the DTO rather
// than through the model, because it is the DTO that a subscriber and an operator read.
func TestNewDeadLetterEvent_ReportsTheKeyThePublisherActuallyUsed(t *testing.T) {
	base := deadLetteredEntry(t)

	t.Run("a ledger-scoped row reports its ledger, not the stored column", func(t *testing.T) {
		require.NotEmpty(t, base.LedgerID)
		require.NotEqual(t, base.LedgerID, base.PartitionKey)

		item := NewDeadLetterEvent(base)

		assert.Equal(t, base.LedgerID, item.PartitionKey)
		assert.Equal(t, base.LedgerID, item.LedgerID,
			"both fields are on the response, so the reader can see WHY the key is what it is")
	})

	t.Run("a ledger-less row reports the stored column", func(t *testing.T) {
		// The events with no ledger — identities, bulk batches, system errors, rejected
		// transactions — are keyed on the stored column, and it is their real key.
		row := base
		row.LedgerID = ""

		item := NewDeadLetterEvent(row)

		assert.Equal(t, base.PartitionKey, item.PartitionKey,
			"the stored key is the actual key when there is no ledger to prefer")
		assert.Empty(t, item.LedgerID, "and an absent ledger is omitted rather than blanked")
	})

	t.Run("whitespace in the ledger reads as absence", func(t *testing.T) {
		// A key made of spaces would be a partition of its own, silently splitting an
		// aggregate's events across two partitions.
		row := base
		row.LedgerID = "   "

		item := NewDeadLetterEvent(row)

		assert.Equal(t, base.PartitionKey, item.PartitionKey)
	})

	t.Run("the DTO and the model resolve one key", func(t *testing.T) {
		// The assertion that keeps them one rule rather than two implementations that agree
		// today. If the projection ever recomputes the key itself, this fails.
		for name, row := range map[string]model.DeadLetterInventoryEntry{
			"ledger present": base,
			"ledger absent":  func() model.DeadLetterInventoryEntry { r := base; r.LedgerID = ""; return r }(),
			"neither": func() model.DeadLetterInventoryEntry {
				r := base
				r.LedgerID = ""
				r.PartitionKey = ""

				return r
			}(),
		} {
			t.Run(name, func(t *testing.T) {
				assert.Equal(t, row.EffectiveKey(), NewDeadLetterEvent(row).PartitionKey)
			})
		}
	})
}

// TestNewDeadLetterEvent_FillsAttemptGapsFromTheFailureMetadata covers a row whose
// own columns are unset — the two are written by different steps of one failure, so a
// row can legitimately carry one and not the other.
func TestNewDeadLetterEvent_FillsAttemptGapsFromTheFailureMetadata(t *testing.T) {
	row := deadLetteredEntry(t)
	row.FirstAttemptedAt = nil
	row.LastAttemptedAt = nil
	row.Attempts = 0
	row.LastError = ""

	item := NewDeadLetterEvent(row)

	require.NotNil(t, item.FirstAttemptedAt, "the metadata's instant must fill the gap")
	require.NotNil(t, item.LastAttemptedAt)
	assert.Equal(t, 5, item.Attempts)
	assert.Equal(t, FailureReasonBrokerUnavailable, item.FailureReason,
		"the metadata's reason is classified when last_error is empty")

	body, err := json.Marshal(item)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "10.0.0.7",
		"filling from the metadata must not import its raw text")
}

// TestNewDeadLetterEvent_HandlesAnUnparseableMetadataBlob proves the projection does
// not fail on Blnk's own bookkeeping being malformed.
func TestNewDeadLetterEvent_HandlesAnUnparseableMetadataBlob(t *testing.T) {
	row := deadLetteredEntry(t)
	row.FailureMetadata = json.RawMessage(`{not json`)

	item := NewDeadLetterEvent(row)

	// The envelope comes from the row's columns, so it is unaffected.
	assert.Equal(t, row.EventID, item.EventID)
	assert.Equal(t, FailureReasonBrokerUnavailable, item.FailureReason)
}

// TestNewDeadLetterEvent_OnANeverFailedRowReportsNoReason covers a pending row read
// through the same projection: no failure means no reason, not "unclassified".
func TestNewDeadLetterEvent_OnANeverFailedRowReportsNoReason(t *testing.T) {
	item := NewDeadLetterEvent(model.DeadLetterInventoryEntry{
		EventID:      "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c",
		EventType:    "transaction.applied",
		Topic:        "blnk.transactions",
		Status:       "pending",
		PayloadBytes: len(`{"event":"transaction.applied","data":{}}`),
	})

	assert.Empty(t, item.FailureReason)
	assert.Nil(t, item.FirstAttemptedAt)
	assert.Nil(t, item.LastAttemptedAt)

	body, err := json.Marshal(item)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "failure_reason",
		"omitempty must drop the key rather than reporting an empty classification")
}

// TestClassifyFailureReason_AlwaysReturnsVocabulary is the property that makes the
// classifier a sanitizer rather than a formatter: no input, however constructed, can
// produce output that describes the deployment.
func TestClassifyFailureReason_AlwaysReturnsVocabulary(t *testing.T) {
	vocabulary := map[string]struct{}{
		FailureReasonBrokerUnavailable:   {},
		FailureReasonAuthorizationDenied: {},
		FailureReasonMessageTooLarge:     {},
		FailureReasonTopicMissing:        {},
		FailureReasonTimeout:             {},
		FailureReasonPersistence:         {},
		FailureReasonUnclassified:        {},
	}

	inputs := []string{
		"write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe",
		"dial tcp 10.1.2.3:9092: connect: connection refused",
		"kafka: TOPIC_AUTHORIZATION_FAILED for blnk.transactions",
		"SASL Authentication failed for user blnk-producer",
		"kafka server: Message was too large, server rejected it",
		"kafka server: Request was for a topic or partition that does not exist: unknown topic",
		"context deadline exceeded",
		`pq: null value in column "partition_key" of relation "event_outbox" violates not-null constraint`,
		"an entirely novel failure nobody anticipated, from /usr/src/blnk/secret.go:42",
		"\x00\x1b[2J",
		strings.Repeat("A", 4096),
	}

	for _, raw := range inputs {
		reason := classifyFailureReason(raw)
		_, known := vocabulary[reason]
		require.True(t, known, "classifying %q produced %q, which is outside the vocabulary", raw, reason)

		// The decisive assertion: the output is never a piece of the input.
		assert.NotContains(t, raw, reason,
			"the classification %q must not be a substring of the input it came from, or it "+
				"would be echoing rather than classifying", reason)
	}

	assert.Empty(t, classifyFailureReason(""), "no failure text means no reason at all")
	assert.Empty(t, classifyFailureReason("   "))
}

// TestClassifyFailureReason_DistinguishesTheActionableCases checks that the
// classification is USEFUL, not merely safe — each value implies a different next
// action for an operator.
func TestClassifyFailureReason_DistinguishesTheActionableCases(t *testing.T) {
	cases := map[string]string{
		"dial tcp 10.1.2.3:9092: connect: connection refused":   FailureReasonBrokerUnavailable,
		"write tcp 10.0.0.4:9092: broken pipe":                  FailureReasonBrokerUnavailable,
		"the Kafka broker did not acknowledge the message":      FailureReasonBrokerUnavailable,
		"kafka: TOPIC_AUTHORIZATION_FAILED":                     FailureReasonAuthorizationDenied,
		"SASL authentication failed":                            FailureReasonAuthorizationDenied,
		"kafka server: Message was too large":                   FailureReasonMessageTooLarge,
		"kafka server: unknown topic or partition":              FailureReasonTopicMissing,
		"context deadline exceeded":                             FailureReasonTimeout,
		"i/o timeout":                                           FailureReasonTimeout,
		`pq: relation "blnk.event_outbox" does not exist (sql)`: FailureReasonPersistence,
		"something nobody has seen before":                      FailureReasonUnclassified,
	}

	for raw, want := range cases {
		assert.Equal(t, want, classifyFailureReason(raw), "misclassified %q", raw)
	}

	t.Run("authorization outranks unavailability", func(t *testing.T) {
		// A broker can report both in one message, and the authorization failure is
		// the actionable half: it will not clear on its own.
		assert.Equal(t, FailureReasonAuthorizationDenied,
			classifyFailureReason("broker unavailable: authorization failed for principal"))
	})
}

// TestNewSubscriberResponse_StatesWhatTheNextCallWillDo covers the two predictions the
// subscriber projection makes about calls the client has not made yet.
//
// Both exist because the state they describe was previously reported accurately field by field
// while the CONSEQUENCE — the only part anybody acts on — was in a Go doc comment.
//
//   - CREDENTIAL ISSUANCE BLOCKED. A row whose next credential call will be refused says so on
//     the row rather than at the call. Two states block it — a deregistration whose broker-side
//     revocation is still owed, and an empty topic grant — and both are properties of the ROW, so
//     both are knowable the moment it is read. A RECORDED partition key prefix is deliberately
//     NOT one of them: that refusal once existed, it implemented no part of the key scope, and it
//     was replaced by issuing the credential and STATING which dimensions the broker enforces.
//     The assertions below pin that down, because a prediction of a refusal that no longer
//     happens would be worse than no prediction at all.
//   - REVOCATION PENDING. docs/metrics.md answers the critical revocation alert with "find the
//     affected subscribers with GET /subscribers", and the response reported none of it — so the
//     documented answer to "which subscribers owe a revocation?" was a psql session.
//
// Both are asserted present-and-false on an ordinary row too. A client that had to infer either
// from a MISSING field would infer it wrong, which is why neither carries omitempty.
func TestNewSubscriberResponse_StatesWhatTheNextCallWillDo(t *testing.T) {
	base := func() model.EventSubscriber {
		return model.EventSubscriber{
			SubscriberID:     "sub_projection",
			Name:             "Projection",
			KafkaPrincipal:   "blnk-sub-sub_projection",
			ConsumerGroupID:  "blnk-sub-sub_projection.default",
			AuthorizedTopics: []string{"blnk.transactions"},
		}
	}

	t.Run("an ordinary row reports both as settled", func(t *testing.T) {
		response := NewSubscriberResponse(base())

		assert.False(t, response.CredentialIssuanceBlocked,
			"a row with no key scope is provisionable, and says so rather than staying silent")
		assert.Empty(t, response.CredentialIssuanceBlockedReason,
			"the reason is present only when there is one, so it cannot be read as a warning")
		assert.False(t, response.RevocationPending)
		assert.Empty(t, response.RevocationPendingReason)
		assert.Nil(t, response.RevocationPendingAt)
	})

	// The key-scoped row is the case that changed. It is asserted here, in the test that
	// covers the predictions, precisely because the prediction it USED to carry was the
	// refusal: a reader of this file has to be able to see that the absence of a block on a
	// key-scoped row is intended and not an omission.
	t.Run("a recorded key scope is provisionable and the scope is declared, not refused", func(t *testing.T) {
		row := base()
		prefix := "ldg_acme"
		row.PartitionKeyPrefix = &prefix

		response := NewSubscriberResponse(row)

		require.False(t, response.CredentialIssuanceBlocked,
			"a recorded partition key prefix no longer withholds the credential: the refusal "+
				"implemented no part of the key scope, it withdrew a mandatory capability, and it "+
				"was replaced by issuing the credential and declaring what the broker enforces")
		assert.Empty(t, response.CredentialIssuanceBlockedReason,
			"and no remedy is offered, because there is nothing to remedy")
		assert.Equal(t, prefix, response.PartitionKeyPrefix,
			"the recorded value is still reported: it is the narrowing Blnk's gateway applies")
		assert.True(t, response.EnforcedAccess.PartitionKeyPrefixEnforced,
			"and the enforcement declaration says it is ENFORCED, which is what replaced both the "+
				"refusal and the consumer-side disclosure that followed it")
		assert.True(t, response.EnforcedAccess.GatewayDeliveryRequired,
			"AND it must say where the records come from, in machine-readable form: a key-scoped "+
				"subscriber holds no topic Read, so a client that tried to fetch from the broker "+
				"would simply be refused")
		assert.False(t, response.EnforcedAccess.BrokerRecordAccess,
			"which is the same fact stated as the grant it rests on")
		assert.Equal(t, prefix, response.EnforcedAccess.PartitionKeyPrefix,
			"restated inside the object that names the component enforcing it, which is the only "+
				"place a reader cannot mistake who keeps it")
	})

	t.Run("an empty topic grant predicts the refusal and names the remedy", func(t *testing.T) {
		row := base()
		row.AuthorizedTopics = nil

		response := NewSubscriberResponse(row)

		require.True(t, response.CredentialIssuanceBlocked,
			"a credential authorised for nothing is a live SCRAM principal with no purpose, so "+
				"issuance refuses it and the row has to say so before the call is made")
		assert.Contains(t, response.CredentialIssuanceBlockedReason, "authorized_topics",
			"the reason must name the field to set")
	})

	t.Run("a deregistration in flight predicts the refusal", func(t *testing.T) {
		row := base()
		pendingAt := time.Date(2026, 5, 1, 14, 2, 0, 0, time.UTC)
		row.RevocationPendingAt = &pendingAt

		response := NewSubscriberResponse(row)

		require.True(t, response.CredentialIssuanceBlocked,
			"issuing here would re-arm a principal whose revocation is already owed, which is a "+
				"safety property rather than a convenience")
		assert.Contains(t, response.CredentialIssuanceBlockedReason, "deregister",
			"and the reason must name the state to complete or abandon")
	})

	t.Run("a tombstoned row reports the outstanding revocation and when it began", func(t *testing.T) {
		row := base()
		pendingAt := time.Date(2026, 5, 1, 14, 2, 0, 0, time.UTC)
		row.RevocationPendingAt = &pendingAt

		response := NewSubscriberResponse(row)

		require.True(t, response.RevocationPending)
		require.NotNil(t, response.RevocationPendingAt,
			"the instant is what turns 'a cleanup is outstanding' into the age the alert is stated "+
				"over")
		assert.Equal(t, pendingAt, response.RevocationPendingAt.UTC())
		assert.Contains(t, response.RevocationPendingReason, "revocation",
			"and the reason must name the outstanding broker-side revocation, which is the only "+
				"thing a tombstone means now that a confirmed revocation clears it")
	})
}
