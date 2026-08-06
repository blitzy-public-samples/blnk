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
		"the system category":           "blnk.system",
		"the system dead-letter topic":  "blnk.system.dlt",
		"the quarantine category":       "blnk.quarantine",
		"a topic under another prefix":  "acme.transactions",
		"an untrimmed grantable name":   " blnk.transactions ",
		"an uppercased category":        "blnk.TRANSACTIONS",
	}
	for name, topic := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			err := validateGrantableTopics([]string{topic}, testTopicPrefix)
			require.Error(t, err, "topic %q must not be grantable", topic)
			assert.Contains(t, err.Error(), "not grantable",
				"the error must say why, so an operator is not left guessing")
		})
	}

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
// SEC-01 / SEC-03 — the advisory key prefix
// ---------------------------------------------------------------------------

// TestValidateAdvisoryKeyPrefix_RefusesUnstorableValues covers the filter hint.
//
// The value grants nothing, so these rules are about what the string does after it
// is stored: it is echoed into API responses, log lines and trace attributes, and a
// newline in it forges a second log entry.
func TestValidateAdvisoryKeyPrefix_RefusesUnstorableValues(t *testing.T) {
	assert.NoError(t, validateAdvisoryKeyPrefix(""), "no filter suggested is legitimate")
	assert.NoError(t, validateAdvisoryKeyPrefix("ldg_9f1c8a72"))

	assert.Error(t, validateAdvisoryKeyPrefix("ldg_9f1c\n8a72"), "a newline forges a log entry")
	assert.Error(t, validateAdvisoryKeyPrefix("ldg\r8a72"), "a carriage return overwrites a line")
	assert.Error(t, validateAdvisoryKeyPrefix("ldg\x008a72"), "a NUL truncates C-side consumers")
	assert.Error(t, validateAdvisoryKeyPrefix("ldg\x7f"), "DEL is a control character too")
	assert.Error(t, validateAdvisoryKeyPrefix(" ldg_9f1c "),
		"surrounding whitespace makes a consumer's prefix comparison match nothing, invisibly")
	assert.Error(t, validateAdvisoryKeyPrefix(strings.Repeat("k", maxAdvisoryKeyPrefixLen+1)))
	assert.NoError(t, validateAdvisoryKeyPrefix(strings.Repeat("k", maxAdvisoryKeyPrefixLen)),
		"the bound itself must be accepted, or the limit is off by one")
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
			assert.Contains(t, err.Error(), "not an allowed destination")
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
	assert.Contains(t, internalWebhookDestinationReason("169.254.169.254"), "metadata")
	assert.Contains(t, internalWebhookDestinationReason("127.0.0.1"), "loopback")
	assert.Contains(t, internalWebhookDestinationReason("10.1.2.3"), "private")
	assert.Contains(t, internalWebhookDestinationReason("postgres"), "unqualified")

	assert.Empty(t, internalWebhookDestinationReason("hooks.example.com"),
		"an external name must produce no reason at all")
	assert.Empty(t, internalWebhookDestinationReason("93.184.216.34"))
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

// TestCreateSubscriber_Validate covers what remains caller-supplied.
func TestCreateSubscriber_Validate(t *testing.T) {
	valid := CreateSubscriber{
		SubscriberID:     "acme_prod",
		Name:             "Acme production",
		AuthorizedTopics: []string{"blnk.transactions"},
	}
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

	t.Run("refuses a hostile advisory prefix", func(t *testing.T) {
		request := valid
		request.PartitionKeyPrefix = "ldg\n_9f1c"
		assert.Error(t, request.Validate(testTopicPrefix))
	})

	t.Run("refuses an internal webhook URL", func(t *testing.T) {
		request := valid
		request.WebhookURL = "https://169.254.169.254/latest/meta-data/"
		assert.Error(t, request.Validate(testTopicPrefix))
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

	t.Run("a present URL is checked at the same standard as create", func(t *testing.T) {
		// A policy applied only on creation is a policy with an edit-shaped hole.
		internal := "https://10.0.0.7/blnk"
		assert.Error(t, UpdateSubscriber{WebhookURL: &internal}.Validate(testTopicPrefix))

		cleared := ""
		assert.NoError(t, UpdateSubscriber{WebhookURL: &cleared}.Validate(testTopicPrefix))
	})
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
	assert.Equal(t, webhook, full.WebhookURL)
	require.NotNil(t, full.MigratedAt)

	bare := NewSubscriberResponse(model.EventSubscriber{SubscriberID: "acme_prod"})
	assert.Empty(t, bare.PartitionKeyPrefix)
	assert.Empty(t, bare.WebhookURL)
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
		EventID:       "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c",
		EventType:     "identity.created",
		AggregateID:   "idt_9f1c8a72",
		LedgerID:      "ldg_4b1e7c30",
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
	row := deadLetteredRow(t)
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

	for _, internal := range []string{"10.0.0.4", "10.0.0.7", "9092", "broken pipe", "write tcp"} {
		assert.NotContains(t, rendered, internal,
			"the projection must not describe the deployment (%q leaked)", internal)
	}

	assert.NotContains(t, rendered, "last_error")
	assert.NotContains(t, rendered, "failure_metadata")
	assert.NotContains(t, rendered, `"payload"`)

	for _, field := range []string{"Payload", "LastError", "FailureMetadata"} {
		_, present := reflect.TypeOf(DeadLetterEvent{}).FieldByName(field)
		assert.False(t, present, "DeadLetterEvent must have no %s field", field)
	}
}

// TestNewDeadLetterEvent_KeepsWhatTriageActuallyNeeds is the other half: minimizing
// must not leave the endpoint useless.
func TestNewDeadLetterEvent_KeepsWhatTriageActuallyNeeds(t *testing.T) {
	row := deadLetteredRow(t)
	item := NewDeadLetterEvent(row)

	assert.Equal(t, row.EventID, item.EventID, "the replay handle must survive")
	assert.Equal(t, row.EventType, item.EventType)
	assert.Equal(t, row.AggregateID, item.AggregateID)
	assert.Equal(t, row.LedgerID, item.LedgerID)
	assert.Equal(t, row.Topic, item.Topic, "replay targets this topic")
	assert.Equal(t, row.DLTTopic, item.DLTTopic)
	assert.Equal(t, row.Status, item.Status)
	assert.Equal(t, row.Attempts, item.Attempts)
	assert.Equal(t, row.SchemaVersion, item.SchemaVersion)
	assert.Equal(t, row.OccurredAt, item.OccurredAt)

	assert.Equal(t, len(row.Payload), item.PayloadBytes,
		"the size is what confirms or refutes a message_too_large classification")
	assert.Positive(t, item.PayloadBytes)

	require.NotNil(t, item.FirstAttemptedAt)
	require.NotNil(t, item.LastAttemptedAt)
	assert.Equal(t, FailureReasonBrokerUnavailable, item.FailureReason,
		"a broken pipe is a broker problem, and saying so is the point of classifying")
}

// TestNewDeadLetterEvent_FillsAttemptGapsFromTheFailureMetadata covers a row whose
// own columns are unset — the two are written by different steps of one failure, so a
// row can legitimately carry one and not the other.
func TestNewDeadLetterEvent_FillsAttemptGapsFromTheFailureMetadata(t *testing.T) {
	row := deadLetteredRow(t)
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
	row := deadLetteredRow(t)
	row.FailureMetadata = json.RawMessage(`{not json`)

	item := NewDeadLetterEvent(row)

	// The envelope comes from the row's columns, so it is unaffected.
	assert.Equal(t, row.EventID, item.EventID)
	assert.Equal(t, FailureReasonBrokerUnavailable, item.FailureReason)
}

// TestNewDeadLetterEvent_OnANeverFailedRowReportsNoReason covers a pending row read
// through the same projection: no failure means no reason, not "unclassified".
func TestNewDeadLetterEvent_OnANeverFailedRowReportsNoReason(t *testing.T) {
	item := NewDeadLetterEvent(model.EventOutbox{
		EventID:   "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c",
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Status:    "pending",
		Payload:   json.RawMessage(`{"event":"transaction.applied","data":{}}`),
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
