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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// credentialReferenceScheme is the fixed, self-describing prefix every credential
// reference carries.
const credentialReferenceScheme = "scram-sha-512-ref-v1"

// credentialReferenceSeparator divides the scheme from the digest.
const credentialReferenceSeparator = "$"

// credentialReferenceDigestHexLen is the length of the hex-encoded digest: SHA-256
// produces 32 bytes, so 64 hex characters.
const credentialReferenceDigestHexLen = 64

// CredentialFingerprintLen is how many leading digest characters a fingerprint
// shows. Twelve hex characters is 48 bits — ample for a human to tell two
// issuances apart in a log or a response, and far too little to attack the digest
// with.
const CredentialFingerprintLen = 12

// ErrInvalidCredentialReference reports a value that is not a credential reference this
// package produced.
var ErrInvalidCredentialReference = errors.New(
	"model: value is not a credential reference derived by DeriveCredentialReference",
)

// DeriveCredentialReference produces the NON-REVERSIBLE reference persisted for an
// issued SASL credential.
//
// Parameters:
//   - principal string: the Kafka principal the credential belongs to.
//   - secret string: the generated SASL secret. Required.
//
// Returns:
//   - string: the reference, in the form "scram-sha-512-ref-v1$<64 hex chars>".
//   - error: when either input is empty, which would derive a reference that several
//     rows could share.
func DeriveCredentialReference(principal, secret string) (string, error) {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return "", errors.New("model: deriving a credential reference requires a principal")
	}
	if secret == "" {
		return "", errors.New("model: deriving a credential reference requires a secret")
	}

	mac := hmac.New(sha256.New, []byte(principal))
	// hash.Hash.Write is documented never to return an error, so there is nothing to
	// check here; the linter's errcheck is satisfied by the explicit discard.
	_, _ = mac.Write([]byte(secret))

	return credentialReferenceScheme + credentialReferenceSeparator +
		hex.EncodeToString(mac.Sum(nil)), nil
}

// ValidateCredentialReference reports whether a value has the exact shape
// DeriveCredentialReference produces.
//
// Parameters:
//   - reference string: the candidate value.
//
// Returns:
//   - error: nil when the value is a well-formed reference, otherwise
//     ErrInvalidCredentialReference.
func ValidateCredentialReference(reference string) error {
	scheme, digest, found := strings.Cut(reference, credentialReferenceSeparator)
	if !found || scheme != credentialReferenceScheme || len(digest) != credentialReferenceDigestHexLen {
		return ErrInvalidCredentialReference
	}

	// hex.DecodeString accepts upper case, which DeriveCredentialReference never produces,
	// so the case is checked as well as the alphabet. Two references over one credential
	// differing only in case would defeat the equality comparison the reference exists to
	// support.
	if strings.ToLower(digest) != digest {
		return ErrInvalidCredentialReference
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ErrInvalidCredentialReference
	}

	return nil
}

// CredentialFingerprint reduces a credential reference to the short, non-sensitive form
// that is safe to put in an API response or a log line.
//
// Parameters:
//   - reference string: a reference as produced by DeriveCredentialReference.
//
// Returns:
//   - string: the first CredentialFingerprintLen characters of the digest, or "".
func CredentialFingerprint(reference string) string {
	if ValidateCredentialReference(reference) != nil {
		return ""
	}

	_, digest, _ := strings.Cut(reference, credentialReferenceSeparator)

	return digest[:CredentialFingerprintLen]
}

// LogIdentifierHashLength is how many hex characters of a SHA-256 digest an identifier
// pseudonym keeps.
const LogIdentifierHashLength = 16

// HashIdentifier is THE canonical pseudonym rule for an identifier that must be
// correlated without being disclosed.
//
// Parameters:
//   - value string: the identifier. Hashed verbatim, with no trimming or case folding,
//     so the caller decides what the canonical form is.
//
// Returns:
//   - string: a short hex token, or "" for an empty input.
func HashIdentifier(value string) string {
	if value == "" {
		return ""
	}

	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])[:LogIdentifierHashLength]
}

// eventIDNamespace is the fixed UUID namespace every derived event id is generated
// under.
var eventIDNamespace = uuid.MustParse("6f2d1a55-9f6b-4a1e-8d2c-0f43a1b7c9e1")

// DeriveEventID derives the deterministic event id for one logical event.
//
// Parameters:
//   - mutationID string: the identity of the mutation the event describes.
//   - eventType string: the event name.
//   - schemaVersion int: the envelope schema version.
//
// Returns:
//   - string: a canonical, textual version-5 UUID.
func DeriveEventID(mutationID, eventType string, schemaVersion int) string {
	name := strings.Join([]string{
		strings.TrimSpace(mutationID),
		strings.TrimSpace(eventType),
		strconv.Itoa(schemaVersion),
	}, "\x1f")

	return uuid.NewSHA1(eventIDNamespace, []byte(name)).String()
}

// NewEventID mints a fresh, random event id for an event that has NO stable identity.
//
// Returns:
//   - string: a canonical, textual version-4 UUID.
func NewEventID() string {
	return uuid.New().String()
}

// EventIdentityFor reports the mutation identity a deterministic event id may be
// derived from, and whether the event has one at all.
//
// Parameters:
//   - eventType string: the event name, which selects the map interpretation.
//   - payload interface{}: the domain object the event carries. May be nil.
//
// Returns:
//   - string: the mutation identity, trimmed. Empty when there is none.
//   - bool: true when a deterministic id may be derived from it.
func EventIdentityFor(eventType string, payload interface{}) (string, bool) {
	identity := ""

	switch typed := payload.(type) {
	case *Transaction:
		if typed != nil {
			identity = typed.TransactionID
		}
	case Transaction:
		identity = typed.TransactionID
	case *Ledger:
		if typed != nil {
			identity = typed.LedgerID
		}
	case Ledger:
		identity = typed.LedgerID
	case *Identity:
		if typed != nil {
			identity = typed.IdentityID
		}
	case Identity:
		identity = typed.IdentityID
	case *Balance:
		if typed != nil {
			identity = typed.BalanceID
		}
	case Balance:
		identity = typed.BalanceID
	case map[string]interface{}:
		// A map payload is only a batch when the event type says so. Reading batch_id out of
		// some other map-shaped payload would derive an id from a field that means something
		// else entirely.
		if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
			if value, ok := typed["batch_id"].(string); ok {
				identity = value
			}
		}
	}

	identity = strings.TrimSpace(identity)

	return identity, identity != ""
}

// EventTypeIsRepeatable reports whether an event type is emitted more than once for one
// subject, and therefore must never carry a derived id.
//
// Parameters:
//   - eventType string: the event name.
//
// Returns:
//   - bool: true when the event repeats for one subject.
func EventTypeIsRepeatable(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case EventTypeBalanceMonitor, EventTypeSystemError:
		return true
	default:
		return false
	}
}

// GenerateSubscriberID mints a subscriber business identifier.
//
// Returns:
//   - string: "sub_<uuid>".
func GenerateSubscriberID() string {
	return GenerateUUIDWithSuffix(SubscriberIDPrefix)
}

// GenerateConsumerGroupID mints a consumer group id inside a freshly generated
// subscriber's namespace.
//
// Returns:
//   - string: "blnk-sub-<generated identifier>.default".
func GenerateConsumerGroupID() string {
	group, err := CanonicalConsumerGroupID(GenerateSubscriberID())
	if err != nil {
		// Unreachable: GenerateSubscriberID is canonical by construction. Falling back to the
		// namespace of a fixed leaf keeps the shape valid rather than returning "", which
		// would read as "no group" to every caller.
		return ConsumerGroupIDPrefix + SubscriberIDPrefix + SubscriberGroupTerminator + SubscriberDefaultGroupLeaf
	}

	return group
}
