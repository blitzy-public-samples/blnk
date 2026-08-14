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

package apierror

import "net/http"

// Domain-prefixed error codes. These are the canonical, client-facing codes
// returned in the `error_detail.code` field of every error response.
const (
	// GEN — generic / cross-cutting
	ErrGenMalformedRequest ErrorCode = "GEN_MALFORMED_REQUEST"
	ErrGenValidation       ErrorCode = "GEN_VALIDATION_ERROR"
	ErrGenMissingParameter ErrorCode = "GEN_MISSING_PARAMETER"
	ErrGenBadRequest       ErrorCode = "GEN_BAD_REQUEST"
	ErrGenNotFound         ErrorCode = "GEN_NOT_FOUND"
	ErrGenConflict         ErrorCode = "GEN_CONFLICT"
	ErrGenGone             ErrorCode = "GEN_GONE" // a deprecated surface past its configured sunset
	ErrGenResourceLocked   ErrorCode = "GEN_RESOURCE_LOCKED"
	ErrGenPayloadTooLarge  ErrorCode = "GEN_PAYLOAD_TOO_LARGE"
	ErrGenRateLimited      ErrorCode = "GEN_RATE_LIMITED"
	ErrGenInternal         ErrorCode = "GEN_INTERNAL"

	// AUTH — authentication / authorization
	ErrAuthMissingAPIKey           ErrorCode = "AUTH_MISSING_API_KEY"
	ErrAuthInvalidAPIKey           ErrorCode = "AUTH_INVALID_API_KEY"
	ErrAuthExpiredAPIKey           ErrorCode = "AUTH_EXPIRED_API_KEY"
	ErrAuthMissingPrincipal        ErrorCode = "AUTH_MISSING_PRINCIPAL"
	ErrAuthInsufficientPermissions ErrorCode = "AUTH_INSUFFICIENT_PERMISSIONS"
	ErrAuthUnknownResource         ErrorCode = "AUTH_UNKNOWN_RESOURCE"
	ErrAuthMasterKeyRequired       ErrorCode = "AUTH_MASTER_KEY_REQUIRED"
	ErrAuthCrossOwnerAccess        ErrorCode = "AUTH_CROSS_OWNER_ACCESS"
	ErrAuthScopeEscalation         ErrorCode = "AUTH_SCOPE_ESCALATION"
	ErrAuthMetricsTokenRequired    ErrorCode = "AUTH_METRICS_TOKEN_REQUIRED"
	ErrAuthInvalidBearerToken      ErrorCode = "AUTH_INVALID_BEARER_TOKEN"
	ErrAuthMetricsDisabled         ErrorCode = "AUTH_METRICS_DISABLED"

	// APIKEY — API-key resource management
	ErrAPIKeyNotFound      ErrorCode = "APIKEY_NOT_FOUND"
	ErrAPIKeyOwnerRequired ErrorCode = "APIKEY_OWNER_REQUIRED"
	ErrAPIKeyInvalid       ErrorCode = "APIKEY_INVALID"

	// TXN — transactions
	ErrTxnNotFound             ErrorCode = "TXN_NOT_FOUND"
	ErrTxnInsufficientFunds    ErrorCode = "TXN_INSUFFICIENT_FUNDS"
	ErrTxnInvalidAmount        ErrorCode = "TXN_INVALID_AMOUNT"
	ErrTxnPrecisionNotInteger  ErrorCode = "TXN_PRECISION_NOT_INTEGER"
	ErrTxnInvalidDistribution  ErrorCode = "TXN_INVALID_DISTRIBUTION"
	ErrTxnDuplicateReference   ErrorCode = "TXN_DUPLICATE_REFERENCE"
	ErrTxnNotInflight          ErrorCode = "TXN_NOT_INFLIGHT"
	ErrTxnAlreadyCommitted     ErrorCode = "TXN_ALREADY_COMMITTED"
	ErrTxnAlreadyVoided        ErrorCode = "TXN_ALREADY_VOIDED"
	ErrTxnCommitAmountExceeded ErrorCode = "TXN_COMMIT_AMOUNT_EXCEEDED"
	ErrTxnInvalidStatusAction  ErrorCode = "TXN_INVALID_STATUS_ACTION"
	ErrTxnBulkEmpty            ErrorCode = "TXN_BULK_EMPTY"
	ErrTxnBulkLimitExceeded    ErrorCode = "TXN_BULK_LIMIT_EXCEEDED"
	ErrTxnValidation           ErrorCode = "TXN_VALIDATION_ERROR"

	// BAL — balances & monitors
	ErrBalNotFound         ErrorCode = "BAL_NOT_FOUND"
	ErrBalHistoryNotFound  ErrorCode = "BAL_HISTORY_NOT_FOUND"
	ErrBalInvalidTimestamp ErrorCode = "BAL_INVALID_TIMESTAMP"
	ErrBalValidation       ErrorCode = "BAL_VALIDATION_ERROR"
	ErrBalMonitorNotFound  ErrorCode = "BAL_MONITOR_NOT_FOUND"

	// LGR — ledgers
	ErrLgrNotFound  ErrorCode = "LGR_NOT_FOUND"
	ErrLgrDuplicate ErrorCode = "LGR_DUPLICATE"

	// ACC — accounts
	ErrAccNotFound         ErrorCode = "ACC_NOT_FOUND"
	ErrAccDuplicate        ErrorCode = "ACC_DUPLICATE"
	ErrAccGenerationFailed ErrorCode = "ACC_GENERATION_FAILED"

	// IDT — identities & tokenization
	ErrIdtNotFound              ErrorCode = "IDT_NOT_FOUND"
	ErrIdtValidation            ErrorCode = "IDT_VALIDATION_ERROR"
	ErrIdtFieldNotTokenizable   ErrorCode = "IDT_FIELD_NOT_TOKENIZABLE"
	ErrIdtFieldAlreadyTokenized ErrorCode = "IDT_FIELD_ALREADY_TOKENIZED"
	ErrIdtFieldNotTokenized     ErrorCode = "IDT_FIELD_NOT_TOKENIZED"
	ErrIdtFieldNotFound         ErrorCode = "IDT_FIELD_NOT_FOUND"
	ErrIdtTokenizationDisabled  ErrorCode = "IDT_TOKENIZATION_DISABLED"

	// RECON — reconciliation
	ErrReconNotFound               ErrorCode = "RECON_NOT_FOUND"
	ErrReconRuleNotFound           ErrorCode = "RECON_RULE_NOT_FOUND"
	ErrReconUploadFailed           ErrorCode = "RECON_UPLOAD_FAILED"
	ErrReconUploadProcessingFailed ErrorCode = "RECON_UPLOAD_PROCESSING_FAILED"
	ErrReconUploadURLInvalid       ErrorCode = "RECON_UPLOAD_URL_INVALID"
	ErrReconUploadHostNotAllowed   ErrorCode = "RECON_UPLOAD_HOST_NOT_ALLOWED"
	ErrReconRuleInvalid            ErrorCode = "RECON_RULE_INVALID"
	ErrReconMatchingRulesRequired  ErrorCode = "RECON_MATCHING_RULES_REQUIRED"
	ErrReconExternalTxnsRequired   ErrorCode = "RECON_EXTERNAL_TXNS_REQUIRED"
	ErrReconStartFailed            ErrorCode = "RECON_START_FAILED"

	// META — entity metadata
	ErrMetaEntityNotFound    ErrorCode = "META_ENTITY_NOT_FOUND"
	ErrMetaUnsupportedEntity ErrorCode = "META_UNSUPPORTED_ENTITY"
	ErrMetaInvalidEntityID   ErrorCode = "META_INVALID_ENTITY_ID"

	// HOOK — webhook management
	ErrHookNotFound        ErrorCode = "HOOK_NOT_FOUND"
	ErrHookInvalid         ErrorCode = "HOOK_INVALID"
	ErrHookOperationFailed ErrorCode = "HOOK_OPERATION_FAILED"

	// SRCH — search & reindex
	ErrSrchQueryInvalid      ErrorCode = "SRCH_QUERY_INVALID"
	ErrSrchFailed            ErrorCode = "SRCH_FAILED"
	ErrSrchReindexInProgress ErrorCode = "SRCH_REINDEX_IN_PROGRESS"
	ErrSrchReindexNotStarted ErrorCode = "SRCH_REINDEX_NOT_STARTED"

	// ADMIN — administrative operations
	ErrAdminBackupFailed ErrorCode = "ADMIN_BACKUP_FAILED"

	// EVENT — event streaming, outbox & dead-letter. ErrKafkaUnavailable intentionally
	// carries the EVENT_ prefix: keeping it in this family means the catalog gains exactly
	// two new families rather than a third one holding a single code. It resolves to 503
	// and not to the 500 used by the other *_FAILED codes because an unreachable broker is
	// a retryable upstream condition, not a defect in this service — do not "correct" it.
	ErrEventNotFound        ErrorCode = "EVENT_NOT_FOUND"
	ErrEventNotDeadLettered ErrorCode = "EVENT_NOT_DEAD_LETTERED"
	ErrEventReplayFailed    ErrorCode = "EVENT_REPLAY_FAILED"
	ErrKafkaUnavailable     ErrorCode = "EVENT_KAFKA_UNAVAILABLE"

	// THERE IS NO SEPARATE TIMEOUT CODE IN THIS FAMILY, and the absence is the contract
	// rather than an oversight. A replay abandoned before the broker acknowledged it — the
	// caller went away, or the request's deadline expired — is answered with
	// EVENT_REPLAY_FAILED, and the accompanying message is what says the event is still
	// dead-lettered and the request may simply be repeated. An EVENT_REPLAY_TIMEOUT value
	// mapped to 504 was added here once and withdrawn: this family's public surface is the
	// approved set of codes above, and widening it is a public API change that belongs to
	// a separately approved plan, not to the implementation of one.
	ErrEventKeyUnresolvable ErrorCode = "EVENT_KEY_UNRESOLVABLE"

	// SUBSCRIBER — Kafka subscriber registry & credentials.
	ErrSubscriberNotFound           ErrorCode = "SUBSCRIBER_NOT_FOUND"
	ErrSubscriberProvisioningFailed ErrorCode = "SUBSCRIBER_PROVISIONING_FAILED"

	// ErrSubscriberKeyScopeUnenforced is the refusal to issue a credential to a subscriber
	// whose recorded partition_key_prefix nothing would enforce.
	ErrSubscriberKeyScopeUnenforced ErrorCode = "SUBSCRIBER_KEY_SCOPE_UNENFORCED"

	// ErrSubscriberKeyScopeRequired is the MIRROR of the code above: the refusal to issue
	// a credential to a subscriber that records NO partition_key_prefix, in a deployment
	// that has declared its subscriber access to be key-scoped.
	ErrSubscriberKeyScopeRequired ErrorCode = "SUBSCRIBER_KEY_SCOPE_REQUIRED"

	// ErrSubscriberSharedTopicAccessUnacknowledged is the refusal to mint a whole-topic
	// credential in a production deployment that has declared nothing about its access
	// model.
	ErrSubscriberSharedTopicAccessUnacknowledged ErrorCode = "SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED"

	// ErrSubscriberKeyScopeUnattested is the refusal to mint a key-scoped credential when
	// the declared key-authorising component did not ATTEST the boundary it is supposed to
	// keep.
	ErrSubscriberKeyScopeUnattested ErrorCode = "SUBSCRIBER_KEY_SCOPE_UNATTESTED"

	// ErrSubscriberAccessExceedsAuthorization is the refusal to issue a credential to a
	// principal the broker would grant MORE access than the registry records.
	ErrSubscriberAccessExceedsAuthorization ErrorCode = "SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION"

	// ErrSubscriberBrokersNotConfigured is the refusal to issue a credential when NO Kafka
	// broker list is configured at all.
	ErrSubscriberBrokersNotConfigured ErrorCode = "SUBSCRIBER_BROKERS_NOT_CONFIGURED"

	// ErrSubscriberDeprovisioning is the refusal to act on a subscriber whose broker-side
	// access is being torn down.
	ErrSubscriberDeprovisioning ErrorCode = "SUBSCRIBER_DEPROVISIONING"

	// ErrSubscriberGrantEmpty is the refusal to mint a credential for a subscriber whose
	// authorized-topic list is empty.
	ErrSubscriberGrantEmpty ErrorCode = "SUBSCRIBER_GRANT_EMPTY"

	// ErrSubscriberInsecureTransport is the refusal to return a one-time SASL password
	// over a channel this deployment has not declared confidential.
	ErrSubscriberInsecureTransport ErrorCode = "SUBSCRIBER_INSECURE_TRANSPORT"

	// THERE IS NO SEPARATE TIMEOUT CODE IN THIS FAMILY EITHER, for the same reason there
	// is none in the EVENT_ family. A credential issuance that runs out of the requirement
	// five-second budget — or whose caller goes away — is answered with
	// SUBSCRIBER_PROVISIONING_FAILED and its 503, which is the retryable answer this
	// contract approves for the condition. Every layer of the issuance path reports that
	// one code, so a client's retry policy does not depend on which dependency noticed the
	// expiry first, and what the broker may be left holding is carried in the error
	// DETAIL, which is the only place a status code could never carry it.
)

// statusByCode is the single source of truth for the default HTTP status of
// every error code, including the six legacy codes.
var statusByCode = map[ErrorCode]int{
	ErrGenMalformedRequest: http.StatusBadRequest,
	ErrGenValidation:       http.StatusBadRequest,
	ErrGenMissingParameter: http.StatusBadRequest,
	ErrGenBadRequest:       http.StatusBadRequest,
	ErrGenNotFound:         http.StatusNotFound,
	ErrGenConflict:         http.StatusConflict,
	ErrGenGone:             http.StatusGone,
	ErrGenResourceLocked:   http.StatusLocked,
	ErrGenPayloadTooLarge:  http.StatusRequestEntityTooLarge,
	ErrGenRateLimited:      http.StatusTooManyRequests,
	ErrGenInternal:         http.StatusInternalServerError,

	ErrAuthMissingAPIKey:           http.StatusUnauthorized,
	ErrAuthInvalidAPIKey:           http.StatusUnauthorized,
	ErrAuthExpiredAPIKey:           http.StatusUnauthorized,
	ErrAuthMissingPrincipal:        http.StatusUnauthorized,
	ErrAuthInsufficientPermissions: http.StatusForbidden,
	ErrAuthUnknownResource:         http.StatusForbidden,
	ErrAuthMasterKeyRequired:       http.StatusForbidden,
	ErrAuthCrossOwnerAccess:        http.StatusForbidden,
	ErrAuthScopeEscalation:         http.StatusForbidden,
	ErrAuthMetricsTokenRequired:    http.StatusUnauthorized,
	ErrAuthInvalidBearerToken:      http.StatusUnauthorized,
	ErrAuthMetricsDisabled:         http.StatusForbidden,

	ErrAPIKeyNotFound:      http.StatusNotFound,
	ErrAPIKeyOwnerRequired: http.StatusBadRequest,
	ErrAPIKeyInvalid:       http.StatusBadRequest,

	ErrTxnNotFound:             http.StatusNotFound,
	ErrTxnInsufficientFunds:    http.StatusBadRequest,
	ErrTxnInvalidAmount:        http.StatusBadRequest,
	ErrTxnPrecisionNotInteger:  http.StatusBadRequest,
	ErrTxnInvalidDistribution:  http.StatusBadRequest,
	ErrTxnDuplicateReference:   http.StatusConflict,
	ErrTxnNotInflight:          http.StatusBadRequest,
	ErrTxnAlreadyCommitted:     http.StatusConflict,
	ErrTxnAlreadyVoided:        http.StatusConflict,
	ErrTxnCommitAmountExceeded: http.StatusBadRequest,
	ErrTxnInvalidStatusAction:  http.StatusBadRequest,
	ErrTxnBulkEmpty:            http.StatusBadRequest,
	ErrTxnBulkLimitExceeded:    http.StatusBadRequest,
	ErrTxnValidation:           http.StatusBadRequest,

	ErrBalNotFound:         http.StatusNotFound,
	ErrBalHistoryNotFound:  http.StatusNotFound,
	ErrBalInvalidTimestamp: http.StatusBadRequest,
	ErrBalValidation:       http.StatusBadRequest,
	ErrBalMonitorNotFound:  http.StatusNotFound,

	ErrLgrNotFound:  http.StatusNotFound,
	ErrLgrDuplicate: http.StatusConflict,

	ErrAccNotFound:         http.StatusNotFound,
	ErrAccDuplicate:        http.StatusConflict,
	ErrAccGenerationFailed: http.StatusInternalServerError,

	ErrIdtNotFound:              http.StatusNotFound,
	ErrIdtValidation:            http.StatusBadRequest,
	ErrIdtFieldNotTokenizable:   http.StatusBadRequest,
	ErrIdtFieldAlreadyTokenized: http.StatusConflict,
	ErrIdtFieldNotTokenized:     http.StatusBadRequest,
	ErrIdtFieldNotFound:         http.StatusBadRequest,
	ErrIdtTokenizationDisabled:  http.StatusForbidden,

	ErrReconNotFound:               http.StatusNotFound,
	ErrReconRuleNotFound:           http.StatusNotFound,
	ErrReconUploadFailed:           http.StatusBadRequest,
	ErrReconUploadProcessingFailed: http.StatusInternalServerError,
	ErrReconUploadURLInvalid:       http.StatusBadRequest,
	ErrReconUploadHostNotAllowed:   http.StatusBadRequest,
	ErrReconRuleInvalid:            http.StatusBadRequest,
	ErrReconMatchingRulesRequired:  http.StatusBadRequest,
	ErrReconExternalTxnsRequired:   http.StatusBadRequest,
	ErrReconStartFailed:            http.StatusInternalServerError,

	ErrMetaEntityNotFound:    http.StatusNotFound,
	ErrMetaUnsupportedEntity: http.StatusBadRequest,
	ErrMetaInvalidEntityID:   http.StatusBadRequest,

	ErrHookNotFound:        http.StatusNotFound,
	ErrHookInvalid:         http.StatusBadRequest,
	ErrHookOperationFailed: http.StatusInternalServerError,

	ErrSrchQueryInvalid:      http.StatusBadRequest,
	ErrSrchFailed:            http.StatusInternalServerError,
	ErrSrchReindexInProgress: http.StatusConflict,
	ErrSrchReindexNotStarted: http.StatusNotFound,

	ErrAdminBackupFailed: http.StatusInternalServerError,

	// EVENT — 503 for the broker being unreachable is deliberate (retryable
	// upstream condition); every other *_FAILED code in this map is 500.
	ErrEventNotFound:        http.StatusNotFound,
	ErrEventNotDeadLettered: http.StatusConflict,
	ErrEventReplayFailed:    http.StatusInternalServerError,
	ErrKafkaUnavailable:     http.StatusServiceUnavailable,
	// 500: an unkeyable event is a producer defect inside this service, not a caller error.
	ErrEventKeyUnresolvable: http.StatusInternalServerError,

	// SUBSCRIBER — 503 on provisioning failure is deliberate for the same reason.
	ErrSubscriberNotFound:           http.StatusNotFound,
	ErrSubscriberProvisioningFailed: http.StatusServiceUnavailable,
	// Also 503: a dependency of issuance is unconfigured, not a malformed request.
	ErrSubscriberBrokersNotConfigured: http.StatusServiceUnavailable,

	// 409 for the three refusals below: the request is well formed and it is the
	// registry row's state that has to change before it can be honoured.
	ErrSubscriberDeprovisioning: http.StatusConflict,
	ErrSubscriberGrantEmpty:     http.StatusConflict,
	// Also 409: the remedy is a state change — declare a key-authorising component in
	// front of the brokers (KAFKA_KEY_SCOPE_ENFORCEMENT), or drop the prefix — and not a
	// retry. Kafka's authorizer has no message-key dimension, so no retry narrows what a
	// credential would carry.
	ErrSubscriberKeyScopeUnenforced: http.StatusConflict,
	// Also 409, and the mirror of the entry above: the deployment declared a key-scoped access
	// model and this row records no key scope, so the remedy is a state change — record the
	// prefix, or acknowledge whole-topic access — and never a retry.
	ErrSubscriberKeyScopeRequired: http.StatusConflict,
	// Also 409: the deployment has declared nothing about its subscriber access model and
	// this is a production posture, so the configuration is what changes. Not 403, which
	// would say the CALLER is not permitted; the caller holds the master key and the
	// deployment is what has not decided.
	ErrSubscriberSharedTopicAccessUnacknowledged: http.StatusConflict,
	// Also 409, and NOT 503 even when the attestation timed out: the declared enforcement
	// point is part of the deployment's state, so a 503 would send an operator to a Kafka
	// that never stopped answering. The detail's `retryable` flag separates a transport
	// failure from a prefix mismatch.
	ErrSubscriberKeyScopeUnattested: http.StatusConflict,
	// Also 409, and NOT the 503 of SUBSCRIBER_PROVISIONING_FAILED: the broker answered and
	// the boundary it would enforce is wider than the row records, which a retry cannot
	// change. Without this entry the refusal would resolve to the unknown-code 500 — which
	// is exactly what it did: the code was declared and returned from
	// IssueSubscriberCredential while its row here was absent, so a deliberate 409
	// judgement reached the caller as a server defect.
	ErrSubscriberAccessExceedsAuthorization: http.StatusConflict,
	// 403: the request is well formed and the caller is authorised; the server is refusing
	// to put a one-time secret on a channel it cannot establish as confidential. Without
	// this entry the refusal would resolve to the unknown-code 500 default and read as a
	// defect in this service.
	ErrSubscriberInsecureTransport: http.StatusForbidden,

	// Legacy codes — same statuses MapErrorToHTTPStatus implied, with the
	// BAD_REQUEST omission fixed (it previously fell through to 500).
	ErrNotFound:       http.StatusNotFound,
	ErrConflict:       http.StatusConflict,
	ErrBadRequest:     http.StatusBadRequest,
	ErrInvalidInput:   http.StatusBadRequest,
	ErrInternalServer: http.StatusInternalServerError,
	ErrRateLimited:    http.StatusTooManyRequests,
}

// legacyToCanonical maps the pre-catalog generic codes to their canonical
// GEN_* replacements. Internal layers (notably database/) keep constructing
// errors with the legacy codes; responses always surface canonical ones.
var legacyToCanonical = map[ErrorCode]ErrorCode{
	ErrNotFound:       ErrGenNotFound,
	ErrConflict:       ErrGenConflict,
	ErrBadRequest:     ErrGenBadRequest,
	ErrInvalidInput:   ErrGenValidation,
	ErrInternalServer: ErrGenInternal,
	ErrRateLimited:    ErrGenRateLimited,
}

// Normalize converts a legacy error code to its canonical equivalent.
func Normalize(code ErrorCode) ErrorCode {
	if canonical, ok := legacyToCanonical[code]; ok {
		return canonical
	}
	return code
}

// StatusForCode returns the default HTTP status for an error code.
func StatusForCode(code ErrorCode) int {
	if status, ok := statusByCode[code]; ok {
		return status
	}
	return http.StatusInternalServerError
}
