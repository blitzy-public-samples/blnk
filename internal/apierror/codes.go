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
// Each code maps to exactly one default HTTP status (see statusByCode).
//
// The six legacy codes in apierror.go (NOT_FOUND, CONFLICT, ...) remain valid
// for internal construction — they are normalized to their GEN_* equivalents
// at the response boundary via Normalize.
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

	// EVENT — event streaming, outbox & dead-letter.
	// ErrKafkaUnavailable intentionally carries the EVENT_ prefix: keeping it in
	// this family means the catalog gains exactly two new families rather than a
	// third one holding a single code. It resolves to 503 and not to the 500 used
	// by the other *_FAILED codes because an unreachable broker is a retryable
	// upstream condition, not a defect in this service — do not "correct" it.
	ErrEventNotFound        ErrorCode = "EVENT_NOT_FOUND"
	ErrEventNotDeadLettered ErrorCode = "EVENT_NOT_DEAD_LETTERED"
	ErrEventReplayFailed    ErrorCode = "EVENT_REPLAY_FAILED"
	ErrKafkaUnavailable     ErrorCode = "EVENT_KAFKA_UNAVAILABLE"

	// SUBSCRIBER — Kafka subscriber registry & credentials.
	// ErrSubscriberProvisioningFailed also resolves to 503 rather than 500 for the
	// same reason: provisioning depends on the Kafka admin API, so a failure is
	// retryable by the caller rather than a server defect.
	ErrSubscriberNotFound           ErrorCode = "SUBSCRIBER_NOT_FOUND"
	ErrSubscriberProvisioningFailed ErrorCode = "SUBSCRIBER_PROVISIONING_FAILED"

	// ErrSubscriberIsolationUnenforceable is the refusal to issue a credential whose
	// record claims an access boundary nothing can enforce.
	//
	// It resolves to 409 CONFLICT rather than 400 or 422, and the distinction carries
	// meaning the caller needs: the REQUEST is well formed and would be honoured
	// against a different registry row. What conflicts is the STATE — the subscriber
	// records a partition-key prefix, and Kafka ACLs are topic-level, so any credential
	// issued would grant strictly wider access than the row describes. 409 is the
	// status that says "fix the resource, then repeat this request unchanged", which is
	// exactly the remedy: clear the prefix, or narrow authorized_topics until
	// topic-level scope is the isolation actually required.
	ErrSubscriberIsolationUnenforceable ErrorCode = "SUBSCRIBER_ISOLATION_UNENFORCEABLE"

	// ErrSubscriberBrokersNotConfigured is the refusal to issue a credential when no
	// SUBSCRIBER-FACING broker list is configured.
	//
	// KAFKA_BROKERS is what Blnk dials, and inside a deployment that address is internal:
	// a compose service name, a ClusterIP. Reporting it to an external subscriber returns
	// an endpoint that does not resolve for them — and Kafka makes it worse, because a
	// broker answers each client with the advertised address of the listener the
	// connection arrived on, so even a reachable bootstrap redirects to internal names.
	// KAFKA_SUBSCRIBER_BROKERS is the externally advertised list, and only an operator
	// knows it.
	//
	// It resolves to 503 SERVICE UNAVAILABLE rather than 500, for the same reason
	// ErrSubscriberProvisioningFailed does: nothing about the request is wrong and no
	// server defect is implied. A dependency of issuance is not configured, the caller
	// can do nothing but retry once it is, and the alternative — succeeding with an
	// unusable endpoint — turns a clear refusal into a subscriber-side connection
	// timeout diagnosed days later.
	ErrSubscriberBrokersNotConfigured ErrorCode = "SUBSCRIBER_BROKERS_NOT_CONFIGURED"

	// ErrSubscriberDeprovisioning is the refusal to act on a subscriber whose
	// broker-side access is being torn down.
	//
	// Also 409, for the same reason and with a different remedy: complete or abandon
	// the deregistration first. Issuing a credential against a row in this state would
	// provision a boundary that the deregistration it is racing is halfway through
	// deleting, and the result would be indistinguishable from a successful issuance
	// to everything except the broker.
	ErrSubscriberDeprovisioning ErrorCode = "SUBSCRIBER_DEPROVISIONING"

	// ErrSubscriberGrantEmpty is the refusal to mint a credential for a subscriber
	// whose authorized-topic list is empty.
	//
	// An empty list is a LEGITIMATE registry state — it is the fail-closed default of a
	// newly registered subscriber, and setting it back to empty is the only way to
	// express "authorised for nothing" on a row that currently holds topics. What it is
	// not is something to issue a credential against: the SASL principal would
	// authenticate, hold no topic binding at all, and read nothing. The response would
	// nonetheless carry a secret, a broker endpoint and a consumer group, which is
	// indistinguishable at a glance from working access, so whoever received it would
	// hand it to a consumer and diagnose the resulting silence as a delivery fault.
	//
	// 409 rather than 400 or 422 for the same reason as the two refusals above: the
	// request carries no body and nothing about it is malformed. It is the STATE of the
	// resource that has to change — grant at least one topic — after which the identical
	// request succeeds.
	ErrSubscriberGrantEmpty ErrorCode = "SUBSCRIBER_GRANT_EMPTY"

	// ErrSubscriberProvisioningTimeout is a credential issuance that ran out of time
	// rather than one that failed.
	//
	// Issuance runs under a single deadline (requirement R-7's five-second budget), and
	// the registry reads it makes before the broker is touched — claiming the
	// provisioning fence, then reading the row — share it. Those reads used to report a
	// spent budget or a cancelled caller through the repository's generic internal-server
	// code, so a pure timeout arrived as HTTP 500: a client cannot tell that from a
	// defect in this service, and the correct reaction to the two is opposite. A defect
	// must not be retried into a loop; a timeout should be retried, and safely can be,
	// because nothing has been written when it happens on these paths.
	//
	// 504 rather than 503, and the distinction is the dependency: 503 (as used by
	// ErrKafkaUnavailable and ErrSubscriberProvisioningFailed) says a dependency is
	// unreachable or unconfigured, while 504 says one was reached and did not answer
	// inside the time allowed. A caller cancelling its own request resolves here too —
	// it is not a server fault either, and the status a caller that has gone away never
	// reads matters far less than the typed code its retry logic and this deployment's
	// logs discriminate on.
	ErrSubscriberProvisioningTimeout ErrorCode = "SUBSCRIBER_PROVISIONING_TIMEOUT"
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

	// SUBSCRIBER — 503 on provisioning failure is deliberate for the same reason.
	ErrSubscriberNotFound:           http.StatusNotFound,
	ErrSubscriberProvisioningFailed: http.StatusServiceUnavailable,
	// Also 503: a dependency of issuance is unconfigured, not a malformed request.
	ErrSubscriberBrokersNotConfigured: http.StatusServiceUnavailable,

	// 409 for the three refusals below: the request is well formed and it is the
	// registry row's state that has to change before it can be honoured.
	ErrSubscriberIsolationUnenforceable: http.StatusConflict,
	ErrSubscriberDeprovisioning:         http.StatusConflict,
	ErrSubscriberGrantEmpty:             http.StatusConflict,
	// 504 rather than 503: the dependency answered too slowly, or the caller went
	// away, and neither is a defect in this service. Without this entry a spent
	// issuance budget resolves to the unknown-code 500 default.
	ErrSubscriberProvisioningTimeout: http.StatusGatewayTimeout,

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
// Canonical codes pass through unchanged.
func Normalize(code ErrorCode) ErrorCode {
	if canonical, ok := legacyToCanonical[code]; ok {
		return canonical
	}
	return code
}

// StatusForCode returns the default HTTP status for an error code.
// Unknown codes default to 500.
func StatusForCode(code ErrorCode) int {
	if status, ok := statusByCode[code]; ok {
		return status
	}
	return http.StatusInternalServerError
}
