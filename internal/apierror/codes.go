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
	//
	// EVENT_ALREADY_RESOLVED WAS RETIRED FROM THIS FAMILY. It answered a second write on
	// the dead-letter surface — an operator recording that a dead-lettered event needed no
	// further action — and both the write and its route are gone: they took the management
	// surface past the approved route count, and a resolved row's broker-acknowledged replay
	// could not then be recorded. Retention now spares every dead-lettered row and a replay
	// is what makes one a purgeable receipt, so there is no second write and no code for its
	// conflict. Do not reintroduce the value without reintroducing that whole design.
	ErrEventNotFound        ErrorCode = "EVENT_NOT_FOUND"
	ErrEventNotDeadLettered ErrorCode = "EVENT_NOT_DEAD_LETTERED"
	ErrEventReplayFailed    ErrorCode = "EVENT_REPLAY_FAILED"
	ErrKafkaUnavailable     ErrorCode = "EVENT_KAFKA_UNAVAILABLE"

	// ErrEventReplayTimeout is a replay that was ABANDONED rather than one that failed:
	// the caller went away, or the request's deadline expired, before the re-publish
	// could be acknowledged.
	//
	// TAXONOMY-01: it exists because the other two answers are both wrong for that
	// case, and one of them was being given. A cancelled or expired publish used to
	// resolve to EVENT_KAFKA_UNAVAILABLE, whose 503 and whose message assert that the
	// BROKER is unavailable and that the request should be repeated once it recovers —
	// sending an operator to look at a healthy Kafka for a deadline that expired on this
	// side of the connection. EVENT_REPLAY_FAILED and its 500 would be the opposite
	// error: nothing in Blnk is broken either, and the event is intact.
	//
	// 504 rather than 503, for the same reason ErrSubscriberProvisioningTimeout is: 503
	// says a dependency is unreachable or unconfigured, 504 says the work did not
	// complete inside the time allowed. The event stays dead-lettered, so the request is
	// safe to repeat.
	ErrEventReplayTimeout ErrorCode = "EVENT_REPLAY_TIMEOUT"
	// ErrEventKeyUnresolvable is the refusal to capture an event that can be assigned no
	// Kafka message key.
	//
	// It resolves to 500 rather than 400 because no request body can cause it. The key is
	// derived from the event name and the payload object a PRODUCER inside this service
	// passes, and every real producer passes at least a name; reaching this code means an
	// event with no type and a payload naming nothing reached PrepareEventOutbox, which is
	// a defect in this service. Requirement R-6 makes the key load-bearing — it selects the
	// partition and therefore the ordering — so an unkeyable event is refused instead of
	// being admitted under a sentinel key that orders it against nothing.
	ErrEventKeyUnresolvable ErrorCode = "EVENT_KEY_UNRESOLVABLE"

	// SUBSCRIBER — Kafka subscriber registry & credentials.
	// ErrSubscriberProvisioningFailed also resolves to 503 rather than 500 for the
	// same reason: provisioning depends on the Kafka admin API, so a failure is
	// retryable by the caller rather than a server defect.
	ErrSubscriberNotFound           ErrorCode = "SUBSCRIBER_NOT_FOUND"
	ErrSubscriberProvisioningFailed ErrorCode = "SUBSCRIBER_PROVISIONING_FAILED"

	// ErrSubscriberKeyScopeUnenforced is the refusal to issue a credential to a subscriber
	// whose recorded partition_key_prefix nothing would enforce.
	//
	// 409, alongside the other state refusals, and for their reason: the request is well
	// formed and it is the deployment's or the row's state that has to change before it can be
	// honoured — deploy and declare a key-authorising gateway, or clear the prefix and narrow
	// authorized_topics, which the broker does enforce. It is emphatically not the 503 of
	// SUBSCRIBER_PROVISIONING_FAILED, which advertises a retryable upstream condition; nothing
	// here is retryable, and repeating the request unchanged will be refused identically.
	//
	// It is a separate code from ErrSubscriberGrantEmpty and ErrSubscriberAccessExceedsAuthorization
	// because the remedy is somewhere else again: not the grant, not a stray ACL, but whether
	// any component in the deployment evaluates record keys at all.
	//
	// # It is returned from BOTH directions of the same state
	//
	// Issuing a credential for a row that records a prefix, and recording a prefix on a row
	// that already holds one. The second matters as much as the first: without it the guard is
	// bypassed by "issue, then record" — and because Kafka stores one SCRAM credential per
	// principal, a row in that state could never rotate its secret, since re-issuance is
	// issuance. The message names both remedies, and after either the identical request
	// succeeds.
	//
	// This code REPLACED SUBSCRIBER_ISOLATION_UNENFORCEABLE, which described the same refusal
	// under a second name while the two disagreed about which one the runtime returned. One
	// refusal, one code, named after the configuration key an operator changes to lift it.
	ErrSubscriberKeyScopeUnenforced ErrorCode = "SUBSCRIBER_KEY_SCOPE_UNENFORCED"

	// ErrSubscriberAccessExceedsAuthorization is the refusal to issue a credential to a
	// principal the broker would grant MORE access than the registry records.
	//
	// It shares its 409 with the other state refusals, and it is a separate code because the
	// remedy is somewhere else entirely. Every other 409 here names something about the
	// registry ROW that has to change, and the fix is to edit the row. Here the row is
	// perfectly expressible and already enforced — what exceeds it is an ACL
	// binding on the principal that Blnk did not create and will not delete, so the fix is at
	// the BROKER. A caller told only "conflict" would edit the subscriber and watch the
	// identical request fail again.
	//
	// It is emphatically not SUBSCRIBER_PROVISIONING_FAILED, whose 503 advertises a retryable
	// upstream condition. Nothing here is retryable: the broker answered, provisioning
	// completed, and the refusal is a deliberate judgement about the resulting boundary. 409
	// carries the right instruction — change the state, then repeat this request unchanged.
	ErrSubscriberAccessExceedsAuthorization ErrorCode = "SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION"

	// ErrSubscriberBrokersNotConfigured is the refusal to issue a credential when NO Kafka
	// broker list is configured at all.
	//
	// KAFKA_SUBSCRIBER_BROKERS is the externally advertised list and is an OPTIONAL
	// OVERRIDE: without it, issuance reports KAFKA_BROKERS and logs that it did, because
	// the eight variables the configuration contract requires must be enough to run every
	// documented endpoint. Reporting the internal list is right when subscribers run inside
	// the deployment and wrong when they do not — and Kafka compounds a wrong answer,
	// because a broker replies to each client with the advertised address of the listener
	// the connection arrived on — so the fallback is warned about rather than silent.
	//
	// This code is therefore reached only when neither list is set, which means Kafka is
	// unconfigured and no credential could work whatever endpoint was reported.
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

	// ErrSubscriberInsecureTransport is the refusal to return a one-time SASL password
	// over a channel this deployment has not declared confidential.
	//
	// Credential issuance is the only endpoint in Blnk that returns a secret in a
	// response body, and it returns it exactly once — so the request that carries it is
	// the single opportunity to disclose it to anybody watching the wire. Blnk's own
	// listener is plaintext unless BLNK_SERVER_SSL is set, and in a Kubernetes
	// deployment TLS is normally terminated at an ingress with a plaintext hop to the
	// pod, so "the caller used https" is a claim the process cannot verify from the
	// request alone. Answering anyway is what turns a correctly authenticated,
	// correctly authorised request into a credential leak.
	//
	// The endpoint therefore requires one of three confidential channels, each of which
	// the process can establish rather than assume: TLS terminated in-process, a
	// deployment-declared proxy boundary (BLNK_SERVER_TRUST_FORWARDED_PROTO) reporting
	// X-Forwarded-Proto: https, or — only where the deployment has declared itself a
	// local-development host with BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE — a
	// loopback peer. The loopback case needs that declaration because a same-host reverse
	// proxy can forward a public plaintext request over 127.0.0.1, so the peer establishes
	// only the last hop and the process cannot see the one before it.
	// See api.ensureCredentialTransportConfidential, which is where each is checked.
	//
	// 403 rather than 400 or 426: the request is well formed and the caller is
	// authenticated and authorised, and the server is refusing to fulfil it — which is
	// what 403 means. It shares that status with ErrAuthMasterKeyRequired, so a test
	// distinguishing "the transport was refused" from "the caller was refused" must
	// assert on error_detail.code and never on the status alone. 426 was considered and
	// rejected: it mandates an Upgrade header describing an in-band protocol switch,
	// which is not what a deployment behind an ingress needs to do about this.
	ErrSubscriberInsecureTransport ErrorCode = "SUBSCRIBER_INSECURE_TRANSPORT"

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
	// 504: the replay was abandoned by a cancelled caller or a spent deadline. Neither
	// the broker (503) nor this service (500) is the culprit, and without this entry the
	// distinction would resolve to the unknown-code 500 default and read as a defect.
	ErrEventReplayTimeout: http.StatusGatewayTimeout,
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
	// Also 409: the remedy is a state change — declare a key-authorising component in front of
	// the brokers (KAFKA_KEY_SCOPE_ENFORCEMENT), or drop the prefix — and not a retry. Kafka's
	// authorizer has no message-key dimension, so no retry narrows what a credential would
	// carry.
	ErrSubscriberKeyScopeUnenforced: http.StatusConflict,
	// Also 409, and NOT the 503 of SUBSCRIBER_PROVISIONING_FAILED: the broker answered and
	// the boundary it would enforce is wider than the row records, which a retry cannot
	// change. Without this entry the refusal would resolve to the unknown-code 500 — which is
	// exactly what it did: the code was declared and returned from IssueSubscriberCredential
	// while its row here was absent, so a deliberate 409 judgement reached the caller as a
	// server defect.
	ErrSubscriberAccessExceedsAuthorization: http.StatusConflict,
	// 403: the request is well formed and the caller is authorised; the server is
	// refusing to put a one-time secret on a channel it cannot establish as
	// confidential. Without this entry the refusal would resolve to the unknown-code
	// 500 default and read as a defect in this service.
	ErrSubscriberInsecureTransport: http.StatusForbidden,
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
