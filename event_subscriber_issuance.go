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

package blnk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// pruneBrokerAccess removes the broker-side grants a subscriber's new authorization no
// longer implies, and reports how many it removed.
func (s *EventSubscriberService) pruneBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		// Narrowing the registry while the broker keeps the wider grant makes the
		// registry under-report real access, which is the dangerous direction.
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "removing this subscriber's obsolete Kafka grants",
		); err != nil {
			return 0, err
		}

		return 0, nil
	}

	report, err := admin.PruneSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
			// SANITIZED AND BOUNDED, not logrus.WithError. A refused administrative request
			// comes back from the Kafka client with the whole request appended to it — hundreds
			// of lines carrying broker addresses, listener names and every field of the call —
			// and an unbounded value with newlines in it can also forge log structure.
			"error_class": kafkaErrorClassField("subscriber_credential_issuance", err),
		}).Error(
			"event subscriber: the obsolete Kafka grants of this subscriber could not be removed, so " +
				"the registry was NOT updated; the subscriber keeps the access it has and the change " +
				"can be retried",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"Failed to remove the subscriber's obsolete Kafka grants, so its authorization was not changed",
			// THE BOUNDED DETAIL, never the cause. NewAPIError does two things with what it is
			// given: it re-logs it through logrus unsanitized, and it serialises it into the
			// response body's `details` member. Passing the wrapped broker error here therefore
			// undid the sanitizing done immediately above AND published whatever the client
			// chose to put in that error.
			NewSubscriberErrorDetail(
				"Removing the subscriber's obsolete Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Removed, nil
}

// grantBrokerAccess creates the broker-side grants a subscriber's authorization
// implies, and reports how many it created.
func (s *EventSubscriberService) grantBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		// The widening half: the row would record a grant nothing created, and a caller told
		// the update succeeded would believe a boundary exists that does not.
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "creating this subscriber's new Kafka grants",
		); err != nil {
			return 0, err
		}

		return 0, nil
	}

	report, err := admin.GrantSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error_class": kafkaErrorClassField("subscriber_credential_issuance", err),
		}).Error(
			"event subscriber: the registry was updated but the subscriber's new Kafka grants could " +
				"not be created, so it currently has LESS access than the registry records; re-run the " +
				"update or issue credentials to complete it",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber was updated but its new Kafka grants could not be created",
			// The bounded detail, never the cause — see pruneBrokerAccess. Retryable, and safely
			// so: the registry already records the intended authorization, and both re-running
			// the update and issuing credentials create the missing bindings.
			NewSubscriberErrorDetail(
				"Creating the subscriber's new Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Created, nil
}

// keyScopeEnforcement resolves whether this deployment enforces subscriber key scopes,
// and where.
func (s *EventSubscriberService) keyScopeEnforcement() (gateway []string, enforced bool) {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return nil, false
	}

	return cnf.Kafka.KeyScopeGateway()
}

// SubscriberAccessDeployment resolves the three configuration facts that decide what a
// subscriber's access ACTUALLY is, for the projection that has to describe it.
//
// Returns:
//   - model.SubscriberAccessDeployment: the resolved deployment state, safe to project
//     from.
func SubscriberAccessDeployment() model.SubscriberAccessDeployment {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return model.SubscriberAccessDeployment{
			KeyScopeEnforcement: model.KeyScopeEnforcementNone,
		}
	}

	deployment := model.SubscriberAccessDeployment{
		KeyScopeEnforcement: model.KeyScopeEnforcementNone,
	}

	if _, enforced := cnf.Kafka.KeyScopeGateway(); enforced {
		deployment.KeyScopeEnforcement = model.KeyScopeEnforcementGateway
	}

	_, deployment.SubscriberBrokersAdvertised = cnf.Kafka.SubscriberFacingBrokers()

	// THE COMPOSED PREDICATE, spelled exactly as requireAcknowledgedSharedTopicAccess
	// spells it: outside secure mode nothing is asked of the operator, and in secure mode
	// the declaration is what unblocks a whole-topic credential. Recomputing it from
	// Server.Secure and the variable separately in the projection is how the two would
	// come to disagree.
	deployment.WholeTopicAccessPermitted = !cnf.Server.Secure || cnf.Kafka.SubscriberSharedTopicAccess

	return deployment
}

// subscriberFacingBrokers resolves the bootstrap list to report to a subscriber, or
// refuses.
func (s *EventSubscriberService) subscriberFacingBrokers(
	subscriber *model.EventSubscriber,
) ([]string, error) {
	identifier := ""
	if subscriber != nil {
		identifier = subscriber.SubscriberID
	}

	// Read through fetchConfiguration — the package's configuration seam, declared in
	// event_sunset.go — for the same reason provisioner does: a test that swaps it sees
	// consistent behaviour across every event file.
	cnf, err := fetchConfiguration()
	if err == nil && cnf != nil {
		if brokers, advertised := cnf.Kafka.SubscriberFacingBrokers(); advertised {
			return brokers, nil
		}
	}

	logrus.WithField("subscriber_id_hash", subscriberLogLabel(identifier)).Error(
		"event subscriber: KAFKA_SUBSCRIBER_BROKERS is not configured, so no credential was " +
			"issued; only an operator knows the externally advertised broker addresses a " +
			"subscriber can dial, and a one-time secret handed out with an address that does not " +
			"resolve for it costs a reissue to diagnose",
	)

	return nil, apierror.NewAPIError(
		apierror.ErrSubscriberBrokersNotConfigured,
		// THE VARIABLE IS NAMED IN THE MESSAGE, not only in the detail and the log. This is
		// the one string that reaches the operator running the request, and "no broker list
		// is configured" without the key is a message that cannot be acted on. A
		// configuration key name discloses nothing: it is documented in.env.example and in
		// the manifests.
		"no subscriber-facing Kafka broker list is configured, so credentials cannot be issued. "+
			"Set KAFKA_SUBSCRIBER_BROKERS to the externally advertised broker addresses "+
			"subscribers connect to — the same value as KAFKA_BROKERS when they run inside this "+
			"deployment. The addresses Blnk dials internally are not reported, because they do "+
			"not resolve for a subscriber outside it",
		NewSubscriberErrorDetail(
			"no subscriber-facing Kafka broker list is configured", identifier,
			// Retryable: nothing was written, and the request succeeds unchanged once the
			// variable is set.
			true,
		),
	)
}

// subscriberPendingSince renders the revocation tombstone for a log field.
func subscriberPendingSince(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.RevocationPendingAt == nil {
		return ""
	}

	return subscriber.RevocationPendingAt.UTC().Format(time.RFC3339)
}

// ---------------------------------------------------------------------------------------
// Credential issuance

// IssueSubscriberCredential mints a subscriber's SASL/SCRAM credential, binds its ACLs,
// and returns the secret to the caller.
//
// Parameters:
//   - ctx context.Context: the caller's context, wrapped in the issuance budget.
//   - subscriberID string: the business key of the subscriber to provision.
//
// Returns:
//   - SubscriberCredential: the connection details and the one-time secret.
//   - error: ErrSubscriberNotFound (404); ErrKafkaUnavailable (503) when no broker is
//     configured, the client cannot be built, or the budget expired;
//     ErrSubscriberProvisioningFailed (503) when the broker refused the credential or
//     the bindings; a typed conflict (409) when a concurrent issuance superseded this
//     one; a typed validation error (400) for an unusable subscriber id.
func (s *EventSubscriberService) IssueSubscriberCredential(
	ctx context.Context,
	subscriberID string,
) (SubscriberCredential, error) {
	store, err := s.requireStore()
	if err != nil {
		return SubscriberCredential{}, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return SubscriberCredential{}, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// THE ONE ABSOLUTE DEADLINE. Everything this request does is inside it: the lookup, up
	// to four broker round trips, the issuance record, AND any compensation that follows a
	// failure.
	absolute := time.Now().Add(s.budget())
	ctx = withSubscriberIssuanceDeadline(ctx, absolute)

	forward, cancel := context.WithDeadline(ctx, absolute.Add(-s.compensationReserve()))
	defer cancel()

	// Shadowed deliberately, so no step below can accidentally use the unbounded parent:
	// the forward path must be the one with the timeout, and the absolute instant reaches
	// the cleanup helpers through the value that travels on it either way.
	ctx = forward

	admin, err := s.provisioner()
	if err != nil {
		return SubscriberCredential{}, err
	}

	if !admin.IsConfigured() {
		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Kafka is not configured, so subscriber credentials cannot be issued",
			ErrKafkaAdminNotConfigured,
		)
	}

	// THE FENCE, taken BEFORE the broker is touched and before a secret is generated.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		// A budget spent on the claim is a TIMEOUT, not a server fault, and this is the
		// commonest place for it to be spent: the claim is the first write of the request. A
		// conflict — another operation holds the claim — passes through unchanged, because it
		// is true whether or not the deadline also expired.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "claiming the subscriber for provisioning", err,
		)
	}

	// THE COMPENSATION THIS ATTEMPT TURNS OUT TO OWE, AND THE CLAIM RELEASE, LEAVE THE
	// RESPONSE PATH TOGETHER — in ONE scheduled task, compensation FIRST.
	var owedCompensation func(base context.Context)

	defer func() {
		compensate := owedCompensation

		s.schedule(func() {
			// THE RELEASE'S WINDOW IS RESOLVED HERE, not when the issuance started.
			releaseCtx, endRelease := subscriberCleanupContext(ctx)
			defer endRelease()

			if compensate != nil {
				compensate(releaseCtx)
			}

			fence.release(releaseCtx)
		})
	}()

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "reading the subscriber's registry row", err,
		)
	}

	// Refuse a subscriber that is on its way out, so an issuance cannot re-arm a principal
	// whose deregistration is already in flight.
	if err := requireActiveSubscriber(subscriber, subscriberRefusedIssuanceMessage); err != nil {
		return SubscriberCredential{}, err
	}

	// THE ENFORCEMENT FACT, READ ONCE. Both halves of the key-scope decision consult it —
	// whether this subscriber may be issued a credential at all, and which endpoint that
	// credential names — so the two cannot disagree.
	keyScopeGateway, keyScopeEnforced := s.keyScopeEnforcement()

	// FAIL CLOSED ON AN UNENFORCEABLE KEY SCOPE. A row recording a partition key prefix
	// describes a boundary Kafka's authorizer has no dimension for, so a credential
	// carrying topic Read would read every record on every authorised topic — other
	// ledgers' and other subscribers' included. Disclosure was tried in place of a
	// boundary and is not one: the party asked to apply the filter is the party holding
	// the credential.
	if err := requireProvisionableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND FAIL CLOSED IN THE OTHER DIRECTION. The refusal above covers a key scope nothing
	// enforces; this one covers the credential that ESCAPES a declared key-scoped model —
	// a subscriber with no prefix, issued literal topic Read, reading every ledger on a
	// shared topic while every other subscriber in the deployment is confined. One
	// prefix-less registration was all it took, and the DTO makes the field optional, so
	// nothing marked the occasion.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND MAKE THE WHOLE-TOPIC MODEL A DECISION RATHER THAN A DEFAULT. Where no key-scoped
	// model is declared, a granted topic is read in full — which is the mandated access
	// model and is correct for a single-tenant deployment. In secure mode Blnk asks the
	// operator to have said so, once, instead of reaching the widest credential it can
	// issue by configuring nothing.
	if err := requireAcknowledgedSharedTopicAccess(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// And refuse a subscriber authorised for nothing, so a live principal that can read
	// nothing is never handed out looking like one that can. Checked in the same place and
	// for the same reason as the three above: before a secret exists and before the broker
	// is touched, so the refusal leaves no residue anywhere.
	if err := requireGrantedTopics(subscriber); err != nil {
		return SubscriberCredential{}, err
	}

	// THE ENDPOINT THE SUBSCRIBER WILL DIAL, resolved before anything is minted.
	subscriberBrokers, err := s.subscriberFacingBrokers(subscriber)
	if err != nil {
		return SubscriberCredential{}, err
	}

	// THE DECLARED ENFORCING ENDPOINT WINS over the advertised broker list for a
	// key-scoped subscriber, and it has to: where one is declared, that subscriber's
	// connection is terminated by it rather than by a broker. Reporting the brokers
	// instead would hand out a credential declaring key-scoped isolation together with an
	// endpoint that bypasses the component enforcing it.
	if subscriber.DeclaresKeyScope() && len(keyScopeGateway) > 0 {
		subscriberBrokers = keyScopeGateway
	}

	// THE DECLARED COMPONENT IS ASKED TO CONFIRM THE BOUNDARY, before a secret exists and
	// before the broker is touched.
	if subscriber.DeclaresKeyScope() && keyScopeEnforced {
		if err := s.attestKeyScope(ctx, subscriber); err != nil {
			return SubscriberCredential{}, err
		}
	}

	// The reference OBSERVED before provisioning. It is what makes the issuance record
	// conditional: if another issuance for this subscriber commits in the meantime, this
	// value no longer matches and the write reports a conflict instead of silently
	// overwriting a record whose secret is the one that works.
	observedReference := subscriber.CredentialReference

	password, err := s.newPassword()
	if err != nil {
		// The cause is not returned, and here that is more than a topology concern: this is
		// the credential-generation path, so its error is the one most likely to render
		// something derived from the secret itself. It is logged bounded and the caller
		// receives the diagnosis only.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"error_class":        kafkaErrorClassField("subscriber_provisioning", err),
		}).Error("event subscriber: generating a SASL credential failed; nothing was written")

		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to generate a SASL credential for the subscriber",
			// Retryable: generation is local and stateless, so nothing was written and
			// repeating the request is safe.
			NewSubscriberErrorDetail(
				"Generating the SASL credential failed", subscriber.SubscriberID, true,
			),
		)
	}

	// Derived from the principal and the secret, before the secret leaves this function's
	// control, so that what is persisted is a digest and never the value itself.
	reference, err := model.DeriveCredentialReference(subscriber.KafkaPrincipal, password)
	if err != nil {
		// The password is an argument to the call that failed, so its error is the single
		// most dangerous cause in this file to propagate. It is logged bounded — and the
		// bounded rendering is of the ERROR, never of the inputs — and the caller receives no
		// cause at all.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"error_class":        kafkaErrorClassField("subscriber_provisioning", err),
		}).Error(
			"event subscriber: deriving the credential reference failed, so nothing was written and " +
				"the generated secret is dead",
		)

		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to derive the subscriber credential reference",
			NewSubscriberErrorDetail(
				"Deriving the credential reference failed", subscriber.SubscriberID, true,
			),
		)
	}

	// The claim is CONFIRMED on the way into the broker phase, so the credential about to
	// be written cannot be interleaved with another operation's. The issuance budget
	// already caps this phase — it caps the whole request — so the phase context adds only
	// the renewal, and the caller's shorter deadline is what continues to govern the round
	// trips.
	provisionPhase, endProvisionPhase, phaseErr := fence.brokerPhaseContext(ctx)
	if phaseErr != nil {
		endProvisionPhase()

		// Nothing has been written and the generated secret is dead. Classified so that a
		// budget spent waiting on the renewal reads as the timeout it was.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "confirming the provisioning claim before provisioning", phaseErr,
		)
	}

	// DeferCompensation is set only when a scheduler is installed. Without one the
	// compensation would be "deferred" to an inline call moments later, which is the same
	// two round trips in a less obvious place — so the request asks for the inline
	// behaviour it is actually going to get, and the result's flags describe what really
	// happened.
	request := NewSubscriberProvisioningRequest(subscriber, password)
	request.DeferCompensation = s.defersWork()

	result, err := admin.ProvisionSubscriberPrincipal(provisionPhase, request)
	endProvisionPhase()

	if err != nil {
		if result.CompensationOwed {
			// The credential is at the broker with no boundary, and undoing it is two round
			// trips this response must not wait for. Scheduled with the fence still held, so a
			// retry cannot have its own fresh credential revoked by this cleanup.
			owed := result
			owedCompensation = func(base context.Context) {
				cleanup, cancel := subscriberCleanupContext(base)
				defer cancel()

				// The outcome is not returned anywhere: this runs after the response. It is logged
				// by the compensation itself — at ERROR, naming the principal and the manual
				// remedy, when the credential could not be revoked.
				_ = admin.CompensateProvisioning(cleanup, owed)
			}
		}

		// ctx and the claim are threaded in because a broker that was left holding something
		// is a DURABLE obligation, not just a log line, and recording it is a write that
		// needs both. The claim is still this caller's here — the deferred release has not
		// run — so the write is fenced like every other mutation on the row.
		return SubscriberCredential{}, s.provisioningFailure(ctx, subscriber, fence.token, result, err)
	}

	// THE RENEWAL, between the last broker round trip and the write it protects.
	if renewErr := fence.renew(ctx); renewErr != nil {
		residue, failure := s.recordFailure(ctx, admin, subscriber, fence.token, renewErr)

		return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
			ctx, subscriber, "confirming the provisioning claim before recording the issuance",
			failure, residue,
		)
	}

	issuedAt := s.clock()

	// Under the claim as well as the reference CAS. The reference alone cannot detect a
	// caller whose lease expired: while the rightful new owner is still provisioning it
	// has recorded nothing, so the stored reference is still the one this caller observed,
	// and its write would land and then refuse the winner.
	if err := store.RecordSubscriberCredentialIfUnchanged(
		ctx, subscriberID, observedReference, reference, issuedAt, fence.token,
	); err != nil {
		// recordFailure owns the compensation and its own logging, and REPORTS WHAT THE
		// BROKER IS LEFT HOLDING once it has run. The classification then re-reports a spent
		// budget here as the timeout it was, and runs on the OUTSIDE so the compensation
		// happens first either way, leaving recordFailure's typed conflict — a superseded
		// issuance — untouched.
		if s.defersWork() {
			recordErr := err
			owedCompensation = func(base context.Context) {
				_, _ = s.recordFailure(base, admin, subscriber, fence.token, recordErr)
			}

			return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
				ctx, subscriber, "recording the issuance in the registry", err,
				SubscriberProvisioningResult{CredentialWritten: true},
			)
		}

		residue, failure := s.recordFailure(ctx, admin, subscriber, fence.token, err)

		return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
			ctx, subscriber, "recording the issuance in the registry", failure, residue,
		)
	}

	credential := SubscriberCredential{
		SubscriberID:     subscriber.SubscriberID,
		Brokers:          subscriberBrokers,
		BrokerEndpoint:   strings.Join(subscriberBrokers, ","),
		AuthorizedTopics: result.Topics,
		ConsumerGroupID:  subscriber.ConsumerGroupID,
		Username:         subscriber.KafkaPrincipal,
		Mechanism:        SubscriberSASLMechanism,
		IssuedAt:         issuedAt,
		// Read from the row this credential was minted against, so the response cannot pair a
		// credential with a prefix that was not in force at issuance.
		PartitionKeyPrefix: subscriberKeyPrefix(subscriber),
		// Derived from the SAME row the prefix above was read from, so the two cannot
		// disagree. Reporting a prefix without saying where it is enforced is what let the
		// field be read as a broker boundary, and reading it as one is what the withheld
		// credential was reaching for. Always populated — "none" when no scope is recorded —
		// so a client branches on this rather than on whether the prefix happens to be empty.
		KeyScopeEnforcement: issuedKeyScopeEnforcement(subscriber, keyScopeEnforced),
		Fingerprint:         model.CredentialFingerprint(reference),
		Replaced:            result.CredentialReplaced,
		password:            NewSubscriberSecret(password),
	}

	// LogFields, never the credential itself: the secret appears here only as its length.
	logrus.WithFields(credential.LogFields()).Info(
		"event subscriber: Kafka credential issued; the secret is returned once and is not " +
			"recoverable afterwards",
	)

	// AND THE KEY-SCOPE LINE, for a key-scoped row only — logKeyScopeDisclosure returns
	// without emitting when no prefix is recorded, so this is one call rather than a
	// condition here.
	logKeyScopeDisclosure(subscriber, describeSubscriberKeyScope(subscriber))

	return credential, nil
}

// newPassword draws a secret through the service's generator seam.
func (s *EventSubscriberService) newPassword() (string, error) {
	if s == nil || s.generatePassword == nil {
		return generateSubscriberPassword()
	}

	return s.generatePassword()
}

// provisioningFailure turns a broker-side provisioning failure into the right typed
// error and records what state the broker was left in.
func (s *EventSubscriberService) provisioningFailure(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	result SubscriberProvisioningResult,
	cause error,
) error {
	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"credential_written": result.CredentialWritten,
		"compensated":        result.Compensated,
		"error_class":        kafkaErrorClassField("subscriber_provisioning", cause),
	}

	// Settled BEFORE the error is chosen, so that every branch below settles — including
	// the timeout and cancellation branches, where the result flags are the only evidence
	// of what the broker was left holding and where the temptation to treat "unknown" as
	// "nothing" is strongest.
	s.settleProvisioningRemnant(ctx, subscriber, fenceToken, result, fields)

	switch {
	case errors.Is(cause, ErrKafkaAdminNotConfigured):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was attempted with no broker configured; nothing " +
				"was written",
		)

		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Kafka is not configured, so subscriber credentials cannot be issued",
			// Bounded, and the cause stays in the log line above. See SubscriberErrorDetail:
			NewSubscriberErrorDetail(
				"Kafka is not configured", subscriber.SubscriberID, false,
			),
		)

	case errors.Is(cause, ErrForeignACLGrantsAccess):
		// Named as its own branch because the three state-based branches below describe what
		// the broker was LEFT in, and none of them describes what went wrong here. Falling
		// through to them logged "the ACL grant failed", which is the opposite of the truth:
		if result.CredentialWritten {
			logrus.WithFields(fields).Error(
				"event subscriber: credential issuance was refused because the principal holds " +
					"foreign ACL bindings granting access beyond its authorization, AND revoking the " +
					"credential failed, so the principal named here can authenticate and read outside " +
					"its authorized topics; revoke it by hand immediately",
			)
		} else {
			logrus.WithFields(fields).Warn(
				"event subscriber: credential issuance was refused because the principal holds " +
					"foreign ACL bindings granting access beyond its authorization; the credential " +
					"was revoked, the generated secret is dead and the issuance was not recorded",
			)
		}

		return apierror.NewAPIError(
			// 409, not the 503 of ErrSubscriberProvisioningFailed. The broker answered and
			// provisioning completed; the refusal is a judgement about the boundary that
			// resulted, and no retry can change it. The code discriminates from the key-scope
			// refusal because the state to fix is at the broker rather than on the row.
			apierror.ErrSubscriberAccessExceedsAuthorization,
			"The subscriber's Kafka principal holds access beyond its authorization, so no credential was issued",
			// NOT retryable: the offending bindings are an operator's, so an immediate retry
			// re-reads the same broker state and refuses again. It becomes retryable only once a
			// human removes the bindings or records the access on the subscriber, which is what
			// the log line above asks for.
			s.provisioningDetail(
				"The Kafka principal holds ACL bindings granting access beyond its recorded authorization",
				subscriber, result, false,
			),
		)

	case errors.Is(cause, ErrSubscriberKeyScopeBoundaryUnverified):
		// Named as its own branch for the same reason the foreign-ACL refusal above is: the
		// state-based branches below describe what the broker was LEFT in, and none of them
		// describes what went wrong here. Provisioning reached the broker and completed; the
		// refusal is that the grant it produced would NOT have kept the boundary the row
		// declares, so a password whose isolation nobody verified was never returned.
		if result.CredentialWritten {
			logrus.WithFields(fields).Error(
				"event subscriber: credential issuance was refused because this subscriber's " +
					"partition-key boundary could not be established at the broker, AND revoking the " +
					"credential failed, so the principal named here can authenticate; revoke it by " +
					"hand immediately",
			)
		} else {
			logrus.WithFields(fields).Warn(
				"event subscriber: credential issuance was refused because this subscriber records a " +
					"partition-key prefix and the grant that resulted would have admitted it to every " +
					"record on its authorised topics; the credential was revoked, the generated secret " +
					"is dead and the issuance was not recorded",
			)
		}

		return apierror.NewAPIError(
			// 409, and the same code the foreign-ACL refusal uses, because it is the same
			// judgement: the effective access the broker would grant exceeds what the
			// subscriber's row authorises. No retry changes it.
			apierror.ErrSubscriberAccessExceedsAuthorization,
			"This subscriber records a partition-key prefix, so its credential must be granted no "+
				"record-level Read and its records must be delivered by the key-authorising component "+
				"declared in KAFKA_KEY_SCOPE_ENFORCEMENT; that boundary could not be established, so "+
				"no credential was issued",
			// NOT retryable: either the broker's ACL enforcement is unconfirmed or a binding
			// outside Blnk's control restores the withheld access, and both need an operator.
			s.provisioningDetail(
				"The subscriber's partition-key boundary could not be enforced at the broker",
				subscriber, result, false,
			),
		)

	case errors.Is(cause, context.DeadlineExceeded):
		logrus.WithFields(fields).Error(
			"event subscriber: credential issuance exceeded its budget, so whether the broker wrote " +
				"the credential is unknown; retrying re-provisions the same boundary idempotently",
		)

		return apierror.NewAPIError(
			// SUBSCRIBER_PROVISIONING_FAILED, not EVENT_KAFKA_UNAVAILABLE, and the same code the
			// registry half of this issuance reports for the identical condition — one wall
			// clock running out — so a client does not have to know which internal dependency
			// was slow in order to recognise the outcome. Its 503 is the approved retryable
			// answer for a spent budget; the error taxonomy carries no separate timeout code,
			// deliberately, so what the broker was left holding stays in the DETAIL. That is the
			// only place it can be anyway: no status code can say whether a credential the
			// caller does not hold may already exist.
			apierror.ErrSubscriberProvisioningFailed,
			fmt.Sprintf(
				"Provisioning Kafka credentials did not complete within %s",
				s.budget(),
			),
			// RETRYABLE, and saying so is the point: a retry re-provisions the same boundary
			// idempotently, which is exactly what the log line above tells an operator. The
			// state flags are carried because whether the broker wrote the credential is
			// genuinely unknown here, and a caller deciding whether to retry needs to know that
			// a secret may already exist that they do not hold.
			s.provisioningDetail(
				"Provisioning the Kafka credential did not complete within the budget",
				subscriber, result, true,
			),
		)

	case errors.Is(cause, context.Canceled):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was cancelled before it completed; whether the " +
				"broker wrote the credential is unknown",
		)

		return apierror.NewAPIError(
			// The same code as the deadline branch above, for the same reason: one wall clock
			// ran out, and the caller should not have to know which dependency noticed first.
			apierror.ErrSubscriberProvisioningFailed,
			"Provisioning Kafka credentials was cancelled before it completed",
			s.provisioningDetail(
				"Provisioning the Kafka credential was cancelled before it completed",
				subscriber, result, true,
			),
		)

	case result.CredentialWritten:
		// Compensation itself failed: the credential exists and has no boundary.
		logrus.WithFields(fields).Error(
			"event subscriber: the SCRAM credential was written, its ACL grant failed AND revoking " +
				"the credential failed, so the principal named here can authenticate with no " +
				"authorization boundary; revoke it by hand immediately",
		)

	case result.Compensated:
		logrus.WithFields(fields).Warn(
			"event subscriber: the ACL grant failed after the SCRAM credential was written, so the " +
				"credential was revoked and the broker is clean; the generated secret is dead and " +
				"the issuance was not recorded",
		)

	default:
		logrus.WithFields(fields).Error(
			"event subscriber: provisioning the Kafka principal failed before any credential was " +
				"written; nothing was changed at the broker",
		)
	}

	// The three broker-side outcomes converge here, and the detail is what distinguishes
	// them for the caller: the state flags say whether a credential reached the broker and
	// whether it was compensated away, which is the difference between "retry and you are
	// fine" and "a principal exists that a human has to revoke". The specific cause, and
	// the principal's name, stay in the log lines above.
	message := "Failed to provision Kafka credentials for the subscriber"
	reason := "Provisioning the Kafka principal failed at the broker"

	if errors.Is(cause, ErrSubscriberPrincipalReserved) {
		logrus.WithFields(fields).Error(
			"event subscriber: credentials were REFUSED because this subscriber's derived principal " +
				"is one of this deployment's own Kafka identities; provisioning it would have rotated " +
				"that credential and returned it. Move KAFKA_SASL_USER and KAFKA_SASL_ADMIN_USER " +
				"outside the reserved 'blnk-sub-' namespace and restart",
		)

		// Deliberately says nothing about WHICH identity it collides with. That is a
		// deployment fault, the remedy is in the log line above and in the start-up
		// validation, and confirming to a caller that a particular principal name is
		// privileged would answer a question they should not be able to ask.
		message = "Kafka credentials cannot be issued for this subscriber because its derived " +
			"principal is reserved by this deployment; contact an administrator"
		reason = "The subscriber's derived Kafka principal is reserved"

		return apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			message,
			s.provisioningDetail(reason, subscriber, result, false),
		)
	}

	if errors.Is(cause, ErrSubscriberForeignACLGrant) {
		logrus.WithFields(fields).Error(
			"event subscriber: credentials were REFUSED because this principal carries ALLOW ACL " +
				"bindings Blnk did not provision, so its effective access is broader than the " +
				"registry records and no isolation boundary can be stated; remove them with " +
				"kafka-acls — the bindings are named in the error above — and retry",
		)

		message = "The subscriber's Kafka principal carries ACL grants Blnk did not provision, " +
			"so its effective access is broader than its authorization records and credentials " +
			"cannot be issued; remove the foreign ACL bindings from the principal and retry"
		reason = "The Kafka principal carries foreign ALLOW ACL bindings, so no access boundary can be stated"
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		message,
		s.provisioningDetail(reason, subscriber, result, false),
	)
}

// settleProvisioningRemnant reconciles the registry with whatever a failed provisioning
// left at the broker, and records a DURABLE obligation whenever it cannot.
func (s *EventSubscriberService) settleProvisioningRemnant(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	result SubscriberProvisioningResult,
	fields logrus.Fields,
) {
	if !result.CredentialWritten && !result.Compensated {
		return
	}

	cleanup, cancel := subscriberCleanupContext(ctx)
	defer cancel()

	// A credential that is still written is a live principal with no boundary. There is
	// nothing to clear on the row — the issuance was never recorded — so the only remedy
	// is broker-side, and that is what the obligation asks settlement to perform.
	if result.CredentialWritten {
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal with no authorization boundary")

		return
	}

	// Confirmed compensated. The broker is clean, so the row is what is wrong.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		return
	}

	// The clear failed, so the registry over-reports. Settlement's remedy is the same one
	// it would apply to a live credential — revoke, which is a harmless no-op against a
	// principal whose credential is already gone, then clear the row — so the same marker
	// covers both.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")
}

// recordCleanupObligation writes the credential-cleanup marker, and escalates when it
// cannot.
func (s *EventSubscriberService) recordCleanupObligation(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	fields logrus.Fields,
	condition string,
) {
	store, storeErr := s.requireStore()
	if storeErr != nil {
		logrus.WithFields(fields).Error(
			"event subscriber: " + condition + ", but the registry is not configured, so no " +
				"settlement obligation could be recorded",
		)

		return
	}

	if err := store.RecordSubscriberCredentialCleanupPending(
		ctx, subscriber.SubscriberID, time.Now(), fenceToken,
	); err != nil {
		logrus.WithFields(fields).WithField(
			"settlement_error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"event subscriber: " + condition + ", AND recording the pending credential cleanup " +
				"failed, so THIS LOG LINE IS THE ONLY RECORD of it; the principal named here must be " +
				"reconciled by hand",
		)

		return
	}

	logrus.WithFields(fields).Warn(
		"event subscriber: " + condition + "; a pending credential cleanup was recorded and " +
			"settlement will reconcile it",
	)
}

// provisioningDetail builds a bounded provisioning-failure detail carrying what reached
// the broker.
func (s *EventSubscriberService) provisioningDetail(
	reason string,
	subscriber *model.EventSubscriber,
	result SubscriberProvisioningResult,
	retryable bool,
) SubscriberErrorDetail {
	subscriberID := ""
	if subscriber != nil {
		subscriberID = subscriber.SubscriberID
	}

	detail := NewSubscriberErrorDetail(reason, subscriberID, retryable)
	detail.CredentialWritten = result.CredentialWritten
	detail.Compensated = result.Compensated
	detail.CompensationPending = result.CompensationOwed

	return detail
}

// classifyIssuanceTimeout re-reports a registry failure that was really the issuance
// budget running out, or the caller going away, as the timeout it was.
func (s *EventSubscriberService) classifyIssuanceTimeout(
	ctx context.Context,
	subscriberID string,
	stage string,
	cause error,
) error {
	verdict := s.issuanceTimeoutFor(ctx, stage, cause)
	if !verdict.Rewrite {
		return cause
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"stage":              stage,
		"budget":             s.budget().String(),
		"cancelled":          verdict.Cancelled,
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}).Warn(
		"event subscriber: credential issuance ran out of time at the registry rather than failing; " +
			"the provisioning claim is released and a retry is safe, because issuance re-provisions " +
			"the same boundary idempotently",
	)

	// SUBSCRIBER_PROVISIONING_FAILED, which resolves to a retryable 503, and the same code
	// the broker half of this issuance reports for a spent budget. The taxonomy carries no
	// separate timeout code; the spent budget is named in verdict.Message and in the log
	// line above, and the detail's retryable flag is what a client branches on.
	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		verdict.Message,
		NewSubscriberErrorDetail(verdict.Reason, subscriberID, true),
	)
}

// issuanceTimeoutVerdict is the shared decision behind both timeout classifications:
type issuanceTimeoutVerdict struct {
	// Rewrite reports whether the cause should be replaced by a typed timeout. False means the
	// caller returns the cause unchanged, which is the common case.
	Rewrite bool

	// Cancelled distinguishes a caller that went away from a budget that ran out. Both are
	// timeouts to this service and neither is a defect in it, but they are worded
	// differently because only one of them is worth an operator's attention.
	Cancelled bool

	// Message is the client-facing sentence.
	Message string

	// Reason is the operator-facing detail. The post-broker classifier appends to it.
	Reason string
}

// issuanceTimeoutFor decides whether a failure during credential issuance is really a
// spent budget, and produces the wording for it.
func (s *EventSubscriberService) issuanceTimeoutFor(
	ctx context.Context,
	stage string,
	cause error,
) issuanceTimeoutVerdict {
	if cause == nil {
		return issuanceTimeoutVerdict{}
	}

	expiry := ctx.Err()
	if expiry == nil || !isInternalServerError(cause) {
		return issuanceTimeoutVerdict{}
	}

	verdict := issuanceTimeoutVerdict{
		Rewrite:   true,
		Cancelled: errors.Is(expiry, context.Canceled),
		Message: fmt.Sprintf(
			"Provisioning Kafka credentials did not complete within %s", s.budget(),
		),
		Reason: "The registry did not answer within the issuance budget while " + stage,
	}

	if verdict.Cancelled {
		verdict.Message = "Provisioning Kafka credentials was cancelled before it completed"
		verdict.Reason = "The request was cancelled while " + stage
	}

	return verdict
}

// classifyPostProvisioningTimeout is classifyIssuanceTimeout for the failures that
// happen AFTER the broker has been written to.
func (s *EventSubscriberService) classifyPostProvisioningTimeout(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	stage string,
	cause error,
	residue SubscriberProvisioningResult,
) error {
	verdict := s.issuanceTimeoutFor(ctx, stage, cause)
	if !verdict.Rewrite {
		return cause
	}

	subscriberID := ""
	principal := ""

	if subscriber != nil {
		subscriberID = subscriber.SubscriberID
		principal = subscriber.KafkaPrincipal
	}

	reason := verdict.Reason

	switch {
	case residue.CredentialWritten:
		reason += ". The credential written at the broker could not be revoked, so a principal " +
			"exists that no registry row records"
	case residue.Compensated:
		reason += ". The credential written at the broker was revoked, so the subscriber has no " +
			"Kafka access until credentials are re-issued"
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"principal_hash":     subscriberLogLabel(principal),
		"stage":              stage,
		"budget":             s.budget().String(),
		"cancelled":          verdict.Cancelled,
		"credential_written": residue.CredentialWritten,
		"compensated":        residue.Compensated,
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}).Warn(
		"event subscriber: credential issuance ran out of time at the registry AFTER the broker " +
			"was written to; the provisioning claim is released and a retry is safe, because " +
			"issuance re-provisions the same boundary idempotently. credential_written here means " +
			"a principal is unaccounted for and needs manual revocation if no retry follows",
	)

	// The same retryable SUBSCRIBER_PROVISIONING_FAILED as every other expiry in the issuance
	// path, with the broker residue carried in the detail rather than in the status.
	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		verdict.Message,
		s.provisioningDetail(reason, subscriber, residue, true),
	)
}

// isInternalServerError reports whether an error carries the generic internal-server
// code.
func isInternalServerError(err error) bool {
	if err == nil {
		return false
	}

	isInternalCode := func(code apierror.ErrorCode) bool {
		return apierror.Normalize(code) == apierror.ErrGenInternal
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return isInternalCode(apiErr.Code)
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return isInternalCode(apiErrPtr.Code)
	}

	return false
}

// recordFailure handles the narrow window in which the broker holds a credential the
// registry could not record.
func (s *EventSubscriberService) recordFailure(
	ctx context.Context,
	admin subscriberPrincipalProvisioner,
	subscriber *model.EventSubscriber,
	fenceToken string,
	cause error,
) (SubscriberProvisioningResult, error) {
	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}

	if isConflictError(cause) {
		// A CONFLICT IS NOT REVOKED, and the marker is recorded instead.
		orphanedAt := s.clock()

		durable, cancelDurable := subscriberDurabilityContext(ctx)
		orphanRecorded := s.markCredentialOrphaned(durable, subscriber, fields, orphanedAt)
		cancelDurable()

		logrus.WithFields(fields).WithFields(logrus.Fields{
			"orphan_recorded":   orphanRecorded,
			"orphaned_at":       orphanedAt.UTC().Format(time.RFC3339),
			"lost_fence":        subscriberFenceWasLost(cause),
			"settlement_remedy": "issue once more, serially",
		}).Warn(
			"event subscriber: a concurrent operation took this subscriber, so the credential this " +
				"call generated was NOT recorded and may or may not be the one now live at the " +
				"broker; the credential was deliberately not revoked, because one SCRAM credential " +
				"exists per principal and revoking would destroy the other issuance's secret too. " +
				"The row is marked credential_orphaned_at so the ambiguity is visible; issue once " +
				"more, serially, to make the registry and the broker agree, which settles the marker",
		)

		return SubscriberProvisioningResult{CredentialWritten: true}, cause
	}

	cleanup, cancelCleanup := subscriberCleanupContext(ctx)
	defer cancelCleanup()

	if err := admin.RevokeSubscriber(cleanup, subscriber); err != nil {
		// The bounded class only: a refused administrative request carries the broker's whole
		// request dump, and the raw text goes to the trace-level diagnostic sink instead.
		logrus.WithFields(fields).WithField(
			"revocation_error_class", kafkaErrorClassField("subscriber_compensating_revocation", err),
		).Error(
			"event subscriber: recording the issuance failed AND revoking the credential it had " +
				"already written failed, so the principal named here holds a credential that no " +
				"registry row records; revoke it by hand",
		)

		// A DURABLE obligation, not only the line above. This is the same state
		// settleProvisioningRemnant records for a failed compensation — a credential live at
		// the broker that nothing in Blnk accounts for — reached by a different route, and it
		// needs the same remedy. Recorded on the cleanup context, so the spent budget that is
		// often the reason for being here does not also lose the record of it.
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal that no registry row records")

		// AND THE ORPHAN MARKER, which is not the same fact. The obligation above is FENCED
		// and tells the settlement pass there is broker-side work to finish; this one is
		// UNFENCED and is what the orphaned-credential gauge and its alert count. Two
		// different readers, two different remedies — settlement discharges the first, and
		// re-issuing or deprovisioning settles the second — so recording only one leaves the
		// other blind.
		orphanedAt := s.clock()

		durable, cancelDurable := subscriberDurabilityContext(ctx)
		orphanRecorded := s.markCredentialOrphaned(durable, subscriber, fields, orphanedAt)
		cancelDurable()

		if !orphanRecorded {
			logrus.WithFields(fields).WithField(
				"orphaned_at", orphanedAt.UTC().Format(time.RFC3339),
			).Error(
				"event subscriber: the orphan marker could not be persisted either, so the " +
					"principal named here holds a credential that NOTHING in Blnk records; this " +
					"log line is the only trace — revoke it by hand",
			)
		}

		return SubscriberProvisioningResult{CredentialWritten: true}, cause
	}

	if isSubscriberNotFoundError(cause) {
		logrus.WithFields(fields).Warn(
			"event subscriber: the subscriber was removed while its credential was being issued, so " +
				"the credential was revoked and nothing was recorded",
		)

		return SubscriberProvisioningResult{Compensated: true}, cause
	}

	// The row survives and still names whatever credential it held BEFORE this issuance —
	// but that credential no longer exists: the upsert replaced it and the revocation
	// above removed the replacement. Clearing the reference is what stops the registry
	// claiming access that has gone, which every reader of it (an operator, the migration
	// report, a reconciliation) would otherwise believe. Best-effort, because the write
	// that just failed is the same connection this one uses.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		logrus.WithFields(fields).Warn(
			"event subscriber: recording the issuance failed, so the credential written at the " +
				"broker was revoked and the registry's credential record was cleared; the " +
				"subscriber has no Kafka access until credentials are re-issued",
		)

		return SubscriberProvisioningResult{Compensated: true}, cause
	}

	// The revocation succeeded but the row still names a credential that no longer exists,
	// so the registry over-reports this subscriber's access. clearCredentialRecord has
	// already said so at ERROR; the obligation is what makes it a to-do item settlement
	// will finish rather than a line waiting to be read.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")

	return SubscriberProvisioningResult{Compensated: true}, cause
}

// clearCredentialRecord erases a subscriber's credential record, best-effort.
func (s *EventSubscriberService) clearCredentialRecord(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	fields logrus.Fields,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.ClearSubscriberCredential(ctx, subscriber.SubscriberID, fenceToken); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the broker-side credential was revoked but clearing the registry's " +
				"credential record failed, so the registry now over-reports this subscriber's " +
				"access; clear it once the write path recovers",
		)

		return false
	}

	return true
}

// isSubscriberNotFoundError reports whether an error means "no such subscriber".
func isSubscriberNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if isNotFoundError(err) {
		return true
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return apierror.Normalize(apiErr.Code) == apierror.ErrSubscriberNotFound
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return apierror.Normalize(apiErrPtr.Code) == apierror.ErrSubscriberNotFound
	}

	return false
}

// RevokeSubscriberCredential ends a subscriber's Kafka access while leaving it
// registered.
//
// Returns:
//   - error: ErrSubscriberNotFound, a revocation failure, or the repository's error.
func (s *EventSubscriberService) RevokeSubscriberCredential(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED, for the same reason issuance is: revoking the credential an issuance is in
	// the middle of writing would leave the broker and the registry describing different
	// states, and which one won would depend on the order two network calls happened to
	// complete in.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer fence.release(ctx)

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return err
	}

	// DELIBERATELY NOT GUARDED BY requireActiveSubscriber, unlike issuance and update. A
	// tombstoned row is a subscriber whose access is being taken away, and this operation
	// takes access away — refusing it would block the one manual remedy available for a
	// deregistration whose broker step keeps failing. The guard exists to stop access
	// being GRANTED or WIDENED mid-removal, and revocation does neither.
	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	// Clearing credential_reference with no broker to revoke against erases the only
	// evidence of what has to be revoked, while the credential keeps authenticating.
	if !admin.IsConfigured() {
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "revoking this subscriber's Kafka credential",
		); err != nil {
			return err
		}
	}

	// With no broker AND no credential evidence — no reference, no orphan marker, no
	// unsettled cleanup obligation — there is nothing to revoke, so the registry record is
	// simply cleared. A deployment that has never configured Kafka can still tidy a row
	// that predates that decision.
	if admin.IsConfigured() {
		// The claim is confirmed on the way in and the phase is bounded, for the same reason
		// deregistration's is: revocation is several administrative round trips and this
		// method arrived with no deadline of its own, so without the bound the phase could
		// outlive the claim and the clear below would then blank a newer issuance's record.
		revokePhase, endRevokePhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endRevokePhase()

			return phaseErr
		}

		revokeErr := admin.RevokeSubscriber(revokePhase, subscriber)
		endRevokePhase()

		if err := revokeErr; err != nil {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
				"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
				// Bounded, for the reason given in pruneBrokerAccess.
				"error_class": kafkaErrorClassField("subscriber_migration", err),
			}).Error(
				"event subscriber: revoking the subscriber's Kafka access failed, so the registry " +
					"record was left untouched and the credential may still work; retry the revocation",
			)

			return apierror.NewAPIError(
				apierror.ErrSubscriberProvisioningFailed,
				"Failed to revoke the subscriber's Kafka access",
				// Same treatment as the deregistration path, for the same reason: the principal and
				// the broker's own words stay in the log, and the caller receives the diagnosis
				// plus the fact that a retry is safe. Revocation is idempotent at the broker, so
				// repeating it cannot make things worse.
				NewSubscriberErrorDetail(
					"Revoking the subscriber's Kafka access failed at the broker",
					subscriber.SubscriberID, true,
				),
			)
		}
	}

	// Under the claim: the broker revocation above may have taken long enough for the lease to
	// have been lost, and clearing then would blank whatever the new owner has since recorded.
	if err := store.ClearSubscriberCredential(ctx, subscriberID, fence.token); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"broker_present":     admin.IsConfigured(),
	}).Info(
		"event subscriber: credential revoked; the subscriber remains registered and can be " +
			"re-issued",
	)

	return nil
}
