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
	"slices"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// createACLBindings sends the bindings and interprets the per-binding results.
func (a *KafkaAdminClient) createACLBindings(ctx context.Context, principal string, bindings []kafka.ACLEntry) error {
	if len(bindings) == 0 {
		return nil
	}

	// the LAST gate, and the only unbypassable one. Three call sites reach this function —
	// provisioning's reconciliation, PruneSubscriberAccess/GrantSubscriberAccess, and
	// ReconcileSubscriberACLs — and a guard placed at any one of them leaves the other two
	// open. Placing it here means every ACL Blnk creates, on every path present or future,
	// has had its shape checked against the two shapes a subscriber grant is made of.
	if err := validateDesiredACLBindings(principal, bindings); err != nil {
		return err
	}

	response, err := a.client.CreateACLs(ctx, &kafka.CreateACLsRequest{ACLs: bindings})
	if err != nil {
		return fmt.Errorf("kafka admin: binding %d ACL(s) for principal %q: %w", len(bindings), principal, err)
	}

	// The results are positional, so an index identifies the binding that failed.
	for index, bindingErr := range response.Errors {
		if bindingErr == nil {
			continue
		}

		binding := kafka.ACLEntry{}
		if index < len(bindings) {
			binding = bindings[index]
		}

		return fmt.Errorf(
			"kafka admin: binding %s %s on %s %q for principal %q: %w",
			binding.PermissionType, binding.Operation, binding.ResourceType, binding.ResourceName, principal,
			bindingErr,
		)
	}

	return nil
}

// ---------------------------------------------------------------------------------------
// ACL RECONCILIATION

// SubscriberACLReconciliation reports what one reconciliation of a principal's bindings
// did.
type SubscriberACLReconciliation struct {
	// Principal is the bare SASL username the bindings belong to.
	Principal string

	// Desired is how many bindings the subscriber's recorded authorization implies.
	Desired int

	// Managed is how many Blnk-shaped bindings the broker held before reconciliation.
	Managed int

	// Created is how many bindings were added.
	Created int

	// Removed is how many surplus Blnk-shaped bindings were deleted.
	Removed int

	// ForeignAllow describes every binding on this principal that Blnk does not own and
	// that WIDENS its access — an ALLOW of a shape Blnk never provisions, or a binding
	// whose permission type the broker did not state definitively.
	ForeignAllow []string

	// ForeignDeny describes every foreign binding that can only NARROW the principal's
	// access: an explicit DENY.
	ForeignDeny []string
}

// Foreign returns every foreign binding, widening ones first.
//
// Returns:
//   - []string: a fresh slice; nil when there are none.
func (r SubscriberACLReconciliation) Foreign() []string {
	if len(r.ForeignAllow) == 0 && len(r.ForeignDeny) == 0 {
		return nil
	}

	foreign := make([]string, 0, len(r.ForeignAllow)+len(r.ForeignDeny))
	foreign = append(foreign, r.ForeignAllow...)
	foreign = append(foreign, r.ForeignDeny...)

	return foreign
}

// describeSubscriberACLs reads every ACL binding the broker currently holds for a
// principal.
func (a *KafkaAdminClient) describeSubscriberACLs(ctx context.Context, principal string) ([]kafka.ACLEntry, error) {
	if err := a.ready(ctx); err != nil {
		return nil, err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return nil, errors.New("kafka admin: a principal is required to read its ACL bindings")
	}

	bound := kafkaPrincipalPrefix + principal

	response, err := a.client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        kafka.ResourceTypeAny,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			PrincipalFilter:           bound,
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading the ACL bindings of principal %q: %w", principal, err)
	}

	if response.Error != nil {
		// SECURITY_DISABLED reaches here on a broker with no authorizer. It is returned as an
		// error rather than as "no bindings": reporting an empty grant would let a caller
		// conclude there was nothing to reconcile on precisely the broker where nothing is
		// enforced at all.
		return nil, fmt.Errorf("kafka admin: reading the ACL bindings of principal %q: %w",
			principal, response.Error)
	}

	bindings := make([]kafka.ACLEntry, 0, 8)
	for _, resource := range response.Resources {
		for _, description := range resource.ACLs {
			if description.Principal != bound {
				continue
			}

			bindings = append(bindings, kafka.ACLEntry{
				ResourceType:        resource.ResourceType,
				ResourceName:        resource.ResourceName,
				ResourcePatternType: resource.PatternType,
				Principal:           description.Principal,
				Host:                description.Host,
				Operation:           description.Operation,
				PermissionType:      description.PermissionType,
			})
		}
	}

	return bindings, nil
}

// blnkManagedACLBinding reports whether a binding is one Blnk itself provisions.
func blnkManagedACLBinding(binding kafka.ACLEntry) bool {
	if binding.PermissionType != kafka.ACLPermissionTypeAllow {
		return false
	}

	switch binding.ResourceType {
	case kafka.ResourceTypeTopic:
		return binding.ResourcePatternType == kafka.PatternTypeLiteral &&
			(binding.Operation == kafka.ACLOperationTypeRead ||
				binding.Operation == kafka.ACLOperationTypeDescribe)
	case kafka.ResourceTypeGroup:
		return binding.ResourcePatternType == kafka.PatternTypePrefixed &&
			binding.Operation == kafka.ACLOperationTypeRead
	default:
		return false
	}
}

// foreignACLBindingWidens reports whether a foreign binding can GRANT access, as
// opposed to only taking it away.
func foreignACLBindingWidens(binding kafka.ACLEntry) bool {
	return binding.PermissionType != kafka.ACLPermissionTypeDeny
}

// reservedKafkaPrincipals returns the SASL usernames this deployment uses for its own
// Kafka access, trimmed and deduplicated, with blanks dropped.
func reservedKafkaPrincipals(kafkaConfig config.KafkaConfig) []string {
	candidates := []string{
		strings.TrimSpace(kafkaConfig.SASLAdminUser),
		strings.TrimSpace(kafkaConfig.SASLUser),
	}

	var reserved []string
	for _, candidate := range candidates {
		if candidate == "" || slices.Contains(reserved, candidate) {
			continue
		}

		reserved = append(reserved, candidate)
	}

	return reserved
}

// ErrSubscriberPrincipalReserved is returned when a subscriber's DERIVED principal
// collides with one of this deployment's own Kafka identities.
var ErrSubscriberPrincipalReserved = errors.New(
	"kafka admin: the subscriber's derived principal is one of this deployment's own Kafka " +
		"identities, so provisioning it would rotate and return that credential; KAFKA_SASL_USER " +
		"and KAFKA_SASL_ADMIN_USER must not sit inside the reserved 'blnk-sub-' namespace",
)

// requirePrincipalNotReserved refuses a principal that is one of the deployment's own.
func (a *KafkaAdminClient) requirePrincipalNotReserved(principal string) error {
	principal = strings.TrimSpace(principal)
	if principal == "" || !slices.Contains(a.reservedPrincipals, principal) {
		return nil
	}

	logrus.WithField("principal_hash", subscriberLogLabel(principal)).Error(
		"kafka admin: refusing to provision a subscriber credential for a principal that is this " +
			"deployment's own Kafka identity; the SCRAM write would rotate that credential and the " +
			"response would return it. Move KAFKA_SASL_USER and KAFKA_SASL_ADMIN_USER outside the " +
			"reserved 'blnk-sub-' namespace",
	)

	return fmt.Errorf("%w (principal %q)",
		ErrSubscriberPrincipalReserved,
		sanitizeLogValue(principal, maxLoggedFilterLength),
	)
}

// ErrSubscriberForeignACLGrant is returned when a subscriber principal carries an ALLOW
// ACL binding Blnk did not provision.
var ErrSubscriberForeignACLGrant = errors.New(
	"kafka admin: this subscriber principal carries ALLOW ACL bindings Blnk did not provision, " +
		"so its effective access is broader than the subscriber registry records and cannot be " +
		"stated; remove the foreign bindings with kafka-acls, or move them to a principal outside " +
		"the reserved subscriber namespace, and retry",
)

// refuseForeignACLGrant builds the fail-closed error for a principal carrying foreign
// ALLOW bindings.
func refuseForeignACLGrant(principal string, widening []string) error {
	return fmt.Errorf("%w (principal %q, %d binding(s): %s)",
		ErrSubscriberForeignACLGrant,
		sanitizeLogValue(principal, maxLoggedFilterLength),
		len(widening),
		strings.Join(widening, "; "),
	)
}

// classifyForeignACLBinding records one foreign binding on the report, in the list its
// permission type puts it in.
func classifyForeignACLBinding(report *SubscriberACLReconciliation, binding kafka.ACLEntry) {
	rendered := fmt.Sprintf("%s %s on %s %q (%s, host %s)",
		binding.PermissionType, binding.Operation, binding.ResourceType,
		sanitizeLogValue(binding.ResourceName, maxLoggedFilterLength),
		binding.ResourcePatternType, sanitizeLogValue(binding.Host, maxLoggedFilterLength))

	if foreignACLBindingWidens(binding) {
		report.ForeignAllow = append(report.ForeignAllow, rendered)

		return
	}

	report.ForeignDeny = append(report.ForeignDeny, rendered)
}

// aclBindingKey renders a binding as the tuple that identifies it, so two bindings can
// be compared as set members.
func aclBindingKey(binding kafka.ACLEntry) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		binding.ResourceType, binding.ResourceName, binding.ResourcePatternType,
		binding.Principal, binding.Host, binding.Operation, binding.PermissionType)
}

// ErrSubscriberBindingShapeUnsupported is returned when a binding Blnk is about to
// CREATE is not one of the two shapes it owns.
var ErrSubscriberBindingShapeUnsupported = errors.New(
	"kafka admin: refusing to create an ACL binding outside the two shapes a subscriber grant is " +
		"made of (Read/Describe Allow on a LITERAL topic name, Read Allow on a PREFIXED consumer " +
		"group namespace)",
)

// validateDesiredACLBindings asserts that every binding about to be WRITTEN is one Blnk
// owns.
func validateDesiredACLBindings(principal string, desired []kafka.ACLEntry) error {
	expected := kafkaPrincipalPrefix + strings.TrimSpace(principal)

	for _, binding := range desired {
		rendered := fmt.Sprintf("%s %s on %s %q (%s)",
			binding.PermissionType, binding.Operation, binding.ResourceType,
			sanitizeLogValue(binding.ResourceName, maxLoggedFilterLength),
			binding.ResourcePatternType)

		if !blnkManagedACLBinding(binding) {
			return fmt.Errorf("%w: %s", ErrSubscriberBindingShapeUnsupported, rendered)
		}

		if binding.Principal != expected {
			return fmt.Errorf(
				"%w: %s names principal %q, but this reconciliation is for %q",
				ErrSubscriberBindingShapeUnsupported, rendered,
				sanitizeLogValue(binding.Principal, maxLoggedFilterLength),
				sanitizeLogValue(expected, maxLoggedFilterLength))
		}

		name := binding.ResourceName
		if name == "" || name != strings.TrimSpace(name) || name == ACLHostAny {
			return fmt.Errorf(
				"%w: %s names a resource that is blank, padded, or the wildcard %q, which Kafka "+
					"reads as matching every resource",
				ErrSubscriberBindingShapeUnsupported, rendered, ACLHostAny)
		}
	}

	return nil
}

// reconcileSubscriberACLs makes the broker's Blnk-owned bindings for a principal
// EXACTLY the desired set.
func (a *KafkaAdminClient) reconcileSubscriberACLs(
	ctx context.Context,
	principal string,
	desired []kafka.ACLEntry,
) (SubscriberACLReconciliation, error) {
	report := SubscriberACLReconciliation{
		Principal: strings.TrimSpace(principal),
		Desired:   len(desired),
	}

	// checked BEFORE the describe, so a widened desired set costs no round trip
	// and leaves the broker untouched.
	if err := validateDesiredACLBindings(report.Principal, desired); err != nil {
		return report, err
	}

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	wanted := make(map[string]struct{}, len(desired))
	for _, binding := range desired {
		wanted[aclBindingKey(binding)] = struct{}{}
	}

	surplus := make([]kafka.ACLEntry, 0, len(observed))
	present := make(map[string]struct{}, len(observed))

	for _, binding := range observed {
		if !blnkManagedACLBinding(binding) {
			classifyForeignACLBinding(&report, binding)

			continue
		}

		report.Managed++

		key := aclBindingKey(binding)
		present[key] = struct{}{}

		if _, keep := wanted[key]; !keep {
			surplus = append(surplus, binding)
		}
	}

	if len(report.ForeignDeny) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(report.Principal),
			"bindings":       report.Foreign,
		}).Warn(
			"kafka admin: this principal carries DENY ACL bindings Blnk does not provision and will " +
				"not remove; they only narrow what the grant allows, so provisioning continues — but " +
				"the subscriber may read less than its authorised topics suggest",
		)
	}

	// REFUSED HERE, before anything is deleted or created, so a principal whose
	// effective grant Blnk cannot state is left exactly as it was found.
	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(report.Principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so its " +
				"effective access is broader than the subscriber registry records; refusing to " +
				"reconcile or issue credentials until they are removed by hand",
		)

		return report, refuseForeignACLGrant(report.Principal, report.ForeignAllow)
	}

	if len(surplus) > 0 {
		if err := a.deleteACLBindings(ctx, report.Principal, surplus); err != nil {
			return report, fmt.Errorf(
				"kafka admin: removing %d obsolete ACL binding(s) from principal %q: %w",
				len(surplus), report.Principal, err,
			)
		}

		report.Removed = len(surplus)
	}

	missing := make([]kafka.ACLEntry, 0, len(desired))
	for _, binding := range desired {
		if _, already := present[aclBindingKey(binding)]; already {
			continue
		}

		missing = append(missing, binding)
	}

	if len(missing) > 0 {
		if err := a.createACLBindings(ctx, report.Principal, missing); err != nil {
			return report, err
		}

		report.Created = len(missing)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(report.Principal),
		"desired":        report.Desired,
		"managed":        report.Managed,
		"created":        report.Created,
		"removed":        report.Removed,
		// Deny only: a widening binding cannot reach this line, because it returned above.
		"foreign_deny": len(report.ForeignDeny),
	}).Info("kafka admin: subscriber ACL bindings reconciled")

	return report, nil
}

// PruneSubscriberAccess removes every Blnk-owned binding the broker holds for a
// subscriber that its RECORDED authorization does not imply, and creates nothing.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the authorization to converge
//     on.
//
// Returns:
//   - SubscriberACLReconciliation: what was found and removed. Created is always zero.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) PruneSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (_ SubscriberACLReconciliation, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "prune_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	desired, principal, err := subscriberDesiredBindings(subscriber)
	if err != nil {
		return SubscriberACLReconciliation{}, err
	}

	report := SubscriberACLReconciliation{Principal: principal, Desired: len(desired)}

	// Compensating class: pruning REMOVES access, and narrowing a boundary must not wait
	// behind the queue of requests trying to widen one.
	ctx, releasePermit, err := a.admitAdminConversation(ctx, kafkaAdminCompensating)
	if err != nil {
		return report, err
	}

	defer releasePermit()

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	wanted := make(map[string]struct{}, len(desired))
	for _, binding := range desired {
		wanted[aclBindingKey(binding)] = struct{}{}
	}

	surplus := make([]kafka.ACLEntry, 0, len(observed))
	for _, binding := range observed {
		if !blnkManagedACLBinding(binding) {
			// CLASSIFIED AND REPORTED, NEVER REFUSED. This is the one operation on this surface
			// that must not fail closed on a foreign ALLOW binding, and the reason is the
			// direction it moves in: pruning only ever REMOVES access, so refusing it would
			// leave the subscriber with MORE access than the operator asked for — precisely the
			// outcome the refusal exists to prevent. The widening binding is surfaced on the
			// report so the caller and the log say so, and the next operation that would hand
			// out a credential or converge a widening is the one that refuses.
			classifyForeignACLBinding(&report, binding)

			continue
		}

		report.Managed++

		if _, keep := wanted[aclBindingKey(binding)]; !keep {
			surplus = append(surplus, binding)
		}
	}

	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so " +
				"narrowing its authorization does NOT narrow its effective access; the obsolete " +
				"Blnk-owned bindings were still removed, but the foreign grants must be removed by hand",
		)
	}

	if len(surplus) == 0 {
		return report, nil
	}

	if err := a.deleteACLBindings(ctx, principal, surplus); err != nil {
		return report, fmt.Errorf(
			"kafka admin: removing %d obsolete ACL binding(s) from principal %q: %w",
			len(surplus), principal, err,
		)
	}

	report.Removed = len(surplus)

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"removed":        report.Removed,
		"desired":        report.Desired,
	}).Info("kafka admin: obsolete subscriber ACL bindings removed")

	return report, nil
}

// GrantSubscriberAccess creates the bindings a subscriber's recorded authorization
// implies, and removes nothing.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the authorization to grant.
//
// Returns:
//   - SubscriberACLReconciliation: what was found and created. Removed is always zero.
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) GrantSubscriberAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (_ SubscriberACLReconciliation, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "grant_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	desired, principal, err := subscriberDesiredBindings(subscriber)
	if err != nil {
		return SubscriberACLReconciliation{}, err
	}

	report := SubscriberACLReconciliation{Principal: principal, Desired: len(desired)}

	if len(desired) == 0 {
		// Nothing to grant. Reported rather than silently skipped, because "authorised for
		// nothing" is a real state and an operator reading the log should see it stated.
		logrus.WithField("principal_hash", subscriberLogLabel(principal)).Info(
			"kafka admin: the subscriber's recorded authorization grants nothing, so no ACL binding " +
				"was created",
		)

		return report, nil
	}

	// Forward class: granting widens access, so it draws on the same bounded pool as
	// provisioning and leaves the compensating reserve untouched.
	ctx, releasePermit, err := a.admitAdminConversation(ctx, kafkaAdminForward)
	if err != nil {
		return report, err
	}

	defer releasePermit()

	observed, err := a.describeSubscriberACLs(ctx, principal)
	if err != nil {
		return report, err
	}

	present := make(map[string]struct{}, len(observed))
	for _, binding := range observed {
		present[aclBindingKey(binding)] = struct{}{}
		if blnkManagedACLBinding(binding) {
			report.Managed++

			continue
		}

		classifyForeignACLBinding(&report, binding)
	}

	// a WIDENING refuses, for the same reason issuance does. Converging a wider grant on a
	// principal whose effective access Blnk cannot state would report the registry and the
	// broker as agreeing when they demonstrably do not. Refusing leaves the subscriber
	// with the access it already had — less than the row now records, which is the
	// documented safe direction this whole three-step surface is built around, and which
	// the caller reports as retryable.
	if len(report.ForeignAllow) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignAllow,
		}).Error(
			"kafka admin: this principal carries ALLOW ACL bindings Blnk did not provision, so its " +
				"effective access is broader than the subscriber registry records; refusing to grant " +
				"further access until they are removed by hand",
		)

		return report, refuseForeignACLGrant(principal, report.ForeignAllow)
	}

	if len(report.ForeignDeny) > 0 {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"bindings":       report.ForeignDeny,
		}).Warn(
			"kafka admin: this principal carries DENY ACL bindings Blnk does not provision; they only " +
				"narrow the grant, so the requested bindings were still created",
		)
	}

	missing := make([]kafka.ACLEntry, 0, len(desired))
	for _, binding := range desired {
		if _, already := present[aclBindingKey(binding)]; already {
			continue
		}

		missing = append(missing, binding)
	}

	if len(missing) == 0 {
		return report, nil
	}

	if err := a.createACLBindings(ctx, principal, missing); err != nil {
		return report, err
	}

	report.Created = len(missing)

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"created":        report.Created,
		"desired":        report.Desired,
	}).Info("kafka admin: subscriber ACL bindings granted")

	return report, nil
}

// subscriberDesiredBindings derives the bindings a registry row implies, and the
// principal they belong to.
func subscriberDesiredBindings(subscriber *model.EventSubscriber) ([]kafka.ACLEntry, string, error) {
	if subscriber == nil {
		return nil, "", errors.New(
			"kafka admin: a subscriber is required to reconcile its ACL bindings",
		)
	}

	request := NewSubscriberProvisioningRequest(subscriber, "")

	principal := request.boundPrincipal()
	if principal == "" {
		return nil, "", errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so its ACL bindings cannot be reconciled",
		)
	}

	if err := request.validateTopics(); err != nil {
		return nil, principal, err
	}

	if err := request.validateHost(); err != nil {
		return nil, principal, err
	}

	return request.aclEntries(), principal, nil
}

// SubscriberCredentialExists reports whether a principal already holds a SCRAM-SHA-512
// credential.
//
// Parameters:
//   - ctx context.Context
//   - principal string: the SASL username.
//
// Returns:
//   - bool: true when a SHA-512 credential is present.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a blank principal, or a
//     wrapped broker error.
func (a *KafkaAdminClient) SubscriberCredentialExists(ctx context.Context, principal string) (_ bool, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "describe_subscriber_credential")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return false, err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return false, errors.New("kafka admin: a principal name is required to describe a SCRAM credential")
	}

	response, err := a.client.DescribeUserScramCredentials(ctx, &kafka.DescribeUserScramCredentialsRequest{
		Users: []kafka.UserScramCredentialsUser{{Name: principal}},
	})
	if err != nil {
		return false, fmt.Errorf("kafka admin: describing the SCRAM credential of principal %q: %w", principal, err)
	}

	if response.Error != nil {
		return false, fmt.Errorf(
			"kafka admin: broker refused to describe SCRAM credentials while looking up principal %q: %w",
			principal, response.Error,
		)
	}

	for _, outcome := range response.Results {
		if outcome.User != principal {
			continue
		}

		if outcome.Error != nil {
			if errors.Is(outcome.Error, kafka.ResourceNotFound) {
				return false, nil
			}

			return false, fmt.Errorf(
				"kafka admin: describing the SCRAM credential of principal %q: %w", principal, outcome.Error,
			)
		}

		for _, info := range outcome.CredentialInfos {
			if info.Mechanism == kafka.ScramMechanismSha512 {
				return true, nil
			}
		}
	}

	return false, nil
}

// AuthorizerActive reports whether the broker enforces ACLs.
//
// Parameters:
//   - ctx context.Context
//
// Returns:
//   - bool: true when the broker enforces ACLs, false when it explicitly reports
//     security disabled.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error when the question
//     could not be answered.
func (a *KafkaAdminClient) AuthorizerActive(ctx context.Context) (_ bool, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "describe_authorizer")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return false, err
	}

	// Every filter field left at its zero value matches anything: the name, principal and
	// host filters are nullable on the wire, and Any matches every pattern type, operation
	// and permission.
	response, err := a.client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        kafka.ResourceTypeTopic,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		},
	})
	if err != nil {
		return false, fmt.Errorf("kafka admin: probing whether the broker enforces ACLs: %w", err)
	}

	if response.Error != nil {
		if errors.Is(response.Error, kafka.SecurityDisabled) {
			return false, nil
		}

		return false, fmt.Errorf("kafka admin: probing whether the broker enforces ACLs: %w", response.Error)
	}

	return true, nil
}

// ErrAuthorizerNotEnforcing reports that the broker does not enforce ACLs, or that its
// enforcement could not be confirmed.
var ErrAuthorizerNotEnforcing = errors.New(
	"kafka admin: the broker's ACL enforcement is not confirmed, so no subscriber credential may be issued",
)

// ErrForeignACLGrantsAccess reports that the principal carries ACL bindings Blnk did
// not create which grant it access wider than its recorded authorization, so no
// credential may be issued.
var ErrForeignACLGrantsAccess = errors.New(
	"kafka admin: the principal holds ACL bindings Blnk did not create that grant access beyond its " +
		"recorded authorization, so no subscriber credential may be issued",
)

// ErrSubscriberKeyScopeBoundaryUnverified reports that a subscriber recording a
// partition-key prefix could not be provisioned with a boundary that actually keeps it,
// so no credential may be issued.
var ErrSubscriberKeyScopeBoundaryUnverified = errors.New(
	"kafka admin: a subscriber recording a partition-key prefix must be granted no record-level " +
		"Read, so that the declared key-authorising component is the only path its records can " +
		"take; the boundary could not be established, so no subscriber credential may be issued",
)

// bindingsGrantTopicRead reports whether a desired binding set includes Read on a
// TOPIC.
func bindingsGrantTopicRead(bindings []kafka.ACLEntry) bool {
	for _, binding := range bindings {
		if binding.ResourceType == kafka.ResourceTypeTopic &&
			binding.Operation == kafka.ACLOperationTypeRead &&
			binding.PermissionType == kafka.ACLPermissionTypeAllow {
			return true
		}
	}

	return false
}

// verifyKeyScopeBoundary is the check that a key-scoped subscriber's partition-key
// boundary is real before its credential can be returned.
func verifyKeyScopeBoundary(
	req SubscriberProvisioningRequest,
	bindings []kafka.ACLEntry,
	reconciliation SubscriberACLReconciliation,
	authorizerActive bool,
) error {
	if !req.KeyScoped {
		return nil
	}

	if bindingsGrantTopicRead(bindings) {
		return fmt.Errorf(
			"%w: the grant reconciled for principal %q includes Read on a topic, so the broker "+
				"would admit it to every record on that shared topic regardless of the recorded "+
				"partition-key prefix",
			ErrSubscriberKeyScopeBoundaryUnverified,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
		)
	}

	if !authorizerActive {
		return fmt.Errorf(
			"%w: the broker's ACL enforcement is not confirmed for principal %q, so withholding "+
				"Read withholds nothing",
			ErrSubscriberKeyScopeBoundaryUnverified,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
		)
	}

	if len(reconciliation.ForeignAllow) > 0 {
		return fmt.Errorf(
			"%w: principal %q carries %d ALLOW ACL binding(s) Blnk did not provision, which may "+
				"restore the record access this boundary withholds",
			ErrSubscriberKeyScopeBoundaryUnverified,
			sanitizeLogValue(reconciliation.Principal, maxLoggedFilterLength),
			len(reconciliation.ForeignAllow),
		)
	}

	return nil
}

// authorizerActiveCached answers "does this broker enforce ACLs" from a memoised probe.
func (a *KafkaAdminClient) authorizerActiveCached(ctx context.Context) (bool, error) {
	ttl := a.snapshotTTL()

	if ttl > 0 {
		a.cacheMu.Lock()
		probe := a.authorizerProbe
		a.cacheMu.Unlock()

		if !probe.checkedAt.IsZero() && a.clock().Sub(probe.checkedAt) < ttl {
			return probe.active, nil
		}
	}

	active, err := a.AuthorizerActive(ctx)
	if err != nil {
		return false, err
	}

	a.cacheMu.Lock()
	a.authorizerProbe = cachedAuthorizerProbe{active: active, checkedAt: a.clock()}
	a.cacheMu.Unlock()

	return active, nil
}

// requireEnforcedAuthorizer is the gate: no credential is written unless the broker has
// affirmatively confirmed that it enforces ACLs.
func (a *KafkaAdminClient) requireEnforcedAuthorizer(ctx context.Context, principal string) error {
	// The MEMOISED probe, not the raw one. Whether the broker enforces ACLs comes from its
	// own startup configuration, so asking on every provisioning call spends a serial
	// round trip out of the five-second budget to re-learn something already known. The
	// memo has a TTL and a failed probe is never cached, so the FAIL-CLOSED behaviour
	// below is unchanged: an unanswerable probe is still treated as an absent boundary.
	active, err := a.authorizerActiveCached(ctx)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"principal_hash": subscriberLogLabel(principal),
			"error_class":    kafkaErrorClassField("revoke_subscriber_principal", err),
		}).Error(
			"kafka admin: refusing to issue a subscriber credential because the broker's ACL enforcement " +
				"could not be confirmed. Confirm the broker runs " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer and that the " +
				"administrative principal is allowed to describe ACLs",
		)

		return fmt.Errorf(
			"%w: the enforcement probe for principal %q did not answer; an unverifiable boundary is "+
				"treated as an absent one. Grant the administrative principal Describe on the cluster, or "+
				"fix broker reachability, then retry",
			ErrAuthorizerNotEnforcing, principal,
		)
	}

	if !active {
		logrus.WithField("principal_hash", subscriberLogLabel(principal)).Error(
			"kafka admin: THE BROKER HAS NO AUTHORIZER CONFIGURED, so no credential was issued. ACL " +
				"bindings would be accepted and never applied, leaving every principal able to read every " +
				"topic including other subscribers' topics and the dead-letter topics. Start the broker with " +
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		)

		return fmt.Errorf(
			"%w: the broker reports that security is disabled, so ACL bindings for principal %q would be "+
				"accepted and never enforced. Start the broker with "+
				"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
			ErrAuthorizerNotEnforcing, principal,
		)
	}

	return nil
}

// kafkaCleanupBudget bounds a compensating broker operation.
const kafkaCleanupBudget = kafkaAdminRequestTimeout

// kafkaCleanupContext derives the context a compensating broker operation runs on.
func kafkaCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, kafkaCleanupBudget, true)
}

// compensateFailedProvisioning undoes a half-completed provisioning.
func (a *KafkaAdminClient) compensateFailedProvisioning(
	ctx context.Context,
	principal string,
	bindings []kafka.ACLEntry,
) error {
	logger := logrus.WithField("principal_hash", subscriberLogLabel(principal))

	ctx, cancel := kafkaCleanupContext(ctx)
	defer cancel()

	if err := a.deleteACLBindings(ctx, principal, bindings); err != nil {
		withKafkaError(logger, "compensate_delete_acl_bindings", err).Error(
			"kafka admin: could not remove the ACL bindings of a failed provisioning; " +
				"remove them manually with kafka-acls before reissuing",
		)
	}

	if err := a.RevokeSubscriberPrincipal(ctx, principal); err != nil {
		withKafkaError(logger, "compensate_revoke_subscriber_principal", err).Error(
			"kafka admin: A SCRAM CREDENTIAL WAS WRITTEN AND COULD NOT BE REVOKED after provisioning " +
				"failed. The principal can authenticate and is not recorded in the registry. Delete it " +
				"manually: kafka-configs --alter --delete-config SCRAM-SHA-512 --entity-type users " +
				"--entity-name <principal>",
		)

		return err
	}

	logger.Warn(
		"kafka admin: provisioning failed after the credential was written; the credential and its " +
			"attempted ACL bindings have been revoked, so no unbounded principal was left behind",
	)

	return nil
}

// RevokeSubscriberPrincipal deletes a subscriber's SCRAM credential.
//
// Parameters:
//   - ctx context.Context: cancels the request.
//   - principal string: the SASL username to delete. Required.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured, a validation error, or a wrapped broker error.
func (a *KafkaAdminClient) RevokeSubscriberPrincipal(ctx context.Context, principal string) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "revoke_subscriber_credential")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("kafka admin: a principal is required to revoke a SCRAM credential")
	}

	// From the compensating reserve: withdrawing a credential is the operation that must
	// not be starved by the attempts to issue more of them.
	ctx, releasePermit, err := a.admitAdminConversation(ctx, kafkaAdminCompensating)
	if err != nil {
		return err
	}

	defer releasePermit()

	response, err := a.client.AlterUserScramCredentials(ctx, &kafka.AlterUserScramCredentialsRequest{
		Deletions: []kafka.UserScramCredentialsDeletion{{
			Name:      principal,
			Mechanism: kafka.ScramMechanismSha512,
		}},
	})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, principal, err)
	}

	for i := range response.Results {
		resultErr := response.Results[i].Error
		if resultErr == nil || errors.Is(resultErr, kafka.ResourceNotFound) {
			// Nothing to delete is the desired end state, so it is success. This is what makes
			// revocation safe to retry, which both operators and the compensation path depend
			// on.
			continue
		}

		return fmt.Errorf("kafka admin: deleting the %s credential for principal %q: %w",
			SubscriberSASLMechanism, response.Results[i].User, resultErr)
	}

	logrus.WithField("principal_hash", subscriberLogLabel(principal)).
		Info("kafka admin: subscriber SCRAM credential revoked")

	return nil
}

// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential,
// ending its access at the broker.
//
// Parameters:
//   - ctx context.Context: cancels the requests.
//   - subscriber *model.EventSubscriber: the registry row. Nil is refused, because
//     there would be no principal to revoke.
//
// Returns:
//   - error: nil when the broker holds neither the bindings nor the credential.
func (a *KafkaAdminClient) RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "revoke_subscriber_access", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	if subscriber == nil {
		return errors.New("kafka admin: a subscriber is required to revoke its Kafka access")
	}

	// The password plays no part in a binding, so revocation builds the request without one.
	request := NewSubscriberProvisioningRequest(subscriber, "")
	principal := request.boundPrincipal()
	if principal == "" {
		return errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so there is nothing to revoke",
		)
	}

	// ONE permit for both round trips below, from the compensating reserve. Taken here
	// rather than left to the two calls it makes, so the pair is admitted together and
	// cannot half-complete because the reserve emptied between them; the permit is
	// reentrant, so RevokeSubscriberPrincipal runs inside this one.
	ctx, releasePermit, err := a.admitAdminConversation(ctx, kafkaAdminCompensating)
	if err != nil {
		return err
	}

	defer releasePermit()

	bindingErr := a.deleteAllPrincipalBindings(ctx, principal)
	credentialErr := a.RevokeSubscriberPrincipal(ctx, principal)

	switch {
	case credentialErr != nil:
		if bindingErr != nil {
			logrus.WithFields(logrus.Fields{
				"principal_hash": subscriberLogLabel(principal),
				"error_class":    kafkaErrorClassField("revoke_subscriber_acl_bindings", bindingErr),
			}).Error(
				"kafka admin: removing a subscriber's ACL bindings also failed; both need manual attention",
			)
		}

		return credentialErr
	case bindingErr != nil:
		return bindingErr
	default:
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(principal),
		}).Info("kafka admin: subscriber access revoked at the broker")

		return nil
	}
}

// ReconcileSubscriberACLs brings the broker's ACL bindings for one subscriber into line
// with the grant recorded on its registry row.
//
// Parameters:
//   - ctx context.Context: cancels the broker round trips.
//   - subscriber *model.EventSubscriber: the registry row AFTER the change, whose
//     authorized_topics and consumer group describe the desired state.
//   - revokedTopics []string: the topics removed from the grant, whose bindings must
//     go.
//
// Returns:
//   - error: ErrKafkaAdminNotConfigured when no broker is configured; the broker's
//     error when a revocation or a creation failed.
func (a *KafkaAdminClient) ReconcileSubscriberACLs(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	revokedTopics []string,
) (err error) {
	ctx, span := startKafkaAdminSpan(ctx, "reconcile_subscriber_acls", subscriberSpanAttribute(subscriber))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	if err := a.ready(ctx); err != nil {
		return err
	}

	if subscriber == nil {
		return errors.New("kafka admin: a subscriber is required to reconcile its ACL bindings")
	}

	// No password: this operation binds and unbinds, it never mints.
	desired := NewSubscriberProvisioningRequest(subscriber, "")
	principal := desired.boundPrincipal()
	if principal == "" {
		return errors.New(
			"kafka admin: the subscriber has neither a derivable nor a recorded Kafka principal, " +
				"so there are no ACL bindings to reconcile",
		)
	}

	// Forward class: this widens or restates access, so it queues with provisioning rather
	// than with the reserve that withdraws access.
	ctx, releasePermit, err := a.admitAdminConversation(ctx, kafkaAdminForward)
	if err != nil {
		return err
	}

	defer releasePermit()

	// The revoked bindings are built from a request carrying ONLY the removed topics, so
	// aclEntries — the single place a subscriber's binding shape is expressed — produces
	// the exact filters to delete. Restating the resource type, pattern type, host and
	// operation list here would be a second copy of that shape, and the two would drift.
	if revoked := normalizeTopicList(revokedTopics); len(revoked) > 0 {
		revocation := desired
		revocation.Topics = revoked
		// The consumer group namespace is NOT revoked: it is derived from the subscriber's
		// identity and survives every change to the topic list. Clearing it here would strip
		// the group binding on an ordinary topic edit and leave the subscriber unable to join
		// its own group.
		revocation.ConsumerGroupPrefix = ""

		if err := a.deleteACLBindings(ctx, principal, revocation.aclEntries()); err != nil {
			return fmt.Errorf(
				"kafka admin: revoking %d topic binding(s) for principal %q: %w",
				len(revoked), principal, err)
		}
	}

	bindings := desired.aclEntries()
	if len(bindings) == 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(principal),
			"revoked":            len(revokedTopics),
		}).Info(
			"kafka admin: subscriber ACL bindings reconciled; the grant is now empty, so the " +
				"principal can authenticate and read nothing",
		)

		return nil
	}

	if err := a.createACLBindings(ctx, principal, bindings); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(principal),
		"granted":            len(desired.normalizedTopics()),
		"revoked":            len(normalizeTopicList(revokedTopics)),
		"acl_entries":        len(bindings),
	}).Info("kafka admin: subscriber ACL bindings reconciled with the registry grant")

	return nil
}

// deleteACLBindings removes exactly the bindings it is given.
func (a *KafkaAdminClient) deleteACLBindings(
	ctx context.Context,
	principal string,
	bindings []kafka.ACLEntry,
) error {
	if len(bindings) == 0 {
		return nil
	}

	filters := make([]kafka.DeleteACLsFilter, 0, len(bindings))
	for _, binding := range bindings {
		filters = append(filters, kafka.DeleteACLsFilter{
			ResourceTypeFilter:        binding.ResourceType,
			ResourceNameFilter:        binding.ResourceName,
			ResourcePatternTypeFilter: binding.ResourcePatternType,
			PrincipalFilter:           binding.Principal,
			HostFilter:                binding.Host,
			Operation:                 binding.Operation,
			PermissionType:            binding.PermissionType,
		})
	}

	response, err := a.client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{Filters: filters})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting %d ACL bindings for principal %q: %w",
			len(filters), principal, err)
	}

	removed := 0
	for i := range response.Results {
		if response.Results[i].Error != nil {
			return fmt.Errorf("kafka admin: deleting ACL bindings for principal %q: %w",
				principal, response.Results[i].Error)
		}

		removed += len(response.Results[i].MatchingACLs)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(principal),
		"filters":        len(filters),
		"removed":        removed,
	}).Info("kafka admin: subscriber ACL bindings removed")

	return nil
}

// deleteAllPrincipalBindings removes EVERY ACL binding the broker holds for one
// principal, with a single match-any filter.
func (a *KafkaAdminClient) deleteAllPrincipalBindings(ctx context.Context, principal string) error {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return errors.New("kafka admin: a principal is required to delete its ACL bindings")
	}

	// Applied here rather than expected from the caller. Every other filter field on this
	// request is a match-any sentinel, so the principal is the ONE exact term the delete
	// turns on — and the failure mode of getting it wrong is silent: Kafka answers an
	// unmatched filter with a success carrying an empty MatchingACLs, so the revocation
	// reports clean while every binding stays live.
	bound := kafkaPrincipalPrefix + principal

	response, err := a.client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{
		Filters: []kafka.DeleteACLsFilter{{
			ResourceTypeFilter:        kafka.ResourceTypeAny,
			ResourceNameFilter:        "",
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			PrincipalFilter:           bound,
			HostFilter:                "",
			Operation:                 kafka.ACLOperationTypeAny,
			PermissionType:            kafka.ACLPermissionTypeAny,
		}},
	})
	if err != nil {
		return fmt.Errorf("kafka admin: deleting every ACL binding for principal %q: %w", bound, err)
	}

	removed := 0
	for i := range response.Results {
		if response.Results[i].Error != nil {
			return fmt.Errorf("kafka admin: deleting every ACL binding for principal %q: %w",
				bound, response.Results[i].Error)
		}

		removed += len(response.Results[i].MatchingACLs)
	}

	logrus.WithFields(logrus.Fields{
		"principal_hash": subscriberLogLabel(bound),
		"removed":        removed,
	}).Info("kafka admin: every ACL binding for the subscriber principal removed")

	return nil
}
