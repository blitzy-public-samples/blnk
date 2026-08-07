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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/internal/tokenization"
	"github.com/blnkfinance/blnk/model"
)

// postIdentityActions performs actions after an identity has been created.
// It sends the newly created identity to the search index queue.
//
// IT NO LONGER CAPTURES THE EVENT, and that is the point rather than an omission. The
// identity.created row is now inserted INSIDE the transaction that inserts the identity, by
// the repository, from the preparer identityCreatedEventPreparer supplies — so the event and
// the identity commit together instead of the event being written from a goroutine after the
// fact. Capturing it here as well would publish the same event twice, and event_id is derived
// from the identity's own identity, so the second insert would be refused by the unique index
// and the only visible result would be a logged conflict on every identity creation.
//
// Indexing stays here because it is genuinely post-commit work: TypeSense is a separate
// system with its own retry queue, and nothing about it belongs in a ledger transaction.
//
// # THE ONE CASE IT STILL PUBLISHES
//
// A webhook-only deployment — a webhook URL and no KAFKA_BROKERS — gets NO preparer, because
// capturing rows no relay can drain is what eventCaptureEnabled exists to avoid. Nothing
// captures the event on that shape, so the legacy publish is retained here as a fallback for
// it alone, exactly as postTransactionActions retains one for a transaction its atomic writer
// did not record. Without it this deployment lost identity.created from BOTH transports.
//
// publishEntityEventWhenUncaptured owns that decision and returns immediately whenever a
// preparer was supplied, so the atomic capture and this call can never both run.
//
// Parameters:
//   - ctx context.Context: the creating request's context. Detached from cancellation before
//     it is handed to the publish, which outlives the request that spawned it.
//   - identity *model.Identity: A pointer to the newly created Identity model.
func (l *Blnk) postIdentityActions(ctx context.Context, identity *model.Identity) {
	// Derived outside the goroutine, while ctx is still live. See postLedgerActions.
	publishCtx := context.WithoutCancel(ctx)

	go func() {
		err := l.queue.queueIndexData(identity.IdentityID, "identities", identity)
		if err != nil {
			notification.NotifyError(err)
		}
		err = l.publishEntityEventWhenUncaptured(publishCtx, identity.IdentityID, NewWebhook{
			Event:   "identity.created",
			Payload: identity,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

// identityCreatedEventPreparer returns the preparer that builds the identity.created outbox
// row, for the repository to insert INSIDE the transaction that inserts the identity.
//
// # Why the event is captured through a callback rather than published here
//
// It used to be published from postIdentityActions, in a goroutine, after CreateIdentity had
// already committed — so an identity could be durable while the event announcing it was
// lost to a crash or a failed insert, with nothing left to replay from. Requirement R-2
// exists to close exactly that window, and the event now commits with the identity or not at
// all.
//
// The callback shape is forced by where the identity id comes from: the repository resolves
// it during the insert — minting idt_<uuid> unless the caller supplied a canonical one — and
// stamps CreatedAt, and both the payload and the event's aggregate id are derived from the
// finished identity. See database.EventPreparer.
//
// # The payload is forwarded VERBATIM, PII included
//
// The event string is still "identity.created" and the payload is still the created
// *model.Identity, unredacted, exactly as the HTTP webhook carried it. Filtering or
// detokenizing it here would change what subscribers receive relative to the HTTP era and
// break the equivalence the transport substitution is measured by; narrowing that exposure
// is a separate, deliberate change to the contract rather than a side effect of moving
// transports.
//
// An identity has NO ledger — it is not a ledger-scoped entity — so no WithEventLedgerID is
// supplied and ledger_id is stored as SQL NULL. The partition key falls back to the identity
// id through the documented chain in PrepareEventOutbox, which gives one identity's events a
// stable partition and therefore a total order among themselves.
//
// The `identity.created` event is captured atomically with the identity row: the capture
// handed to the datasource is invoked with the finalised identity and its row is inserted
// inside the same database transaction, so the identity and its event commit or roll back
// together.
//
// Parameters:
//   - ctx context.Context: the creating request's context, captured for tracing only. The
//     preparer performs no I/O.
//
// Returns:
//   - database.EventPreparer[model.Identity]: the preparer to hand to the repository.
func (l *Blnk) identityCreatedEventPreparer(ctx context.Context) database.EventPreparer[model.Identity] {
	// A NIL PREPARER when nothing is configured, so the repository stays on its
	// single-statement path instead of opening a transaction to insert no event. See
	// eventCaptureEnabled.
	if !l.eventCaptureEnabled() {
		return nil
	}

	return func(created model.Identity) (*model.EventOutbox, error) {
		return l.PrepareEventOutbox(ctx, NewWebhook{
			Event:   "identity.created",
			Payload: &created,
		})
	}
}

// CreateIdentity creates a new identity together with its identity.created event, atomically.
//
// The event preparer is handed to the repository, which inserts the identity and the event
// row in one transaction (requirement R-2). A failure to prepare or insert the event
// therefore fails the creation, and the caller sees no identity — which is the correct
// outcome: an identity whose event was lost is one no subscriber knows exists, and the
// alternative trades a visible failure for an invisible one.
//
// postIdentityActions then performs the remaining post-commit work, which is indexing only.
//
// Parameters:
// # This is requirement R-2 applied to identity creation
//
// The identity row and its identity.created event are written in ONE database transaction:
// the event builder is invoked inside it with the created identity, so the payload carries
// the generated id the legacy webhook body carried, and an event that cannot be recorded
// rolls the identity back rather than leaving it committed and unannounced.
//
// - identity model.Identity: The Identity model to be created.
//
// Returns:
// - model.Identity: The created Identity model.
// - error: An error if the identity could not be created, or if its event could not be captured.
func (l *Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	ctx := context.Background()

	identity, err := l.datasource.CreateIdentity(identity, l.identityCreatedEventPreparer(ctx))
	if err != nil {
		return model.Identity{}, err
	}
	l.postIdentityActions(ctx, &identity)
	return identity, nil
}

// GetIdentity retrieves an identity by its ID.
//
// Parameters:
// - id string: The ID of the identity to retrieve.
//
// Returns:
// - *model.Identity: A pointer to the Identity model if found.
// - error: An error if the identity could not be retrieved.
func (l *Blnk) GetIdentity(id string) (*model.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

// GetAllIdentities retrieves all identities from the database.
//
// Returns:
// - []model.Identity: A slice of Identity models.
// - error: An error if the identities could not be retrieved.
func (l *Blnk) GetAllIdentities() ([]model.Identity, error) {
	return l.datasource.GetAllIdentities()
}

// GetAllIdentitiesWithFilter retrieves identities using advanced filters.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - filters *filter.QueryFilterSet: Filter conditions to apply.
// - limit int: Maximum number of identities to return.
// - offset int: Offset for pagination.
//
// Returns:
// - []model.Identity: A slice of Identity models matching the filter criteria.
// - error: An error if the identities could not be retrieved.
func (l *Blnk) GetAllIdentitiesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Identity, error) {
	return l.datasource.GetAllIdentitiesWithFilter(ctx, filters, limit, offset)
}

// GetAllIdentitiesWithFilterAndOptions retrieves identities with filters, sorting, and optional count.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - filters *filter.QueryFilterSet: Filter conditions to apply.
// - opts *filter.QueryOptions: Query options including sorting and count settings.
// - limit int: Maximum number of identities to return.
// - offset int: Offset for pagination.
//
// Returns:
// - []model.Identity: A slice of Identity models matching the filter criteria.
// - *int64: Optional total count of matching records (if opts.IncludeCount is true).
// - error: An error if the identities could not be retrieved.
func (l *Blnk) GetAllIdentitiesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Identity, *int64, error) {
	return l.datasource.GetAllIdentitiesWithFilterAndOptions(ctx, filters, opts, limit, offset)
}

// UpdateIdentity updates an existing identity in the database.
//
// Parameters:
// - identity *model.Identity: A pointer to the Identity model to be updated.
//
// Returns:
// - error: An error if the identity could not be updated.
func (l *Blnk) UpdateIdentity(identity *model.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

// DeleteIdentity deletes an identity by its ID.
//
// Parameters:
// - id string: The ID of the identity to delete.
//
// Returns:
// - error: An error if the identity could not be deleted.
func (l *Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}

// TokenizeIdentityField tokenizes a specific field in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fieldName string: The name of the field to tokenize.
//
// Returns:
// - error: An error if the field could not be tokenized.
func (l *Blnk) TokenizeIdentityField(identityID, fieldName string) error {
	// Convert field name to struct field format for reflection
	structFieldName := convertToStructFieldName(fieldName)

	// Check if field is tokenizable
	validField := false
	for _, field := range tokenization.TokenizableFields {
		if field == structFieldName {
			validField = true
			break
		}
	}

	if !validField {
		return fmt.Errorf("field %s is not tokenizable", fieldName)
	}

	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return err
	}

	// Check if field is already tokenized using the original field name
	// as IsFieldTokenized will handle the conversion internally
	if identity.IsFieldTokenized(fieldName) {
		return fmt.Errorf("field %s is already tokenized", fieldName)
	}

	// Get the field value using reflection with struct field name
	val := reflect.ValueOf(identity).Elem()
	fieldVal := val.FieldByName(structFieldName)

	if !fieldVal.IsValid() || !fieldVal.CanSet() {
		return fmt.Errorf("field %s not found or cannot be set", fieldName)
	}

	// Get the string value
	strVal := fieldVal.String()

	// Tokenize the value
	token, err := l.tokenizer.TokenizeWithMode(strVal, tokenization.FormatPreservingMode)
	if err != nil {
		return err
	}

	// Set the tokenized value
	fieldVal.SetString(token)

	// Mark the field as tokenized using the original field name
	// as MarkFieldAsTokenized will handle the conversion internally
	identity.MarkFieldAsTokenized(fieldName)

	// Update the identity
	return l.UpdateIdentity(identity)
}

// DetokenizeIdentityField detokenizes a specific field in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fieldName string: The name of the field to detokenize.
//
// Returns:
// - string: The detokenized field value.
// - error: An error if the field could not be detokenized.
func (l *Blnk) DetokenizeIdentityField(identityID, fieldName string) (string, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return "", err
	}

	// Try both original and capitalized field name
	structFieldName := convertToStructFieldName(fieldName)

	// Check if field is tokenized
	if !identity.IsFieldTokenized(fieldName) && !identity.IsFieldTokenized(structFieldName) {
		// Debug info
		if identity.MetaData != nil {
			metaStr, _ := json.Marshal(identity.MetaData)
			return "", fmt.Errorf("field %s is not tokenized. Metadata: %s", fieldName, metaStr)
		}
		return "", fmt.Errorf("field %s is not tokenized", fieldName)
	}

	// Get the field value using reflection - try both field name versions
	val := reflect.ValueOf(identity).Elem()
	fieldVal := val.FieldByName(structFieldName)

	if !fieldVal.IsValid() {
		fieldVal = val.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			return "", fmt.Errorf("field %s not found", fieldName)
		}
	}

	// Get the tokenized value
	tokenVal := fieldVal.String()

	// Detokenize the value
	originalValue, err := l.tokenizer.Detokenize(tokenVal)
	if err != nil {
		return "", err
	}

	return originalValue, nil
}

// TokenizeIdentity tokenizes all specified fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fields []string: The names of the fields to tokenize.
//
// Returns:
// - error: An error if any field could not be tokenized.
func (l *Blnk) TokenizeIdentity(identityID string, fields []string) error {
	for _, field := range fields {
		err := l.TokenizeIdentityField(identityID, field)
		if err != nil {
			return err
		}
	}
	return nil
}

// DetokenizeIdentity detokenizes and returns all tokenized fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - map[string]string: A map of field names to their detokenized values.
// - error: An error if any field could not be detokenized.
func (l *Blnk) DetokenizeIdentity(identityID string) (map[string]string, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)

	// Check each tokenized field in metadata. The map is stored as
	// map[string]bool in memory but unmarshals from the database as
	// map[string]interface{}, so both shapes must be handled.
	if identity.MetaData != nil {
		tokenized := make(map[string]bool)
		switch fields := identity.MetaData["tokenized_fields"].(type) {
		case map[string]bool:
			tokenized = fields
		case map[string]interface{}:
			for fieldName, val := range fields {
				if boolVal, ok := val.(bool); ok {
					tokenized[fieldName] = boolVal
				}
			}
		}
		for fieldName, isTokenized := range tokenized {
			if isTokenized {
				originalValue, err := l.DetokenizeIdentityField(identityID, fieldName)
				if err != nil {
					return nil, err
				}
				result[fieldName] = originalValue
			}
		}
	}

	return result, nil
}

// TokenizeAllPII tokenizes all eligible PII fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - error: An error if any field could not be tokenized.
func (l *Blnk) TokenizeAllPII(identityID string) error {
	for _, field := range tokenization.TokenizableFields {
		// Ignore errors for fields that might already be tokenized
		_ = l.TokenizeIdentityField(identityID, field)
	}
	return nil
}

// GetDetokenizedIdentity returns a copy of the identity with all fields detokenized.
// Note: This does not modify the stored identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - *model.Identity: A pointer to the detokenized Identity model.
// - error: An error if the identity could not be detokenized.
func (l *Blnk) GetDetokenizedIdentity(identityID string) (*model.Identity, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return nil, err
	}

	// Create a copy
	detokenizedIdentity := *identity

	// Detokenize all tokenized fields
	if identity.MetaData != nil {
		tokenizedFields, ok := identity.MetaData["tokenized_fields"].(map[string]bool)
		if ok {
			for field, isTokenized := range tokenizedFields {
				if isTokenized {
					originalValue, err := l.DetokenizeIdentityField(identityID, field)
					if err != nil {
						return nil, err
					}

					// Set the original value in the copy
					val := reflect.ValueOf(&detokenizedIdentity).Elem()
					fieldVal := val.FieldByName(field)
					if fieldVal.IsValid() && fieldVal.CanSet() {
						fieldVal.SetString(originalValue)
					}
				}
			}
		}
	}

	// Create a clean copy of metadata without tokenized_fields
	if detokenizedIdentity.MetaData != nil {
		newMetaData := make(map[string]interface{})
		for k, v := range detokenizedIdentity.MetaData {
			if k != "tokenized_fields" {
				newMetaData[k] = v
			}
		}
		detokenizedIdentity.MetaData = newMetaData
	}

	return &detokenizedIdentity, nil
}

// convertToStructFieldName ensures consistent field name format by returning
// the Go struct field name (typically capitalized) for the given input
func convertToStructFieldName(fieldName string) string {
	// For simple cases, just capitalize the first letter
	if len(fieldName) > 0 {
		return strings.ToUpper(fieldName[0:1]) + fieldName[1:]
	}
	return fieldName
}
