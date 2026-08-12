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

package database

import (
	"context"
	"database/sql"
	"errors"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------------------
// Driver errors must not become API response bodies

// postgresSQLState extracts the five-character SQLSTATE from a driver error.
func postgresSQLState(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}

	return "unknown"
}

// ---------------------------------------------------------------------------------------
// The driver's own words must not reach the standard log or a span either

// The bounded classes a database failure is reported as. Every value is a fixed literal
// and the set is closed, which is what makes them safe as a log field and as a span
// status.
const (
	databaseErrorClassUnknown       = "unknown"
	databaseErrorClassNoRows        = "no_rows"
	databaseErrorClassCancelled     = "context_cancelled"
	databaseErrorClassDeadline      = "context_deadline_exceeded"
	databaseErrorClassConnection    = "connection"
	databaseErrorClassData          = "data_exception"
	databaseErrorClassIntegrity     = "integrity_constraint"
	databaseErrorClassContention    = "serialization_or_deadlock"
	databaseErrorClassSchemaOrGrant = "syntax_or_access_rule"
	databaseErrorClassResources     = "insufficient_resources"
	databaseErrorClassIntervention  = "operator_intervention"
	databaseErrorClassServer        = "internal_server_error"
	databaseErrorClassDriver        = "driver"
	databaseErrorClassApplication   = "application"
)

// databaseErrorClass maps a failure to one of the bounded classes above.
func databaseErrorClass(err error) string {
	var apiErr apierror.APIError

	switch {
	case err == nil:
		return databaseErrorClassUnknown
	case errors.Is(err, sql.ErrNoRows):
		return databaseErrorClassNoRows
	case errors.Is(err, context.Canceled):
		return databaseErrorClassCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return databaseErrorClassDeadline
	case errors.As(err, &apiErr):
		return "api:" + string(apiErr.Code)
	}

	state := postgresSQLState(err)
	if len(state) < 2 {
		return databaseErrorClassDriver
	}

	switch state[:2] {
	case "08":
		return databaseErrorClassConnection
	case "22":
		return databaseErrorClassData
	case "23":
		return databaseErrorClassIntegrity
	case "40":
		return databaseErrorClassContention
	case "42":
		return databaseErrorClassSchemaOrGrant
	case "53":
		return databaseErrorClassResources
	case "57":
		return databaseErrorClassIntervention
	case "XX":
		return databaseErrorClassServer
	default:
		return databaseErrorClassDriver
	}
}

// ErrorClass is the exported form of databaseErrorClass, for a caller OUTSIDE this
// package that has to log a failure this package returned.
//
// Parameters:
//   - err error: the failure to classify. May be nil or wrapped.
//
// Returns:
//   - string: a bounded class. Never empty.
func ErrorClass(err error) string {
	return databaseErrorClass(err)
}

// SQLState is the exported form of postgresSQLState, so a caller outside this package
// can carry the standardised five-character error class beside the bounded class from
// ErrorClass.
//
// Parameters:
//   - err error: the failure to inspect. May be nil or wrapped.
//
// Returns:
//   - string: the SQLSTATE, or "unknown".
func SQLState(err error) string {
	return postgresSQLState(err)
}

// LogDiagnostic is the exported form of logDatabaseDiagnostic, so a caller outside this package
// routes a raw cause to the same trace-level sink rather than to its own log line.
//
// Parameters:
//   - operation string: a fixed literal naming what was attempted.
//   - cause error: the raw failure. A nil cause is a no-op.
func LogDiagnostic(operation string, cause error) {
	logDatabaseDiagnostic(operation, cause)
}

// logDatabaseDiagnostic sends the raw cause to the trace-level diagnostic sink.
func logDatabaseDiagnostic(operation string, cause error) {
	if cause == nil || !logrus.IsLevelEnabled(logrus.TraceLevel) {
		return
	}

	// THE RAW CAUSE IS ATTACHED VERBATIM HERE, and only here. This sink exists precisely
	// so the driver's or client's own text is reachable when an operator asks for it
	// explicitly, which is why it is gated on the trace level and why it does NOT go
	// through the redacting helper every other site uses — redacting the one place the
	// full text is supposed to be available would leave it available nowhere.
	logrus.WithField("operation", operation).WithField(logrus.ErrorKey, cause).Trace(
		"database diagnostic: the driver's own error text, which may name schema objects, " +
			"constraints and broker or host addresses and is therefore emitted at trace only",
	)
}

// failDatabaseSpan marks a span failed with a bounded class instead of recording the
// raw error.
func failDatabaseSpan(span trace.Span, cause error) {
	if span == nil || cause == nil {
		return
	}

	class := databaseErrorClass(cause)
	span.SetAttributes(
		attribute.String("db.error_class", class),
		attribute.String("db.sqlstate", postgresSQLState(cause)),
	)
	span.SetStatus(codes.Error, class)
}

// loggedDatabaseError logs a driver-origin failure as a bounded class and returns a
// typed API error that carries NO details.
func loggedDatabaseError(code apierror.ErrorCode, message, operation string, cause error) error {
	logrus.WithFields(logrus.Fields{
		"operation":   operation,
		"error_class": databaseErrorClass(cause),
		"sqlstate":    postgresSQLState(cause),
		"code":        string(code),
	}).Error(message)
	logDatabaseDiagnostic(operation, cause)

	// The code is carried through UNNORMALIZED, exactly as apierror.NewAPIError does.
	return apierror.APIError{Code: code, Message: message}
}

// withLoggableCause attaches a driver or dependency error to a log entry in the two
// renderings an operator needs, and it is the ONLY way the event repositories should
// put an error into a line.
func withLoggableCause(entry *logrus.Entry, err error) *logrus.Entry {
	if entry == nil {
		entry = logrus.NewEntry(logrus.StandardLogger())
	}

	if err == nil {
		return entry
	}

	entry = entry.WithField("cause", logsafe.Cause(err))

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		entry = entry.WithField("cause_verbatim", logsafe.CauseVerbatim(err))
	}

	return entry
}
