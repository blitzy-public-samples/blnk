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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/internal/apierror"
)

// event_error_telemetry_test.go covers DATA-02: the driver's own words must not reach the
// standard log or a span.
//
// DATA-01 stopped a *pq.Error being serialised into an API RESPONSE. The same value was still
// rendered into an "error" log field and written as a span's exception.message, and both of
// those leave the process — the log to an indexed aggregator, the span to a trace backend that
// is routinely readable by more people than the database is. A PostgreSQL message names the
// schema, the table, the column, the violated constraint and the server source file that raised
// it; a connection failure names the host and port.
//
// The fixture below is therefore a fully populated *pq.Error rather than a bare errors.New:
// every field it carries is a thing that must not appear, and asserting on a stub would prove
// nothing about the case that matters.

// disclosivePostgresError is a driver error carrying every field lib/pq exposes.
//
// Modelled on a real unique-violation, because that is the most common driver error this
// repository produces and the one whose message is richest in schema detail.
func disclosivePostgresError() *pq.Error {
	return &pq.Error{
		Severity:   "ERROR",
		Code:       "23505",
		Message:    `duplicate key value violates unique constraint "uq_event_outbox_event_id"`,
		Detail:     `Key (event_id)=(3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b) already exists.`,
		Hint:       "consider an upsert",
		Schema:     "blnk",
		Table:      "event_outbox",
		Column:     "event_id",
		Constraint: "uq_event_outbox_event_id",
		File:       "/build/postgresql-16/src/backend/access/nbtree/nbtinsert.c",
		Line:       "666",
		Routine:    "_bt_check_unique",
	}
}

// disclosiveSubstrings is what must never appear in a bounded log line or a span status.
//
// Every entry is a value that can ONLY have come from the driver. The bare table name
// "event_outbox" is deliberately absent even though a *pq.Error carries it: Blnk's own fixed
// operation literals are named after the operations they describe — "insert_event_outbox" —
// and forbidding the substring would flag a value this code chose and controls, which is the
// false positive that makes a rule like this get deleted rather than fixed. The constraint
// name, the server source path and the routine are unambiguous, and they are the parts that
// actually map the database.
func disclosiveSubstrings() []string {
	return []string{
		"uq_event_outbox_event_id",
		"duplicate key value",
		"3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		"/build/postgresql-16",
		"_bt_check_unique",
		"consider an upsert",
	}
}

// TestDatabaseErrorClass_MapsEverySQLStateClassAndTheGoLevelConditions pins the vocabulary.
//
// The SQLSTATE CLASS is what decides where an operator looks — 08 is the network or the pool,
// 23 is the data, 40 is contention a retry resolves, 42 is the schema or the grant and is a
// genuine deployment fault — so each is asserted rather than lumped into "some class".
func TestDatabaseErrorClass_MapsEverySQLStateClassAndTheGoLevelConditions(t *testing.T) {
	apiErr := apierror.APIError{Code: apierror.ErrSubscriberNotFound, Message: "Subscriber not found"}

	cases := map[string]struct {
		cause error
		want  string
	}{
		"a nil cause still yields a value, so a log field is always present": {
			cause: nil, want: databaseErrorClassUnknown,
		},
		"no rows is not a fault on the paths that use RETURNING to detect a lost claim": {
			cause: sql.ErrNoRows, want: databaseErrorClassNoRows,
		},
		"a wrapped no-rows classifies identically": {
			cause: fmt.Errorf("scanning: %w", sql.ErrNoRows), want: databaseErrorClassNoRows,
		},
		"a cancelled caller is not a database fault": {
			cause: context.Canceled, want: databaseErrorClassCancelled,
		},
		"an expired statement budget is its own condition": {
			cause: context.DeadlineExceeded, want: databaseErrorClassDeadline,
		},
		"a typed application error is classified by its CODE and never by its message": {
			cause: apiErr, want: "api:" + string(apierror.ErrSubscriberNotFound),
		},
		"connection class 08": {
			cause: &pq.Error{Code: "08006"}, want: databaseErrorClassConnection,
		},
		"data exception class 22": {
			cause: &pq.Error{Code: "22001"}, want: databaseErrorClassData,
		},
		"integrity constraint class 23": {
			cause: disclosivePostgresError(), want: databaseErrorClassIntegrity,
		},
		"serialization or deadlock class 40": {
			cause: &pq.Error{Code: "40P01"}, want: databaseErrorClassContention,
		},
		"syntax or access class 42, which is a deployment fault rather than data": {
			cause: &pq.Error{Code: "42P01"}, want: databaseErrorClassSchemaOrGrant,
		},
		"insufficient resources class 53": {
			cause: &pq.Error{Code: "53300"}, want: databaseErrorClassResources,
		},
		"operator intervention class 57": {
			cause: &pq.Error{Code: "57014"}, want: databaseErrorClassIntervention,
		},
		"internal server class XX": {
			cause: &pq.Error{Code: "XX000"}, want: databaseErrorClassServer,
		},
		"an unrecognised SQLSTATE class is still the driver rather than unknown": {
			cause: &pq.Error{Code: "99999"}, want: databaseErrorClassDriver,
		},
		"an error that is not a driver error at all": {
			cause: errors.New("dial unix /var/run/postgresql/.s.PGSQL.5432: connect: no such file"),
			want:  databaseErrorClassDriver,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, databaseErrorClass(tc.cause))
		})
	}

	t.Run("a wrapped driver error keeps its class, because errors.As unwraps", func(t *testing.T) {
		wrapped := fmt.Errorf("inserting the outbox row: %w", disclosivePostgresError())
		assert.Equal(t, databaseErrorClassIntegrity, databaseErrorClass(wrapped))
		assert.Equal(t, "23505", postgresSQLState(wrapped))
	})

	t.Run("no class ever contains any part of the cause", func(t *testing.T) {
		class := databaseErrorClass(disclosivePostgresError())
		for _, secret := range disclosiveSubstrings() {
			assert.NotContains(t, class, secret,
				"every class is a fixed literal, so no part of the cause may appear in it")
		}
	})
}

// TestLoggedDatabaseError_LogsTheClassAndNotTheDriversWords is the log half of DATA-02.
//
// Both the field NAME and every field VALUE are inspected. Asserting only that there is no
// field called "error" would pass for an implementation that renamed the field and kept the
// text, which is the mistake this guards.
func TestLoggedDatabaseError_LogsTheClassAndNotTheDriversWords(t *testing.T) {
	restore := pinDatabaseLogLevel(t, logrus.InfoLevel)
	defer restore()

	hook := logtest.NewGlobal()
	cause := disclosivePostgresError()

	err := loggedDatabaseError(apierror.ErrInternalServer,
		"Failed to record the event in the outbox", "insert_event_outbox", cause)

	entries := hook.AllEntries()
	require.Len(t, entries, 1, "exactly one bounded line, and no diagnostic below trace")

	entry := entries[0]
	assert.Equal(t, "Failed to record the event in the outbox", entry.Message)
	assert.Equal(t, "insert_event_outbox", entry.Data["operation"])
	assert.Equal(t, databaseErrorClassIntegrity, entry.Data["error_class"])
	assert.Equal(t, "23505", entry.Data["sqlstate"],
		"the standardised five characters travel beside the coarse class, so nothing is lost")
	assert.NotContains(t, entry.Data, logrus.ErrorKey,
		"logrus.WithError is what rendered the driver's message, and it must be gone")

	rendered := fmt.Sprint(entry.Data) + entry.Message
	for _, secret := range disclosiveSubstrings() {
		assert.NotContains(t, rendered, secret,
			"no field of the bounded line may carry %q", secret)
	}

	t.Run("the returned error carries a code and a message and no details", func(t *testing.T) {
		var typed apierror.APIError
		require.ErrorAs(t, err, &typed)
		assert.Equal(t, apierror.ErrInternalServer, typed.Code)
		assert.Nil(t, typed.Details, "DATA-01: Details is what reached the response body")
		for _, secret := range disclosiveSubstrings() {
			assert.NotContains(t, typed.Error(), secret)
		}
	})
}

// TestLogDatabaseDiagnostic_EmitsTheRawCauseOnlyAtTrace proves the sink is separate from the
// level operators already raise.
//
// DEBUG is asserted SILENT. That is the substance of the design rather than a detail: debug is
// already the event pipeline's routine per-event volume, so if the sink lived there, following
// a delivery would begin shipping schema detail as a side effect.
func TestLogDatabaseDiagnostic_EmitsTheRawCauseOnlyAtTrace(t *testing.T) {
	cause := disclosivePostgresError()

	for _, level := range []logrus.Level{logrus.InfoLevel, logrus.DebugLevel} {
		t.Run("silent at "+level.String(), func(t *testing.T) {
			restore := pinDatabaseLogLevel(t, level)
			defer restore()

			hook := logtest.NewGlobal()
			logDatabaseDiagnostic("insert_event_outbox", cause)

			assert.Empty(t, hook.AllEntries())
		})
	}

	t.Run("emitted at trace with the operation for correlation", func(t *testing.T) {
		restore := pinDatabaseLogLevel(t, logrus.TraceLevel)
		defer restore()

		hook := logtest.NewGlobal()
		logDatabaseDiagnostic("insert_event_outbox", cause)

		entries := hook.AllEntries()
		require.Len(t, entries, 1)
		assert.Equal(t, logrus.TraceLevel, entries[0].Level)
		assert.Equal(t, "insert_event_outbox", entries[0].Data["operation"])
		assert.Contains(t, fmt.Sprint(entries[0].Data[logrus.ErrorKey]), "uq_event_outbox_event_id",
			"the sink exists precisely so the raw text is reachable when asked for explicitly")
	})

	t.Run("a nil cause is a no-op even at trace", func(t *testing.T) {
		restore := pinDatabaseLogLevel(t, logrus.TraceLevel)
		defer restore()

		hook := logtest.NewGlobal()
		logDatabaseDiagnostic("insert_event_outbox", nil)

		assert.Empty(t, hook.AllEntries())
	})
}

// TestFailDatabaseSpan_SetsABoundedStatusAndRecordsNoException is the trace half of DATA-02.
//
// A real SDK span is recorded and its exported snapshot inspected, because the defect is only
// observable in what the exporter emits: RecordError adds an EVENT, not an attribute, so a test
// that looked only at attributes would miss the disclosure entirely.
func TestFailDatabaseSpan_SetsABoundedStatusAndRecordsNoException(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()

	_, span := provider.Tracer("test").Start(context.Background(), "InsertEventOutbox")
	failDatabaseSpan(span, disclosivePostgresError())
	span.End()

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	snapshot := ended[0]

	assert.Empty(t, snapshot.Events(),
		"span.RecordError adds an exception event carrying the error's own text; there must be none")

	assert.Equal(t, databaseErrorClassIntegrity, snapshot.Status().Description,
		"the status description is the bounded class")

	attributes := map[string]string{}
	for _, attribute := range snapshot.Attributes() {
		attributes[string(attribute.Key)] = attribute.Value.AsString()
	}
	assert.Equal(t, databaseErrorClassIntegrity, attributes["db.error_class"])
	assert.Equal(t, "23505", attributes["db.sqlstate"],
		"set as an attribute rather than folded into the description, so a backend can facet on it")

	rendered := fmt.Sprint(snapshot.Attributes()) + snapshot.Status().Description
	for _, secret := range disclosiveSubstrings() {
		assert.NotContains(t, rendered, secret)
	}

	t.Run("a nil cause leaves the span unset, so one call site can route both outcomes", func(t *testing.T) {
		_, clean := provider.Tracer("test").Start(context.Background(), "Clean")
		failDatabaseSpan(clean, nil)
		clean.End()

		ended := recorder.Ended()
		last := ended[len(ended)-1]
		assert.NotEqual(t, "Error", last.Status().Code.String())
		assert.Empty(t, last.Events())
	})

	t.Run("a nil span is tolerated", func(t *testing.T) {
		assert.NotPanics(t, func() { failDatabaseSpan(nil, sql.ErrNoRows) })
	})
}

// TestNeitherEventRepositoryCallsSpanRecordError is the lasting guard.
//
// The two repositories hold a hundred failure paths between them, and a single reintroduced
// span.RecordError re-opens the disclosure on whichever path nobody happened to exercise. No
// runtime assertion can see an unexercised call, so the absence is asserted over the source.
//
// Comments are excluded from the parse deliberately: the DATA-02 note explains what
// span.RecordError did, and a text scan would count the explanation as the violation.
func TestNeitherEventRepositoryCallsSpanRecordError(t *testing.T) {
	for _, name := range []string{"event_outbox.go", "event_subscriber.go"} {
		t.Run(name, func(t *testing.T) {
			calls := recordErrorCallCount(t, name)
			assert.Zero(t, calls,
				"%s calls span.RecordError %d time(s); every failure must go through "+
					"failDatabaseSpan, which sets a bounded class instead of writing the error's "+
					"own text as an exception event", name, calls)
		})
	}
}

// TestExportedErrorTelemetryHelpers_MatchTheirUnexportedForms covers the exported surface the
// API layer and the server role use.
//
// They exist so a handler classifies an error the repository returned instead of rendering it,
// and thin as they are, a delegation that drifted would leave those callers logging under a
// different vocabulary from the repository's own lines.
func TestExportedErrorTelemetryHelpers_MatchTheirUnexportedForms(t *testing.T) {
	for _, cause := range []error{
		nil,
		sql.ErrNoRows,
		context.Canceled,
		disclosivePostgresError(),
		fmt.Errorf("wrapped: %w", disclosivePostgresError()),
		errors.New("something else"),
	} {
		assert.Equal(t, databaseErrorClass(cause), ErrorClass(cause))
		assert.Equal(t, postgresSQLState(cause), SQLState(cause))
	}

	t.Run("LogDiagnostic routes to the same trace-level sink", func(t *testing.T) {
		restore := pinDatabaseLogLevel(t, logrus.TraceLevel)
		defer restore()

		hook := logtest.NewGlobal()
		LogDiagnostic("audit_terminal_event_records", disclosivePostgresError())

		entries := hook.AllEntries()
		require.Len(t, entries, 1)
		assert.Equal(t, logrus.TraceLevel, entries[0].Level)
		assert.Equal(t, "audit_terminal_event_records", entries[0].Data["operation"])
	})
}

// pinDatabaseLogLevel sets the standard logger's level for one test and returns the restorer.
//
// The level is process-wide, so restoring is not tidiness: a test that left it at trace would
// make every later test in the package emit its diagnostics.
//
// Parameters:
//   - t *testing.T: the test, for the helper marker.
//   - level logrus.Level: the level to pin.
//
// Returns:
//   - func(): restores the previous level.
func pinDatabaseLogLevel(t *testing.T, level logrus.Level) func() {
	t.Helper()

	previous := logrus.GetLevel()
	logrus.SetLevel(level)

	return func() { logrus.SetLevel(previous) }
}

// compile-time assurance that failDatabaseSpan accepts the interface the repositories hold,
// rather than a concrete SDK span. A signature narrowed to the SDK type would compile at every
// call site here and fail against the otel no-op span a caller without a provider receives.
var _ func(trace.Span, error) = failDatabaseSpan

// recordErrorCallCount counts calls to a method named RecordError in a database-package file.
//
// The file is parsed rather than scanned, and comments are not parsed, so the DATA-02 note that
// names span.RecordError in prose cannot be mistaken for a call to it.
//
// Parameters:
//   - t *testing.T: the test; a parse failure is a hard failure.
//   - name string: a file name relative to the database package directory.
//
// Returns:
//   - int: how many call expressions select a method named RecordError.
func recordErrorCallCount(t *testing.T, name string) int {
	t.Helper()

	parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
	require.NoError(t, err, "%s must be parseable to assert its structure", name)

	calls := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector &&
			selector.Sel.Name == "RecordError" {
			calls++
		}

		return true
	})

	return calls
}
