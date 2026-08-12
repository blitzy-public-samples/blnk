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
	"path/filepath"
	"sort"
	"strings"
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

// event_error_telemetry_test.go covers the driver's own words must not reach the
// standard log or a span.

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

// disclosiveSubstrings is what must never appear in a bounded log line or a span
// status.
//
// Every entry is a value that can ONLY have come from the driver.
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

// TestDatabaseErrorClass_MapsEverySQLStateClassAndTheGoLevelConditions pins the
// vocabulary.
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

// TestLoggedDatabaseError_LogsTheClassAndNotTheDriversWords is the log half of the redaction rule.
//
// Both the field NAME and every field VALUE are inspected.
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

// TestLogDatabaseDiagnostic_EmitsTheRawCauseOnlyAtTrace proves the sink is separate
// from the level operators already raise.
//
// DEBUG is asserted SILENT.
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

// TestFailDatabaseSpan_SetsABoundedStatusAndRecordsNoException is the trace half of
// the redaction rule.
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
// The two repositories hold a hundred failure paths between them, and a single
// reintroduced span.RecordError re-opens the disclosure on whichever path nobody
// happened to exercise.
func TestNeitherEventRepositoryCallsSpanRecordError(t *testing.T) {
	for _, name := range packageSourceGroups(t, "event_outbox.go", "event_subscriber.go") {
		t.Run(name, func(t *testing.T) {
			calls := recordErrorCallCount(t, name)
			assert.Zero(t, calls,
				"%s calls span.RecordError %d time(s); every failure must go through "+
					"failDatabaseSpan, which sets a bounded class instead of writing the error's "+
					"own text as an exception event", name, calls)
		})
	}
}

// TestExportedErrorTelemetryHelpers_MatchTheirUnexportedForms covers the exported
// surface the API layer and the server role use.
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

// pinDatabaseLogLevel sets the standard logger's level for one test and returns the
// restorer.
//
// The level is process-wide, so restoring is not tidiness: a test that left it at trace
// would make every later test in the package emit its diagnostics.
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

// recordErrorCallCount counts calls to a method named RecordError in a database-package
// file.
//
// The file is parsed rather than scanned, and comments are not parsed, so the note that
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

// packageSourceGroup returns the file names, relative to this package directory, that
// together constitute the logical unit named by base: base itself plus the files it was
// split into, which by convention carry base's stem followed by an underscore.
//
// The package-local counterpart of the root package's eventSourceGroup, and it exists for
// the same reason. Both repositories in this package were split for size, and a structural
// guard that kept naming one file would stop covering every failure path that moved into a
// sibling — while still passing, which is the failure mode worth engineering against.
//
// Parameters:
//   - t *testing.T: the test.
//   - base string: a file name in this directory, e.g. "event_outbox.go".
//
// Returns:
//   - []string: sorted file names, always including base.
func packageSourceGroup(t *testing.T, base string) []string {
	t.Helper()

	stem := strings.TrimSuffix(base, ".go")
	matches, err := filepath.Glob(stem + "*.go")
	require.NoErrorf(t, err, "the source group for %s must be enumerable", base)

	group := make([]string, 0, len(matches))

	for _, match := range matches {
		if strings.HasSuffix(match, "_test.go") {
			continue
		}

		if match != base && !strings.HasPrefix(match, stem+"_") {
			continue
		}

		group = append(group, match)
	}

	sort.Strings(group)
	require.Containsf(t, group, base, "%s must exist and must belong to its own source group", base)

	return group
}

// packageSourceGroups expands every base through packageSourceGroup, preserving order and
// dropping duplicates.
func packageSourceGroups(t *testing.T, bases ...string) []string {
	t.Helper()

	seen := make(map[string]struct{}, len(bases))
	expanded := make([]string, 0, len(bases))

	for _, base := range bases {
		for _, member := range packageSourceGroup(t, base) {
			if _, already := seen[member]; already {
				continue
			}

			seen[member] = struct{}{}
			expanded = append(expanded, member)
		}
	}

	return expanded
}
