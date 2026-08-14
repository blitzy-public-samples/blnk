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

// THE ROWS THAT WERE ALREADY WRITTEN.
//
// model.ValidateWebhookURL now refuses userinfo and literal spaces, and
// model/event_predicates_test.go pins that refusal thoroughly. What no unit test can reach
// is the table: a rule that only applies to the next write leaves every value already
// stored exactly as it was, and for userinfo that value is a plaintext credential sitting
// in a column that GET /subscribers/{id} returns.
//
// sql/1781252700.sql is the half that deals with those rows, and this is where it is
// tested — against a real database, through the EMBEDDED migration the binary ships,
// applied in two steps so the legacy rows exist before it runs. Executing a hand-copied
// version of the SQL here would test a copy and leave the shipped file unexercised, which
// is the one thing a data migration cannot afford.
package blnk

import (
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// webhookURLScrubMigration is the migration under test. Named rather than inferred, so a
// later migration cannot silently become the thing this test applies.
const webhookURLScrubMigration = "1781252700.sql"

// webhookScrubCase is one legacy row and what the migration must make of it.
type webhookScrubCase struct {
	// suffix distinguishes the subscriber id, which the table's CHECK constraints derive the
	// principal and the consumer group from.
	suffix string

	// stored is the value a pre-policy Blnk accepted and wrote verbatim.
	stored string

	// want is the value the migration must leave behind.
	want string

	// why states what the case is for, so a failure reads as a defect rather than a diff.
	why string
}

// webhookScrubCases enumerates every spelling that reached storage, and every spelling that
// must be left alone.
//
// The two "unchanged" cases carry the weight here. A scrub that also rewrote them would be
// changing a destination an operator recorded, and the column exists to record exactly
// that.
var webhookScrubCases = []webhookScrubCase{
	{
		suffix: "userpass",
		stored: "https://user:secret@hooks.example.com/blnk",
		want:   "https://hooks.example.com/blnk",
		why:    "a username and password is the disclosure this migration exists for",
	},
	{
		suffix: "useronly",
		stored: "https://user@hooks.example.com/blnk",
		want:   "https://hooks.example.com/blnk",
		why:    "a username alone is still userinfo, and url.Parse reads it as such",
	},
	{
		suffix: "encoded",
		stored: "https://user:p%40ss@hooks.example.com:8443/blnk?v=1",
		want:   "https://hooks.example.com:8443/blnk?v=1",
		why:    "a percent-encoded password is still a password, and the port and query must survive",
	},
	{
		suffix: "empty",
		stored: "https://@hooks.example.com/blnk",
		want:   "https://hooks.example.com/blnk",
		why:    "an empty userinfo carries no secret but is still a separator url.Parse honours",
	},
	{
		suffix: "doubled",
		stored: "https://user@@hooks.example.com/blnk",
		want:   "https://hooks.example.com/blnk",
		why: "url.Parse ends userinfo at the LAST '@' before the authority ends, so the scrub " +
			"has to be greedy in the same place or it would leave '@hooks.example.com' as the host",
	},
	{
		suffix: "space",
		stored: "https://hooks.example.com/a b",
		want:   "https://hooks.example.com/a%20b",
		why: "url.Parse ACCEPTS a literal space and folds it into the path, so this is the one " +
			"non-conforming value that could actually be stored",
	},
	{
		suffix: "spacequery",
		stored: "https://hooks.example.com/hook?note=a b",
		want:   "https://hooks.example.com/hook?note=a%20b",
		why:    "a space in the query is the same case as one in the path",
	},
	{
		suffix: "both",
		stored: "https://user:secret@hooks.example.com/a b",
		want:   "https://hooks.example.com/a%20b",
		why:    "the two statements must compose: the second reads what the first left",
	},
	{
		suffix: "atpath",
		stored: "https://hooks.example.com/hook@v2",
		want:   "https://hooks.example.com/hook@v2",
		why: "an '@' after the authority is an ordinary character, not userinfo. Rewriting this " +
			"would send Blnk's record of the endpoint to a different path",
	},
	{
		suffix: "atquery",
		stored: "https://hooks.example.com/hook?notify=ops@example.com",
		want:   "https://hooks.example.com/hook?notify=ops@example.com",
		why:    "an '@' in a query value is an ordinary character too",
	},
	{
		suffix: "escaped",
		stored: "https://hooks.example.com/a%20b",
		want:   "https://hooks.example.com/a%20b",
		why:    "the canonical spelling is already correct and must not be re-encoded to %2520",
	},
	{
		suffix: "clean",
		stored: "https://hooks.example.com/blnk",
		want:   "https://hooks.example.com/blnk",
		why:    "an ordinary endpoint must be untouched, including its updated_at",
	},
}

// TestWebhookURLScrubMigration_RemovesStoredCredentialsAndLiteralSpaces_RealDB applies the
// embedded migrations in two steps around a set of legacy rows and asserts what
// sql/1781252700.sql makes of each.
//
// TWO STEPS, and that is the whole design: the rows this migration exists for cannot be
// written after it runs, because the policy refuses them. So every migration BUT this one
// is applied, the legacy rows are inserted straight into the table the way a pre-policy
// Blnk wrote them, and only then is the scrub applied.
func TestWebhookURLScrubMigration_RemovesStoredCredentialsAndLiteralSpaces_RealDB(t *testing.T) {
	base := recoveryPostgresDSN()
	if address := recoveryDatabaseAddress(base); address != "" {
		recoveryRequireReachable(t, "PostgreSQL", address,
			"Start it with `docker compose up -d postgres`, or point BLNK_DATA_SOURCE_DNS at a "+
				"reachable database.")
	}

	source := migrate.EmbedFileSystemMigrationSource{FileSystem: SQLFiles, Root: "sql"}
	all, err := source.FindMigrations()
	require.NoError(t, err, "the embedded migration set must be readable")
	require.NotEmpty(t, all)

	// THE SCRUB MUST BE THE LAST ONE, or "apply all but one" applies the wrong file and this
	// test silently stops testing anything. It is asserted rather than assumed because a new
	// migration is exactly the change that would break it, and the failure would otherwise be
	// a confusing assertion about a row.
	require.Equalf(t, webhookURLScrubMigration, all[len(all)-1].Id,
		"this test applies every migration but the last and expects the last to be the webhook "+
			"URL scrub. A newer migration has been added after it: apply the scrub by id here "+
			"instead of by position, or move the assertion to match")

	db, dsn := createWebhookScrubDatabase(t, base)

	// EVERY MIGRATION BUT THE SCRUB.
	applied, err := migrate.ExecMax(db, "postgres", source, migrate.Up, len(all)-1)
	require.NoErrorf(t, err, "applying the first %d migrations to %s", len(all)-1, dsn)
	require.Equal(t, len(all)-1, applied, "every migration but the scrub must apply")

	seedLegacyWebhookRows(t, db)

	// AND NOW THE SCRUB, on its own, so the count proves which file did the work.
	applied, err = migrate.Exec(db, "postgres", source, migrate.Up)
	require.NoError(t, err, "applying the webhook URL scrub")
	require.Equal(t, 1, applied, "exactly the scrub must remain to apply")

	stored := readLegacyWebhookRows(t, db)

	for _, expected := range webhookScrubCases {
		t.Run(expected.suffix, func(t *testing.T) {
			row, found := stored[webhookScrubSubscriberID(expected.suffix)]
			require.Truef(t, found, "the seeded row for %s is missing", expected.suffix)

			assert.Equalf(t, expected.want, row.url, "%s", expected.why)

			// A CREDENTIAL MUST NOT SURVIVE IN ANY FORM, asserted separately from the exact value
			// so a future change to the rewrite cannot pass by producing a different string that
			// still carries the secret.
			assert.NotContains(t, row.url, "secret", "no plaintext credential may remain")
			assert.NotContains(t, row.url, "p%40ss", "no encoded credential may remain")

			// AND THE RESULT MUST SATISFY THE POLICY THAT NOW GOVERNS THE COLUMN. This is the
			// point of the whole migration: after it, nothing in the table is a value Blnk would
			// refuse if it were submitted today.
			message, reason := model.ValidateWebhookURL(row.url)
			assert.Emptyf(t, message,
				"the scrubbed value must pass the policy it is now judged by, got %q (%s)",
				message, reason)

			// UNCHANGED ROWS MUST BE UNTOUCHED, updated_at included. A migration that stamps every
			// row makes "when did this subscriber's endpoint last change" unanswerable.
			if expected.stored == expected.want {
				assert.Falsef(t, row.touched,
					"%s was not rewritten, so its updated_at must not have moved", expected.suffix)
			} else {
				assert.Truef(t, row.touched,
					"%s was rewritten, so updated_at must record that", expected.suffix)
			}
		})
	}

	t.Run("the rewrite is idempotent", func(t *testing.T) {
		// Two independent claims, because they fail for different reasons. sql-migrate not
		// re-running the file is bookkeeping; the SQL matching nothing is the property that
		// makes an operator's replay of a range safe.
		reapplied, execErr := migrate.Exec(db, "postgres", source, migrate.Up)
		require.NoError(t, execErr)
		assert.Zero(t, reapplied, "the scrub is recorded, so a second Up must apply nothing")

		for _, pattern := range []struct {
			name  string
			regex string
		}{
			{"userinfo", `^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*@`},
			{"a literal space after the authority", `^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]*[/?#].* `},
		} {
			var remaining int
			require.NoError(t, db.QueryRow(`
				SELECT count(*) FROM blnk.event_subscribers
				WHERE webhook_url IS NOT NULL AND webhook_url ~ $1
			`, pattern.regex).Scan(&remaining))

			assert.Zerof(t, remaining,
				"no row may still match %s: the statement's own WHERE would select it again, so a "+
					"replayed migration range would rewrite it a second time", pattern.name)
		}
	})
}

// webhookScrubSubscriberID builds the subscriber id for a case.
//
// The table derives kafka_principal and consumer_group_id from it by CHECK constraint, so
// the shape is not cosmetic: a value that does not match
// '^[a-z0-9][a-z0-9_-]{2,127}$' is rejected at insert.
//
// Parameters:
//   - suffix string: the case's distinguishing fragment.
//
// Returns:
//   - string: the subscriber id.
func webhookScrubSubscriberID(suffix string) string {
	return "sub-scrub-" + suffix
}

// scrubbedRow is one row as the migration left it.
type scrubbedRow struct {
	// url is the stored webhook_url.
	url string

	// touched reports whether updated_at moved past created_at, which is how an untouched
	// row is told from a rewritten one.
	touched bool
}

// seedLegacyWebhookRows writes the cases straight into the table, the way a pre-policy
// Blnk wrote them.
//
// Straight into the table deliberately: every one of these values is refused by
// model.ValidateWebhookURL now, so the repository and the DTO cannot produce them, and a
// migration that only ever sees conforming rows tests nothing.
//
// Parameters:
//   - t *testing.T: fails the test on any insert error.
//   - db *sql.DB: the migrated disposable database.
func seedLegacyWebhookRows(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, seed := range webhookScrubCases {
		id := webhookScrubSubscriberID(seed.suffix)

		_, err := db.Exec(`
			INSERT INTO blnk.event_subscribers
				(subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, webhook_url)
			VALUES ($1, $2, 'blnk-sub-' || $1, 'blnk-sub-' || $1 || '.g', ARRAY['blnk.transactions'], $3)
		`, id, "scrub "+seed.suffix, seed.stored)
		require.NoErrorf(t, err, "seeding the legacy row %s with %q", id, seed.stored)
	}

	// THE SEED MUST BE THE VALUE, VERBATIM. If an insert trigger or a column default had
	// normalised anything, the migration would be tested against a value it will never meet.
	for _, seed := range webhookScrubCases {
		var readBack string
		require.NoError(t, db.QueryRow(
			`SELECT webhook_url FROM blnk.event_subscribers WHERE subscriber_id = $1`,
			webhookScrubSubscriberID(seed.suffix),
		).Scan(&readBack))

		require.Equalf(t, seed.stored, readBack,
			"the legacy row must be stored verbatim before the migration runs")
	}
}

// readLegacyWebhookRows reads every seeded row back, keyed by subscriber id.
//
// Parameters:
//   - t *testing.T: fails the test on any query error.
//   - db *sql.DB: the migrated disposable database.
//
// Returns:
//   - map[string]scrubbedRow: one entry per seeded subscriber.
func readLegacyWebhookRows(t *testing.T, db *sql.DB) map[string]scrubbedRow {
	t.Helper()

	rows, err := db.Query(`
		SELECT subscriber_id, COALESCE(webhook_url, ''), updated_at > created_at
		FROM blnk.event_subscribers
		WHERE subscriber_id LIKE 'sub-scrub-%'
	`)
	require.NoError(t, err, "reading the seeded rows back")
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Logf("closing the seeded-row cursor: %v", closeErr)
		}
	}()

	stored := make(map[string]scrubbedRow, len(webhookScrubCases))
	for rows.Next() {
		var (
			id  string
			row scrubbedRow
		)
		require.NoError(t, rows.Scan(&id, &row.url, &row.touched))
		stored[id] = row
	}

	require.NoError(t, rows.Err())
	require.Len(t, stored, len(webhookScrubCases), "every seeded row must be readable")

	return stored
}

// createWebhookScrubDatabase creates a disposable database beside the configured one and
// returns an open pool on it, UNMIGRATED.
//
// Unmigrated is the point: the caller applies the migrations in two steps around the
// legacy rows, which cannot be done to a database that is already at head.
//
// Parameters:
//   - t *testing.T: owns the database's lifetime; it is dropped in cleanup.
//   - base string: the configured DSN, used for the server address and credentials.
//
// Returns:
//   - *sql.DB: an open pool on the new database.
//   - string: its DSN, for error messages.
func createWebhookScrubDatabase(t *testing.T, base string) (*sql.DB, string) {
	t.Helper()

	parsed, err := url.Parse(base)
	require.NoErrorf(t, err,
		"the configured DSN %q must be a URL so a sibling database can be named", base)

	// Lower-case and unquoted: PostgreSQL folds an unquoted identifier to lower case, and a
	// name that needed quoting here would need quoting in the DROP as well.
	name := "blnk_urlscrub_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]

	control, err := sql.Open("postgres", base)
	require.NoError(t, err, "opening the control connection that creates the database")
	defer func() {
		if closeErr := control.Close(); closeErr != nil {
			t.Logf("closing the control connection: %v", closeErr)
		}
	}()
	require.NoError(t, control.Ping(), "the control connection must reach PostgreSQL")

	// A leftover from a crashed run is removed first; the name carries a fresh uuid fragment,
	// and "all but impossible" is not a reason to fail on the second attempt.
	_, err = control.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`)
	require.NoErrorf(t, err, "clearing any leftover database %s", name)

	// CREATE DATABASE cannot run inside a transaction, which is why this is a bare Exec.
	_, err = control.Exec(`CREATE DATABASE ` + name)
	require.NoErrorf(t, err,
		"creating the disposable database %s. This test needs a PostgreSQL role that may create "+
			"databases: it applies the migration set in two steps, which cannot be done to the "+
			"shared database that is already at head", name)

	t.Cleanup(func() {
		dropper, openErr := sql.Open("postgres", base)
		if openErr != nil {
			t.Errorf("could not open a connection to drop %s: %v", name, openErr)

			return
		}
		defer func() { _ = dropper.Close() }()

		if _, dropErr := dropper.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`); dropErr != nil {
			t.Errorf("could not drop the disposable database %s: %v", name, dropErr)
		}
	})

	parsed.Path = "/" + name
	dsn := parsed.String()

	db, err := sql.Open("postgres", dsn)
	require.NoErrorf(t, err, "opening %s", dsn)
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("closing the scrub pool: %v", closeErr)
		}
	})

	// The migration runner writes to blnk's own schema, and every migration names it, so the
	// runner has to be pointed at it exactly as cmd/migrate.go does.
	migrate.SetSchema("blnk")

	// A short deadline on the first contact, so an unreachable server fails here with the
	// remedy above rather than inside a migration.
	db.SetConnMaxLifetime(time.Minute)
	require.NoErrorf(t, db.Ping(), "the disposable database %s must be reachable", dsn)

	return db, dsn
}
