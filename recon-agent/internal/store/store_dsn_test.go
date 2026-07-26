// Copyright 2024 Blnk Finance Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"errors"
	"strings"
	"testing"
)

// The password marker used throughout: if it survives redaction/sanitization,
// the credential leaked (finding F07).
const secretMarker = "SUPERSECRETMARKER_9c1f"

func TestRedactDSN_MasksPassword(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"url form", "postgres://recon:" + secretMarker + "@localhost:5432/blnk?sslmode=disable"},
		{"url form malformed port", "postgres://recon:" + secretMarker + "@localhost:xxx/blnk?sslmode=disable"},
		{"url form space in host", "postgres://recon:" + secretMarker + "@local host:5432/blnk"},
		{"keyword form", "host=localhost user=recon password=" + secretMarker + " dbname=blnk"},
		{"keyword form quoted", "host=localhost user=recon password='" + secretMarker + "' dbname=blnk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactDSN(tc.dsn)
			if strings.Contains(got, secretMarker) {
				t.Fatalf("redactDSN leaked the password: %q", got)
			}
			if !strings.Contains(got, "xxxxx") {
				t.Fatalf("redactDSN did not insert the mask: %q", got)
			}
		})
	}
}

func TestRedactDSN_Empty(t *testing.T) {
	if got := redactDSN("   "); got != "(empty)" {
		t.Fatalf("redactDSN(empty) = %q, want (empty)", got)
	}
}

func TestRedactDSN_NoPasswordUnchanged(t *testing.T) {
	dsn := "postgres://recon@localhost:5432/blnk?sslmode=disable"
	if got := redactDSN(dsn); strings.Contains(got, "xxxxx") {
		t.Fatalf("redactDSN masked a password-less DSN: %q", got)
	}
}

func TestDSNSecrets_ExtractsPassword(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"url form", "postgres://recon:" + secretMarker + "@localhost:5432/blnk"},
		{"url malformed port", "postgres://recon:" + secretMarker + "@localhost:xxx/blnk"},
		{"keyword form", "host=localhost user=recon password=" + secretMarker + " dbname=blnk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secrets := dsnSecrets(tc.dsn)
			found := false
			for _, s := range secrets {
				if s == secretMarker {
					found = true
				}
			}
			if !found {
				t.Fatalf("dsnSecrets(%q) did not extract the password; got %v", tc.dsn, secrets)
			}
		})
	}
}

// TestSanitizeDBError_ScrubsLeak reproduces the exact finding-F07 leak surfaces
// (Go net/url parse errors that echo the RAW malformed DSN, password included)
// and asserts the sanitized error no longer contains the secret.
func TestSanitizeDBError_ScrubsLeak(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		raw  string
	}{
		{
			name: "url parse invalid port",
			dsn:  "postgres://recon:" + secretMarker + "@localhost:xxx/blnk?sslmode=disable",
			raw:  `parse "postgres://recon:` + secretMarker + `@localhost:xxx/blnk?sslmode=disable": invalid port ":xxx" after host`,
		},
		{
			name: "url parse space in host",
			dsn:  "postgres://recon:" + secretMarker + "@local host:5432/blnk",
			raw:  `parse "postgres://recon:` + secretMarker + `@local host:5432/blnk": invalid character " " in host name`,
		},
		{
			name: "keyword form echoed",
			dsn:  "host=localhost user=recon password=" + secretMarker + " port=99999 dbname=blnk",
			raw:  "dial tcp: address 99999: cannot parse host=localhost user=recon password=" + secretMarker + " port=99999 dbname=blnk",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sanitized := sanitizeDBError(errors.New(tc.raw), tc.dsn)
			if sanitized == nil {
				t.Fatal("sanitizeDBError returned nil for a non-nil error")
			}
			if strings.Contains(sanitized.Error(), secretMarker) {
				t.Fatalf("sanitizeDBError leaked the password: %q", sanitized.Error())
			}
		})
	}
}

func TestSanitizeDBError_NilAndNoSecret(t *testing.T) {
	if got := sanitizeDBError(nil, "postgres://recon:pw@h/db"); got != nil {
		t.Fatalf("sanitizeDBError(nil) = %v, want nil", got)
	}
	// An error with no DSN content should pass through with its message intact.
	orig := "pq: relation \"agent.agent_break\" does not exist"
	got := sanitizeDBError(errors.New(orig), "postgres://recon:pw@h/db")
	if got == nil || got.Error() != orig {
		t.Fatalf("sanitizeDBError mangled a secret-free error: %v", got)
	}
}

func TestRuntimeRoleFromDSN(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want string
	}{
		{"url form", "postgres://recon_agent:pw@localhost:5432/blnk?sslmode=disable", "recon_agent"},
		{"url no password", "postgres://recon_agent@localhost:5432/blnk", "recon_agent"},
		{"keyword form", "host=localhost user=recon_agent password=pw dbname=blnk", "recon_agent"},
		{"keyword quoted user", "host=localhost user='recon agent' password=pw", "recon agent"},
		{"empty", "   ", ""},
		{"no user", "postgres://localhost:5432/blnk", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := runtimeRoleFromDSN(tc.dsn); got != tc.want {
				t.Fatalf("runtimeRoleFromDSN(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}

// TestRuntimeGrantStatements is the static regression guard for the F05 two-role
// least-privilege model. It pins the exact privilege set the restricted runtime
// role receives so that (a) agent_audit can never be granted a mutating
// privilege (append-only, Rule 5.5), (b) every table written via an
// "INSERT ... ON CONFLICT DO UPDATE" upsert (agent_break, agent_hitl_queue) is
// granted UPDATE — PostgreSQL checks the DO UPDATE arm as an UPDATE, so omitting
// it makes the upsert fail at runtime with "permission denied" (the bug this
// test locks out) — and (c) no ownership/CREATE/TRUNCATE/ALL is ever granted.
func TestRuntimeGrantStatements(t *testing.T) {
	stmts := runtimeGrantStatements("recon_runtime")

	// Index the per-object grant lines by the object they target.
	byObject := map[string]string{}
	for _, s := range stmts {
		switch {
		case strings.Contains(s, "ON SCHEMA agent "):
			byObject["schema"] = s
		case strings.Contains(s, "agent.agent_audit "):
			byObject["agent_audit"] = s
		case strings.Contains(s, "agent.agent_break "):
			byObject["agent_break"] = s
		case strings.Contains(s, "agent.agent_hitl_queue "):
			byObject["agent_hitl_queue"] = s
		case strings.Contains(s, "agent.agent_rule_outbox "):
			byObject["agent_rule_outbox"] = s
		case strings.Contains(s, "SEQUENCE agent.agent_rule_outbox_id_seq "):
			byObject["sequence"] = s
		case strings.Contains(s, "agent.agent_run "):
			byObject["agent_run"] = s
		}
	}

	// Every statement must target the quoted runtime role and nothing broader.
	for _, s := range stmts {
		if !strings.Contains(s, `TO "recon_runtime"`) {
			t.Errorf("grant does not target the quoted runtime role: %q", s)
		}
		if strings.Contains(s, " ALL ") || strings.Contains(s, "GRANT ALL") {
			t.Errorf("grant must never use ALL PRIVILEGES: %q", s)
		}
		if strings.Contains(s, "TRUNCATE") {
			t.Errorf("grant must never include TRUNCATE (audit immutability, Rule 5.5): %q", s)
		}
		if strings.Contains(s, "CREATE") {
			t.Errorf("grant must never include CREATE (no schema/db object creation): %q", s)
		}
		if strings.Contains(s, "WITH GRANT OPTION") {
			t.Errorf("grant must never be delegable: %q", s)
		}
	}

	// Exact per-object privilege expectations.
	wantExact := map[string]string{
		"schema":            "GRANT USAGE ON SCHEMA agent TO \"recon_runtime\"",
		"agent_audit":       "GRANT SELECT, INSERT ON agent.agent_audit TO \"recon_runtime\"",
		"agent_break":       "GRANT SELECT, INSERT, UPDATE ON agent.agent_break TO \"recon_runtime\"",
		"agent_hitl_queue":  "GRANT SELECT, INSERT, UPDATE, DELETE ON agent.agent_hitl_queue TO \"recon_runtime\"",
		"agent_rule_outbox": "GRANT SELECT, INSERT, UPDATE ON agent.agent_rule_outbox TO \"recon_runtime\"",
		"sequence":          "GRANT USAGE ON SEQUENCE agent.agent_rule_outbox_id_seq TO \"recon_runtime\"",
		"agent_run":         "GRANT SELECT, INSERT, UPDATE ON agent.agent_run TO \"recon_runtime\"",
	}
	for key, want := range wantExact {
		if got := byObject[key]; got != want {
			t.Errorf("grant for %q =\n  %q\nwant\n  %q", key, got, want)
		}
	}

	// Append-only invariant (Rule 5.5): agent_audit must be SELECT+INSERT ONLY.
	if a := byObject["agent_audit"]; strings.Contains(a, "UPDATE") || strings.Contains(a, "DELETE") {
		t.Errorf("agent_audit must be append-only (SELECT+INSERT only), got: %q", a)
	}

	// Upsert invariant: every table the store upserts into via ON CONFLICT DO
	// UPDATE must carry UPDATE, or the upsert is refused at runtime.
	for _, tbl := range []string{"agent_break", "agent_hitl_queue"} {
		if !strings.Contains(byObject[tbl], "UPDATE") {
			t.Errorf("%s is written via ON CONFLICT DO UPDATE and MUST be granted UPDATE, got: %q", tbl, byObject[tbl])
		}
	}
}
