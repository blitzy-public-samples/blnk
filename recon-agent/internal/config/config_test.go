package config

import (
	"fmt"
	"strings"
	"testing"
)

// clearEnv forces every recon-agent environment variable to empty for the
// duration of the test so Load observes a clean, host-independent environment.
// t.Setenv restores the previous values automatically when the test ends and
// also guarantees the test is not run in parallel.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL",
		"BLNK_BASE_URL", "BLNK_API_KEY",
		"CONF_AUTO_THRESHOLD", "HITL_PORT", "AGENT_DATABASE_URL",
	} {
		t.Setenv(k, "")
	}
}

const testDSN = "postgres://u:p@localhost:5432/blnk?sslmode=disable"

// TestLoad_Defaults verifies that, with only the required AGENT_DATABASE_URL
// set, every optional field falls back to its documented default.
func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", testDSN)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	// M-04 / Finding #2 / Rule 5.6: the shipped default MUST be a non-routable
	// loopback placeholder, never a live third-party provider, so copying the
	// defaults cannot silently transmit transaction data off-box.
	if cfg.LLMBaseURL != "http://localhost:11434/v1" {
		t.Errorf("LLMBaseURL default = %q, want %q", cfg.LLMBaseURL, "http://localhost:11434/v1")
	}
	if strings.Contains(cfg.LLMBaseURL, "moonshot.ai") || strings.Contains(cfg.LLMBaseURL, "api.openai.com") {
		t.Errorf("LLMBaseURL default = %q must not point at a live external provider", cfg.LLMBaseURL)
	}
	if cfg.LLMModel != "kimi-k3" {
		t.Errorf("LLMModel default = %q, want %q", cfg.LLMModel, "kimi-k3")
	}
	if cfg.BlnkBaseURL != "http://localhost:5001" {
		t.Errorf("BlnkBaseURL default = %q, want %q", cfg.BlnkBaseURL, "http://localhost:5001")
	}
	if cfg.ConfAutoThreshold != 0.85 {
		t.Errorf("ConfAutoThreshold default = %v, want 0.85", cfg.ConfAutoThreshold)
	}
	if cfg.HitlPort != "8088" {
		t.Errorf("HitlPort default = %q, want %q", cfg.HitlPort, "8088")
	}
	if cfg.LLMApiKey != "" {
		t.Errorf("LLMApiKey = %q, want empty", cfg.LLMApiKey)
	}
	if cfg.BlnkApiKey != "" {
		t.Errorf("BlnkApiKey = %q, want empty", cfg.BlnkApiKey)
	}
	if cfg.AgentDatabaseURL != testDSN {
		t.Errorf("AgentDatabaseURL = %q, want %q", cfg.AgentDatabaseURL, testDSN)
	}
}

// TestLoad_DefaultLLMBaseURLIsNotPublic pins Rule 5.6 / config-safety
// (Finding #2): when LLM_BASE_URL is unset OR explicitly blank, the resolved
// base URL must be a LOCAL (loopback) endpoint and must NOT be any public SaaS
// host. Defaulting to the internet would risk transmitting transaction bodies
// (PII) off-box the moment the agent is run without a configured endpoint.
func TestLoad_DefaultLLMBaseURLIsNotPublic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		unset bool
	}{
		{name: "unset", unset: true},
		{name: "blank", unset: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			if !tc.unset {
				t.Setenv("LLM_BASE_URL", "") // explicitly blank
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}

			// The default must be a loopback URL.
			if !strings.HasPrefix(cfg.LLMBaseURL, "http://localhost") &&
				!strings.HasPrefix(cfg.LLMBaseURL, "http://127.0.0.1") {
				t.Errorf("default LLMBaseURL %q is not a loopback endpoint; a missing base URL must fail closed to a non-public target", cfg.LLMBaseURL)
			}
			// Belt-and-suspenders: it must not be a known public host.
			for _, publicHost := range []string{"moonshot.ai", "api.openai.com", "googleapis.com", "anthropic.com"} {
				if strings.Contains(cfg.LLMBaseURL, publicHost) {
					t.Errorf("default LLMBaseURL %q must never resolve to a public host (%q)", cfg.LLMBaseURL, publicHost)
				}
			}
		})
	}
}

// TestLoad_Overrides verifies that every field is taken from the environment
// when set, and that CONF_AUTO_THRESHOLD is parsed as a float64.
func TestLoad_Overrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("LLM_BASE_URL", "http://localhost:8000/v1")
	t.Setenv("LLM_API_KEY", "sk-test")
	t.Setenv("LLM_MODEL", "kimi-k3-preview")
	t.Setenv("BLNK_BASE_URL", "http://server:5001")
	t.Setenv("BLNK_API_KEY", "blnk-secret")
	t.Setenv("CONF_AUTO_THRESHOLD", "0.5")
	t.Setenv("HITL_PORT", "9099")
	t.Setenv("AGENT_DATABASE_URL", "postgres://x:y@postgres:5432/blnk?sslmode=disable")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.LLMBaseURL != "http://localhost:8000/v1" {
		t.Errorf("LLMBaseURL = %q", cfg.LLMBaseURL)
	}
	if cfg.LLMApiKey != "sk-test" {
		t.Errorf("LLMApiKey = %q", cfg.LLMApiKey)
	}
	if cfg.LLMModel != "kimi-k3-preview" {
		t.Errorf("LLMModel = %q", cfg.LLMModel)
	}
	if cfg.BlnkBaseURL != "http://server:5001" {
		t.Errorf("BlnkBaseURL = %q", cfg.BlnkBaseURL)
	}
	if cfg.BlnkApiKey != "blnk-secret" {
		t.Errorf("BlnkApiKey = %q", cfg.BlnkApiKey)
	}
	if cfg.ConfAutoThreshold != 0.5 {
		t.Errorf("ConfAutoThreshold = %v, want 0.5", cfg.ConfAutoThreshold)
	}
	if cfg.HitlPort != "9099" {
		t.Errorf("HitlPort = %q", cfg.HitlPort)
	}
	if cfg.AgentDatabaseURL != "postgres://x:y@postgres:5432/blnk?sslmode=disable" {
		t.Errorf("AgentDatabaseURL = %q", cfg.AgentDatabaseURL)
	}
}

// TestLoad_MissingRequiredDatabaseURL asserts that Load fails closed when the
// required AGENT_DATABASE_URL is absent.
func TestLoad_MissingRequiredDatabaseURL(t *testing.T) {
	clearEnv(t) // AGENT_DATABASE_URL forced empty
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for missing AGENT_DATABASE_URL")
	}
}

// TestLoad_InvalidThreshold asserts that a non-numeric CONF_AUTO_THRESHOLD is a
// hard error rather than being silently ignored.
func TestLoad_InvalidThreshold(t *testing.T) {
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", testDSN)
	t.Setenv("CONF_AUTO_THRESHOLD", "not-a-float")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error for invalid CONF_AUTO_THRESHOLD")
	}
}

// TestLoad_ThresholdOutOfRange asserts that a CONF_AUTO_THRESHOLD outside the
// finite closed interval [0,1] is rejected with a descriptive error naming the
// key. Accepting such a value would subvert the Rule 5.4 auto-remediation gate
// (a threshold <= 0 auto-remediates everything; NaN or > 1 disables it), so the
// loader must fail fast. Covers the QA boundary set -0.01/1.01/NaN/+Inf/-Inf
// plus the float64-overflow case 1e309.
func TestLoad_ThresholdOutOfRange(t *testing.T) {
	for _, raw := range []string{"-0.01", "1.01", "NaN", "+Inf", "-Inf", "Inf", "1e309"} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("CONF_AUTO_THRESHOLD", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want out-of-range error for CONF_AUTO_THRESHOLD=%q", raw)
			}
			if !strings.Contains(err.Error(), "CONF_AUTO_THRESHOLD") {
				t.Fatalf("error %q does not name CONF_AUTO_THRESHOLD", err.Error())
			}
		})
	}
}

// TestLoad_ThresholdBoundaryValid asserts the inclusive [0,1] endpoints and
// interior values are accepted and stored exactly.
func TestLoad_ThresholdBoundaryValid(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want float64
	}{{"0", 0}, {"0.5", 0.5}, {"1", 1}, {"0.85", 0.85}} {
		t.Run(tc.raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("CONF_AUTO_THRESHOLD", tc.raw)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() unexpected error for CONF_AUTO_THRESHOLD=%q: %v", tc.raw, err)
			}
			if cfg.ConfAutoThreshold != tc.want {
				t.Fatalf("ConfAutoThreshold = %v, want %v", cfg.ConfAutoThreshold, tc.want)
			}
		})
	}
}

// TestLoad_DatabaseURLWhitespaceRejected asserts that a whitespace-only
// AGENT_DATABASE_URL (spaces, tabs, newlines) fails fast with the required-key
// error rather than being stored verbatim and failing later at connect time.
func TestLoad_DatabaseURLWhitespaceRejected(t *testing.T) {
	for _, raw := range []string{" ", "   ", "\t", "\n", " \t \n "} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want required error for whitespace AGENT_DATABASE_URL=%q", raw)
			}
			if !strings.Contains(err.Error(), "AGENT_DATABASE_URL") {
				t.Fatalf("error %q does not name AGENT_DATABASE_URL", err.Error())
			}
		})
	}
}

// TestLoad_DatabaseURLTrimmed asserts that surrounding whitespace on an
// otherwise valid DSN is trimmed and the canonical value is stored.
func TestLoad_DatabaseURLTrimmed(t *testing.T) {
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", "  "+testDSN+"  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if cfg.AgentDatabaseURL != testDSN {
		t.Fatalf("AgentDatabaseURL = %q, want trimmed %q", cfg.AgentDatabaseURL, testDSN)
	}
}

// TestLoad_HitlPortInvalid asserts that a non-numeric or out-of-range HITL_PORT
// is rejected with a descriptive error naming the key instead of surfacing only
// later as an opaque net.Listen bind error.
func TestLoad_HitlPortInvalid(t *testing.T) {
	for _, raw := range []string{"abc", "-1", "0", "70000", "65536", "99999999999", "80;rm -rf", "8.5"} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("HITL_PORT", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want error for HITL_PORT=%q", raw)
			}
			if !strings.Contains(err.Error(), "HITL_PORT") {
				t.Fatalf("error %q does not name HITL_PORT", err.Error())
			}
		})
	}
}

// TestLoad_HitlPortBoundaryValid asserts the inclusive 1..65535 endpoints and
// representative interior ports are accepted and stored exactly.
func TestLoad_HitlPortBoundaryValid(t *testing.T) {
	for _, raw := range []string{"1", "65535", "8088", "9099"} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("HITL_PORT", raw)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() unexpected error for HITL_PORT=%q: %v", raw, err)
			}
			if cfg.HitlPort != raw {
				t.Fatalf("HitlPort = %q, want %q", cfg.HitlPort, raw)
			}
		})
	}
}

// TestLoad_HitlPortTrimmed asserts that a HITL_PORT with surrounding whitespace
// (e.g. "8088 ") is trimmed to its canonical form and accepted.
func TestLoad_HitlPortTrimmed(t *testing.T) {
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", testDSN)
	t.Setenv("HITL_PORT", "8088 ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if cfg.HitlPort != "8088" {
		t.Fatalf("HitlPort = %q, want trimmed %q", cfg.HitlPort, "8088")
	}
}

// TestLoad_SecretsNotEchoed asserts that an error path (here, an out-of-range
// threshold) never leaks the DB password or the LLM/Blnk API keys in its
// message, even though all three are present in the environment.
func TestLoad_SecretsNotEchoed(t *testing.T) {
	const (
		dbPass  = "sup3r-secret-db-pass"
		llmKey  = "sk-secret-llm-key"
		blnkKey = "secret-blnk-key"
	)
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", "postgres://u:"+dbPass+"@localhost:5432/blnk?sslmode=disable")
	t.Setenv("LLM_API_KEY", llmKey)
	t.Setenv("BLNK_API_KEY", blnkKey)
	t.Setenv("CONF_AUTO_THRESHOLD", "1.5") // forces the validation error path

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid-threshold error")
	}
	for _, secret := range []string{dbPass, llmKey, blnkKey} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error message leaks secret %q: %s", secret, err.Error())
		}
	}
}

// TestLoad_InvalidBaseURL asserts that a malformed, non-HTTP(S), host-less, or
// userinfo-bearing LLM_BASE_URL or BLNK_BASE_URL is rejected at load time
// rather than surfacing only at request time (or silently transmitting data to
// an unintended destination). Covers M-04.
func TestLoad_InvalidBaseURL(t *testing.T) {
	for _, key := range []string{"LLM_BASE_URL", "BLNK_BASE_URL"} {
		for _, raw := range []string{
			"   ",                           // whitespace-only
			"not a url",                     // unparseable / spaces
			"ftp://host/v1",                 // unsupported scheme
			"file:///etc/passwd",            // unsupported scheme
			"://missing-scheme",             // no scheme
			"http://",                       // missing host
			"https://",                      // missing host
			"http://user:pass@host:8000/v1", // embedded userinfo (credential leak)
			"localhost:8000",                // relative, no scheme/host
		} {
			t.Run(key+"="+raw, func(t *testing.T) {
				clearEnv(t)
				t.Setenv("AGENT_DATABASE_URL", testDSN)
				t.Setenv(key, raw)

				_, err := Load()
				if err == nil {
					t.Fatalf("Load() error = nil, want error for %s=%q", key, raw)
				}
				if !strings.Contains(err.Error(), key) {
					t.Fatalf("error %q does not name %s", err.Error(), key)
				}
			})
		}
	}
}

// TestLoad_BaseURLUserinfoNotEchoed asserts that when a URL embeds credentials
// the resulting error never echoes the secret back (defense against leaking a
// misplaced token into logs). Covers M-04.
func TestLoad_BaseURLUserinfoNotEchoed(t *testing.T) {
	const secret = "sup3r-secret-token"
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", testDSN)
	t.Setenv("LLM_BASE_URL", "https://user:"+secret+"@api.example.com/v1")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want userinfo-rejected error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error message leaks embedded URL secret: %s", err.Error())
	}
}

// TestLoad_ValidBaseURLAccepted asserts representative valid absolute http(s)
// URLs are accepted and stored trimmed/canonical for both LLM and Blnk bases.
func TestLoad_ValidBaseURLAccepted(t *testing.T) {
	for _, raw := range []string{
		"http://localhost:11434/v1",
		"https://api.example.com/v1",
		"http://server:5001",
		"  http://localhost:8000/v1  ", // surrounding whitespace trimmed
	} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("LLM_BASE_URL", raw)
			t.Setenv("BLNK_BASE_URL", raw)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() unexpected error for base URL %q: %v", raw, err)
			}
			want := strings.TrimSpace(raw)
			if cfg.LLMBaseURL != want {
				t.Fatalf("LLMBaseURL = %q, want canonical %q", cfg.LLMBaseURL, want)
			}
			if cfg.BlnkBaseURL != want {
				t.Fatalf("BlnkBaseURL = %q, want canonical %q", cfg.BlnkBaseURL, want)
			}
		})
	}
}

// TestLoad_BlankModelRejected asserts that a blank or whitespace-only LLM_MODEL
// (an explicit misconfiguration, since the model name is Rule 5.6 config-driven)
// fails fast rather than sending an empty model to the endpoint.
func TestLoad_BlankModelRejected(t *testing.T) {
	for _, raw := range []string{" ", "   ", "\t", "\n"} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			clearEnv(t)
			t.Setenv("AGENT_DATABASE_URL", testDSN)
			t.Setenv("LLM_MODEL", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want error for blank LLM_MODEL=%q", raw)
			}
			if !strings.Contains(err.Error(), "LLM_MODEL") {
				t.Fatalf("error %q does not name LLM_MODEL", err.Error())
			}
		})
	}
}

// TestLoad_ModelTrimmed asserts a valid model with surrounding whitespace is
// stored in canonical trimmed form.
func TestLoad_ModelTrimmed(t *testing.T) {
	clearEnv(t)
	t.Setenv("AGENT_DATABASE_URL", testDSN)
	t.Setenv("LLM_MODEL", "  kimi-k3  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if cfg.LLMModel != "kimi-k3" {
		t.Fatalf("LLMModel = %q, want trimmed %q", cfg.LLMModel, "kimi-k3")
	}
}
