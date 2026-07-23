package config

import "testing"

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

	if cfg.LLMBaseURL != "https://api.moonshot.ai/v1" {
		t.Errorf("LLMBaseURL default = %q, want %q", cfg.LLMBaseURL, "https://api.moonshot.ai/v1")
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
