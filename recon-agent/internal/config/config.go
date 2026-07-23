// Package config loads and validates all runtime configuration for the
// recon-agent sidecar from environment variables.
//
// It is the single write-site for every configuration field (Gate 12 --
// Config Propagation Tracing): cmd/main.go calls Load once at startup and
// passes the resulting Config (or its fields) into every other package's
// constructor. All other packages read configuration from the returned
// Config and never call os.Getenv directly.
//
// Rule 5.1 (native-API-only): this package imports only the Go standard
// library and never any Blnk internal package.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Default values applied when the corresponding environment variable is unset
// or empty. These mirror the host-side defaults documented in the repository
// .env.example so the agent has sensible fallbacks without a .env file.
const (
	// defaultLLMBaseURL is a documentation-only placeholder pointing at an
	// OpenAI-compatible endpoint that serves the open-source Kimi K3 model.
	defaultLLMBaseURL = "https://api.moonshot.ai/v1"
	// defaultLLMModel is the open-source model name. Rule 5.6 requires the
	// model to be config-driven; this is the ONLY place the default name is
	// defined and it must never be hardcoded in downstream inference code.
	defaultLLMModel = "kimi-k3"
	// defaultBlnkBaseURL is the host-side Blnk reconciliation API URL used by
	// `make seed` / `make demo`. The docker-compose service overrides it to
	// http://server:5001 through BLNK_BASE_URL.
	defaultBlnkBaseURL = "http://localhost:5001"
	// defaultConfAutoThreshold is the confidence gate (Rule 5.4): the agent
	// auto-remediates only when confidence >= this value AND !regulated.
	defaultConfAutoThreshold = 0.85
	// defaultHitlPort is the listen port for the HITL review API + status page.
	defaultHitlPort = "8088"
)

// Config holds all runtime configuration for the recon-agent sidecar.
//
// Every exported field has a corresponding read-site in another package
// (Gate 12): the LLM* fields are read by internal/classifier, the Blnk*
// fields by internal/blnk, ConfAutoThreshold by internal/remediator, HitlPort
// by internal/hitl (via cmd/main.go), and AgentDatabaseURL by internal/store.
type Config struct {
	// LLMBaseURL is the OpenAI-compatible Chat Completions base URL
	// (LLM_BASE_URL). Read by the classifier.
	LLMBaseURL string
	// LLMApiKey is the API key/token for the LLM endpoint (LLM_API_KEY). It may
	// be empty for self-hosted endpoints. Read by the classifier.
	LLMApiKey string
	// LLMModel is the model name sent on every inference request (LLM_MODEL,
	// default "kimi-k3"). Read by the classifier. Rule 5.6.
	LLMModel string
	// BlnkBaseURL is the base URL of the Blnk reconciliation API
	// (BLNK_BASE_URL). Read by the blnk client.
	BlnkBaseURL string
	// BlnkApiKey is the value sent in the X-Blnk-Key header on every Blnk
	// request (BLNK_API_KEY). Read by the blnk client.
	BlnkApiKey string
	// ConfAutoThreshold is the auto-remediation confidence gate
	// (CONF_AUTO_THRESHOLD, default 0.85). Read by the remediator. Rule 5.4.
	ConfAutoThreshold float64
	// HitlPort is the listen port for the HITL API + status page (HITL_PORT,
	// default "8088"). Read by the hitl server via cmd/main.go.
	HitlPort string
	// AgentDatabaseURL is the PostgreSQL DSN for the agent-owned tables
	// (AGENT_DATABASE_URL). Required. Read by the store.
	AgentDatabaseURL string
}

// Load reads the recon-agent configuration from the process environment,
// applying defaults for optional fields and validating required ones.
//
// It returns an error when a required field is missing (AGENT_DATABASE_URL) or
// a typed field fails to parse (CONF_AUTO_THRESHOLD). Callers (cmd/main.go)
// should treat a non-nil error as fatal at startup.
func Load() (Config, error) {
	cfg := Config{
		LLMBaseURL:       getEnv("LLM_BASE_URL", defaultLLMBaseURL),
		LLMApiKey:        os.Getenv("LLM_API_KEY"),
		LLMModel:         getEnv("LLM_MODEL", defaultLLMModel),
		BlnkBaseURL:      getEnv("BLNK_BASE_URL", defaultBlnkBaseURL),
		BlnkApiKey:       os.Getenv("BLNK_API_KEY"),
		HitlPort:         getEnv("HITL_PORT", defaultHitlPort),
		AgentDatabaseURL: os.Getenv("AGENT_DATABASE_URL"),
	}

	threshold, err := parseFloat("CONF_AUTO_THRESHOLD", defaultConfAutoThreshold)
	if err != nil {
		return Config{}, err
	}
	cfg.ConfAutoThreshold = threshold

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// validate enforces required-field constraints. AGENT_DATABASE_URL is the only
// strictly required field; every other field has a safe default.
func (c Config) validate() error {
	if c.AgentDatabaseURL == "" {
		return fmt.Errorf("config: AGENT_DATABASE_URL is required")
	}
	return nil
}

// getEnv returns the value of the environment variable named by key, or def
// when the variable is unset or empty.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseFloat reads a float64 environment variable named by key. It returns def
// when the variable is unset or empty, and an error when a non-empty value
// cannot be parsed as a float64.
func parseFloat(key string, def float64) (float64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s %q: %w", key, raw, err)
	}
	return v, nil
}
