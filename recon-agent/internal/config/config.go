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
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Default values applied when the corresponding environment variable is unset
// or empty. These mirror the host-side defaults documented in the repository
// .env.example so the agent has sensible fallbacks without a .env file.
const (
	// defaultLLMBaseURL is a NON-ROUTABLE, loopback-only placeholder pointing at
	// the conventional local OpenAI-compatible port (e.g. a self-hosted vLLM or
	// Ollama server). It is deliberately NOT a live third-party provider: a
	// developer who copies the shipped defaults and runs the agent must never
	// silently transmit transaction data to an external endpoint (M-04). If no
	// local server is listening, the classifier fails closed and every break is
	// routed to HITL (Rule 5.7), which is the safe outcome. Operators point
	// LLM_BASE_URL at their own OpenAI-compatible endpoint explicitly.
	defaultLLMBaseURL = "http://localhost:11434/v1"
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
// fields by internal/blnk, ConfAutoThreshold and AgentBaseCurrency by
// internal/remediator, HitlPort by internal/hitl (via cmd/main.go), and
// AgentDatabaseURL by internal/store.
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
	// AgentBaseCurrency is the ledger's settlement (base) currency
	// (AGENT_BASE_CURRENCY). OPTIONAL — when empty, the independent non-LLM
	// regulated backstop it powers is disabled. When set, the remediator treats
	// any break whose currency differs from it as regulated REGARDLESS of the
	// classifier's LLM-sourced regulated flag, so a prompt-injection that flips
	// regulated=false can never open the auto path for a foreign-currency break
	// (INFO#2 defense-in-depth for finding F-1, above the always-on deterministic
	// cohort dry-run). Read by the remediator (via cmd/main.go).
	AgentBaseCurrency string
}

// Load reads the recon-agent configuration from the process environment,
// applying defaults for optional fields and validating required and typed
// ones.
//
// It returns a descriptive error when a required field is missing or
// whitespace-only (AGENT_DATABASE_URL, LLM_MODEL) or a typed field fails to
// parse or falls outside its accepted range: CONF_AUTO_THRESHOLD must be a
// finite value in [0,1]; HITL_PORT must be an integer TCP port in 1..65535; and
// LLM_BASE_URL / BLNK_BASE_URL must each be an absolute http(s) URL with a host
// and NO embedded userinfo/credentials (M-04). Failing fast on an unusable
// value is deliberate: silently accepting an out-of-range CONF_AUTO_THRESHOLD
// would subvert the Rule 5.4 auto-remediation gate; a malformed or non-HTTP(S)
// LLM_BASE_URL could send transaction data to an unintended destination; and an
// unusable DSN or port would otherwise surface only later as an opaque
// connect/bind error. Error messages deliberately never echo the offending URL
// or DSN, so a credential accidentally embedded in one is not leaked. Callers
// (cmd/main.go) should treat a non-nil error as fatal at startup.
func Load() (Config, error) {
	cfg := Config{
		LLMBaseURL:  getEnv("LLM_BASE_URL", defaultLLMBaseURL),
		LLMApiKey:   os.Getenv("LLM_API_KEY"),
		LLMModel:    getEnv("LLM_MODEL", defaultLLMModel),
		BlnkBaseURL: getEnv("BLNK_BASE_URL", defaultBlnkBaseURL),
		BlnkApiKey:  os.Getenv("BLNK_API_KEY"),
		// Trim surrounding whitespace so a whitespace-only DSN (e.g. from a
		// templating slip) fails the required-field check below instead of
		// being stored verbatim and failing later at DB-connect time.
		AgentDatabaseURL: strings.TrimSpace(os.Getenv("AGENT_DATABASE_URL")),
		// Optional independent (non-LLM) regulated backstop of INFO#2. Trimmed;
		// empty disables the backstop. Not validated as required — a deployment
		// that omits it simply relies on the always-on deterministic cohort
		// dry-run (Rule 5.3) without the extra currency guard.
		AgentBaseCurrency: strings.TrimSpace(os.Getenv("AGENT_BASE_CURRENCY")),
	}

	threshold, err := parseFloat("CONF_AUTO_THRESHOLD", defaultConfAutoThreshold)
	if err != nil {
		return Config{}, err
	}
	cfg.ConfAutoThreshold = threshold

	port, err := parsePort("HITL_PORT", defaultHitlPort)
	if err != nil {
		return Config{}, err
	}
	cfg.HitlPort = port

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// validate enforces required-field, URL, and range constraints, and rewrites
// the URL/model fields in place to their trimmed canonical form. It uses a
// pointer receiver so the canonicalized values persist on the Config returned
// by Load.
//
// AGENT_DATABASE_URL and LLM_MODEL are required (LLM_MODEL has a default but a
// caller may not blank it out). LLM_BASE_URL and BLNK_BASE_URL must each be an
// absolute http(s) URL with a host and no embedded userinfo (see
// validateHTTPURL). The confidence threshold must be a finite value within the
// closed interval [0,1] so the Rule 5.4 auto-remediation gate
// (confidence >= threshold) behaves as intended: a threshold <= 0 would
// auto-remediate every break regardless of LLM confidence; NaN or > 1 would
// silently disable auto-remediation. Each of these is a misconfiguration that
// must fail fast rather than subvert a safety gate or exfiltrate data.
func (c *Config) validate() error {
	if c.AgentDatabaseURL == "" {
		return fmt.Errorf("config: AGENT_DATABASE_URL is required")
	}

	llmBase, err := validateHTTPURL("LLM_BASE_URL", c.LLMBaseURL)
	if err != nil {
		return err
	}
	c.LLMBaseURL = llmBase

	blnkBase, err := validateHTTPURL("BLNK_BASE_URL", c.BlnkBaseURL)
	if err != nil {
		return err
	}
	c.BlnkBaseURL = blnkBase

	model := strings.TrimSpace(c.LLMModel)
	if model == "" {
		return fmt.Errorf("config: LLM_MODEL is required and must not be blank")
	}
	c.LLMModel = model

	if math.IsNaN(c.ConfAutoThreshold) || math.IsInf(c.ConfAutoThreshold, 0) ||
		c.ConfAutoThreshold < 0 || c.ConfAutoThreshold > 1 {
		return fmt.Errorf(
			"config: CONF_AUTO_THRESHOLD %v is out of range; must be a finite value in [0,1]",
			c.ConfAutoThreshold,
		)
	}
	return nil
}

// validateHTTPURL parses raw as an absolute OpenAI-compatible / Blnk HTTP(S)
// service URL and returns its trimmed canonical form. It fails closed on the
// classes of malformed value that would otherwise surface only at request time
// or, worse, silently transmit data to an unintended destination (M-04): a
// value that is empty/whitespace-only, is not parseable, uses a scheme other
// than http or https, omits a host, or embeds userinfo (credentials in the
// URL).
//
// Embedded userinfo is rejected outright because it both leaks a secret into
// logs/redirect targets and signals a misconfiguration; the API key belongs in
// the dedicated *_API_KEY variable and the X-Blnk-Key / Authorization header,
// never in the base URL. The returned error names only the variable and (for an
// unsupported scheme) the scheme; it never echoes the raw value, so a
// credential accidentally embedded in the URL is not leaked through the error.
func validateHTTPURL(name, raw string) (string, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "", fmt.Errorf("config: %s is required and must be an absolute http(s) URL", name)
	}
	u, err := url.Parse(v)
	if err != nil {
		return "", fmt.Errorf("config: invalid %s: not a valid URL", name)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("config: invalid %s: scheme %q is not supported; use http or https", name, u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("config: invalid %s: must be an absolute URL including a host", name)
	}
	if u.User != nil {
		return "", fmt.Errorf("config: invalid %s: URL must not embed userinfo/credentials; put the secret in the API-key variable instead", name)
	}
	return v, nil
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

// parsePort reads a TCP port environment variable named by key. Surrounding
// whitespace is trimmed first, so an accidental value like "8088 " is accepted
// and canonicalized. It returns def when the variable is unset or empty (after
// trimming), and a descriptive error when the value does not parse to an
// integer within the valid TCP port range 1..65535. The returned value is the
// trimmed, canonical port string the HITL server binds its listener to (as
// ":" + port); validating here fails fast instead of surfacing an opaque
// net.Listen error later at bind time.
func parsePort(key, def string) (string, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		raw = def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return "", fmt.Errorf("config: invalid %s %q: must be an integer TCP port in 1..65535", key, raw)
	}
	if n < 1 || n > 65535 {
		return "", fmt.Errorf("config: %s %d is out of range; must be in 1..65535", key, n)
	}
	return raw, nil
}
