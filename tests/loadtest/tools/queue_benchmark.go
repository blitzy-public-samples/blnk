package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
	"github.com/hibiken/asynq"
)

type benchmarkConfig struct {
	configPath    string
	redisDSN      string
	redisSkipTLS  bool
	outPath       string
	queueNames    string
	queuePrefix   string
	queuePrefixes string
	label         string
	interval      time.Duration
	timeout       time.Duration
	wait          bool
}

type snapshot struct {
	timestamp      time.Time
	queueNames     []string
	pending        int
	active         int
	scheduled      int
	retry          int
	aggregating    int
	archived       int
	size           int
	processedTotal int
	failedTotal    int
	latencyMs      float64
}

type metric struct {
	Type       string                 `json:"type"`
	Contains   string                 `json:"contains"`
	Values     map[string]interface{} `json:"values"`
	Thresholds map[string]interface{} `json:"thresholds,omitempty"`
}

func main() {
	cfg := parseFlags()

	redisDSN, skipTLSVerify, err := resolveRedisSettings(cfg)
	if err != nil {
		exitf("failed to resolve redis settings: %v", err)
	}

	redisOpt, err := redis_db.ParseRedisURL(redisDSN, skipTLSVerify)
	if err != nil {
		exitf("failed to parse redis config: %v", err)
	}

	inspector := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:      redisOpt.Addr,
		Password:  redisOpt.Password,
		DB:        redisOpt.DB,
		TLSConfig: redisOpt.TLSConfig,
	})
	defer func() { _ = inspector.Close() }()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signalCh)
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	startDeadline := time.Now().Add(cfg.timeout)
	var started bool
	var startSnapshot snapshot
	var samples []snapshot
	var latestSnapshot snapshot
	var haveLatestSnapshot bool
	interrupted := false

	// Whether the loop ended because the clock ran out rather than because the queues
	// drained. Recorded at the break rather than inferred afterwards, because "ended with work
	// still queued" has three causes — the timeout, an interrupt, and a genuine drain that a
	// later arrival then added to — and only the first is a failure of the thing being
	// measured.
	timedOut := false

	for {
		snap, err := takeSnapshot(inspector, cfg)
		if err != nil {
			exitf("failed to inspect queues: %v", err)
		}
		latestSnapshot = snap
		haveLatestSnapshot = true

		if !started {
			if cfg.wait && !hasWork(snap) {
				if time.Now().After(startDeadline) {
					exitf("timed out waiting for queue backlog")
				}
				if waitForNextTickOrInterrupt(ticker, signalCh) {
					interrupted = true
					break
				}
				continue
			}

			started = true
			startSnapshot = snap
			samples = append(samples, snap)
			if err := writeCurrentSummary(cfg, startSnapshot, snap, samples); err != nil {
				exitf("failed to write summary: %v", err)
			}

			if isDrained(snap) {
				break
			}
			if waitForNextTickOrInterrupt(ticker, signalCh) {
				interrupted = true
				break
			}
			continue
		}

		samples = append(samples, snap)
		if err := writeCurrentSummary(cfg, startSnapshot, snap, samples); err != nil {
			exitf("failed to write summary: %v", err)
		}

		if isDrained(snap) {
			break
		}

		if time.Now().After(startDeadline) {
			timedOut = true

			break
		}

		if waitForNextTickOrInterrupt(ticker, signalCh) {
			interrupted = true
			break
		}
	}

	if !started {
		if interrupted && haveLatestSnapshot {
			if err := writeCurrentSummary(cfg, latestSnapshot, latestSnapshot, []snapshot{latestSnapshot}); err != nil {
				exitf("failed to write interrupted summary: %v", err)
			}
			fmt.Printf("queue benchmark interrupted before backlog was detected; snapshot written to %s\n", cfg.outPath)
			return
		}
		exitf("benchmark ended before any backlog was detected")
	}

	endSnapshot := samples[len(samples)-1]
	if interrupted && haveLatestSnapshot && latestSnapshot.timestamp.After(endSnapshot.timestamp) {
		endSnapshot = latestSnapshot
	}
	if err := writeCurrentSummary(cfg, startSnapshot, endSnapshot, samples); err != nil {
		exitf("failed to write final summary: %v", err)
	}

	fmt.Printf("queue benchmark summary written to %s\n", cfg.outPath)
	fmt.Printf("queues=%s drained=%t processed=%d failed=%d duration=%s\n",
		strings.Join(endSnapshot.queueNames, ","),
		isDrained(endSnapshot),
		maxInt(0, endSnapshot.processedTotal-startSnapshot.processedTotal),
		maxInt(0, endSnapshot.failedTotal-startSnapshot.failedTotal),
		endSnapshot.timestamp.Sub(startSnapshot.timestamp).Round(time.Millisecond),
	)
	if interrupted {
		fmt.Println("benchmark interrupted; summary contains partial results up to the last poll")
	}

	// A RUN THAT NEVER DRAINED IS A FAILED RUN, and it has to say so in its exit status.
	//
	// This tool exists to answer one question — do these queues clear, and how fast — and a
	// timeout means the answer was no. It previously reported that answer in the summary
	// (`drained: false`, `pass: 0`, `fail: 1`) and then exited 0 anyway, so every caller that
	// checks a status rather than parsing JSON recorded a pass. tests/loadtest/run_case.sh
	// runs under `set -e` and waits on this process, so the acceptance run it drives reported
	// success for a backlog that was still growing when the clock ran out.
	//
	// THE ORDER MATTERS: the summary is written and the human-readable lines are printed
	// FIRST, above, and only then does this exit. The artifact is how anyone diagnoses the
	// failure — how far the backlog got, how fast it was draining, whether it was moving at
	// all — so exiting before writing it would report the failure and destroy the evidence for
	// it in the same step.
	//
	// AN INTERRUPT IS NOT THIS CASE and deliberately still exits 0. SIGINT and SIGTERM mean
	// someone or something stopped the run on purpose — run_case.sh's own EXIT trap sends
	// SIGINT when it tears the benchmark down early — and a partial result that was asked for
	// is not a failed measurement. It is reported on its own line above so it cannot be
	// mistaken for a completed run.
	if timedOut && !isDrained(endSnapshot) {
		fmt.Fprintf(os.Stderr,
			"queue benchmark FAILED: the queues did not drain within %s. %d task(s) were still "+
				"queued at the last poll (%s). The summary at %s carries the full sample series; "+
				"read the drain rate against the arrival rate to tell a slow consumer from a "+
				"stalled one, and raise -timeout only once you know which it is.\n",
			cfg.timeout, outstandingWork(endSnapshot), describeOutstandingWork(endSnapshot), cfg.outPath)
		os.Exit(1)
	}
}

func parseFlags() benchmarkConfig {
	cfg := benchmarkConfig{}
	flag.StringVar(&cfg.configPath, "config", "", "Optional path to blnk config file. Only the redis section is read.")
	flag.StringVar(&cfg.redisDSN, "redis-dsn", "", "Redis DSN. Overrides BLNK_REDIS_DNS and config file.")
	flag.BoolVar(&cfg.redisSkipTLS, "redis-skip-tls-verify", false, "Skip TLS verification for Redis when using -redis-dsn.")
	flag.StringVar(&cfg.outPath, "out", "tests/loadtest/queue-summary.json", "Output path for the generated summary JSON.")
	flag.StringVar(&cfg.queueNames, "queues", "", "Comma-separated queue names to track.")
	flag.StringVar(&cfg.queuePrefix, "queue-prefix", "", "Track all queues whose name starts with this prefix.")
	flag.StringVar(&cfg.queuePrefixes, "queue-prefixes", "", "Comma-separated queue prefixes to track.")
	flag.StringVar(&cfg.label, "label", "queue_drain", "Scenario label to embed in the generated summary.")
	flag.DurationVar(&cfg.interval, "interval", time.Second, "Polling interval for queue inspection.")
	flag.DurationVar(&cfg.timeout, "timeout", 15*time.Minute, "Maximum benchmark duration.")
	flag.BoolVar(&cfg.wait, "wait", false, "Wait for backlog to appear before starting the benchmark.")
	flag.Parse()

	if strings.TrimSpace(cfg.queueNames) == "" && strings.TrimSpace(cfg.queuePrefix) == "" && strings.TrimSpace(cfg.queuePrefixes) == "" {
		exitf("either -queues, -queue-prefix, or -queue-prefixes must be set")
	}
	if cfg.interval <= 0 {
		exitf("interval must be greater than zero")
	}
	if cfg.timeout <= 0 {
		exitf("timeout must be greater than zero")
	}
	return cfg
}

func resolveRedisSettings(cfg benchmarkConfig) (dsn string, skipTLSVerify bool, err error) {
	if strings.TrimSpace(cfg.redisDSN) != "" {
		return strings.TrimSpace(cfg.redisDSN), cfg.redisSkipTLS, nil
	}

	if envDSN := strings.TrimSpace(os.Getenv("BLNK_REDIS_DNS")); envDSN != "" {
		return envDSN, parseEnvBool("BLNK_REDIS_SKIP_TLS_VERIFY"), nil
	}

	if strings.TrimSpace(cfg.configPath) == "" {
		return "", false, fmt.Errorf("redis DSN not found; pass -redis-dsn, set BLNK_REDIS_DNS, or use -config")
	}

	type redisSection struct {
		DNS           string `json:"dns"`
		SkipTLSVerify bool   `json:"skip_tls_verify"`
	}
	type fileConfig struct {
		Redis redisSection `json:"redis"`
	}

	data, readErr := os.ReadFile(cfg.configPath)
	if readErr != nil {
		return "", false, readErr
	}

	var parsed fileConfig
	if unmarshalErr := json.Unmarshal(data, &parsed); unmarshalErr != nil {
		return "", false, unmarshalErr
	}

	if strings.TrimSpace(parsed.Redis.DNS) == "" {
		return "", false, fmt.Errorf("redis.dns missing in config file")
	}

	return strings.TrimSpace(parsed.Redis.DNS), parsed.Redis.SkipTLSVerify, nil
}

func takeSnapshot(inspector *asynq.Inspector, cfg benchmarkConfig) (snapshot, error) {
	queueNames, err := resolveQueueNames(inspector, cfg)
	if err != nil {
		return snapshot{}, err
	}

	sort.Strings(queueNames)

	snap := snapshot{
		timestamp:  time.Now().UTC(),
		queueNames: queueNames,
	}

	for _, name := range queueNames {
		info, err := inspector.GetQueueInfo(name)
		if err != nil {
			return snapshot{}, fmt.Errorf("queue %s: %w", name, err)
		}

		snap.pending += info.Pending
		snap.active += info.Active
		snap.scheduled += info.Scheduled
		snap.retry += info.Retry
		snap.aggregating += info.Aggregating
		snap.archived += info.Archived
		snap.size += info.Size
		snap.processedTotal += info.ProcessedTotal
		snap.failedTotal += info.FailedTotal
		if latencyMs := float64(info.Latency.Milliseconds()); latencyMs > snap.latencyMs {
			snap.latencyMs = latencyMs
		}
	}

	return snap, nil
}

func resolveQueueNames(inspector *asynq.Inspector, cfg benchmarkConfig) ([]string, error) {
	names := make(map[string]struct{})

	for _, raw := range strings.Split(cfg.queueNames, ",") {
		name := strings.TrimSpace(raw)
		if name != "" {
			names[name] = struct{}{}
		}
	}

	prefixes := configuredPrefixes(cfg)
	if len(prefixes) > 0 {
		all, err := inspector.Queues()
		if err != nil {
			return nil, err
		}
		for _, name := range all {
			for _, prefix := range prefixes {
				if strings.HasPrefix(name, prefix) {
					names[name] = struct{}{}
					break
				}
			}
		}
	}

	resolved := make([]string, 0, len(names))
	for name := range names {
		resolved = append(resolved, name)
	}
	return resolved, nil
}

func configuredPrefixes(cfg benchmarkConfig) []string {
	prefixSet := make(map[string]struct{})

	if prefix := strings.TrimSpace(cfg.queuePrefix); prefix != "" {
		prefixSet[prefix] = struct{}{}
	}

	for _, raw := range strings.Split(cfg.queuePrefixes, ",") {
		prefix := strings.TrimSpace(raw)
		if prefix != "" {
			prefixSet[prefix] = struct{}{}
		}
	}

	prefixes := make([]string, 0, len(prefixSet))
	for prefix := range prefixSet {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	return prefixes
}

func hasWork(s snapshot) bool {
	return s.pending+s.active+s.scheduled+s.retry+s.aggregating > 0
}

func isDrained(s snapshot) bool {
	return !hasWork(s)
}

// outstandingWork totals the task states hasWork counts, so the number reported when a run
// fails is the same number that decided it failed. Summing a different set — `size`, say, or
// pending alone — would let the tool exit non-zero while reporting nothing outstanding.
func outstandingWork(s snapshot) int {
	return s.pending + s.active + s.scheduled + s.retry + s.aggregating
}

// describeOutstandingWork breaks that total down by state, because the states mean different
// things to whoever has to act on the failure: pending is work the consumers have not reached,
// active is work in flight, retry is work that FAILED and is coming back, and scheduled is
// work not yet due — a backlog that is entirely scheduled has not stalled at all, it is
// waiting for its own clock. Only non-zero states are listed so the line stays readable.
func describeOutstandingWork(s snapshot) string {
	states := []struct {
		name  string
		count int
	}{
		{"pending", s.pending},
		{"active", s.active},
		{"scheduled", s.scheduled},
		{"retry", s.retry},
		{"aggregating", s.aggregating},
	}

	parts := make([]string, 0, len(states))
	for _, state := range states {
		if state.count > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", state.name, state.count))
		}
	}

	if len(parts) == 0 {
		return "no outstanding tasks"
	}

	return strings.Join(parts, " ")
}

func buildSummary(cfg benchmarkConfig, start, end snapshot, samples []snapshot) map[string]interface{} {
	durationMs := end.timestamp.Sub(start.timestamp).Seconds() * 1000
	if durationMs < 0 {
		durationMs = 0
	}

	processed := maxInt(0, end.processedTotal-start.processedTotal)
	failed := maxInt(0, end.failedTotal-start.failedTotal)
	successes := maxInt(0, processed-failed)
	failRate := rate(failed, processed)
	requestRate := counterRate(processed, durationMs)
	latencySeries := collectLatencySeries(samples)
	latencyStats := trendStats(latencySeries)
	queueSizeStats := trendStats(collectIntSeries(samples, func(s snapshot) int { return s.size }))
	queueDepthStats := trendStats(collectIntSeries(samples, func(s snapshot) int {
		return s.pending + s.active + s.scheduled + s.retry + s.aggregating
	}))
	drained := isDrained(end)

	metrics := map[string]interface{}{
		fmt.Sprintf("http_req_failed{scenario:%s}", cfg.label): metric{
			Type:     "rate",
			Contains: "default",
			Values: map[string]interface{}{
				"rate":   failRate,
				"passes": float64(successes),
				"fails":  float64(failed),
			},
			Thresholds: map[string]interface{}{
				"rate<0.001": map[string]interface{}{"ok": failRate < 0.001},
			},
		},
		"http_req_failed": metric{
			Type:     "rate",
			Contains: "default",
			Values: map[string]interface{}{
				"rate":   failRate,
				"passes": float64(successes),
				"fails":  float64(failed),
			},
		},
		"http_req_duration": metric{
			Type:     "trend",
			Contains: "time",
			Values:   latencyStats,
		},
		"http_req_duration{expected_response:true}": metric{
			Type:     "trend",
			Contains: "time",
			Values:   latencyStats,
		},
		fmt.Sprintf("http_req_duration{scenario:%s,expected_response:true}", cfg.label): metric{
			Type:     "trend",
			Contains: "time",
			Values:   latencyStats,
		},
		"http_reqs": metric{
			Type:     "counter",
			Contains: "default",
			Values: map[string]interface{}{
				"count": float64(processed),
				"rate":  requestRate,
			},
		},
		"iterations": metric{
			Type:     "counter",
			Contains: "default",
			Values: map[string]interface{}{
				"count": float64(processed),
				"rate":  requestRate,
			},
		},
		"checks": metric{
			Type:     "rate",
			Contains: "default",
			Values: map[string]interface{}{
				"rate":   boolRate(drained),
				"passes": boolCount(drained),
				"fails":  boolCount(!drained),
			},
		},
		"vus": metric{
			Type:     "gauge",
			Contains: "default",
			Values: map[string]interface{}{
				"value": 1.0,
				"min":   1.0,
				"max":   1.0,
			},
		},
		"vus_max": metric{
			Type:     "gauge",
			Contains: "default",
			Values: map[string]interface{}{
				"value": 1.0,
				"min":   1.0,
				"max":   1.0,
			},
		},
		"data_sent": metric{
			Type:     "counter",
			Contains: "data",
			Values: map[string]interface{}{
				"count": 0.0,
				"rate":  0.0,
			},
		},
		"data_received": metric{
			Type:     "counter",
			Contains: "data",
			Values: map[string]interface{}{
				"count": 0.0,
				"rate":  0.0,
			},
		},
		"queue_size": metric{
			Type:     "trend",
			Contains: "default",
			Values:   queueSizeStats,
		},
		"queue_depth": metric{
			Type:     "trend",
			Contains: "default",
			Values:   queueDepthStats,
		},
		"queue_tasks_processed": metric{
			Type:     "counter",
			Contains: "default",
			Values: map[string]interface{}{
				"count": float64(processed),
				"rate":  requestRate,
			},
		},
		"queue_tasks_failed": metric{
			Type:     "counter",
			Contains: "default",
			Values: map[string]interface{}{
				"count": float64(failed),
				"rate":  counterRate(failed, durationMs),
			},
		},
		"queue_drain_duration": metric{
			Type:     "trend",
			Contains: "time",
			Values:   singleValueTrend(durationMs),
		},
	}

	return map[string]interface{}{
		"metrics": metrics,
		"root_group": map[string]interface{}{
			"id":     "queue-drain-root",
			"name":   "",
			"path":   "",
			"groups": []interface{}{},
			"checks": []map[string]interface{}{
				{
					"name":   "queue drained",
					"path":   "::queue drained",
					"id":     "queue-drain-check",
					"passes": boolInt(drained),
					"fails":  boolInt(!drained),
				},
			},
		},
		"options": map[string]interface{}{
			"summaryTrendStats": []string{"avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"},
			"summaryTimeUnit":   "ms",
			"noColor":           false,
		},
		"state": map[string]interface{}{
			"isStdOutTTY":       false,
			"isStdErrTTY":       false,
			"testRunDurationMs": durationMs,
			"queues":            end.queueNames,
			"queuePrefix":       cfg.queuePrefix,
		},
	}
}

func writeCurrentSummary(cfg benchmarkConfig, start, end snapshot, samples []snapshot) error {
	summary := buildSummary(cfg, start, end, samples)
	if err := os.MkdirAll(filepath.Dir(cfg.outPath), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(cfg.outPath, data, 0o644)
}

func waitForNextTickOrInterrupt(ticker *time.Ticker, signalCh <-chan os.Signal) bool {
	select {
	case <-ticker.C:
		return false
	case <-signalCh:
		return true
	}
}

func collectLatencySeries(samples []snapshot) []float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, sample.latencyMs)
	}
	return values
}

func collectIntSeries(samples []snapshot, pick func(snapshot) int) []float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, float64(pick(sample)))
	}
	return values
}

func trendStats(values []float64) map[string]interface{} {
	if len(values) == 0 {
		return map[string]interface{}{
			"avg":   0.0,
			"min":   0.0,
			"med":   0.0,
			"max":   0.0,
			"p(90)": 0.0,
			"p(95)": 0.0,
			"p(99)": 0.0,
		}
	}

	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	return map[string]interface{}{
		"avg":   average(sorted),
		"min":   sorted[0],
		"med":   percentile(sorted, 50),
		"max":   sorted[len(sorted)-1],
		"p(90)": percentile(sorted, 90),
		"p(95)": percentile(sorted, 95),
		"p(99)": percentile(sorted, 99),
	}
}

func singleValueTrend(value float64) map[string]interface{} {
	return map[string]interface{}{
		"avg":   value,
		"min":   value,
		"med":   value,
		"max":   value,
		"p(90)": value,
		"p(95)": value,
		"p(99)": value,
	}
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}

	position := (pct / 100) * float64(len(values)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return values[lower]
	}
	weight := position - float64(lower)
	return values[lower] + (values[upper]-values[lower])*weight
}

func rate(failures, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(failures) / float64(total)
}

func counterRate(count int, durationMs float64) float64 {
	if count <= 0 || durationMs <= 0 {
		return 0
	}
	return float64(count) / (durationMs / 1000)
}

func boolRate(ok bool) float64 {
	if ok {
		return 1
	}
	return 0
}

func boolCount(ok bool) float64 {
	if ok {
		return 1
	}
	return 0
}

func boolInt(ok bool) int {
	if ok {
		return 1
	}
	return 0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func parseEnvBool(key string) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false
	}
	value, err := strconv.ParseBool(raw)
	return err == nil && value
}
