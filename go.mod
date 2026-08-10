module github.com/blnkfinance/blnk

// The Go language and MINIMUM TOOLCHAIN version, and 1.25.12 is a security floor
// rather than a preference. 1.25.0 shipped with fixed vulnerabilities that this
// service is exposed to through code it runs on every request: net/url parsing
// (every inbound URL and every configured webhook endpoint), crypto/x509 and
// crypto/tls certificate handling (the Kafka SASL/TLS transport, the outbound
// webhook client and the PostgreSQL connection), and cmd/go plus cgo in the build
// itself. 1.25.12 is the current 1.25 maintenance release and carries all of those
// fixes.
//
// Keep this in step with the three go-version pins in .github/workflows/go.yml and
// with CONTRIBUTING.md. The Dockerfile deliberately tracks the floating
// golang:1.25-alpine tag, which always resolves to the newest 1.25 patch, so it
// needs no edit when this floor moves.
go 1.25.12

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/alicebob/miniredis/v2 v2.34.0
	github.com/aws/aws-sdk-go v1.55.6
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/caddyserver/certmagic v0.22.0
	github.com/didip/tollbooth/v7 v7.0.2
	github.com/gin-gonic/gin v1.10.0
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0
	github.com/go-redis/cache/v9 v9.0.0
	github.com/go-redis/redismock/v9 v9.2.0
	github.com/google/uuid v1.6.0
	github.com/hibiken/asynq v0.25.1
	github.com/hibiken/asynqmon v0.7.2
	github.com/jarcoal/httpmock v1.3.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.27
	github.com/pkg/errors v0.9.1
	github.com/posthog/posthog-go v1.3.3
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.67.5
	github.com/redis/go-redis/v9 v9.7.3
	github.com/rubenv/sql-migrate v1.7.1
	github.com/segmentio/kafka-go v0.4.51
	github.com/shopspring/decimal v1.4.0
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.11.1
	github.com/texttheater/golang-levenshtein/levenshtein v0.0.0-20200805054039-cae8b0eaed6c
	github.com/typesense/typesense-go v1.1.0
	github.com/wacul/ptr v1.0.0
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.60.0
	// The OpenTelemetry Go modules are a SECURITY FLOOR at v1.44.0 (and the paired
	// v0.65.0 / v0.18.0 releases of the exporter and log modules), not a preference.
	//
	// v1.43.0 and earlier are affected by the baggage-parsing denial-of-service
	// advisory in go.opentelemetry.io/otel/baggage, and that code is REACHABLE ON
	// EVERY REQUEST here rather than latent: internal/traces/otel.go installs a
	// composite propagator that includes baggage propagation, and the otelgin
	// middleware in api/api.go extracts it from inbound headers. An attacker-supplied
	// `baggage` header is therefore parsed before any handler runs. v1.44.0 is the
	// first release the advisory records as patched.
	//
	// Keep the whole family on one release line. The three module groups version
	// independently — stable at v1.x, exporters and the log signal at v0.x — so the
	// mapping is: otel/metric/trace/sdk/sdk-metric and the OTLP exporters at v1.44.0,
	// exporters/prometheus at v0.65.0, log/sdk-log/stdoutlog at v0.18.0. Mixing lines
	// compiles and then fails at run time on an interface the SDK and API disagree
	// about.
	//
	// otelgin is deliberately NOT moved in step. It is a contrib module on its own
	// v0.x line, it contains no baggage parser, and minimal version selection already
	// resolves go.opentelemetry.io/otel to the v1.44.0 required above, so the patched
	// parser is what the binary links. The contrib release paired with v1.44.0
	// (v0.69.0) additionally requires a newer gin, and upgrading the HTTP framework
	// inside a security patch is a blast radius this change has no reason to take on.
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0
	go.opentelemetry.io/otel/exporters/prometheus v0.65.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.18.0
	go.opentelemetry.io/otel/log v0.18.0
	go.opentelemetry.io/otel/metric v1.44.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/sdk/log v0.18.0
	go.opentelemetry.io/otel/sdk/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	golang.org/x/crypto v0.52.0
	golang.org/x/sync v0.20.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/alicebob/gopher-json v0.0.0-20230218143504-906a9b012302 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/sonic v1.12.10 // indirect
	github.com/bytedance/sonic/loader v0.2.3 // indirect
	github.com/caddyserver/zerossl v0.1.3 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/gin-contrib/sse v1.0.0 // indirect
	github.com/go-gorp/gorp/v3 v3.1.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-pkgz/expirable-cache/v3 v3.0.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.25.0 // indirect
	github.com/go-sql-driver/mysql v1.9.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/libdns/libdns v0.2.3 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mholt/acmez/v3 v3.1.0 // indirect
	github.com/miekg/dns v1.1.63 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oapi-codegen/runtime v1.1.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/sony/gobreaker v0.5.0 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/vmihailenco/go-tinylfu v0.2.2 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.uber.org/zap/exp v0.3.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/arch v0.14.0 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.81.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
