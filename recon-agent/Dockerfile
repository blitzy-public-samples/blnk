# recon-agent — build/runtime image for the reconciliation sidecar.
#
# Multi-stage build (golang:1.25-alpine builder -> minimal alpine runtime),
# tailored to the self-contained recon-agent module.
#
# BUILD CONTEXT: the repository ROOT (not ./recon-agent). The docker-compose
# recon-agent service sets `build.context: .` with `dockerfile:
# recon-agent/Dockerfile`, and CI builds it with `docker build -f
# recon-agent/Dockerfile .`. A root context is REQUIRED (finding C-04) so the
# runtime image can PACKAGE the seed statement fixture
# (seed/external_transactions.csv); without it the container has no CSV to
# ingest, performs zero work, and never becomes ready. Only the recon-agent
# module's Go code is compiled — no Blnk source is copied or imported (Rule 5.1).
#
# The image is configured entirely at runtime by the environment variables
# docker-compose injects (LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, BLNK_BASE_URL,
# BLNK_API_KEY, CONF_AUTO_THRESHOLD, HITL_PORT, AGENT_DATABASE_URL).

# ---- build stage ----
# Digest-pinned, supported base (finding M-20): the tag comment is for humans /
# Dependabot; the @sha256 is what Docker resolves, so the builder is immutable.
FROM golang:1.25-alpine3.22@sha256:65b4400aee0927412e9ed791a11893273a49d55df24841f7599660fb80dae464 AS build-env
WORKDIR /src

# Leverage layer caching: download modules before copying the full source so the
# (slow) dependency layer is reused on every build where only source changes.
# go.sum is committed and complete, so this download is deterministic. Paths are
# relative to the repository-root build context, so the recon-agent module's
# manifests are addressed explicitly.
COPY recon-agent/go.mod recon-agent/go.sum ./
RUN go mod download

# Build the statically-linked agent binary from the recon-agent module ONLY.
# CGO_ENABLED=0 produces a fully static executable — every dependency
# (sashabaranov/go-openai, gin-gonic/gin, lib/pq, google/uuid) is pure Go, so
# CGO is unnecessary and disabling it lets the binary run on any slim base with
# no libc surprises. `./cmd` is this module's main package (recon-agent/cmd).
COPY recon-agent/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /recon-agent ./cmd

# ---- runtime stage (minimal, non-root) ----
# Digest-pinned, currently-supported Alpine (finding M-20 — replaces the
# end-of-life alpine:3.20). The @sha256 makes the base immutable; the tag comment
# tracks the human-readable version for updates.
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

# Pin exact package versions (finding M-20 — no floating apk pins). ca-certificates
# is REQUIRED for HTTPS to the OpenAI-compatible LLM endpoint (without it TLS
# fails with x509 errors, which per fail-closed Rule 5.7 would route every break
# to HITL). tzdata yields correct RFC3339 audit timestamps when TZ is a named
# zone. busybox already provides wget, used by the HEALTHCHECK below.
RUN apk add --no-cache ca-certificates=20260611-r0 tzdata=2026c-r0

# Run as a dedicated non-root user (finding M-20). /app is the agent's home and
# working directory and must be writable so serve mode can emit the resolved /
# summary artifacts.
RUN addgroup -S recon && adduser -S -G recon -h /app recon
WORKDIR /app

COPY --from=build-env /recon-agent /usr/local/bin/recon-agent

# Package the seed statement fixture (finding C-04). The agent's default -csv
# path is ../seed/external_transactions.csv, which from WORKDIR /app resolves to
# /seed/external_transactions.csv, so serve mode has a real 6-break statement to
# ingest on startup with no host mount required.
COPY seed/external_transactions.csv /seed/external_transactions.csv

# The non-root user owns its working tree so pipeline artifacts can be written.
RUN chown -R recon:recon /app && chmod 0555 /seed/external_transactions.csv
USER recon

# HITL review API + server-rendered status page (default 8088; overridable via
# the HITL_PORT env injected by docker-compose). recon-agent has no other
# listeners, so no other port is exposed.
EXPOSE 8088

# Container liveness (finding M-16): probe the always-on /healthz endpoint (the
# HITL server binds and reports live before any long work; /readyz separately
# gates true readiness). Shell form so ${HITL_PORT} is expanded at run time.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD wget -q -O- "http://127.0.0.1:${HITL_PORT:-8088}/healthz" >/dev/null 2>&1 || exit 1

# Default serve mode (NO -once flag): run the pipeline once, then serve the HITL
# API and status page on HITL_PORT. The -once flag is a host-only `make demo`
# tool and is never used inside the container. docker-compose may pass a
# `-source` to isolate the container's boot run from host-driven `make demo`.
CMD ["recon-agent"]
