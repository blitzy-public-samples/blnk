# recon-agent — build/runtime image for the reconciliation sidecar.
#
# Multi-stage build mirroring Blnk's root Dockerfile (golang:1.25-alpine builder
# -> slim runtime), tailored to the self-contained recon-agent module.
#
# BUILD CONTEXT: ./recon-agent (see the docker-compose recon-agent service's
# `build.context: ./recon-agent`). Every COPY path below is therefore relative
# to the recon-agent module directory only — no Blnk source is (or may be)
# copied, and no blnk.json is mounted or baked in. The image is configured
# entirely at runtime by the environment variables docker-compose injects
# (LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, BLNK_BASE_URL, BLNK_API_KEY,
# CONF_AUTO_THRESHOLD, HITL_PORT, AGENT_DATABASE_URL).

# ---- build stage ----
FROM golang:1.25-alpine AS build-env
WORKDIR /src

# Leverage layer caching: download modules before copying the full source so the
# (slow) dependency layer is reused on every build where only source changes.
# go.sum is committed and complete, so this download is deterministic.
COPY go.mod go.sum ./
RUN go mod download

# Build the statically-linked agent binary. CGO_ENABLED=0 produces a fully
# static executable — every dependency (sashabaranov/go-openai, gin-gonic/gin,
# lib/pq, google/uuid) is pure Go, so CGO is unnecessary and disabling it lets
# the binary run on any slim base with no libc surprises. `./cmd` is this
# module's main package (recon-agent/cmd), the idiomatic equivalent of Blnk's
# root `./cmd/*.go`.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /recon-agent ./cmd

# ---- runtime stage (slim) ----
FROM alpine:3.20
# ca-certificates: REQUIRED for HTTPS calls to the OpenAI-compatible LLM
#   endpoint (e.g. https://api.moonshot.ai/v1). Without it, TLS fails with x509
#   errors, which (per fail-closed Rule 5.7) would route every break to HITL and
#   break the demo's auto-remediation success criterion.
# tzdata: correct RFC3339 audit timestamps when TZ is set to a named zone.
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build-env /recon-agent /usr/local/bin/recon-agent
# HITL review API + server-rendered status page (default 8088; overridable via
# the HITL_PORT env injected by docker-compose). recon-agent has no other
# listeners, so no other port is exposed.
EXPOSE 8088
# Default serve mode (NO -once flag): run the pipeline once, then serve the HITL
# API and status page on HITL_PORT. The -once flag is a host-only `make demo`
# tool and is never used inside the container.
CMD ["recon-agent"]
