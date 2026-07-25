#!/usr/bin/env python3
# Copyright 2024 Blnk Finance Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A tiny, dependency-free OpenAI-compatible Chat Completions stub for CI.

Why this exists
---------------
`make demo` runs the recon-agent pipeline end-to-end and must AUTO-REMEDIATE at
least one break, proving the deterministic-arbiter loop (Rule 5.3) works: the
agent proposes a Blnk matching rule, Blnk's dry-run reconciliation confirms the
break cleared, and the break is marked resolved. The classifier
(internal/classifier) obtains its verdict from an open-source model over an
OpenAI-compatible endpoint (Rule 5.6) at LLM_BASE_URL. In CI there is no real
Kimi K3 endpoint, and with LLM_BASE_URL unset the classifier fail-closes every
break to HITL (Rule 5.7) — so nothing auto-remediates and the demo's
"auto-resolved >= 1" success criterion (AAP §0.1.1) cannot be exercised.

This stub is that endpoint. It speaks the minimal slice of the OpenAI Chat
Completions HTTP contract that github.com/sashabaranov/go-openai uses
(POST <base>/chat/completions returning choices[0].message.content), and returns
a DETERMINISTIC, per-break classification whose JSON shape exactly matches what
internal/classifier.parseClassification expects:

    {"root_cause","confidence","regulated","proposed_rule":{"field","operator",
     "value"},"rationale"}

It is NOT a model: it classifies each break by the distinctive keywords in the
seed CSV's Description column (seed/external_transactions.csv), which survive the
pipeline's run-scoped id rewrite (only the id changes; amount/currency/reference/
description are preserved), so it can key off them without depending on the id.
The proposed rule mirrors the proven mechanism used by the module's own
integration test: an {amount, equals} criterion clears a break when the external
amount equals an internal booking's amount (Blnk compares field-to-field; the
literal `value` is unused for the equals field-match, hence empty).

Against the six seeded breaks and their internal counterparts
(INV-1001=1500, INV-1002=250, INV-1003=980, INV-1006=700 USD) this yields a
deterministic, realistic mix — two auto-resolved (timing 1500 -> INV-1001,
reference_mismatch 980 -> INV-1003) and four escalated (amount_drift 250.75 does
NOT equal 250; duplicate, missing_internal and the regulated currency_mismatch
are never auto-eligible) — satisfying auto-resolved >= 1 while never
auto-remediating a regulated or non-eligible break (Rules 5.3/5.4).

Usage
-----
    python3 .github/scripts/llm_stub.py [--port PORT] [--host HOST]

Defaults: host 127.0.0.1, port from $LLM_STUB_PORT or 8099. The server prints
"llm-stub: listening on http://HOST:PORT" to stdout once ready (poll it, or the
/healthz route, for readiness), and logs each classification to stderr. Point the
agent at it with LLM_BASE_URL=http://HOST:PORT/v1 and any non-empty LLM_API_KEY.
It is stdlib-only (no pip install) so it runs anywhere Python 3 is available.
"""

import argparse
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The closed root-cause enumeration (mirrors internal/model.RootCause). Anything
# the classifier does not recognize is normalized to "unknown".
ROOT_TIMING = "timing"
ROOT_AMOUNT_DRIFT = "amount_drift"
ROOT_REFERENCE_MISMATCH = "reference_mismatch"
ROOT_DUPLICATE = "duplicate"
ROOT_MISSING_INTERNAL = "missing_internal"
ROOT_CURRENCY_MISMATCH = "currency_mismatch"
ROOT_UNKNOWN = "unknown"

# Field extractors for the user prompt rendered by classifier.buildUserPrompt,
# whose lines look like "- amount: 1500.0000", "- currency: USD", etc.
_AMOUNT_RE = re.compile(r"^\s*-\s*amount:\s*([0-9]+(?:\.[0-9]+)?)", re.MULTILINE)
_CURRENCY_RE = re.compile(r"^\s*-\s*currency:\s*([A-Za-z]{3})", re.MULTILINE)
_REFERENCE_RE = re.compile(r"^\s*-\s*reference:\s*(.+?)\s*$", re.MULTILINE)
_DESCRIPTION_RE = re.compile(r"^\s*-\s*description:\s*(.+?)\s*$", re.MULTILINE)


def _extract(regex, text, default=""):
    m = regex.search(text or "")
    return m.group(1).strip() if m else default


def _user_content(payload):
    """Return the concatenated user-role message content from a request body."""
    parts = []
    for msg in payload.get("messages", []) or []:
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str):
                parts.append(content)
    return "\n".join(parts)


def classify(user_content):
    """Deterministically classify one break from the user prompt.

    Keys off the seed CSV's distinctive Description keywords (plus currency for
    the cross-border case). Returns the dict the recon-agent classifier parses.
    A rule is proposed ONLY for the three auto-eligible root causes (timing,
    amount_drift, reference_mismatch); duplicate/missing_internal/
    currency_mismatch/unknown omit `proposed_rule` so the break is escalated
    (Rules 5.3/5.4). currency_mismatch is additionally flagged regulated.
    """
    desc = _extract(_DESCRIPTION_RE, user_content).lower()
    currency = _extract(_CURRENCY_RE, user_content).upper()
    reference = _extract(_REFERENCE_RE, user_content).upper()

    # An {amount, equals} criterion; Blnk matches external.amount to an internal
    # booking's amount field-to-field, so `value` is intentionally empty.
    amount_equals_rule = {"field": "amount", "operator": "equals", "value": ""}

    # Order matters: check the more specific keywords first so, e.g., the
    # "Duplicate re-posting of ... INV-1001" line is not mistaken for timing.
    if "duplicate" in desc:
        root, conf, regulated, rule = ROOT_DUPLICATE, 0.95, False, None
    elif "unrecognized" in desc or "no internal booking" in desc or reference.startswith("UNKN"):
        root, conf, regulated, rule = ROOT_MISSING_INTERNAL, 0.92, False, None
    elif currency == "EUR" or "cross-border" in desc or "sepa" in desc:
        # Cross-border FX / currency mismatch: treat as regulated -> escalate.
        root, conf, regulated, rule = ROOT_CURRENCY_MISMATCH, 0.93, True, None
    elif "fee" in desc or "drift" in desc:
        # Small amount difference vs the internal booking (fees/FX rounding).
        root, conf, regulated, rule = ROOT_AMOUNT_DRIFT, 0.90, False, amount_equals_rule
    elif "mismatch" in desc and "reference" in desc:
        root, conf, regulated, rule = ROOT_REFERENCE_MISMATCH, 0.95, False, amount_equals_rule
    elif "value date" in desc or "settlement" in desc or "t+2" in desc:
        root, conf, regulated, rule = ROOT_TIMING, 0.96, False, amount_equals_rule
    else:
        root, conf, regulated, rule = ROOT_UNKNOWN, 0.30, False, None

    result = {
        "root_cause": root,
        "confidence": conf,
        "regulated": regulated,
        "rationale": "stub classification keyed off the seed statement description",
    }
    if rule is not None:
        result["proposed_rule"] = rule
    return result


def _completion_response(model_name, content):
    """Wrap a content string in an OpenAI-compatible ChatCompletionResponse."""
    return {
        "id": "chatcmpl-stub",
        "object": "chat.completion",
        "created": 0,
        "model": model_name or "kimi-k3",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


class Handler(BaseHTTPRequestHandler):
    # Silence the default noisy per-request logging; classifications are logged
    # explicitly to stderr in do_POST.
    def log_message(self, *args):  # noqa: N802 (stdlib signature)
        return

    def _send_json(self, status, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (stdlib signature)
        # Readiness endpoints so callers can poll before pointing the agent here.
        if self.path in ("/", "/healthz", "/health"):
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802 (stdlib signature)
        # Accept any path ending in /chat/completions (base URL is .../v1).
        if not self.path.rstrip("/").endswith("/chat/completions"):
            self._send_json(404, {"error": {"message": "unsupported path %s" % self.path}})
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, OSError) as exc:
            self._send_json(400, {"error": {"message": "bad request: %s" % exc}})
            return

        model_name = payload.get("model", "kimi-k3")
        user_content = _user_content(payload)
        classification = classify(user_content)
        # The classifier expects the JSON object as the message content string.
        content = json.dumps(classification)
        sys.stderr.write(
            "llm-stub: classified root_cause=%s confidence=%s regulated=%s rule=%s\n"
            % (
                classification["root_cause"],
                classification["confidence"],
                classification["regulated"],
                "yes" if "proposed_rule" in classification else "no",
            )
        )
        sys.stderr.flush()
        self._send_json(200, _completion_response(model_name, content))


def main(argv=None):
    parser = argparse.ArgumentParser(description="OpenAI-compatible Chat Completions stub for the recon-agent demo")
    parser.add_argument("--host", default=os.environ.get("LLM_STUB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("LLM_STUB_PORT", "8099")))
    args = parser.parse_args(argv)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    # Print AFTER bind so a caller polling stdout knows the socket is accepting.
    print("llm-stub: listening on http://%s:%d" % (args.host, args.port), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
